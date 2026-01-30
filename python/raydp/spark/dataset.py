#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import logging
import uuid
from typing import Callable, List, Optional, Union
from dataclasses import dataclass

from packaging import version
import pandas as pd
import pyarrow as pa
import pyspark.sql as sql
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.pandas.types import from_arrow_type
from pyspark.storagelevel import StorageLevel
import ray
from ray._private.client_mode_hook import client_mode_wrap
try:
    # Ray cross-language calls require enabling load_code_from_local.
    # This is an internal Ray API; keep it isolated and optional.
    from ray._private.worker import global_worker as _ray_global_worker  # type: ignore
except Exception:  # pragma: no cover
    _ray_global_worker = None
from ray.data import Dataset, from_arrow_refs
from ray.types import ObjectRef

from raydp.spark.ray_cluster_master import RAYDP_SPARK_MASTER_SUFFIX
from raydp.utils import parse_memory_size


logger = logging.getLogger(__name__)

def _enable_load_code_from_local() -> None:
    """Enable Ray cross-language support via internal API (driver only)."""
    if _ray_global_worker is not None:
        _ray_global_worker.set_load_code_from_local(True)

@ray.remote(max_retries=-1)
def _fetch_arrow_table_from_executor(executor_actor_name: str,
                                     rdd_id: int,
                                     partition_id: int,
                                     schema_json: str,
                                     driver_agent_url: str) -> pa.Table:
    """Fetch cached Arrow IPC bytes from a JVM executor actor and decode into a pyarrow.Table.

    This is used by `from_spark_recoverable` to build Ray Dataset blocks with Ray task lineage,
    enabling Ray object reconstruction after node loss.

    NOTE: Calling JVM actors from Python requires Ray cross-language support to be enabled
    https://docs.ray.io/en/latest/ray-core/cross-language.html.
    """
    # Enable cross-language calls in the task worker before invoking JVM actor methods.
    _enable_load_code_from_local()
    executor_actor = ray.get_actor(executor_actor_name)
    # cross-language actor call requires a bug fix in ray 2.37.0 to work,
    # https://github.com/ray-project/ray/pull/46770
    # particular:
    # https://github.com/ray-project/ray/commit/a1be06346e532e656c128ffd2230f06a4d72679e
    ipc_bytes = ray.get(
        executor_actor.getRDDPartition.remote(
            rdd_id, partition_id, schema_json, driver_agent_url))
    reader = pa.ipc.open_stream(pa.BufferReader(ipc_bytes))
    return reader.read_all()


class RecordPiece:
    def __init__(self, row_ids, num_rows: int):
        self.row_ids = row_ids
        self.num_rows = num_rows

    def read(self, shuffle: bool) -> pd.DataFrame:
        raise NotImplementedError

    def with_row_ids(self, new_row_ids) -> "RecordPiece":
        raise NotImplementedError

    def __len__(self):
        """Return the number of rows"""
        return self.num_rows


class RayObjectPiece(RecordPiece):
    def __init__(self,
                 obj_id: ray.ObjectRef,
                 row_ids: Optional[List[int]],
                 num_rows: int):
        super().__init__(row_ids, num_rows)
        self.obj_id = obj_id

    def read(self, shuffle: bool) -> pd.DataFrame:
        data = ray.get(self.obj_id)
        reader = pa.ipc.open_stream(data)
        tb = reader.read_all()
        df: pd.DataFrame = tb.to_pandas()
        if self.row_ids:
            df = df.loc[self.row_ids]

        if shuffle:
            df = df.sample(frac=1.0)
        return df

    def with_row_ids(self, new_row_ids) -> "RayObjectPiece":
        """chang the num_rows to the length of new_row_ids. Keep the original size if
        the new_row_ids is None.
        """

        if new_row_ids:
            num_rows = len(new_row_ids)
        else:
            num_rows = self.num_rows

        return RayObjectPiece(self.obj_id, new_row_ids, num_rows)



@dataclass
class PartitionObjectsOwner:
    # Actor owner name
    actor_name: str
    # Function that set serialized parquet objects to actor owner state
    # and return result of .remote() calling
    set_reference_as_state: Callable[[ray.actor.ActorHandle, List[ObjectRef]], ObjectRef]


def get_raydp_master_owner(spark: Optional[SparkSession] = None) -> PartitionObjectsOwner:
    if spark is None:
        spark = SparkSession.getActiveSession()
    obj_holder_name = spark.sparkContext.appName + RAYDP_SPARK_MASTER_SUFFIX

    def raydp_master_set_reference_as_state(
            raydp_master_actor: ray.actor.ActorHandle,
            objects: List[ObjectRef]) -> ObjectRef:
        return raydp_master_actor.add_objects.remote(uuid.uuid4(), objects)

    return PartitionObjectsOwner(
        obj_holder_name,
        raydp_master_set_reference_as_state)


def spark_dataframe_to_ray_dataset(df: sql.DataFrame,
                                   parallelism: Optional[int] = None,
                                   owner: Union[PartitionObjectsOwner, None] = None):
    """Convert Spark DataFrame to Ray Dataset using the recoverable pipeline by default."""
    if owner is not None:
        logger.warning(
            "spark_dataframe_to_ray_dataset now uses recoverable conversion by default; "
            "the 'owner' argument is ignored."
        )
    return from_spark_recoverable(df, parallelism=parallelism)

def from_spark_recoverable(df: sql.DataFrame,
                           storage_level: StorageLevel = StorageLevel.MEMORY_AND_DISK,
                           parallelism: Optional[int] = None):
    """Recoverable Spark->Ray conversion that survives executor loss."""
    num_part = df.rdd.getNumPartitions()
    if parallelism is not None:
        if parallelism != num_part:
            df = df.repartition(parallelism)
    sc = df.sql_ctx.sparkSession.sparkContext
    storage_level = sc._getJavaStorageLevel(storage_level)
    object_store_writer = sc._jvm.org.apache.spark.sql.raydp.ObjectStoreWriter
    # Recoverable conversion for Ray node loss:
    # - cache Arrow bytes in Spark
    # - build Ray Dataset blocks via Ray tasks (lineage), each task refetches bytes via JVM actors
    info = object_store_writer.prepareRecoverableRDD(df._jdf, storage_level)
    rdd_id = info.rddId()
    num_partitions = info.numPartitions()
    schema_json = info.schemaJson()
    driver_agent_url = info.driverAgentUrl()
    locations = info.locations()

    spark_conf = sc.getConf()
    fetch_num_cpus = float(
        spark_conf.get("spark.ray.raydp_recoverable_fetch.task.resource.CPU", "0") or 0)
    fetch_memory_str = spark_conf.get(
        "spark.ray.raydp_recoverable_fetch.task.resource.memory", "0")
    fetch_memory = float(parse_memory_size(fetch_memory_str))


    refs: List[ObjectRef] = []
    for i in range(num_partitions):
        executor_id = locations[i]
        executor_actor_name = f"raydp-executor-{executor_id}"
        task_opts = {
            "num_cpus": fetch_num_cpus,
            "memory": fetch_memory,
        }
        fetch_task = _fetch_arrow_table_from_executor.options(**task_opts)
        refs.append(
            fetch_task.remote(
                executor_actor_name,
                rdd_id,
                i,
                schema_json,
                driver_agent_url,
            )
        )

    return from_arrow_refs(refs)

def _convert_by_udf(spark: sql.SparkSession,
                    blocks: List[ObjectRef],
                    locations: List[bytes],
                    schema: StructType) -> DataFrame:
    holder_name  = spark.sparkContext.appName + RAYDP_SPARK_MASTER_SUFFIX
    holder = ray.get_actor(holder_name)
    df_id = uuid.uuid4()
    ray.get(holder.add_objects.remote(df_id, blocks))
    jvm = spark.sparkContext._jvm
    object_store_reader = jvm.org.apache.spark.sql.raydp.ObjectStoreReader
    # create the rdd then dataframe to utilize locality
    jdf = object_store_reader.createRayObjectRefDF(spark._jsparkSession, locations)
    current_namespace = ray.get_runtime_context().namespace
    ray_address = ray.get(holder.get_ray_address.remote())
    blocks_df = DataFrame(jdf, spark._wrapped if hasattr(spark, "_wrapped") else spark)
    def _convert_blocks_to_dataframe(blocks):
        # connect to ray
        if not ray.is_initialized():
            ray.init(address=ray_address,
                     namespace=current_namespace,
                     logging_level=logging.WARN)
        obj_holder = ray.get_actor(holder_name)
        for block in blocks:
            dfs = []
            for idx in block["idx"]:
                ref = ray.get(obj_holder.get_object.remote(df_id, idx))
                data = ray.get(ref)
                dfs.append(data.to_pandas())
            yield pd.concat(dfs)
    df = blocks_df.mapInPandas(_convert_blocks_to_dataframe, schema)
    return df

def _convert_by_rdd(spark: sql.SparkSession,
                    blocks: Dataset,
                    locations: List[bytes],
                    schema: StructType) -> DataFrame:
    object_ids = [block.binary() for block in blocks]
    schema_str = schema.json()
    jvm = spark.sparkContext._jvm
    # create rdd in java
    rdd = jvm.org.apache.spark.rdd.RayDatasetRDD(spark._jsc, object_ids, locations)
    # convert the rdd to dataframe
    object_store_reader = jvm.org.apache.spark.sql.raydp.ObjectStoreReader
    jdf = object_store_reader.RayDatasetToDataFrame(spark._jsparkSession, rdd, schema_str)
    return DataFrame(jdf, spark._wrapped if hasattr(spark, "_wrapped") else spark)

@client_mode_wrap
def get_locations(blocks):
    core_worker = ray.worker.global_worker.core_worker
    return [
        core_worker.get_owner_address(block)
        for block in blocks
    ]

def ray_dataset_to_spark_dataframe(spark: sql.SparkSession,
                                   arrow_schema,
                                   blocks: List[ObjectRef],
                                   locations = None) -> DataFrame:
    locations = get_locations(blocks)
    if hasattr(arrow_schema, "base_schema"):
        arrow_schema = arrow_schema.base_schema
    if not isinstance(arrow_schema, pa.lib.Schema):  # pylint: disable=c-extension-no-member
        raise RuntimeError(f"Schema is {type(arrow_schema)}, required pyarrow.lib.Schema. \n" \
                            f"to_spark does not support converting non-arrow ray datasets.")
    schema = StructType()
    for field in arrow_schema:
        schema.add(field.name, from_arrow_type(field.type), nullable=field.nullable)
    #TODO how to branch on type of block?
    sample = ray.get(blocks[0])
    if isinstance(sample, bytes):
        return _convert_by_rdd(spark, blocks, locations, schema)
    elif isinstance(sample, pa.Table):
        return _convert_by_udf(spark, blocks, locations, schema)
    else:
        raise RuntimeError("ray.to_spark only supports arrow type blocks")


def read_spark_parquet(path: str) -> Dataset:
    if version.parse(ray.__version__) < version.parse("2.50.0"):
        return ray.data.read_parquet(path)
    else:
        return ray.data.read_parquet(path, file_extensions=["parquet"])
