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
#

import platform

import pytest
from pyspark.storagelevel import StorageLevel
import ray
import ray.util.client as ray_client

from raydp.spark import dataset as spark_dataset


if platform.system() == "Darwin":
    # Spark-on-Ray recoverable path is unstable on macOS and can crash the raylet.
    pytest.skip("Skip recoverable forwarding test on macOS", allow_module_level=True)


@pytest.mark.parametrize("spark_on_ray_2_executors", ["local"], indirect=True)
def test_recoverable_forwarding_via_fetch_task(spark_on_ray_2_executors):
    """Verify JVM-side forwarding in recoverable Spark->Ray conversion.

    We deliberately trigger the recoverable fetch task to contact an executor actor that is not
    the current owner of the cached Spark block for the chosen partition. The request should still
    succeed because the executor refreshes the block owner and forwards the fetch one hop.
    """
    if ray_client.ray.is_connected():
        pytest.skip("Skip forwarding test in Ray client mode")

    spark = spark_on_ray_2_executors

    # Create enough partitions so that at least two different executors own cached blocks.
    df = spark.range(0, 10000, numPartitions=8)

    sc = spark.sparkContext
    storage_level = sc._getJavaStorageLevel(StorageLevel.MEMORY_AND_DISK)
    object_store_writer = sc._jvm.org.apache.spark.sql.raydp.ObjectStoreWriter

    info = object_store_writer.prepareRecoverableRDD(df._jdf, storage_level)
    rdd_id = info.rddId()
    schema_json = info.schemaJson()
    driver_agent_url = info.driverAgentUrl()
    locations = list(info.locations())

    assert locations
    unique_execs = sorted(set(locations))
    assert len(unique_execs) >= 2, f"Need >=2 executors, got {unique_execs}"

    # Pick a partition and intentionally target the *wrong* executor actor.
    partition_id = 0
    owner_executor_id = locations[partition_id]
    wrong_executor_id = next(e for e in unique_execs if e != owner_executor_id)

    # Ensure Ray cross-language calls are enabled for the worker side.
    spark_dataset._enable_load_code_from_local()

    wrong_executor_actor_name = f"raydp-executor-{wrong_executor_id}"
    table = ray.get(
        spark_dataset._fetch_arrow_table_from_executor.remote(
            wrong_executor_actor_name, rdd_id, partition_id, schema_json, driver_agent_url
        )
    )
    assert table.num_rows > 0

