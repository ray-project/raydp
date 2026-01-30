
import sys
import time

import pytest
import ray
from ray._private.client_mode_hook import client_mode_wrap
import ray.util.client as ray_client
import raydp
from pyspark.sql import SparkSession


def gen_test_data(spark_session: SparkSession):
  data = []
  tmp = [("ming", 20, 15552211521),
          ("hong", 19, 13287994007),
          ("dave", 21, 15552211523),
          ("john", 40, 15322211523),
          ("wong", 50, 15122211523)]

  for _ in range(10):
    data += tmp

  rdd = spark_session.sparkContext.parallelize(data)
  out = spark_session.createDataFrame(rdd, ["Name", "Age", "Phone"])
  return out

@client_mode_wrap
def ray_gc():
  ray._private.internal_api.global_gc()


@pytest.mark.parametrize("ray_cluster", ["local"], indirect=True)
def test_api_compatibility(ray_cluster, jdk17_extra_spark_configs):
  """
  Test the changes been made are not to break public APIs.
  """

  if ray_client.ray.is_connected():
    pytest.skip("Skip this test if using ray client")

  num_executor = 1

  spark = raydp.init_spark(
    app_name = "test_api_compatibility",
    num_executors = num_executor,
    executor_cores = 1,
    executor_memory = "500M",
    configs=jdk17_extra_spark_configs
    )

  df_train = gen_test_data(spark)

  resource_stats = ray.available_resources()
  cpu_cnt = resource_stats['CPU']

  # check compatibility of ray 1.9.0 API: no data onwership transfer
  ds = ray.data.from_spark(df_train)
  if not ray_client.ray.is_connected():
    ds.show(1)
  ray_gc() # ensure GC kicked in
  time.sleep(3)

  # confirm that resources is still being occupied
  resource_stats = ray.available_resources()
  assert resource_stats['CPU'] == cpu_cnt

  # final clean up
  raydp.stop_spark()

if __name__ == '__main__':
  sys.exit(pytest.main(["-v", __file__]))

  # test_api_compatibility()
  # test_data_ownership_transfer()
  # test_fail_without_data_ownership_transfer()

