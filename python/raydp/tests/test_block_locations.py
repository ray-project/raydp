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

from types import SimpleNamespace
from unittest.mock import Mock

from raydp.spark import dataset


def test_recoverable_fetch_uses_actor_ids_in_partition_order(monkeypatch):
    def location(executor_id, actor_id):
        return SimpleNamespace(getExecutorId=lambda: executor_id,
                               getActorId=lambda: actor_id)

    frame = Mock()
    frame.rdd.getNumPartitions.return_value = 4
    sc = frame.sql_ctx.sparkSession.sparkContext
    writer = sc._jvm.org.apache.spark.sql.raydp.ObjectStoreWriter
    writer.prepareRecoverableRDD.return_value = SimpleNamespace(
        rddId=lambda: 42,
        numPartitions=lambda: 4,
        schemaJson=lambda: "schema",
        driverAgentUrl=lambda: "driver",
        locations=lambda: ["1", "7", "4", "1"],
        blockLocations=lambda: [location("1", "0"), location("7", "7"),
                                location("4", "3"), location("1", "0")],
    )
    sc.getConf.return_value.get.side_effect = lambda key, default: default
    references = [object() for _ in range(4)]
    requests = []

    def fetch(actor_name, rdd_id, partition_id, schema_json, driver_agent_url):
        requests.append((actor_name, rdd_id, partition_id, schema_json, driver_agent_url))
        return references[partition_id]

    remote_task = Mock()
    remote_task.options.return_value.remote.side_effect = fetch
    monkeypatch.setattr(dataset, "_fetch_arrow_table_from_executor", remote_task)
    monkeypatch.setattr(dataset, "from_arrow_refs", lambda refs: refs)

    try:
        result = dataset.from_spark_recoverable(frame)
    finally:
        dataset._recoverable_rdd_ids.pop(frame, None)

    assert requests == [
        ("raydp-executor-0", 42, 0, "schema", "driver"),
        ("raydp-executor-7", 42, 1, "schema", "driver"),
        ("raydp-executor-3", 42, 2, "schema", "driver"),
        ("raydp-executor-0", 42, 3, "schema", "driver"),
    ]
    assert result == references
