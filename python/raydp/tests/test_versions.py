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

import pytest

from raydp import versions


@pytest.mark.parametrize(
    "spark_version",
    ["3.3.0", "3.5.7", "3.5.0.dev0", "3.10.0", "4.0.0"])
def test_spark_versions_3_3_and_newer_use_log4j2(spark_version):
    assert versions._uses_log4j2(spark_version)


@pytest.mark.parametrize(
    "spark_version",
    ["2.4.8", "3.1.3", "3.2.0", "3.2.2", "3.3.0rc1"])
def test_spark_versions_older_than_3_3_use_log4j1(spark_version):
    assert not versions._uses_log4j2(spark_version)


def test_invalid_spark_version_raises_value_error():
    with pytest.raises(ValueError, match="Invalid Spark version"):
        versions._uses_log4j2("not-a-version")
