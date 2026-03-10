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

import glob
import os
import re
import subprocess
import sys
import platform
import pyspark
from typing import Any, Dict, Optional

import ray
from pyspark.sql.session import SparkSession

from raydp.services import Cluster
from .ray_cluster_master import (RAYDP_SPARK_MASTER_SUFFIX, SPARK_RAY_LOG4J_FACTORY_CLASS_KEY,
                                 SPARK_LOG4J_CONFIG_FILE_NAME, RAY_LOG4J_CONFIG_FILE_NAME,
                                 RayDPSparkMaster, SPARK_JAVAAGENT, SPARK_PREFER_CLASSPATH,
                                 RAYDP_APPMASTER_EXTRA_JAVA_OPTIONS)
from raydp import versions

_JDK17_ADD_OPENS = " ".join([
    "-XX:+IgnoreUnrecognizedVMOptions",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.net=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.math=ALL-UNNAMED",
    "--add-opens=java.base/java.text=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
])

_cached_java_version: Optional[int] = None


def _get_java_major_version() -> Optional[int]:
    """Return the major version of the default ``java`` command, or None if
    it cannot be determined."""
    global _cached_java_version
    if _cached_java_version is not None:
        return _cached_java_version
    try:
        out = subprocess.check_output(
            ["java", "-version"], stderr=subprocess.STDOUT, timeout=10
        ).decode("utf-8", errors="replace")
        match = re.search(r'"(\d+)[\._]', out)
        if match:
            major = int(match.group(1))
            # Java 1.x style (e.g. "1.8.0_xxx") means major = 8
            if major == 1:
                m2 = re.search(r'"1\.(\d+)', out)
                if m2:
                    major = int(m2.group(1))
            _cached_java_version = major
            return major
    except Exception:
        pass
    return None


def _needs_add_opens() -> bool:
    """Return True if the JVM is version 17+ and needs ``--add-opens`` flags."""
    ver = _get_java_major_version()
    return ver is not None and ver >= 17


class SparkCluster(Cluster):
    def __init__(self,
                 app_name,
                 num_executors,
                 executor_cores,
                 executor_memory,
                 enable_hive,
                 configs):
        super().__init__(None)
        self._app_name = app_name
        self._spark_master = None
        self._num_executors = num_executors
        self._executor_cores = executor_cores
        self._executor_memory = executor_memory
        self._enable_hive = enable_hive
        self._configs = configs
        self._prepare_spark_configs()
        self._set_up_master(resources=self._get_master_resources(self._configs), kwargs=None)
        self._spark_session: SparkSession = None

    def _set_up_master(self, resources: Dict[str, float], kwargs: Dict[Any, Any]):
        # TODO: specify the app master resource
        spark_master_name = self._app_name + RAYDP_SPARK_MASTER_SUFFIX

        if resources:
            num_cpu = 1
            if "CPU" in resources:
                num_cpu = resources["CPU"]
                resources.pop("CPU", None)

            memory = None
            if "memory" in resources:
                memory = resources["memory"]
                resources.pop("memory", None)

            self._spark_master_handle = RayDPSparkMaster.options(name=spark_master_name,
                                                                 num_cpus=num_cpu,
                                                                 memory=memory,
                                                                 resources=resources) \
                                                        .remote(self._configs)
        else:
            self._spark_master_handle = RayDPSparkMaster.options(name=spark_master_name) \
                .remote(self._configs)

        ray.get(self._spark_master_handle.start_up.remote())

    def _get_master_resources(self, configs: Dict[str, str]) -> Dict[str, float]:
        resources = {}
        deprecated_spark_master_config_prefix = "spark.ray.raydp_spark_master.resource."
        spark_master_actor_resource_prefix = "spark.ray.raydp_spark_master.actor.resource."

        def get_master_actor_resource(key_prefix: str,
                                      resource: Dict[str, float]) -> Dict[str, float]:
            for key in configs:
                if key.startswith(key_prefix):
                    resource_name = key[len(key_prefix):]
                    resource[resource_name] = float(configs[key])
            return resource

        resources = get_master_actor_resource(deprecated_spark_master_config_prefix,
                                              resources)
        resources = get_master_actor_resource(spark_master_actor_resource_prefix,
                                              resources)

        return resources

    def _set_up_worker(self, resources: Dict[str, float], kwargs: Dict[str, str]):
        raise Exception("Unsupported operation")

    def get_cluster_url(self) -> str:
        return ray.get(self._spark_master_handle.get_master_url.remote())

    def connect_spark_driver_to_ray(self):
        # provide ray cluster config through jvm properties
        # this is needed to connect to ray cluster
        jvm_properties_ref = self._spark_master_handle._generate_ray_configs.remote()
        jvm_properties = ray.get(jvm_properties_ref)
        jvm = self._spark_session._jvm
        jvm.org.apache.spark.deploy.raydp.RayAppMaster.setProperties(jvm_properties)
        jvm.org.apache.spark.sql.raydp.ObjectStoreWriter.connectToRay()

    def _prepare_spark_configs(self):
        if self._configs is None:
            self._configs = {}
        self._configs["spark.executor.instances"] = str(self._num_executors)
        self._configs["spark.executor.cores"] = str(self._executor_cores)
        self._configs["spark.executor.memory"] = str(self._executor_memory)
        if platform.system() != "Darwin":
            driver_node_ip = ray.util.get_node_ip_address()
            if "spark.driver.host" not in self._configs:
                self._configs["spark.driver.host"] = str(driver_node_ip)
                self._configs["spark.driver.bindAddress"] = str(driver_node_ip)

        raydp_cp = os.path.abspath(os.path.join(os.path.abspath(__file__), "../../jars/*"))
        ray_cp = os.path.abspath(os.path.join(os.path.dirname(ray.__file__), "jars/*"))
        spark_home = os.environ.get("SPARK_HOME", os.path.dirname(pyspark.__file__))
        spark_jars_dir = os.path.abspath(os.path.join(spark_home, "jars/*"))

        raydp_agent_path = os.path.abspath(os.path.join(os.path.abspath(__file__),
                                                        "../../jars/raydp-agent*.jar"))
        raydp_agent_jar = glob.glob(raydp_agent_path)[0]
        self._configs[SPARK_JAVAAGENT] = raydp_agent_jar
        # for JVM running in ray
        self._configs[SPARK_RAY_LOG4J_FACTORY_CLASS_KEY] = versions.RAY_LOG4J_VERSION

        if SPARK_LOG4J_CONFIG_FILE_NAME not in self._configs:
            # If the config is not passed in by the user, check
            # by environment variable SPARK_LOG4J_CONFIG_FILE_NAME. This
            # will give the system admin/infra team a chance to set the
            # value for all users.
            self._configs[SPARK_LOG4J_CONFIG_FILE_NAME] =\
                os.environ.get("SPARK_LOG4J_CONFIG_FILE_NAME",
                               versions.SPARK_LOG4J_CONFIG_FILE_NAME_DEFAULT)

        if RAY_LOG4J_CONFIG_FILE_NAME not in self._configs:
            # If the config is not passed in by the user, check
            # by environment variable RAY_LOG4J_CONFIG_FILE_NAME. This
            # will give the system admin/infra team a chance to set the
            # value for all users.
            self._configs[RAY_LOG4J_CONFIG_FILE_NAME] =\
                os.environ.get("RAY_LOG4J_CONFIG_FILE_NAME",
                               versions.RAY_LOG4J_CONFIG_FILE_NAME_DEFAULT)

        prefer_cp = []
        if SPARK_PREFER_CLASSPATH in self._configs:
            prefer_cp.extend(self._configs[SPARK_PREFER_CLASSPATH].split(os.pathsep))

        raydp_jars = glob.glob(raydp_cp)
        driver_cp_key = "spark.driver.extraClassPath"
        driver_cp = ":".join(prefer_cp + raydp_jars + [spark_jars_dir] + glob.glob(ray_cp))
        if driver_cp_key in self._configs:
            self._configs[driver_cp_key] = self._configs[driver_cp_key] + ":" + driver_cp
        else:
            self._configs[driver_cp_key] = driver_cp
        dyn_alloc_key = "spark.dynamicAllocation.enabled"
        if dyn_alloc_key in self._configs and self._configs[dyn_alloc_key] == "true":
            max_executor_key = "spark.dynamicAllocation.maxExecutors"
            # set max executors if not set. otherwise spark might request too many actors
            if max_executor_key not in self._configs:
                print("Warning: spark.dynamicAllocation.maxExecutors is not set.\n" \
                      "Consider to set it to match the cluster configuration. " \
                      "If used with autoscaling, calculate it from max_workers.",
                      file=sys.stderr)

        # On JDK 17+, inject --add-opens flags for driver and app master when
        # the user has not explicitly provided them.
        add_opens = _JDK17_ADD_OPENS if _needs_add_opens() else ""

        if add_opens and "spark.driver.extraJavaOptions" not in self._configs:
            self._configs["spark.driver.extraJavaOptions"] = add_opens

        if add_opens and RAYDP_APPMASTER_EXTRA_JAVA_OPTIONS not in self._configs:
            self._configs[RAYDP_APPMASTER_EXTRA_JAVA_OPTIONS] = add_opens

        # set spark.driver.extraJavaOptions for driver (spark-submit)
        java_opts = ["-javaagent:" + self._configs[SPARK_JAVAAGENT],
                     "-D" + SPARK_RAY_LOG4J_FACTORY_CLASS_KEY + "=" + versions.SPARK_LOG4J_VERSION,
                     "-D" + versions.SPARK_LOG4J_CONFIG_FILE_NAME_KEY + "=" +
                     self._configs[SPARK_LOG4J_CONFIG_FILE_NAME]
                     ]
        # Append to existing driver options (user-provided or auto-injected above)
        existing_driver_opts = self._configs.get("spark.driver.extraJavaOptions", "")
        if existing_driver_opts:
            all_opts = existing_driver_opts + " " + " ".join(java_opts)
            self._configs["spark.driver.extraJavaOptions"] = all_opts
        else:
            self._configs["spark.driver.extraJavaOptions"] = " ".join(java_opts)

    def get_spark_session(self) -> SparkSession:
        if self._spark_session is not None:
            return self._spark_session
        spark_builder = SparkSession.builder
        for k, v in self._configs.items():
            spark_builder.config(k, v)
        if self._enable_hive:
            spark_builder.enableHiveSupport()
        self._spark_session = \
            spark_builder.appName(self._app_name).master(self.get_cluster_url()).getOrCreate()
        return self._spark_session

    def stop(self, cleanup_data):
        if self._spark_session is not None:
            self._spark_session.stop()
            self._spark_session = None
        if self._spark_master_handle is not None:
            self._spark_master_handle.stop.remote(cleanup_data)
            if cleanup_data:
                self._spark_master_handle = None
