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
import io
import os
import sys
from datetime import datetime
from shutil import copy2, rmtree

from setuptools import find_packages, setup, Command

build_mode = os.getenv("RAYDP_BUILD_MODE", "")
package_name = os.getenv("RAYDP_PACKAGE_NAME", "raydp")
BASE_VERSION = "2.0.0"
if build_mode == "nightly":
    VERSION = BASE_VERSION + datetime.today().strftime("b%Y%m%d.dev0")
# for legacy raydp_nightly package
elif package_name == 'raydp_nightly':
    VERSION = datetime.today().strftime("%Y.%m.%d.dev0")
else:
    VERSION = BASE_VERSION + ".dev0"

ROOT_DIR = os.path.dirname(__file__)

CORE_DIR = os.path.abspath("../core")
BIN_DIR = os.path.abspath("../bin")

JARS_PATH = glob.glob(os.path.join(CORE_DIR, f"**/target/raydp-*.jar"), recursive=True)
JARS_TARGET = os.path.join(ROOT_DIR, "raydp", "jars")

SCRIPT_PATH = os.path.join(BIN_DIR, f"raydp-submit")
SCRIPT_TARGET = os.path.join(ROOT_DIR, "raydp", "bin")

class CustomBuildPackageProtos(Command):
    """Command to generate project *_pb2.py modules from proto files.

    Copied from grpc_tools.command.BuildPackageProtos and change the proto root dir.
    """

    description = 'build grpc protobuf modules'
    user_options = [('strict-mode', 's',
                     'exit with non-zero value if the proto compiling fails.')]

    def initialize_options(self):
        self.strict_mode = False

    def finalize_options(self):
        pass

    def run(self):
        from grpc_tools.command import build_package_protos
        # due to limitations of the proto generator, we require that only *one*
        # directory is provided as an 'include' directory. We assume it's the '' key
        # to `self.distribution.package_dir` (and get a key error if it's not
        # there).
        build_package_protos(self.distribution.package_dir["mpi_network_proto"],
                             self.strict_mode)

if __name__ == "__main__":
    if len(JARS_PATH) == 0:
        print("Can't find core module jars, you need to build the jars with 'mvn clean package'"
              " under core directory first.", file=sys.stderr)
        sys.exit(-1)

    # build the temp dir
    if os.path.exists(JARS_TARGET):
        rmtree(JARS_TARGET)
    if os.path.exists(SCRIPT_TARGET):
        rmtree(SCRIPT_TARGET)
    os.mkdir(JARS_TARGET)
    os.mkdir(SCRIPT_TARGET)
    with open(os.path.join(JARS_TARGET, "__init__.py"), "w") as f:
        f.write("")
    with open(os.path.join(SCRIPT_TARGET, "__init__.py"), "w") as f:
        f.write("")

    for jar_path in JARS_PATH:
        print(f"Copying {jar_path} to {JARS_TARGET}")
        copy2(jar_path, JARS_TARGET)
    copy2(SCRIPT_PATH, SCRIPT_TARGET)

    _packages = find_packages()
    _packages.append("raydp.jars")
    _packages.append("raydp.bin")

    setup(
        name=package_name,
        version=VERSION,
        long_description=io.open(
            os.path.join(ROOT_DIR, os.path.pardir, "README.md"),
            "r",
            encoding="utf-8").read(),
        long_description_content_type="text/markdown",
        packages=_packages,
        include_package_data=True,
        package_dir={"mpi_network_proto": "raydp/mpi/network"},
        package_data={"raydp.jars": ["*.jar"], "raydp.bin": ["raydp-submit"]},
        cmdclass={
            'build_proto_modules': CustomBuildPackageProtos,
        },
    )
