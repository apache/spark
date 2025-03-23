#!/usr/bin/env python3

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

# cd python
# python packaging/connect/setup.py sdist

# cd python/packaging/connect
# python setup.py sdist

import sys
from setuptools import setup
import os
from shutil import copyfile, copytree, move, rmtree
import glob
from pathlib import Path

if (
    # When we package, the parent directory 'connect' dir
    # (as we pip install -e python/packaging/connect)
    os.getcwd() == str(Path(__file__).parent.absolute())
    and str(Path(__file__).parent.name) == "connect"
):
    # For:
    # - pip install -e python/packaging/connect
    #     It moves the current working directory to 'connect'
    # - cd python/packaging/connect; python setup.py sdist
    #
    # For:
    # - python packaging/connect/setup.py sdist, it does not
    #     execute this branch.
    #
    # Move to spark/python
    os.chdir(Path(__file__).parent.parent.parent.absolute())

# Check and see if we are under the spark path in which case we need to build the symlink farm.
# This is important because we only want to build the symlink farm while under Spark otherwise we
# want to use the symlink farm. And if the symlink farm exists under while under Spark (e.g. a
# partially built sdist) we should error and have the user sort it out.
in_spark = os.path.isfile("../core/src/main/scala/org/apache/spark/SparkContext.scala") or (
    os.path.isfile("../RELEASE") and len(glob.glob("../jars/spark*core*.jar")) == 1
)

try:
    if in_spark:
        # !!HACK ALTERT!!
        # 1. `setup.py` has to be located with the same directory with the package.
        #    Therefore, we copy the current file, and place it at `spark/python` directory.
        #    After that, we remove it in the end.
        # 2. Here it renames `pyspark` and `lib` to `pyspark.back` and `lib.back` so MANIFEST.in
        #    does not pick `pyspark` and `py4j` up. We rename it back in the end.
        move("pyspark", "pyspark.back")
        move("lib", "lib.back")
        copyfile("packaging/connect/setup.py", "setup.py")
        copyfile("packaging/connect/setup.cfg", "setup.cfg")
        copytree("packaging/connect/pyspark_connect", "pyspark_connect")
        copyfile("pyspark.back/version.py", "pyspark_connect/version.py")

    try:
        exec(open("pyspark_connect/version.py").read())
    except IOError:
        print(
            "Failed to load PySpark Connect version file for packaging. "
            "You must be in Spark's python dir.",
            file=sys.stderr,
        )
        sys.exit(-1)
    VERSION = __version__  # noqa

    # If you are changing the versions here, please also change ./python/pyspark/sql/pandas/utils.py
    # For Arrow, you should also check ./pom.xml and ensure there are no breaking changes in the
    # binary format protocol with the Java version, see ARROW_HOME/format/* for specifications.
    # Also don't forget to update python/docs/source/getting_started/install.rst,
    # python/packaging/classic/setup.py, and python/packaging/client/setup.py
    _minimum_pandas_version = "2.0.0"
    _minimum_numpy_version = "1.21"
    _minimum_pyarrow_version = "11.0.0"
    _minimum_grpc_version = "1.67.0"
    _minimum_googleapis_common_protos_version = "1.65.0"

    with open("README.md") as f:
        long_description = f.read()

    connect_packages = [
        "pyspark_connect",
    ]

    setup(
        name="pyspark-connect",
        version=VERSION,
        description="Apache Spark Python API with Spark Connect by default",
        long_description=long_description,
        long_description_content_type="text/markdown",
        author="Spark Developers",
        author_email="dev@spark.apache.org",
        url="https://github.com/apache/spark/tree/master/python",
        packages=connect_packages,
        include_package_data=True,
        license="http://www.apache.org/licenses/LICENSE-2.0",
        # Don't forget to update python/docs/source/getting_started/install.rst
        # if you're updating the versions or dependencies.
        install_requires=[
            "pyspark==%s" % VERSION,
            "pandas>=%s" % _minimum_pandas_version,
            "pyarrow>=%s" % _minimum_pyarrow_version,
            "grpcio>=%s" % _minimum_grpc_version,
            "grpcio-status>=%s" % _minimum_grpc_version,
            "googleapis-common-protos>=%s" % _minimum_googleapis_common_protos_version,
            "numpy>=%s" % _minimum_numpy_version,
        ],
        python_requires=">=3.9",
        classifiers=[
            "Development Status :: 5 - Production/Stable",
            "License :: OSI Approved :: Apache Software License",
            "Programming Language :: Python :: 3.9",
            "Programming Language :: Python :: 3.10",
            "Programming Language :: Python :: 3.11",
            "Programming Language :: Python :: 3.12",
            "Programming Language :: Python :: 3.13",
            "Programming Language :: Python :: Implementation :: CPython",
            "Programming Language :: Python :: Implementation :: PyPy",
            "Typing :: Typed",
        ],
    )
finally:
    if in_spark:
        move("pyspark.back", "pyspark")
        move("lib.back", "lib")
        os.remove("setup.py")
        os.remove("setup.cfg")
        rmtree("pyspark_connect")
