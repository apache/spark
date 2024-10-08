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

import importlib.util
import glob
import os
import sys
import ctypes
from setuptools import setup
from setuptools.command.install import install
from shutil import copyfile, copytree, rmtree
from pathlib import Path

if (
    # When we package, the parent diectory 'classic' dir
    # (as we pip install -e python/packaging/classic)
    os.getcwd() == str(Path(__file__).parent.absolute())
    and str(Path(__file__).parent.name) == "classic"
):
    # For:
    # - pip install -e python/packaging/classic
    #     It moves the current working directory to 'classic'
    # - cd python/packaging/classic; python setup.py sdist
    #
    # For:
    # - python packaging/classic/setup.py sdist, it does not
    #     execute this branch.
    #
    # Move to spark/python
    os.chdir(Path(__file__).parent.parent.parent.absolute())

try:
    exec(open("pyspark/version.py").read())
except IOError:
    print(
        "Failed to load PySpark version file for packaging. You must be in Spark's python dir.",
        file=sys.stderr,
    )
    sys.exit(-1)
try:
    spec = importlib.util.spec_from_file_location("install", "pyspark/install.py")
    install_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(install_module)
except IOError:
    print(
        "Failed to load the installing module (pyspark/install.py) which had to be "
        "packaged together.",
        file=sys.stderr,
    )
    sys.exit(-1)
VERSION = __version__  # noqa
# A temporary path so we can access above the Python project root and fetch scripts and jars we need
TEMP_PATH = "deps"
SPARK_HOME = os.path.abspath("../")

# Provide guidance about how to use setup.py
incorrect_invocation_message = """
If you are installing pyspark from spark source, you must first build Spark and
run sdist.

    To build Spark with maven you can run:
      ./build/mvn -DskipTests clean package
    Building the source dist is done in the Python directory:
      cd python
      python packaging/classic/setup.py sdist
      pip install dist/*.tar.gz"""

# Figure out where the jars are we need to package with PySpark.
JARS_PATH = glob.glob(os.path.join(SPARK_HOME, "assembly/target/scala-*/jars/"))

if len(JARS_PATH) == 1:
    JARS_PATH = JARS_PATH[0]
elif os.path.isfile("../RELEASE") and len(glob.glob("../jars/spark*core*.jar")) == 1:
    # Release mode puts the jars in a jars directory
    JARS_PATH = os.path.join(SPARK_HOME, "jars")
elif len(JARS_PATH) > 1:
    print(
        "Assembly jars exist for multiple scalas ({0}), please cleanup assembly/target".format(
            JARS_PATH
        ),
        file=sys.stderr,
    )
    sys.exit(-1)
elif len(JARS_PATH) == 0 and not os.path.exists(TEMP_PATH):
    print(incorrect_invocation_message, file=sys.stderr)
    sys.exit(-1)

EXAMPLES_PATH = os.path.join(SPARK_HOME, "examples/src/main/python")
SCRIPTS_PATH = os.path.join(SPARK_HOME, "bin")
USER_SCRIPTS_PATH = os.path.join(SPARK_HOME, "sbin")
DATA_PATH = os.path.join(SPARK_HOME, "data")
LICENSES_PATH = os.path.join(SPARK_HOME, "licenses")

SCRIPTS_TARGET = os.path.join(TEMP_PATH, "bin")
USER_SCRIPTS_TARGET = os.path.join(TEMP_PATH, "sbin")
JARS_TARGET = os.path.join(TEMP_PATH, "jars")
EXAMPLES_TARGET = os.path.join(TEMP_PATH, "examples")
DATA_TARGET = os.path.join(TEMP_PATH, "data")
LICENSES_TARGET = os.path.join(TEMP_PATH, "licenses")

# Check and see if we are under the spark path in which case we need to build the symlink farm.
# This is important because we only want to build the symlink farm while under Spark otherwise we
# want to use the symlink farm. And if the symlink farm exists under while under Spark (e.g. a
# partially built sdist) we should error and have the user sort it out.
in_spark = os.path.isfile("../core/src/main/scala/org/apache/spark/SparkContext.scala") or (
    os.path.isfile("../RELEASE") and len(glob.glob("../jars/spark*core*.jar")) == 1
)


def _supports_symlinks():
    """Check if the system supports symlinks (e.g. *nix) or not."""
    return hasattr(os, "symlink") and (
        (not hasattr(ctypes, "windll"))  # Non-Windows
        or (
            # In some Windows, `os.symlink` works but only for admins.
            hasattr(ctypes.windll, "shell32")
            and hasattr(ctypes.windll.shell32, "IsUserAnAdmin")
            and bool(ctypes.windll.shell32.IsUserAnAdmin())
        )
    )


if in_spark:
    # Construct links for setup
    try:
        os.mkdir(TEMP_PATH)
    except BaseException:
        print(
            "Temp path for symlink to parent already exists {0}".format(TEMP_PATH), file=sys.stderr
        )
        sys.exit(-1)

# If you are changing the versions here, please also change ./python/pyspark/sql/pandas/utils.py
# For Arrow, you should also check ./pom.xml and ensure there are no breaking changes in the
# binary format protocol with the Java version, see ARROW_HOME/format/* for specifications.
# Also don't forget to update python/docs/source/getting_started/install.rst, and
# python/packaging/connect/setup.py
_minimum_pandas_version = "2.0.0"
_minimum_numpy_version = "1.21"
_minimum_pyarrow_version = "10.0.0"
_minimum_grpc_version = "1.62.0"
_minimum_googleapis_common_protos_version = "1.56.4"


class InstallCommand(install):
    # TODO(SPARK-32837) leverage pip's custom options

    def run(self):
        install.run(self)

        # Make sure the destination is always clean.
        spark_dist = os.path.join(self.install_lib, "pyspark", "spark-distribution")
        rmtree(spark_dist, ignore_errors=True)

        if ("PYSPARK_HADOOP_VERSION" in os.environ) or ("PYSPARK_HIVE_VERSION" in os.environ):
            # Note that PYSPARK_VERSION environment is just a testing purpose.
            # PYSPARK_HIVE_VERSION environment variable is also internal for now in case
            # we support another version of Hive in the future.
            spark_version, hadoop_version, hive_version = install_module.checked_versions(
                os.environ.get("PYSPARK_VERSION", VERSION).lower(),
                os.environ.get("PYSPARK_HADOOP_VERSION", install_module.DEFAULT_HADOOP).lower(),
                os.environ.get("PYSPARK_HIVE_VERSION", install_module.DEFAULT_HIVE).lower(),
            )

            if "PYSPARK_VERSION" not in os.environ and (
                (install_module.DEFAULT_HADOOP, install_module.DEFAULT_HIVE)
                == (hadoop_version, hive_version)
            ):
                # Do not download and install if they are same as default.
                return

            install_module.install_spark(
                dest=spark_dist,
                spark_version=spark_version,
                hadoop_version=hadoop_version,
                hive_version=hive_version,
            )


try:
    # We copy the shell script to be under pyspark/python/pyspark so that the launcher scripts
    # find it where expected. The rest of the files aren't copied because they are accessed
    # using Python imports instead which will be resolved correctly.
    try:
        os.makedirs("pyspark/python/pyspark")
    except OSError:
        # Don't worry if the directory already exists.
        pass
    copyfile("pyspark/shell.py", "pyspark/python/pyspark/shell.py")

    if in_spark:
        # !!HACK ALTERT!!
        # `setup.py` has to be located with the same directory with the package.
        # Therefore, we copy the current file, and place it at `spark/python` directory.
        # After that, we remove it in the end.
        copyfile("packaging/classic/setup.py", "setup.py")
        copyfile("packaging/classic/setup.cfg", "setup.cfg")

        # Construct the symlink farm - this is nein_sparkcessary since we can't refer to
        # the path above the package root and we need to copy the jars and scripts which
        # are up above the python root.
        if _supports_symlinks():
            os.symlink(JARS_PATH, JARS_TARGET)
            os.symlink(SCRIPTS_PATH, SCRIPTS_TARGET)
            os.symlink(USER_SCRIPTS_PATH, USER_SCRIPTS_TARGET)
            os.symlink(EXAMPLES_PATH, EXAMPLES_TARGET)
            os.symlink(DATA_PATH, DATA_TARGET)
            os.symlink(LICENSES_PATH, LICENSES_TARGET)
        else:
            # For windows fall back to the slower copytree
            copytree(JARS_PATH, JARS_TARGET)
            copytree(SCRIPTS_PATH, SCRIPTS_TARGET)
            copytree(USER_SCRIPTS_PATH, USER_SCRIPTS_TARGET)
            copytree(EXAMPLES_PATH, EXAMPLES_TARGET)
            copytree(DATA_PATH, DATA_TARGET)
            copytree(LICENSES_PATH, LICENSES_TARGET)
    else:
        # If we are not inside of SPARK_HOME verify we have the required symlink farm
        if not os.path.exists(JARS_TARGET):
            print(
                "To build packaging must be in the python directory under the SPARK_HOME.",
                file=sys.stderr,
            )

    if not os.path.isdir(SCRIPTS_TARGET):
        print(incorrect_invocation_message, file=sys.stderr)
        sys.exit(-1)

    # Scripts directive requires a list of each script path and does not take wild cards.
    script_names = os.listdir(SCRIPTS_TARGET)
    scripts = list(map(lambda script: os.path.join(SCRIPTS_TARGET, script), script_names))
    # We add find_spark_home.py to the bin directory we install so that pip installed PySpark
    # will search for SPARK_HOME with Python.
    scripts.append("pyspark/find_spark_home.py")

    with open("README.md") as f:
        long_description = f.read()

    setup(
        name="pyspark",
        version=VERSION,
        description="Apache Spark Python API",
        long_description=long_description,
        long_description_content_type="text/markdown",
        author="Spark Developers",
        author_email="dev@spark.apache.org",
        url="https://github.com/apache/spark/tree/master/python",
        packages=[
            "pyspark",
            "pyspark.core",
            "pyspark.cloudpickle",
            "pyspark.mllib",
            "pyspark.mllib.linalg",
            "pyspark.mllib.stat",
            "pyspark.ml",
            "pyspark.ml.connect",
            "pyspark.ml.linalg",
            "pyspark.ml.param",
            "pyspark.ml.torch",
            "pyspark.ml.deepspeed",
            "pyspark.sql",
            "pyspark.sql.avro",
            "pyspark.sql.classic",
            "pyspark.sql.connect",
            "pyspark.sql.connect.avro",
            "pyspark.sql.connect.client",
            "pyspark.sql.connect.functions",
            "pyspark.sql.connect.proto",
            "pyspark.sql.connect.protobuf",
            "pyspark.sql.connect.resource",
            "pyspark.sql.connect.shell",
            "pyspark.sql.connect.streaming",
            "pyspark.sql.connect.streaming.worker",
            "pyspark.sql.functions",
            "pyspark.sql.pandas",
            "pyspark.sql.plot",
            "pyspark.sql.protobuf",
            "pyspark.sql.streaming",
            "pyspark.sql.worker",
            "pyspark.streaming",
            "pyspark.bin",
            "pyspark.sbin",
            "pyspark.jars",
            "pyspark.pandas",
            "pyspark.pandas.data_type_ops",
            "pyspark.pandas.indexes",
            "pyspark.pandas.missing",
            "pyspark.pandas.plot",
            "pyspark.pandas.spark",
            "pyspark.pandas.typedef",
            "pyspark.pandas.usage_logging",
            "pyspark.python.pyspark",
            "pyspark.python.lib",
            "pyspark.testing",
            "pyspark.data",
            "pyspark.licenses",
            "pyspark.resource",
            "pyspark.errors",
            "pyspark.errors.exceptions",
            "pyspark.examples.src.main.python",
            "pyspark.logger",
        ],
        include_package_data=True,
        package_dir={
            "pyspark.jars": "deps/jars",
            "pyspark.bin": "deps/bin",
            "pyspark.sbin": "deps/sbin",
            "pyspark.python.lib": "lib",
            "pyspark.data": "deps/data",
            "pyspark.licenses": "deps/licenses",
            "pyspark.examples.src.main.python": "deps/examples",
        },
        package_data={
            "pyspark.jars": ["*.jar"],
            "pyspark.bin": ["*"],
            "pyspark.sbin": [
                "spark-config.sh",
                "spark-daemon.sh",
                "start-history-server.sh",
                "stop-history-server.sh",
            ],
            "pyspark.python.lib": ["*.zip"],
            "pyspark.data": ["*.txt", "*.data"],
            "pyspark.licenses": ["*.txt"],
            "pyspark.examples.src.main.python": ["*.py", "*/*.py"],
        },
        scripts=scripts,
        license="http://www.apache.org/licenses/LICENSE-2.0",
        # Don't forget to update python/docs/source/getting_started/install.rst
        # if you're updating the versions or dependencies.
        install_requires=["py4j==0.10.9.7"],
        extras_require={
            "ml": ["numpy>=%s" % _minimum_numpy_version],
            "mllib": ["numpy>=%s" % _minimum_numpy_version],
            "sql": [
                "pandas>=%s" % _minimum_pandas_version,
                "pyarrow>=%s" % _minimum_pyarrow_version,
                "numpy>=%s" % _minimum_numpy_version,
            ],
            "pandas_on_spark": [
                "pandas>=%s" % _minimum_pandas_version,
                "pyarrow>=%s" % _minimum_pyarrow_version,
                "numpy>=%s" % _minimum_numpy_version,
            ],
            "connect": [
                "pandas>=%s" % _minimum_pandas_version,
                "pyarrow>=%s" % _minimum_pyarrow_version,
                "grpcio>=%s" % _minimum_grpc_version,
                "grpcio-status>=%s" % _minimum_grpc_version,
                "googleapis-common-protos>=%s" % _minimum_googleapis_common_protos_version,
                "numpy>=%s" % _minimum_numpy_version,
            ],
        },
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
        cmdclass={
            "install": InstallCommand,
        },
    )
finally:
    # We only cleanup the symlink farm if we were in Spark, otherwise we are installing rather than
    # packaging.
    if in_spark:
        os.remove("setup.py")
        os.remove("setup.cfg")
        # Depending on cleaning up the symlink farm or copied version
        if _supports_symlinks():
            os.remove(os.path.join(TEMP_PATH, "jars"))
            os.remove(os.path.join(TEMP_PATH, "bin"))
            os.remove(os.path.join(TEMP_PATH, "sbin"))
            os.remove(os.path.join(TEMP_PATH, "examples"))
            os.remove(os.path.join(TEMP_PATH, "data"))
            os.remove(os.path.join(TEMP_PATH, "licenses"))
        else:
            rmtree(os.path.join(TEMP_PATH, "jars"))
            rmtree(os.path.join(TEMP_PATH, "bin"))
            rmtree(os.path.join(TEMP_PATH, "sbin"))
            rmtree(os.path.join(TEMP_PATH, "examples"))
            rmtree(os.path.join(TEMP_PATH, "data"))
            rmtree(os.path.join(TEMP_PATH, "licenses"))
        os.rmdir(TEMP_PATH)
