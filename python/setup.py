#!/usr/bin/env python

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

from __future__ import print_function
import glob
import os
import sys
from setuptools import setup, find_packages
from shutil import copyfile, copytree, rmtree

if sys.version_info < (2, 7):
    print("Python versions prior to 2.7 are not supported for pip installed PySpark.",
          file=sys.stderr)
    exit(-1)

try:
    exec(open('pyspark/version.py').read())
except IOError:
    print("Failed to load PySpark version file for packaging. You must be in Spark's python dir.",
          file=sys.stderr)
    sys.exit(-1)
VERSION = __version__
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
      python setup.py sdist
      pip install dist/*.tar.gz"""

# Figure out where the jars are we need to package with PySpark.
JARS_PATH = glob.glob(os.path.join(SPARK_HOME, "assembly/target/scala-*/jars/"))

if len(JARS_PATH) == 1:
    JARS_PATH = JARS_PATH[0]
elif (os.path.isfile("../RELEASE") and len(glob.glob("../jars/spark*core*.jar")) == 1):
    # Release mode puts the jars in a jars directory
    JARS_PATH = os.path.join(SPARK_HOME, "jars")
elif len(JARS_PATH) > 1:
    print("Assembly jars exist for multiple scalas ({0}), please cleanup assembly/target".format(
        JARS_PATH), file=sys.stderr)
    sys.exit(-1)
elif len(JARS_PATH) == 0 and not os.path.exists(TEMP_PATH):
    print(incorrect_invocation_message, file=sys.stderr)
    sys.exit(-1)

EXAMPLES_PATH = os.path.join(SPARK_HOME, "examples/src/main/python")
SCRIPTS_PATH = os.path.join(SPARK_HOME, "bin")
DATA_PATH = os.path.join(SPARK_HOME, "data")
LICENSES_PATH = os.path.join(SPARK_HOME, "licenses")

SCRIPTS_TARGET = os.path.join(TEMP_PATH, "bin")
JARS_TARGET = os.path.join(TEMP_PATH, "jars")
EXAMPLES_TARGET = os.path.join(TEMP_PATH, "examples")
DATA_TARGET = os.path.join(TEMP_PATH, "data")
LICENSES_TARGET = os.path.join(TEMP_PATH, "licenses")

# Check and see if we are under the spark path in which case we need to build the symlink farm.
# This is important because we only want to build the symlink farm while under Spark otherwise we
# want to use the symlink farm. And if the symlink farm exists under while under Spark (e.g. a
# partially built sdist) we should error and have the user sort it out.
in_spark = (os.path.isfile("../core/src/main/scala/org/apache/spark/SparkContext.scala") or
            (os.path.isfile("../RELEASE") and len(glob.glob("../jars/spark*core*.jar")) == 1))


def _supports_symlinks():
    """Check if the system supports symlinks (e.g. *nix) or not."""
    return getattr(os, "symlink", None) is not None


if (in_spark):
    # Construct links for setup
    try:
        os.mkdir(TEMP_PATH)
    except:
        print("Temp path for symlink to parent already exists {0}".format(TEMP_PATH),
              file=sys.stderr)
        exit(-1)

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

    if (in_spark):
        # Construct the symlink farm - this is necessary since we can't refer to the path above the
        # package root and we need to copy the jars and scripts which are up above the python root.
        if _supports_symlinks():
            os.symlink(JARS_PATH, JARS_TARGET)
            os.symlink(SCRIPTS_PATH, SCRIPTS_TARGET)
            os.symlink(EXAMPLES_PATH, EXAMPLES_TARGET)
            os.symlink(DATA_PATH, DATA_TARGET)
            os.symlink(LICENSES_PATH, LICENSES_TARGET)
        else:
            # For windows fall back to the slower copytree
            copytree(JARS_PATH, JARS_TARGET)
            copytree(SCRIPTS_PATH, SCRIPTS_TARGET)
            copytree(EXAMPLES_PATH, EXAMPLES_TARGET)
            copytree(DATA_PATH, DATA_TARGET)
            copytree(LICENSES_PATH, LICENSES_TARGET)
    else:
        # If we are not inside of SPARK_HOME verify we have the required symlink farm
        if not os.path.exists(JARS_TARGET):
            print("To build packaging must be in the python directory under the SPARK_HOME.",
                  file=sys.stderr)

    if not os.path.isdir(SCRIPTS_TARGET):
        print(incorrect_invocation_message, file=sys.stderr)
        exit(-1)

    # Scripts directive requires a list of each script path and does not take wild cards.
    script_names = os.listdir(SCRIPTS_TARGET)
    scripts = list(map(lambda script: os.path.join(SCRIPTS_TARGET, script), script_names))
    # We add find_spark_home.py to the bin directory we install so that pip installed PySpark
    # will search for SPARK_HOME with Python.
    scripts.append("pyspark/find_spark_home.py")

    # Parse the README markdown file into rst for PyPI
    long_description = "!!!!! missing pandoc do not upload to PyPI !!!!"
    try:
        import pypandoc
        long_description = pypandoc.convert('README.md', 'rst')
    except ImportError:
        print("Could not import pypandoc - required to package PySpark", file=sys.stderr)

    setup(
        name='pyspark',
        version=VERSION,
        description='Apache Spark Python API',
        long_description=long_description,
        author='Spark Developers',
        author_email='dev@spark.apache.org',
        url='https://github.com/apache/spark/tree/master/python',
        packages=['pyspark',
                  'pyspark.mllib',
                  'pyspark.mllib.linalg',
                  'pyspark.mllib.stat',
                  'pyspark.ml',
                  'pyspark.ml.linalg',
                  'pyspark.ml.param',
                  'pyspark.sql',
                  'pyspark.streaming',
                  'pyspark.bin',
                  'pyspark.jars',
                  'pyspark.python.pyspark',
                  'pyspark.python.lib',
                  'pyspark.data',
                  'pyspark.licenses',
                  'pyspark.examples.src.main.python'],
        include_package_data=True,
        package_dir={
            'pyspark.jars': 'deps/jars',
            'pyspark.bin': 'deps/bin',
            'pyspark.python.lib': 'lib',
            'pyspark.data': 'deps/data',
            'pyspark.licenses': 'deps/licenses',
            'pyspark.examples.src.main.python': 'deps/examples',
        },
        package_data={
            'pyspark.jars': ['*.jar'],
            'pyspark.bin': ['*'],
            'pyspark.python.lib': ['*.zip'],
            'pyspark.data': ['*.txt', '*.data'],
            'pyspark.licenses': ['*.txt'],
            'pyspark.examples.src.main.python': ['*.py', '*/*.py']},
        scripts=scripts,
        license='http://www.apache.org/licenses/LICENSE-2.0',
        install_requires=['py4j==0.10.4'],
        setup_requires=['pypandoc'],
        extras_require={
            'ml': ['numpy>=1.7'],
            'mllib': ['numpy>=1.7'],
            'sql': ['pandas']
        },
        classifiers=[
            'Development Status :: 5 - Production/Stable',
            'License :: OSI Approved :: Apache Software License',
            'Programming Language :: Python :: 2.7',
            'Programming Language :: Python :: 3',
            'Programming Language :: Python :: 3.4',
            'Programming Language :: Python :: 3.5',
            'Programming Language :: Python :: Implementation :: CPython',
            'Programming Language :: Python :: Implementation :: PyPy']
    )
finally:
    # We only cleanup the symlink farm if we were in Spark, otherwise we are installing rather than
    # packaging.
    if (in_spark):
        # Depending on cleaning up the symlink farm or copied version
        if _supports_symlinks():
            os.remove(os.path.join(TEMP_PATH, "jars"))
            os.remove(os.path.join(TEMP_PATH, "bin"))
            os.remove(os.path.join(TEMP_PATH, "examples"))
            os.remove(os.path.join(TEMP_PATH, "data"))
            os.remove(os.path.join(TEMP_PATH, "licenses"))
        else:
            rmtree(os.path.join(TEMP_PATH, "jars"))
            rmtree(os.path.join(TEMP_PATH, "bin"))
            rmtree(os.path.join(TEMP_PATH, "examples"))
            rmtree(os.path.join(TEMP_PATH, "data"))
            rmtree(os.path.join(TEMP_PATH, "licenses"))
        os.rmdir(TEMP_PATH)
