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
import os, sys
from setuptools import setup, find_packages
from shutil import copyfile

exec(open('pyspark/version.py').read())
VERSION = __version__
# A temporary path so we can access above the Python project root and fetch scripts and jars we need
TEMP_PATH = "deps"
SPARK_HOME = os.path.abspath("../")
JARS_PATH = "%s/assembly/target/scala-2.11/jars/" % SPARK_HOME
EXAMPLES_PATH = "%s/examples/src/main/python" % SPARK_HOME
SCRIPTS_PATH = "%s/bin" % SPARK_HOME
SCRIPTS_TARGET = "%s/bin" % TEMP_PATH
JARS_TARGET = "%s/jars" % TEMP_PATH
EXAMPLES_TARGET = "%s/examples" % TEMP_PATH

if sys.version_info < (2, 7):
        print("Python versions prior to 2.7 are not supported.", file=sys.stderr)
        exit(-1)

# Check and see if we are under the spark path in which case we need to build the symlink farm.
# This is important because we only want to build the symlink farm while under Spark otherwise we
# want to use the symlink farm. And if the symlink farm exists under while under Spark (e.g. a
# partially built sdist) we should error and have the user sort it out.
in_spark = os.path.isfile("../core/src/main/scala/org/apache/spark/SparkContext.scala")
if (in_spark):
    # Construct links for setup
    try:
        os.mkdir(TEMP_PATH)
    except:
        print("Temp path for symlink to parent already exists %s" % TEMP_PATH, file=sys.stderr)
        exit(-1)

try:
    if (in_spark):
        # Construct the symlink farm
        os.symlink(JARS_PATH, JARS_TARGET)
        os.symlink(SCRIPTS_PATH, SCRIPTS_TARGET)
        os.symlink(EXAMPLES_PATH, EXAMPLES_TARGET)
    else:
        # We add find_spark_home.py to the bin directory we install so that pip installed PySpark
        # will search for SPARK_HOME with Python.
        # We only do this copy when we aren't inside of Spark (e.g. the packaging tool has copied
        # all the files into a temp directory) since otherwise the copy would go into the symlinked
        # directory.
        copyfile("pyspark/find_spark_home.py", SCRIPTS_TARGET + "/find_spark_home.py")
        # We copy the shell script to be under pyspark/python/pyspark so that the launcher scripts
        # find it where expected. The rest of the files aren't copied because they are accessed
        # using Python imports instead which will be resolved correctly.
        try:
            os.makedirs("pyspark/python/pyspark")
        except OSError:
            # Don't worry if the directory already exists.
            True
        copyfile("pyspark/shell.py", "pyspark/python/pyspark/shell.py")

    if not os.path.isdir(SCRIPTS_TARGET):
        print("For packaging reasons you must first create a source dist and install that source dist.", file=sys.stderr)
        exit(-1)

    # Scripts directive requires a list of each script path and does not take wild cards.
    script_names = os.listdir(SCRIPTS_TARGET)
    scripts = map(lambda script: SCRIPTS_TARGET + "/" + script, script_names)

    # Parse the README markdown file into rst for PyPi
    long_description = "!!!!! missing pandoc do not upload to PyPi !!!!"
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
                  'pyspark.ml',
                  'pyspark.sql',
                  'pyspark.streaming',
                  'pyspark.bin',
                  'pyspark.jars',
                  'pyspark.python.lib',
                  'pyspark.examples.src.main.python'],
        include_package_data=True,
        package_dir={
            'pyspark.jars': 'deps/jars',
            'pyspark.bin': 'deps/bin',
            'pyspark.python.lib': 'lib',
            'pyspark.examples.src.main.python': 'deps/examples',
        },
        package_data={
            'pyspark.jars': ['*.jar'],
            'pyspark.bin': ['*'],
            'pyspark.python.lib': ['*.zip'],
            'pyspark.examples.src.main.python': ['*.py', '*/*.py']},
        scripts=scripts,
        license='http://www.apache.org/licenses/LICENSE-2.0',
        install_requires=['py4j==0.10.4'],
        setup_requires=['pypandoc'],
        extras_require={
            'ml': ['numpy>=1.7'],
            'mllib': ['numpy<=1.7'],
            'sql': ['pandas']
        },
        classifiers=[
            'Development Status :: 5 - Production/Stable',
            'License :: OSI Approved :: Apache Software License',
            'Programming Language :: Python :: 2.7',
            'Programming Language :: Python :: 3',
            'Programming Language :: Python :: 3.0',
            'Programming Language :: Python :: 3.1',
            'Programming Language :: Python :: 3.2',
            'Programming Language :: Python :: 3.3',
            'Programming Language :: Python :: 3.4',
            'Programming Language :: Python :: 3.5',
            'Programming Language :: Python :: Implementation :: CPython',
            'Programming Language :: Python :: Implementation :: PyPy']
    )
finally:
    # We only cleanup the symlink farm if we were in Spark, otherwise we are installing rather than
    # packaging.
    if (in_spark):
        os.remove("%s/jars" % TEMP_PATH)
        os.remove("%s/bin" % TEMP_PATH)
        os.remove("%s/examples" % TEMP_PATH)
        os.rmdir(TEMP_PATH)
