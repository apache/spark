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

"""
PySpark is the Python API for Spark.

Public classes:

  - :class:`SparkContext`:
      Main entry point for Spark functionality.
  - :class:`RDD`:
      A Resilient Distributed Dataset (RDD), the basic abstraction in Spark.
  - :class:`Broadcast`:
      A broadcast variable that gets reused across tasks.
  - :class:`Accumulator`:
      An "add-only" shared variable that tasks can only add values to.
  - :class:`SparkConf`:
      For configuring Spark.
  - :class:`SparkFiles`:
      Access files shipped with jobs.
  - :class:`StorageLevel`:
      Finer-grained cache persistence levels.

"""
import os
import re
import sys

from os.path import isfile, join

import xml.etree.ElementTree as ET

if os.environ.get("SPARK_HOME") is None:
    raise ImportError("Environment variable SPARK_HOME is undefined.")

spark_home = os.environ['SPARK_HOME']
pom_xml_file_path = join(spark_home, 'pom.xml')
snapshot_version = None

if isfile(pom_xml_file_path):
    try:
        tree = ET.parse(pom_xml_file_path)
        root = tree.getroot()
        version_tag = root[4].text
        snapshot_version = version_tag[:5]
    except:
        raise ImportError("Could not read the spark version, because pom.xml file" +
                          " could not be read.")
else:
    try:
        lib_file_path = join(spark_home, "lib")
        jars = [f for f in os.listdir(lib_file_path) if isfile(join(lib_file_path, f))]

        for jar in jars:
            m = re.match(r"^spark-assembly-([0-9\.]+).*\.jar$", jar)
            if m is not None and len(m.groups()) > 0:
                snapshot_version = m.group(1)

        if snapshot_version is None:
            raise ImportError("Could not read the spark version, because pom.xml or spark" +
                              " assembly jar could not be found.")
    except OSError:
        raise ImportError("Could not read the spark version, because pom.xml or lib directory" +
                          " could not be found in SPARK_HOME")


from pyspark.pyspark_version import __version__
if (snapshot_version != __version__):
    raise ImportError("Incompatible version of Spark(%s) and PySpark(%s)." %
                      (snapshot_version, __version__))


from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.rdd import RDD
from pyspark.files import SparkFiles
from pyspark.storagelevel import StorageLevel
from pyspark.accumulators import Accumulator, AccumulatorParam
from pyspark.broadcast import Broadcast
from pyspark.serializers import MarshalSerializer, PickleSerializer
from pyspark.status import *
from pyspark.profiler import Profiler, BasicProfiler

# for back compatibility
from pyspark.sql import SQLContext, HiveContext, SchemaRDD, Row

__all__ = [
    "SparkConf", "SparkContext", "SparkFiles", "RDD", "StorageLevel", "Broadcast",
    "Accumulator", "AccumulatorParam", "MarshalSerializer", "PickleSerializer",
    "StatusTracker", "SparkJobInfo", "SparkStageInfo", "Profiler", "BasicProfiler",
]
