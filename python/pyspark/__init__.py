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
  - L{RDD}
      A Resilient Distributed Dataset (RDD), the basic abstraction in Spark.
  - L{Broadcast}
      A broadcast variable that gets reused across tasks.
  - L{Accumulator}
      An "add-only" shared variable that tasks can only add values to.
  - L{SparkConf}
      For configuring Spark.
  - L{SparkFiles}
      Access files shipped with jobs.
  - L{StorageLevel}
      Finer-grained cache persistence levels.

"""

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.rdd import RDD
from pyspark.files import SparkFiles
from pyspark.storagelevel import StorageLevel
from pyspark.accumulators import Accumulator, AccumulatorParam
from pyspark.broadcast import Broadcast
from pyspark.serializers import MarshalSerializer, PickleSerializer
from pyspark.profiler import Profiler, BasicProfiler

# for back compatibility
from pyspark.sql import SQLContext, HiveContext, SchemaRDD, Row

__all__ = [
    "SparkConf", "SparkContext", "SparkFiles", "RDD", "StorageLevel", "Broadcast",
    "Accumulator", "AccumulatorParam", "MarshalSerializer", "PickleSerializer",
    "Profiler", "BasicProfiler",
]
