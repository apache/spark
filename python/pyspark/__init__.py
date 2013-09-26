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

    - L{SparkContext<pyspark.context.SparkContext>}
        Main entry point for Spark functionality.
    - L{RDD<pyspark.rdd.RDD>}
        A Resilient Distributed Dataset (RDD), the basic abstraction in Spark.
    - L{Broadcast<pyspark.broadcast.Broadcast>}
        A broadcast variable that gets reused across tasks.
    - L{Accumulator<pyspark.accumulators.Accumulator>}
        An "add-only" shared variable that tasks can only add values to.
    - L{SparkFiles<pyspark.files.SparkFiles>}
        Access files shipped with jobs.
    - L{StorageLevel<pyspark.storagelevel.StorageLevel>}
        Finer-grained cache persistence levels.
"""
import sys
import os
sys.path.insert(0, os.path.join(os.environ["SPARK_HOME"], "python/lib/py4j0.7.egg"))


from pyspark.context import SparkContext
from pyspark.rdd import RDD
from pyspark.files import SparkFiles
from pyspark.storagelevel import StorageLevel


__all__ = ["SparkContext", "RDD", "SparkFiles", "StorageLevel"]
