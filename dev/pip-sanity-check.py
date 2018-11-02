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

from __future__ import print_function

from pyspark.sql import SparkSession
from pyspark.ml.param import Params
from pyspark.mllib.linalg import *
import sys

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("PipSanityCheck")\
        .getOrCreate()
    sc = spark.sparkContext
    rdd = sc.parallelize(range(100), 10)
    value = rdd.reduce(lambda x, y: x + y)
    if (value != 4950):
        print("Value {0} did not match expected value.".format(value), file=sys.stderr)
        sys.exit(-1)
    print("Successfully ran pip sanity check")

    spark.stop()
