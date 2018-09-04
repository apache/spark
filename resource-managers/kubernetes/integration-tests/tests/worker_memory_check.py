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

import resource
import sys

from pyspark.sql import SparkSession


if __name__ == "__main__":
    """
        Usage: worker_memory_check [Memory_in_Mi]
    """
    spark = SparkSession \
        .builder \
        .appName("PyMemoryTest") \
        .getOrCreate()
    sc = spark.sparkContext
    if len(sys.argv) < 2:
        print("Usage: worker_memory_check [Memory_in_Mi]", file=sys.stderr)
        sys.exit(-1)

    def f(x):
        rLimit = resource.getrlimit(resource.RLIMIT_AS)
        print("RLimit is " + str(rLimit))
        return rLimit
    resourceValue = sc.parallelize([1]).map(f).collect()[0][0]
    print("Resource Value is " + str(resourceValue))
    truthCheck = (resourceValue == int(sys.argv[1]))
    print("PySpark Worker Memory Check is: " + str(truthCheck))
    spark.stop()
