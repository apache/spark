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

import sys
import timeit
from random import random
from operator import add

from pyspark.sql import SparkSession


if __name__ == "__main__":
    """
        Usage: prefetch [partitions] [iterations]
        Uses timeit to demonstrate the benefit of prefetch.
    """
    spark = SparkSession\
        .builder\
        .appName("PrefetchDemo")\
        .getOrCreate()

    partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 4
    iterations = int(sys.argv[2]) if len(sys.argv) > 2 else 4

    elems = spark.sparkContext.parallelize(range(1, partitions * 2), partitions)
    elems.cache()
    elems.count()

    def slowCompute(elem):
        """
        Wait ten seconds to simulate some computation then return.
        """
        import time
        time.sleep(10)
        return elem

    def localCompute(elem):
        """
        Simulate processing the data locally.
        """
        import time
        time.sleep(1)
        return elem

    def fetchWithPrefetch():
        prefetchIter = elems.mapPartitions(slowCompute).toLocalIterator(prefetchPartitions=True)
        localCollection = list(map(localCompute, prefetchIter))
        return localCollection

    def fetchRegular():
        regularIter = elems.mapPartitions(slowCompute).toLocalIterator(prefetchPartitions=False)
        localCollection = list(map(localCompute, regularIter))
        return localCollection

    print("Running timers:\n")
    prefetchTimer = timeit.Timer(fetchWithPrefetch)
    prefetchTime = prefetchTimer.timeit(number=iterations)

    regularTimer = timeit.Timer(fetchRegular)
    regularTime = regularTimer.timeit(number=iterations)
    print("\nResults:\n")
    print("Prefetch time:\n")
    print(prefetchTime)
    print("\n")

    print("Regular time:\n")
    print(regularTime)
    print("\n")

    spark.stop()
