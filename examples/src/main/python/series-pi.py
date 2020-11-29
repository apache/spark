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
import time
from decimal import *
from operator import add

from pyspark.sql import SparkSession

# To keep the runtime reasonable, process up to 1 billion terms
minTerms = 10_000_000
maxTerms = 1_000_000_000
# Default number of partitions
defPartitions = 16

def partialSum(k):
    """Returns the sum S of terms i.e. Sigma(1/i**2) for i in [a..b]
    where a=k[0] and b=k[1]."""

    partialSum = Decimal(0)
    for i in range(k[0], k[1] + 1):
        partialSum += Decimal(1 / i**2)
    return partialSum

if __name__ == "__main__":
    """
        Usage: series-pi {terms} {partitions}
    """

    # Configure result for a precision of 30 decimal places.
    getcontext().prec = 30

    n = min(maxTerms, int(sys.argv[1])) if len(sys.argv) > 1 else minTerms
    p = int(sys.argv[2]) if len(sys.argv) > 2 else defPartitions

    # Partition the 'n' terms into 'p' partitions such that each 
    # partition contains 'nums' terms 

    nums = n // p
    l1 = [ (1 + (k - 1) * nums, k * nums) for k in range(1, p + 1) ]

    # If n is not a multiple of p, extend the last
    # partition to include all the terms.
    if l1[p - 1][1] < n:
        l1[p - 1][1] = n

    spark = SparkSession.builder.appName("series-Pi").getOrCreate()

    t1=time.perf_counter()
    val = spark.sparkContext.parallelize(l1, p).map(partialSum).reduce(add)
    # pi^2 = 6 * val
    print(f"Pi is approximately {Decimal(6 * val).sqrt()} using {n} terms of the power series.")
    print(f"Calculation took {time.perf_counter() - t1:.3f} seconds")

    spark.stop()
