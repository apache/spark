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

import sys
import time

from pyspark.sql import SparkSession


if __name__ == "__main__":
    """
        Usage: decommissioning
    """
    print("Starting decom test")
    spark = SparkSession \
        .builder \
        .appName("DecomTest") \
        .getOrCreate()
    sc = spark._sc
    acc = sc.accumulator(0)

    def addToAcc(x):
        acc.add(1)
        return x

    initialRdd = sc.parallelize(range(100), 5)
    accRdd = initialRdd.map(addToAcc)
    # Trigger a shuffle so there are shuffle blocks to migrate
    rdd = accRdd.map(lambda x: (x, x)).groupByKey()
    rdd.collect()
    print("1st accumulator value is: " + str(acc.value))
    print("Waiting to give nodes time to finish migration, decom exec 1.")
    print("...")
    time.sleep(30)
    rdd.count()
    rdd.collect()
    print("Final accumulator value is: " + str(acc.value))
    print("Finished waiting, stopping Spark.")
    spark.stop()
    print("Done, exiting Python")
    sys.exit(0)
