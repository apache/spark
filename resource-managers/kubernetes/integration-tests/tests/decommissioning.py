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
        .appName("PyMemoryTest") \
        .getOrCreate()
    sc = spark._sc
    rdd = sc.parallelize(range(10))
    rdd.collect()
    print("Waiting to give nodes time to finish.")
    time.sleep(5)
    rdd.collect()
    print("Waiting some more....")
    time.sleep(10)
    rdd.collect()
    print("Finished waiting, stopping Spark.")
    spark.stop()
    print("Done, exiting Python")
    sys.exit(0)
