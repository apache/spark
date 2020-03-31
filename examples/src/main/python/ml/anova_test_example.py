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
An example for ANOVA testing.
Run with:
  bin/spark-submit examples/src/main/python/ml/anova_test_example.py
"""
from __future__ import print_function

from pyspark.sql import SparkSession
# $example on$
from pyspark.ml.linalg import Vectors
from pyspark.ml.stat import ANOVATest
# $example off$

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("ANOVATestExample")\
        .getOrCreate()

    # $example on$
    data = [(3.0, Vectors.dense([1.7, 4.4, 7.6, 5.8, 9.6, 2.3])),
            (2.0, Vectors.dense([8.8, 7.3, 5.7, 7.3, 2.2, 4.1])),
            (1.0, Vectors.dense([1.2, 9.5, 2.5, 3.1, 8.7, 2.5])),
            (2.0, Vectors.dense([3.7, 9.2, 6.1, 4.1, 7.5, 3.8])),
            (4.0, Vectors.dense([8.9, 5.2, 7.8, 8.3, 5.2, 3.0])),
            (4.0, Vectors.dense([7.9, 8.5, 9.2, 4.0, 9.4, 2.1]))]
    df = spark.createDataFrame(data, ["label", "features"])

    r = ANOVATest.test(df, "features", "label").head()
    print("pValues: " + str(r.pValues))
    print("degreesOfFreedom: " + str(r.degreesOfFreedom))
    print("fValues: " + str(r.fValues))
    # $example off$

    spark.stop()
