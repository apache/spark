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
An example for FValue testing.
Run with:
  bin/spark-submit examples/src/main/python/ml/fvalue_test_example.py
"""
from pyspark.sql import SparkSession
# $example on$
from pyspark.ml.linalg import Vectors
from pyspark.ml.stat import FValueTest
# $example off$

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("FValueTestExample") \
        .getOrCreate()

    # $example on$
    data = [(4.6, Vectors.dense(6.0, 7.0, 0.0, 7.0, 6.0, 0.0)),
            (6.6, Vectors.dense(0.0, 9.0, 6.0, 0.0, 5.0, 9.0)),
            (5.1, Vectors.dense(0.0, 9.0, 3.0, 0.0, 5.0, 5.0)),
            (7.6, Vectors.dense(0.0, 9.0, 8.0, 5.0, 6.0, 4.0)),
            (9.0, Vectors.dense(8.0, 9.0, 6.0, 5.0, 4.0, 4.0)),
            (9.0, Vectors.dense(8.0, 9.0, 6.0, 4.0, 0.0, 0.0))]
    df = spark.createDataFrame(data, ["label", "features"])

    ftest = FValueTest.test(df, "features", "label").head()
    print("pValues: " + str(ftest.pValues))
    print("degreesOfFreedom: " + str(ftest.degreesOfFreedom))
    print("fvalues: " + str(ftest.fValues))
    # $example off$

    spark.stop()
