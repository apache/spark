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
Isotonic Regression Example.

Run with:
  bin/spark-submit examples/src/main/python/ml/isotonic_regression_example.py
"""
# $example on$
from pyspark.ml.regression import IsotonicRegression
# $example off$
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("IsotonicRegressionExample")\
        .getOrCreate()

    # $example on$
    # Loads data.
    dataset = spark.read.format("libsvm")\
        .load("data/mllib/sample_isotonic_regression_libsvm_data.txt")

    # Trains an isotonic regression model.
    model = IsotonicRegression().fit(dataset)
    print("Boundaries in increasing order: %s\n" % str(model.boundaries))
    print("Predictions associated with the boundaries: %s\n" % str(model.predictions))

    # Makes predictions.
    model.transform(dataset).show()
    # $example off$

    spark.stop()
