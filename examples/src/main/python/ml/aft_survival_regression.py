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

# $example on$
from pyspark.ml.regression import AFTSurvivalRegression
from pyspark.mllib.linalg import Vectors
# $example off$
from pyspark.sql import SparkSession

"""
An example demonstrating aft survival regression.
Run with:
  bin/spark-submit examples/src/main/python/ml/aft_survival_regression.py
"""

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("PythonAFTSurvivalRegressionExample") \
        .getOrCreate()

    # $example on$
    training = spark.createDataFrame([
        (1.218, 1.0, Vectors.dense(1.560, -0.605)),
        (2.949, 0.0, Vectors.dense(0.346, 2.158)),
        (3.627, 0.0, Vectors.dense(1.380, 0.231)),
        (0.273, 1.0, Vectors.dense(0.520, 1.151)),
        (4.199, 0.0, Vectors.dense(0.795, -0.226))], ["label", "censor", "features"])
    quantileProbabilities = [0.3, 0.6]
    aft = AFTSurvivalRegression(quantileProbabilities=quantileProbabilities,
                                quantilesCol="quantiles")

    model = aft.fit(training)

    # Print the coefficients, intercept and scale parameter for AFT survival regression
    print("Coefficients: " + str(model.coefficients))
    print("Intercept: " + str(model.intercept))
    print("Scale: " + str(model.scale))
    model.transform(training).show(truncate=False)
    # $example off$

    spark.stop()
