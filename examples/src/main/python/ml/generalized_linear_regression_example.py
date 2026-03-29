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
An example demonstrating generalized linear regression.
Run with:
  bin/spark-submit examples/src/main/python/ml/generalized_linear_regression_example.py
"""
from pyspark.sql import SparkSession
# $example on$
from pyspark.ml.regression import GeneralizedLinearRegression
# $example off$

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("GeneralizedLinearRegressionExample")\
        .getOrCreate()

    # $example on$
    # Load training data
    dataset = spark.read.format("libsvm")\
        .load("data/mllib/sample_linear_regression_data.txt")

    glr = GeneralizedLinearRegression(family="gaussian", link="identity", maxIter=10, regParam=0.3)

    # Fit the model
    model = glr.fit(dataset)

    # Print the coefficients and intercept for generalized linear regression model
    print("Coefficients: " + str(model.coefficients))
    print("Intercept: " + str(model.intercept))

    # Summarize the model over the training set and print out some metrics
    summary = model.summary
    print("Coefficient Standard Errors: " + str(summary.coefficientStandardErrors))
    print("T Values: " + str(summary.tValues))
    print("P Values: " + str(summary.pValues))
    print("Dispersion: " + str(summary.dispersion))
    print("Null Deviance: " + str(summary.nullDeviance))
    print("Residual Degree Of Freedom Null: " + str(summary.residualDegreeOfFreedomNull))
    print("Deviance: " + str(summary.deviance))
    print("Residual Degree Of Freedom: " + str(summary.residualDegreeOfFreedom))
    print("AIC: " + str(summary.aic))
    print("Deviance Residuals: ")
    summary.residuals().show()
    # $example off$

    spark.stop()
