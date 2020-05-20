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
FMRegressor Example.
"""
from __future__ import print_function

# $example on$
from pyspark.ml import Pipeline
from pyspark.ml.regression import FMRegressor
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.evaluation import RegressionEvaluator
# $example off$
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("FMRegressorExample") \
        .getOrCreate()

    # $example on$
    # Load and parse the data file, converting it to a DataFrame.
    data = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

    # Scale features.
    featureScaler = MinMaxScaler(inputCol="features", outputCol="scaledFeatures").fit(data)

    # Split the data into training and test sets (30% held out for testing)
    (trainingData, testData) = data.randomSplit([0.7, 0.3])

    # Train a FM model.
    fm = FMRegressor(featuresCol="scaledFeatures", stepSize=0.001)

    # Create a Pipeline.
    pipeline = Pipeline(stages=[featureScaler, fm])

    # Train model.
    model = pipeline.fit(trainingData)

    # Make predictions.
    predictions = model.transform(testData)

    # Select example rows to display.
    predictions.select("prediction", "label", "features").show(5)

    # Select (prediction, true label) and compute test error
    evaluator = RegressionEvaluator(
        labelCol="label", predictionCol="prediction", metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

    fmModel = model.stages[1]
    print("Factors: " + str(fmModel.factors))
    print("Linear: " + str(fmModel.linear))
    print("Intercept: " + str(fmModel.intercept))
    # $example off$

    spark.stop()
