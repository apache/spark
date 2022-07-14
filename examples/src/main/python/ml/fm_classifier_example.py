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
FMClassifier Example.
"""
# $example on$
from pyspark.ml import Pipeline
from pyspark.ml.classification import FMClassifier
from pyspark.ml.feature import MinMaxScaler, StringIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
# $example off$
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("FMClassifierExample") \
        .getOrCreate()

    # $example on$
    # Load and parse the data file, converting it to a DataFrame.
    data = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

    # Index labels, adding metadata to the label column.
    # Fit on whole dataset to include all labels in index.
    labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(data)
    # Scale features.
    featureScaler = MinMaxScaler(inputCol="features", outputCol="scaledFeatures").fit(data)

    # Split the data into training and test sets (30% held out for testing)
    (trainingData, testData) = data.randomSplit([0.7, 0.3])

    # Train a FM model.
    fm = FMClassifier(labelCol="indexedLabel", featuresCol="scaledFeatures", stepSize=0.001)

    # Create a Pipeline.
    pipeline = Pipeline(stages=[labelIndexer, featureScaler, fm])

    # Train model.
    model = pipeline.fit(trainingData)

    # Make predictions.
    predictions = model.transform(testData)

    # Select example rows to display.
    predictions.select("prediction", "indexedLabel", "features").show(5)

    # Select (prediction, true label) and compute test accuracy
    evaluator = MulticlassClassificationEvaluator(
        labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    print("Test set accuracy = %g" % accuracy)

    fmModel = model.stages[2]
    print("Factors: " + str(fmModel.factors))  # type: ignore
    print("Linear: " + str(fmModel.linear))  # type: ignore
    print("Intercept: " + str(fmModel.intercept))  # type: ignore
    # $example off$

    spark.stop()
