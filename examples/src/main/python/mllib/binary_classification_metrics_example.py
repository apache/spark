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
Binary Classification Metrics Example.
"""
from __future__ import print_function
from pyspark.sql import SparkSession
# $example on$
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.mllib.regression import LabeledPoint
# $example off$

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("BinaryClassificationMetricsExample")\
        .getOrCreate()

    # $example on$
    # Several of the methods available in scala are currently missing from pyspark
    # Load training data in LIBSVM format
    data = spark\
        .read.format("libsvm").load("data/mllib/sample_binary_classification_data.txt")\
        .rdd.map(lambda row: LabeledPoint(row[0], row[1]))

    # Split data into training (60%) and test (40%)
    training, test = data.randomSplit([0.6, 0.4], seed=11)
    training.cache()

    # Run training algorithm to build the model
    model = LogisticRegressionWithLBFGS.train(training)

    # Compute raw scores on the test set
    predictionAndLabels = test.map(lambda lp: (float(model.predict(lp.features)), lp.label))

    # Instantiate metrics object
    metrics = BinaryClassificationMetrics(predictionAndLabels)

    # Area under precision-recall curve
    print("Area under PR = %s" % metrics.areaUnderPR)

    # Area under ROC curve
    print("Area under ROC = %s" % metrics.areaUnderROC)
    # $example off$

    spark.stop()
