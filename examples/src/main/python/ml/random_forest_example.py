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

import sys

from pyspark import SparkContext
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import StringIndexer
from pyspark.ml.regression import RandomForestRegressor
from pyspark.mllib.evaluation import MulticlassMetrics, RegressionMetrics
from pyspark.mllib.util import MLUtils
from pyspark.sql import Row, SQLContext

"""
A simple example demonstrating a RandomForest Classification/Regression Pipeline.
Run with:
  bin/spark-submit examples/src/main/python/ml/random_forest_example.py
"""


def testClassification(train, test):
    # Train a RandomForest model.
    # Setting featureSubsetStrategy="auto" lets the algorithm choose.
    # Note: Use larger numTrees in practice.

    rf = RandomForestClassifier(labelCol="indexedLabel", numTrees=3, maxDepth=4)

    model = rf.fit(train)
    predictionAndLabels = model.transform(test).select("prediction", "indexedLabel") \
        .map(lambda x: (x.prediction, x.indexedLabel))

    metrics = MulticlassMetrics(predictionAndLabels)
    print("weighted f-measure %.3f" % metrics.weightedFMeasure())
    print("precision %s" % metrics.precision())
    print("recall %s" % metrics.recall())


def testRegression(train, test):
    # Train a RandomForest model.
    # Note: Use larger numTrees in practice.

    rf = RandomForestRegressor(labelCol="indexedLabel", numTrees=3, maxDepth=4)

    model = rf.fit(train)
    predictionAndLabels = model.transform(test).select("prediction", "indexedLabel") \
        .map(lambda x: (x.prediction, x.indexedLabel))

    metrics = RegressionMetrics(predictionAndLabels)
    print("rmse %.3f" % metrics.rootMeanSquaredError)
    print("r2 %.3f" % metrics.r2)
    print("mae %.3f" % metrics.meanAbsoluteError)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        print("Usage: random_forest_example", file=sys.stderr)
        exit(1)
    sc = SparkContext(appName="PythonRandomForestExample")
    sqlContext = SQLContext(sc)

    # Load and parse the data file into a dataframe.
    df = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt").toDF()

    # Map labels into an indexed column of labels in [0, numLabels)
    stringIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel")
    si_model = stringIndexer.fit(df)
    td = si_model.transform(df)
    [train, test] = td.randomSplit([0.7, 0.3])
    testClassification(train, test)
    testRegression(train, test)
    sc.stop()
