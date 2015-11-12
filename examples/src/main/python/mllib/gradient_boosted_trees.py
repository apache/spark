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
Gradient boosted Trees classification and regression using MLlib.
"""
from __future__ import print_function

import sys

from pyspark.context import SparkContext
from pyspark.mllib.tree import GradientBoostedTrees
from pyspark.mllib.util import MLUtils


def testClassification(trainingData, testData):
    # Train a GradientBoostedTrees model.
    #  Empty categoricalFeaturesInfo indicates all features are continuous.
    model = GradientBoostedTrees.trainClassifier(trainingData, categoricalFeaturesInfo={},
                                                 numIterations=30, maxDepth=4)
    # Evaluate model on test instances and compute test error
    predictions = model.predict(testData.map(lambda x: x.features))
    labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
    testErr = labelsAndPredictions.filter(lambda v_p: v_p[0] != v_p[1]).count() \
        / float(testData.count())
    print('Test Error = ' + str(testErr))
    print('Learned classification ensemble model:')
    print(model.toDebugString())


def testRegression(trainingData, testData):
    # Train a GradientBoostedTrees model.
    #  Empty categoricalFeaturesInfo indicates all features are continuous.
    model = GradientBoostedTrees.trainRegressor(trainingData, categoricalFeaturesInfo={},
                                                numIterations=30, maxDepth=4)
    # Evaluate model on test instances and compute test error
    predictions = model.predict(testData.map(lambda x: x.features))
    labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
    testMSE = labelsAndPredictions.map(lambda vp: (vp[0] - vp[1]) * (vp[0] - vp[1])).sum() \
        / float(testData.count())
    print('Test Mean Squared Error = ' + str(testMSE))
    print('Learned regression ensemble model:')
    print(model.toDebugString())


if __name__ == "__main__":
    if len(sys.argv) > 1:
        print("Usage: gradient_boosted_trees", file=sys.stderr)
        exit(1)
    sc = SparkContext(appName="PythonGradientBoostedTrees")

    # Load and parse the data file into an RDD of LabeledPoint.
    data = MLUtils.loadLibSVMFile(sc, 'data/mllib/sample_libsvm_data.txt')
    # Split the data into training and test sets (30% held out for testing)
    (trainingData, testData) = data.randomSplit([0.7, 0.3])

    print('\nRunning example of classification using GradientBoostedTrees\n')
    testClassification(trainingData, testData)

    print('\nRunning example of regression using GradientBoostedTrees\n')
    testRegression(trainingData, testData)

    sc.stop()
