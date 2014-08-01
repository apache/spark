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
Decision tree classification and regression using MLlib.
"""

import sys, numpy

from operator import add

from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import DecisionTree


# Parse a line of text into an MLlib LabeledPoint object
def parsePoint(line):
    values = [float(s) for s in line.split(',')]
    if values[0] == -1:   # Convert -1 labels to 0 for MLlib
        values[0] = 0
    return LabeledPoint(values[0], values[1:])

# Return accuracy of DecisionTreeModel on the given RDD[LabeledPoint].
def getAccuracy(dtModel, data):
    seqOp = (lambda acc, x: acc + (x[0] == x[1]))
    predictions = dtModel.predict(data)
    truth = data.map(lambda p: p.label)
    trainCorrect = predictions.zip(truth).aggregate(0, seqOp, add)
    return trainCorrect / (0.0 + data.count())

# Return mean squared error (MSE) of DecisionTreeModel on the given RDD[LabeledPoint].
def getMSE(dtModel, data):
    seqOp = (lambda acc, x: acc + numpy.square(x[0] - x[1]))
    predictions = dtModel.predict(data)
    truth = data.map(lambda p: p.label)
    trainMSE = predictions.zip(truth).aggregate(0, seqOp, add)
    return trainMSE / (0.0 + data.count())

# Return a new LabeledPoint with the label and feature 0 swapped.
def swapLabelAndFeature0(labeledPoint):
    newLabel = labeledPoint.label
    newFeatures = labeledPoint.features
    (newLabel, newFeatures[0]) = (newFeatures[0], newLabel)
    return LabeledPoint(newLabel, newFeatures)


if __name__ == "__main__":
    if len(sys.argv) != 1:
        print >> sys.stderr, "Usage: logistic_regression"
        exit(-1)
    sc = SparkContext(appName="PythonDT")

    # Load data.
    dataPath = 'data/mllib/sample_tree_data.csv'
    points = sc.textFile(dataPath).map(parsePoint)

    # Train a classifier.
    classificationModel = DecisionTree.trainClassifier(points, numClasses=2)
    # Print learned tree and stats.
    print "Trained DecisionTree for classification:"
    print "  Model numNodes: %d\n" % classificationModel.numNodes()
    print "  Model depth: %d\n" % classificationModel.depth()
    print "  Training accuracy: %g\n" % getAccuracy(classificationModel, points)
    print classificationModel

    # Switch labels and first feature to create a regression dataset with categorical features.
    # Feature 0 is now categorical with 2 categories, and labels are real numbers.
    regressionPoints = points.map(lambda labeledPoint: swapLabelAndFeature0(labeledPoint))
    categoricalFeaturesInfo = {0: 2}
    regressionModel = \
        DecisionTree.trainRegressor(regressionPoints, categoricalFeaturesInfo=categoricalFeaturesInfo)
    # Print learned tree and stats.
    print "Trained DecisionTree for regression:"
    print "  Model numNodes: %d\n" % regressionModel.numNodes()
    print "  Model depth: %d\n" % regressionModel.depth()
    print "  Training MSE: %g\n" % getMSE(regressionModel, regressionPoints)
    print regressionModel
