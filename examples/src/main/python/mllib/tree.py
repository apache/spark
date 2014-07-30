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

import sys

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
    trainCorrect = \
        dtModel.predict(data).zip(data.map((lambda p => p.label))).aggregate(0, seqOp, add)
    return trainCorrect / (0.0 + data.count())


if __name__ == "__main__":
    if len(sys.argv) != 1:
        print >> sys.stderr, "Usage: logistic_regression"
        exit(-1)
    sc = SparkContext(appName="PythonDT")

    # Load data.
    dataPath = 'data/mllib/sample_tree_data.csv'
    points = sc.textFile(dataPath).map(parsePoint)

    # Train a classifier.
    model = DecisionTree.trainClassifier(points, numClasses=2)
    # Print learned tree.
    print "Model numNodes: " + model.numNodes() + "\n"
    print "Model depth: " + model.depth() + "\n"
    print model
    # Check accuracy.
    print "Training accuracy: " + getAccuracy(model, points) + "\n"

    # Switch labels and first feature to create a regression dataset with categorical features.
    """
    datasetInfo = DatasetInfo(numClasses=0, numFeatures=numFeatures)
    dtParams = DecisionTreeRegressor.defaultParams()
    model = DecisionTreeRegressor.train(points, datasetInfo, dtParams)
    # Print learned tree.
    print "Model numNodes: " + model.numNodes() + "\n"
    print "Model depth: " + model.depth() + "\n"
    print model
    # Check error.
    print "Training accuracy: " + getAccuracy(model, points) + "\n"
    """
