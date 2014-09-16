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

This example requires NumPy (http://www.numpy.org/).
"""

import numpy
import os
import sys

from operator import add

from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import DecisionTree
from pyspark.mllib.util import MLUtils


def getAccuracy(dtModel, data):
    """
    Return accuracy of DecisionTreeModel on the given RDD[LabeledPoint].
    """
    seqOp = (lambda acc, x: acc + (x[0] == x[1]))
    predictions = dtModel.predict(data.map(lambda x: x.features))
    truth = data.map(lambda p: p.label)
    trainCorrect = predictions.zip(truth).aggregate(0, seqOp, add)
    if data.count() == 0:
        return 0
    return trainCorrect / (0.0 + data.count())


def getMSE(dtModel, data):
    """
    Return mean squared error (MSE) of DecisionTreeModel on the given
    RDD[LabeledPoint].
    """
    seqOp = (lambda acc, x: acc + numpy.square(x[0] - x[1]))
    predictions = dtModel.predict(data.map(lambda x: x.features))
    truth = data.map(lambda p: p.label)
    trainMSE = predictions.zip(truth).aggregate(0, seqOp, add)
    if data.count() == 0:
        return 0
    return trainMSE / (0.0 + data.count())


def reindexClassLabels(data):
    """
    Re-index class labels in a dataset to the range {0,...,numClasses-1}.
    If all labels in that range already appear at least once,
     then the returned RDD is the same one (without a mapping).
    Note: If a label simply does not appear in the data,
          the index will not include it.
          Be aware of this when reindexing subsampled data.
    :param data: RDD of LabeledPoint where labels are integer values
                 denoting labels for a classification problem.
    :return: Pair (reindexedData, origToNewLabels) where
             reindexedData is an RDD of LabeledPoint with labels in
              the range {0,...,numClasses-1}, and
             origToNewLabels is a dictionary mapping original labels
              to new labels.
    """
    # classCounts: class --> # examples in class
    classCounts = data.map(lambda x: x.label).countByValue()
    numExamples = sum(classCounts.values())
    sortedClasses = sorted(classCounts.keys())
    numClasses = len(classCounts)
    # origToNewLabels: class --> index in 0,...,numClasses-1
    if (numClasses < 2):
        print >> sys.stderr, \
            "Dataset for classification should have at least 2 classes." + \
            " The given dataset had only %d classes." % numClasses
        exit(1)
    origToNewLabels = dict([(sortedClasses[i], i) for i in range(0, numClasses)])

    print "numClasses = %d" % numClasses
    print "Per-class example fractions, counts:"
    print "Class\tFrac\tCount"
    for c in sortedClasses:
        frac = classCounts[c] / (numExamples + 0.0)
        print "%g\t%g\t%d" % (c, frac, classCounts[c])

    if (sortedClasses[0] == 0 and sortedClasses[-1] == numClasses - 1):
        return (data, origToNewLabels)
    else:
        reindexedData = \
            data.map(lambda x: LabeledPoint(origToNewLabels[x.label], x.features))
        return (reindexedData, origToNewLabels)


def usage():
    print >> sys.stderr, \
        "Usage: decision_tree_runner [libsvm format data filepath]\n" + \
        " Note: This only supports binary classification."
    exit(1)


if __name__ == "__main__":
    if len(sys.argv) > 2:
        usage()
    sc = SparkContext(appName="PythonDT")

    # Load data.
    dataPath = 'data/mllib/sample_libsvm_data.txt'
    if len(sys.argv) == 2:
        dataPath = sys.argv[1]
    if not os.path.isfile(dataPath):
        sc.stop()
        usage()
    points = MLUtils.loadLibSVMFile(sc, dataPath)

    # Re-index class labels if needed.
    (reindexedData, origToNewLabels) = reindexClassLabels(points)

    # Train a classifier.
    categoricalFeaturesInfo = {}  # no categorical features
    model = DecisionTree.trainClassifier(reindexedData, numClasses=2,
                                         categoricalFeaturesInfo=categoricalFeaturesInfo)
    # Print learned tree and stats.
    print "Trained DecisionTree for classification:"
    print "  Model numNodes: %d\n" % model.numNodes()
    print "  Model depth: %d\n" % model.depth()
    print "  Training accuracy: %g\n" % getAccuracy(model, reindexedData)
    print model

    sc.stop()
