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
Gradient Boosted Trees Classification Example.
"""
from __future__ import print_function

import sys

from pyspark import SparkContext
# $example on$
from pyspark.mllib.tree import GradientBoostedTrees, GradientBoostedTreesModel
from pyspark.mllib.util import MLUtils
# $example off$

if __name__ == "__main__":
    sc = SparkContext(appName="PythonGradientBoostedTreesClassificationExample")
    # $example on$
    # Load and parse the data file.
    data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
    # Split the data into training and test sets (30% held out for testing)
    (trainingData, testData) = data.randomSplit([0.7, 0.3])

    # Train a GradientBoostedTrees model.
    #  Notes: (a) Empty categoricalFeaturesInfo indicates all features are continuous.
    #         (b) Use more iterations in practice.
    model = GradientBoostedTrees.trainClassifier(trainingData,
                                                 categoricalFeaturesInfo={}, numIterations=3)

    # Evaluate model on test instances and compute test error
    predictions = model.predict(testData.map(lambda x: x.features))
    labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
    testErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(testData.count())
    print('Test Error = ' + str(testErr))
    print('Learned classification GBT model:')
    print(model.toDebugString())

    # Save and load model
    model.save(sc, "target/tmp/myGradientBoostingClassificationModel")
    sameModel = GradientBoostedTreesModel.load(sc,
                                               "target/tmp/myGradientBoostingClassificationModel")
    # $example off$
