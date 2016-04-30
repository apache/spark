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
from optparse import OptionParser

from pyspark import SparkContext

# $example on$
from pyspark.ml.classification import LogisticRegression, OneVsRest
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.sql import Row, SQLContext
# $example off$

"""
An example runner for Multiclass to Binary Reduction with One Vs Rest.
The example uses Logistic Regression as the base classifier. All parameters that
can be specified on the base classifier can be passed in to the runner options.
Run with:

  bin/spark-submit examples/src/main/python/ml/one_vs_rest_example.py
"""

class Params:
    def __init__(self, input, testInput, maxIter, tol, fitIntercept, regParam,
                 elasticNetParam, fracTest):
        self.input = input
        self.testInput = testInput
        self.maxIter = maxIter
        self.tol = tol
        self.fitIntercept = fitIntercept
        self.regParam = regParam
        self.elasticNetParam = elasticNetParam
        self.fracTest = fracTest


def parse(args):
    parser = OptionParser()

    return Params

if __name__ == "__main__":

    params = parse(sys.argv)

    sc = SparkContext(appName="OneVsRestExample")
    sqlContext = SQLContext(sc)

    # $example on$
    inputData = sqlContext.read.format("libsvm").load(params.input)
    # compute the train/test split: if testInput is not provided use part of input.
    if params.testInput is not None:
        train = inputData
        test = sqlContext.read.format("libsvm").load(params.testInput)
    else:
        f = params.fracTest
        (train, test) = inputData.randomSplit([1 - f, f])

    lrParams = {'maxIter': params.maxIter, 'tol': params.tol, 'fitIntercept': params.fitIntercept}
    if params.regParam is not None:
        lrParams['regParam'] = params.regParam
    if params.elasticNetParam is not None:
        lrParams['elasticNetParam'] = params.elasticNetParam

    # instantiate the base classifier
    lr = LogisticRegression(**lrParams)

    # instantiate the One Vs Rest Classifier.
    ovr = OneVsRest(classifier=lr)

    # train the multiclass model.
    ovrModel = ovr.fit(train)

    # score the model on test data.
    predictions = ovrModel.transform(test)

    # evaluate the model
    predictionsAndLabels = predictions.select("prediction", "label").rdd.map(row => (row.getDouble(0), row.getDouble(1)))

    evaluator = MulticlassClassificationEvaluator(
        labelCol="indexedLabel", predictionCol="prediction", metricName="precision")
    accuracy = evaluator.evaluate(predictions)
    # $example off$

    sc.stop()
