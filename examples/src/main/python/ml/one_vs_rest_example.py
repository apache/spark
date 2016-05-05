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

import argparse

from pyspark import SparkContext

# $example on$
from pyspark.ml.classification import LogisticRegression, OneVsRest
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.sql import SQLContext
# $example off$

"""
An example runner for Multiclass to Binary Reduction with One Vs Rest.
The example uses Logistic Regression as the base classifier. All parameters that
can be specified on the base classifier can be passed in to the runner options.
Run with:

  bin/spark-submit examples/src/main/python/ml/one_vs_rest_example.py
"""


def parse():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",
                        help="input path to labeled examples. This path must be specified")
    parser.add_argument("--fracTest", type=float, default=0.2,
                        help="fraction of data to hold out for testing.  If given option testInput,"
                             " this option is ignored. default: 0.2")
    parser.add_argument("--testInput",
                        help="iinput path to test dataset. If given, option fracTest is ignored")
    parser.add_argument("--maxIter", type=int, default=100,
                        help="maximum number of iterations for Logistic Regression. default: 100")
    parser.add_argument("--tol", type=float, default=1e-6,
                        help="the convergence tolerance of iterations for Logistic Regression."
                             " default: 1e-6")
    parser.add_argument("--fitIntercept", default="true",
                        help="fit intercept for Logistic Regression. default: true")
    parser.add_argument("--regParam", type=float,
                        help="the regularization parameter for Logistic Regression. default: None")
    parser.add_argument("--elasticNetParam", type=float,
                        help="the ElasticNet mixing parameter for Logistic Regression. default:"
                             " None")
    params = parser.parse_args()

    assert params.input is not None, "input is required"
    assert 0 <= params.fracTest < 1, "fracTest value incorrect; should be in [0,1)."
    assert params.fitIntercept in ("true", "false")
    params.fitIntercept = params.fitIntercept == "true"

    return params

if __name__ == "__main__":

    params = parse()

    sc = SparkContext(appName="PythonOneVsRestExample")
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
    predictionAndLabels = predictions.rdd.map(lambda r: (r.prediction, r.label))

    metrics = MulticlassMetrics(predictionAndLabels)

    confusionMatrix = metrics.confusionMatrix()

    # compute the false positive rate per label
    numClasses = train.select('label').distinct().count()

    fprs = [(p, metrics.falsePositiveRate(float(p))) for p in range(numClasses)]

    print("Confusion Matrix")
    print(confusionMatrix)

    print("label\tfpr")
    for label, fpr in fprs:
        print(str(label) + "\t" + str(fpr))
    # $example off$

    sc.stop()
