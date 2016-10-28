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

# $example on$
from pyspark.ml.classification import LogisticRegression, OneVsRest
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
# $example off$
from pyspark.sql import SparkSession

"""
An example of Multiclass to Binary Reduction with One Vs Rest,
using Logistic Regression as the base classifier.
Run with:
  bin/spark-submit examples/src/main/python/ml/one_vs_rest_example.py
"""

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("OneVsRestExample") \
        .getOrCreate()

    # $example on$
    # load data file.
    inputData = spark.read.format("libsvm") \
        .load("data/mllib/sample_multiclass_classification_data.txt")

    # generate the train/test split.
    (train, test) = inputData.randomSplit([0.8, 0.2])

    # instantiate the base classifier.
    lr = LogisticRegression(maxIter=10, tol=1E-6, fitIntercept=True)

    # instantiate the One Vs Rest Classifier.
    ovr = OneVsRest(classifier=lr)

    # train the multiclass model.
    ovrModel = ovr.fit(train)

    # score the model on test data.
    predictions = ovrModel.transform(test)

    # obtain evaluator.
    evaluator = MulticlassClassificationEvaluator(metricName="accuracy")

    # compute the classification error on test data.
    accuracy = evaluator.evaluate(predictions)
    print("Test Error = %g" % (1.0 - accuracy))
    # $example off$

    spark.stop()
