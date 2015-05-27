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

import pprint, sys

from pyspark import SparkContext
from pyspark.ml.classification import LogisticRegression
from pyspark.mllib.linalg import DenseVector
from pyspark.mllib.regression import LabeledPoint
from pyspark.sql import SQLContext

"""
A simple example demonstrating ways to specify parameters for Estimators and Transformers.
Run with:
  bin/spark-submit examples/src/main/python/ml/simple_params_example.py
"""

if __name__ == "__main__":
    if len(sys.argv) > 1:
        print("Usage: random_forest_example", file=sys.stderr)
        exit(1)
    sc = SparkContext(appName="PythonSimpleParamsExample")
    sqlContext = SQLContext(sc)

    # prepare training data.
    training = sc.parallelize([
        LabeledPoint(1.0, DenseVector([0.0, 1.1, 0.1])),
        LabeledPoint(0.0, DenseVector([2.0, 1.0, -1.0])),
        LabeledPoint(0.0, DenseVector([2.0, 1.3, 1.0])),
        LabeledPoint(1.0, DenseVector([0.0, 1.2, -0.5]))]).toDF()

    # Create a LogisticRegression instance.  This instance is an Estimator.
    lr = LogisticRegression()
    # Print out the parameters, documentation, and any default values.
    print("LogisticRegression parameters:\n" + lr.explainParams() + "\n")

    # We may set parameters using setter methods.
    lr.setMaxIter(10) \
        .setRegParam(0.01)

    # Learn a LogisticRegression model.  This uses the parameters stored in lr.
    model1 = lr.fit(training)

    # Since model1 is a Model (i.e., a Transformer produced by an Estimator),
    # we can view the parameters it used during fit().
    # This prints the parameter (name: value) pairs, where names are unique IDs for this
    # LogisticRegression instance.
    print("Model 1 was fit using parameters:\n")
    pprint.pprint(model1.extractParamMap())

    # We may alternatively specify parameters using a parameter map,
    # either overriding the default parameters or specifying new values.
    paramMap = {lr.maxIter : 20, lr.threshold : 0.55, lr.probabilityCol : "myProbability"}

    # Now learn a new model using the new parameters.
    model2 = lr.fit(training, paramMap)
    print("Model 2 was fit using parameters:\n")
    pprint.pprint(model2.extractParamMap())

    # prepare test data.
    test = sc.parallelize([
        LabeledPoint(1.0, DenseVector([-1.0, 1.5, 1.3])),
        LabeledPoint(0.0, DenseVector([3.0, 2.0, -0.1])),
        LabeledPoint(0.0, DenseVector([0.0, 2.2, -1.5]))]).toDF()

    # Make predictions on test data using the Transformer.transform() method.
    # LogisticRegression.transform will only use the 'features' column.
    # Note that model2.transform() outputs a 'myProbability' column instead of the usual
    # 'probability' column since we renamed the lr.probabilityCol parameter previously.
    model2.transform(test) \
    .select("features", "label", "myProbability", "prediction") \
    .foreach(lambda x: print("features=%s,label=%s -> prob=%s, prediction=%s"
                             % (x.features, x.label, x.myProbability, x.prediction)))

    sc.stop()
