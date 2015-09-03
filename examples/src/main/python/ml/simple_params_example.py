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

import pprint
import sys

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
        print("Usage: simple_params_example", file=sys.stderr)
        exit(1)
    sc = SparkContext(appName="PythonSimpleParamsExample")
    sqlContext = SQLContext(sc)

    # prepare training data.
    # We create an RDD of LabeledPoints and convert them into a DataFrame.
    # A LabeledPoint is an Object with two fields named label and features
    # and Spark SQL identifies these fields and creates the schema appropriately.
    training = sc.parallelize([
        LabeledPoint(1.0, DenseVector([0.0, 1.1, 0.1])),
        LabeledPoint(0.0, DenseVector([2.0, 1.0, -1.0])),
        LabeledPoint(0.0, DenseVector([2.0, 1.3, 1.0])),
        LabeledPoint(1.0, DenseVector([0.0, 1.2, -0.5]))]).toDF()

    # Create a LogisticRegression instance with maxIter = 10.
    # This instance is an Estimator.
    lr = LogisticRegression(maxIter=10)
    # Print out the parameters, documentation, and any default values.
    print("LogisticRegression parameters:\n" + lr.explainParams() + "\n")

    # We may also set parameters using setter methods.
    lr.setRegParam(0.01)

    # Learn a LogisticRegression model.  This uses the parameters stored in lr.
    model1 = lr.fit(training)

    # Since model1 is a Model (i.e., a Transformer produced by an Estimator),
    # we can view the parameters it used during fit().
    # This prints the parameter (name: value) pairs, where names are unique IDs for this
    # LogisticRegression instance.
    print("Model 1 was fit using parameters:\n")
    pprint.pprint(model1.extractParamMap())

    # We may alternatively specify parameters using a parameter map.
    # paramMap overrides all lr parameters set earlier.
    paramMap = {lr.maxIter: 20, lr.thresholds: [0.45, 0.55], lr.probabilityCol: "myProbability"}

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
    # LogisticRegressionModel.transform will only use the 'features' column.
    # Note that model2.transform() outputs a 'myProbability' column instead of the usual
    # 'probability' column since we renamed the lr.probabilityCol parameter previously.
    result = model2.transform(test) \
        .select("features", "label", "myProbability", "prediction") \
        .collect()

    for row in result:
        print("features=%s,label=%s -> prob=%s, prediction=%s"
              % (row.features, row.label, row.myProbability, row.prediction))

    sc.stop()
