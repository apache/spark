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
Isotonic Regression Example.
"""
from __future__ import print_function

from pyspark import SparkContext
# $example on$
import math
from pyspark.mllib.regression import LabeledPoint, IsotonicRegression, IsotonicRegressionModel
from pyspark.mllib.util import MLUtils
# $example off$

if __name__ == "__main__":

    sc = SparkContext(appName="PythonIsotonicRegressionExample")

    # $example on$
    # Load and parse the data
    def parsePoint(labeledData):
        return (labeledData.label, labeledData.features[0], 1.0)

    data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_isotonic_regression_libsvm_data.txt")

    # Create label, feature, weight tuples from input data with weight set to default value 1.0.
    parsedData = data.map(parsePoint)

    # Split data into training (60%) and test (40%) sets.
    training, test = parsedData.randomSplit([0.6, 0.4], 11)

    # Create isotonic regression model from training data.
    # Isotonic parameter defaults to true so it is only shown for demonstration
    model = IsotonicRegression.train(training)

    # Create tuples of predicted and real labels.
    predictionAndLabel = test.map(lambda p: (model.predict(p[1]), p[0]))

    # Calculate mean squared error between predicted and real labels.
    meanSquaredError = predictionAndLabel.map(lambda pl: math.pow((pl[0] - pl[1]), 2)).mean()
    print("Mean Squared Error = " + str(meanSquaredError))

    # Save and load model
    model.save(sc, "target/tmp/myIsotonicRegressionModel")
    sameModel = IsotonicRegressionModel.load(sc, "target/tmp/myIsotonicRegressionModel")
    # $example off$
