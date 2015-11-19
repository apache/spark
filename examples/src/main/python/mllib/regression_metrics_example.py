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
# $example on$
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD
from pyspark.mllib.evaluation import RegressionMetrics
from pyspark.mllib.linalg import DenseVector
# $example off$

from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext(appName="Regression Metrics Example")

    # $example on$
    # Load and parse the data
    def parsePoint(line):
        values = line.split()
        return LabeledPoint(float(values[0]),
                            DenseVector([float(x.split(':')[1]) for x in values[1:]]))

    data = sc.textFile("data/mllib/sample_linear_regression_data.txt")
    parsedData = data.map(parsePoint)

    # Build the model
    model = LinearRegressionWithSGD.train(parsedData)

    # Get predictions
    valuesAndPreds = parsedData.map(lambda p: (float(model.predict(p.features)), p.label))

    # Instantiate metrics object
    metrics = RegressionMetrics(valuesAndPreds)

    # Squared Error
    print("MSE = %s" % metrics.meanSquaredError)
    print("RMSE = %s" % metrics.rootMeanSquaredError)

    # R-squared
    print("R-squared = %s" % metrics.r2)

    # Mean absolute error
    print("MAE = %s" % metrics.meanAbsoluteError)

    # Explained variance
    print("Explained variance = %s" % metrics.explainedVariance)
    # $example off$
