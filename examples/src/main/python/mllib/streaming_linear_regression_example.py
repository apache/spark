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
Streaming Linear Regression Example.
"""
# $example on$
import sys
# $example off$

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
# $example on$
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.regression import StreamingLinearRegressionWithSGD
# $example off$

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: streaming_linear_regression_example.py <trainingDir> <testDir>",
              file=sys.stderr)
        sys.exit(-1)

    sc = SparkContext(appName="PythonLogisticRegressionWithLBFGSExample")
    ssc = StreamingContext(sc, 1)

    # $example on$
    def parse(lp):
        label = float(lp[lp.find('(') + 1: lp.find(',')])
        vec = Vectors.dense(lp[lp.find('[') + 1: lp.find(']')].split(','))
        return LabeledPoint(label, vec)

    trainingData = ssc.textFileStream(sys.argv[1]).map(parse).cache()
    testData = ssc.textFileStream(sys.argv[2]).map(parse)

    numFeatures = 3
    model = StreamingLinearRegressionWithSGD()
    model.setInitialWeights([0.0, 0.0, 0.0])

    model.trainOn(trainingData)
    print(model.predictOnValues(testData.map(lambda lp: (lp.label, lp.features))))

    ssc.start()
    ssc.awaitTermination()
    # $example off$
