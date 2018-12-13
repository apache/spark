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
Correlations using MLlib.
"""
from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.stat import Statistics
from pyspark.mllib.util import MLUtils


if __name__ == "__main__":
    if len(sys.argv) not in [1, 2]:
        print("Usage: correlations (<file>)", file=sys.stderr)
        sys.exit(-1)
    sc = SparkContext(appName="PythonCorrelations")
    if len(sys.argv) == 2:
        filepath = sys.argv[1]
    else:
        filepath = 'data/mllib/sample_linear_regression_data.txt'
    corrType = 'pearson'

    points = MLUtils.loadLibSVMFile(sc, filepath)\
        .map(lambda lp: LabeledPoint(lp.label, lp.features.toArray()))

    print()
    print('Summary of data file: ' + filepath)
    print('%d data points' % points.count())

    # Statistics (correlations)
    print()
    print('Correlation (%s) between label and each feature' % corrType)
    print('Feature\tCorrelation')
    numFeatures = points.take(1)[0].features.size
    labelRDD = points.map(lambda lp: lp.label)
    for i in range(numFeatures):
        featureRDD = points.map(lambda lp: lp.features[i])
        corr = Statistics.corr(labelRDD, featureRDD, corrType)
        print('%d\t%g' % (i, corr))
    print()

    sc.stop()
