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
from pyspark import SparkContext

import numpy as np

# $example on$
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.feature import ChiSqSelector
from pyspark.mllib.util import MLUtils
# $example off$

if __name__ == "__main__":
    sc = SparkContext(appName="ChiSqSelectorExample")

    # $example on$
    # Load and parse the data file into an RDD of LabeledPoint.
    data = MLUtils.loadLibSVMFile(sc, 'data/mllib/sample_libsvm_data.txt')

    # Discretize data in 16 equal bins since ChiSqSelector requires categorical features
    def distributeOverBins(lp):
        return np.floor(lp.features.toArray() / 16)

    # Even though features are doubles, the ChiSqSelector treats each unique value as a category
    discretizedData = data.map(lambda lp: LabeledPoint(lp.label, distributeOverBins(lp)))

    # Create ChiSqSelector that will select top 50 of 692 features
    selector = ChiSqSelector(numTopFeatures=50)

    # Create ChiSqSelector model (selecting features)
    transformer = selector.fit(discretizedData)

    # Filter the top 50 features from each feature vector
    filteredData = transformer.transform(discretizedData.map(lambda lp: lp.features))
    # $example off$

    print('filtered data:')
    filteredData.foreach(print)

    sc.stop()
