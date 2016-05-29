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
# $example on$
from pyspark.mllib.feature import StandardScaler, StandardScalerModel
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.util import MLUtils
# $example off$

if __name__ == "__main__":
    sc = SparkContext(appName="StandardScalerExample")  # SparkContext

    # $example on$
    data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
    label = data.map(lambda x: x.label)
    features = data.map(lambda x: x.features)

    scaler1 = StandardScaler().fit(features)
    scaler2 = StandardScaler(withMean=True, withStd=True).fit(features)

    # data1 will be unit variance.
    data1 = label.zip(scaler1.transform(features))

    # Without converting the features into dense vectors, transformation with zero mean will raise
    # exception on sparse vector.
    # data2 will be unit variance and zero mean.
    data2 = label.zip(scaler2.transform(features.map(lambda x: Vectors.dense(x.toArray()))))
    # $example off$

    print("data1:")
    for each in data1.collect():
        print(each)

    print("data2:")
    for each in data2.collect():
        print(each)

    sc.stop()
