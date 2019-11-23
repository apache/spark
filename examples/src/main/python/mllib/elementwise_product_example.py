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
from pyspark.mllib.feature import ElementwiseProduct
from pyspark.mllib.linalg import Vectors
# $example off$

if __name__ == "__main__":
    sc = SparkContext(appName="ElementwiseProductExample")  # SparkContext

    # $example on$
    data = sc.textFile("data/mllib/kmeans_data.txt")
    parsedData = data.map(lambda x: [float(t) for t in x.split(" ")])

    # Create weight vector.
    transformingVector = Vectors.dense([0.0, 1.0, 2.0])
    transformer = ElementwiseProduct(transformingVector)

    # Batch transform
    transformedData = transformer.transform(parsedData)
    # Single-row transform
    transformedData2 = transformer.transform(parsedData.first())
    # $example off$

    print("transformedData:")
    for each in transformedData.collect():
        print(each)

    print("transformedData2:")
    for each in transformedData2:
        print(each)

    sc.stop()
