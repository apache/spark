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
from pyspark.ml.clustering import BisectingKMeans, BisectingKMeansModel
from pyspark.mllib.linalg import VectorUDT, _convert_to_vector, Vectors
# $example off$
from pyspark.sql import SQLContext

"""
A simple example demonstrating a bisecting k-means clustering.
"""

if __name__ == "__main__":

    sc = SparkContext(appName="PythonBisectingKMeansExample")
    sqlContext = SQLContext(sc)

    # $example on$
    training = sqlContext.createDataFrame([
        (0, Vectors.dense(0.1, 0.1, 0.1)),
        (1, Vectors.dense(0.3, 0.3, 0.25)),
        (2, Vectors.dense(0.1, 0.1, -0.1)),
        (3, Vectors.dense(20.3, 20.1, 19.9)),
        (4, Vectors.dense(20.2, 20.1, 19.7)),
        (5, Vectors.dense(18.9, 20.0, 19.7))], ["id", "features"])

    k = 2
    kmeans = BisectingKMeans().setK(k).setSeed(1).setFeaturesCol("features")

    model = kmeans.fit(training)

    # Evaluate clustering
    cost = model.computeCost(training)
    print("Bisecting K-means Cost = " + str(cost))

    centers = model.clusterCenters()
    print("Cluster Centers: ")
    for center in centers:
        print(center)
    # $example off$

    sc.stop()
