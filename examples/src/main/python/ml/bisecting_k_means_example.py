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
from pyspark.mllib.linalg import Vectors
from pyspark.sql.types import Row
# $example off$
from pyspark.sql import SQLContext

"""
A simple example demonstrating a bisecting k-means clustering.
"""

if __name__ == "__main__":

    sc = SparkContext(appName="PythonBisectingKMeansExample")
    sqlContext = SQLContext(sc)

    # $example on$
    data = sc.textFile("data/mllib/kmeans_data.txt")
    parsed = data.map(lambda l: Row(features=Vectors.dense([float(x) for x in l.split(' ')])))
    training = sqlContext.createDataFrame(parsed)

    kmeans = BisectingKMeans().setK(2).setSeed(1).setFeaturesCol("features")

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
