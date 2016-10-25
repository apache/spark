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

# $example on$
from pyspark.ml.clustering import BisectingKMeans
# $example off$
from pyspark.sql import SparkSession

"""
An example demonstrating bisecting k-means clustering.
Run with:
  bin/spark-submit examples/src/main/python/ml/bisecting_k_means_example.py
"""

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("BisectingKMeansExample")\
        .getOrCreate()

    # $example on$
    # Loads data.
    dataset = spark.read.format("libsvm").load("data/mllib/sample_kmeans_data.txt")

    # Trains a bisecting k-means model.
    bkm = BisectingKMeans().setK(2).setSeed(1)
    model = bkm.fit(dataset)

    # Evaluate clustering.
    cost = model.computeCost(dataset)
    print("Within Set Sum of Squared Errors = " + str(cost))

    # Shows the result.
    print("Cluster Centers: ")
    centers = model.clusterCenters()
    for center in centers:
        print(center)
    # $example off$

    spark.stop()
