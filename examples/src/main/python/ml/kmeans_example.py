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

import sys
import re

import numpy as np
from pyspark import SparkContext
from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.mllib.linalg import VectorUDT, _convert_to_vector
from pyspark.sql import SQLContext
from pyspark.sql.types import Row, StructField, StructType

"""
A simple example demonstrating a k-means clustering.
Run with:
  bin/spark-submit examples/src/main/python/ml/kmeans_example.py <input> <k>

This example requires NumPy (http://www.numpy.org/).
"""


def parseVector(line):
    array = np.array([float(x) for x in line.split(' ')])
    return _convert_to_vector(array)


if __name__ == "__main__":

    FEATURES_COL = "features"

    if len(sys.argv) != 3:
        print("Usage: kmeans_example.py <file> <k>", file=sys.stderr)
        exit(-1)
    path = sys.argv[1]
    k = sys.argv[2]

    sc = SparkContext(appName="PythonKMeansExample")
    sqlContext = SQLContext(sc)

    lines = sc.textFile(path)
    data = lines.map(parseVector)
    row_rdd = data.map(lambda x: Row(x))
    schema = StructType([StructField(FEATURES_COL, VectorUDT(), False)])
    df = sqlContext.createDataFrame(row_rdd, schema)

    kmeans = KMeans().setK(2).setSeed(1).setFeaturesCol(FEATURES_COL)
    model = kmeans.fit(df)
    centers = model.clusterCenters()

    print("Cluster Centers: ")
    for center in centers:
        print(center)

    sc.stop()
