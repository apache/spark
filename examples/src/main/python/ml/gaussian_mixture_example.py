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

import numpy as np
# $example on$
from pyspark import SparkContext
from pyspark.ml.clustering import GaussianMixture, GaussianMixtureModel
from pyspark.mllib.linalg import VectorUDT, _convert_to_vector
from pyspark.sql import SQLContext
from pyspark.sql.types import Row, StructField, StructType
# $example off$

"""
A simple example demonstrating a Gaussian Mixture Model (GMM).
Run with:
  bin/spark-submit examples/src/main/python/ml/gaussian_mixture_example.py
"""


def parseVector(line):
    line_strip = line.strip()
    array = np.array([float(x) for x in line_strip.split(' ')])
    return _convert_to_vector(array)


if __name__ == "__main__":

    FEATURES_COL = "features"

    path = "data/mllib/gmm_data.txt"
    k = 2

    sc = SparkContext(appName="PythonGuassianMixtureExample")
    sqlContext = SQLContext(sc)

    # $example on$
    lines = sc.textFile(path)
    data = lines.map(parseVector)
    row_rdd = data.map(lambda x: Row(x))
    schema = StructType([StructField("features", VectorUDT(), False)])
    df = sqlContext.createDataFrame(row_rdd, schema)

    gmm = GaussianMixture().setK(k).setSeed(10).setFeaturesCol("features")
    model = gmm.fit(df)

    print("Gaussians: ")
    model.gaussiansDF.show()

    transformed = model.transform(df).select("prediction")
    rows = transformed.collect()

    print("Prediction: ")
    for row in rows:
        print(row)
    # $example off$

    sc.stop()
