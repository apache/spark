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
from pyspark.ml.clustering import GaussianMixture
# $example off$
from pyspark.sql import SparkSession

"""
A simple example demonstrating Gaussian Mixture Model (GMM).
Run with:
  bin/spark-submit examples/src/main/python/ml/gaussian_mixture_example.py
"""

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("GaussianMixtureExample")\
        .getOrCreate()

    # $example on$
    # loads data
    dataset = spark.read.format("libsvm").load("data/mllib/sample_kmeans_data.txt")

    gmm = GaussianMixture().setK(2).setSeed(538009335L)
    model = gmm.fit(dataset)

    print("Gaussians shown as a DataFrame: ")
    model.gaussiansDF.show(truncate=False)
    # $example off$

    spark.stop()
