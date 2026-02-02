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
An example demonstrating PowerIterationClustering.
Run with:
  bin/spark-submit examples/src/main/python/ml/power_iteration_clustering_example.py
"""
# $example on$
from pyspark.ml.clustering import PowerIterationClustering
# $example off$
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("PowerIterationClusteringExample")\
        .getOrCreate()

    # $example on$
    df = spark.createDataFrame([
        (0, 1, 1.0),
        (0, 2, 1.0),
        (1, 2, 1.0),
        (3, 4, 1.0),
        (4, 0, 0.1)
    ], ["src", "dst", "weight"])

    pic = PowerIterationClustering(k=2, maxIter=20, initMode="degree", weightCol="weight")

    # Shows the cluster assignment
    pic.assignClusters(df).show()
    # $example off$

    spark.stop()
