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
from pyspark.ml.feature import MinHashLSH
from pyspark.ml.linalg import Vectors
# $example off$
from pyspark.sql import SparkSession

"""
An example demonstrating MinHashLSH.
Run with:
  bin/spark-submit examples/src/main/python/ml/min_hash_lsh_example.py
"""

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("MinHashLSHExample") \
        .getOrCreate()

    # $example on$
    dataA = [(0, Vectors.sparse(6, [0, 1, 2], [1.0, 1.0, 1.0]),),
             (1, Vectors.sparse(6, [2, 3, 4], [1.0, 1.0, 1.0]),),
             (2, Vectors.sparse(6, [0, 2, 4], [1.0, 1.0, 1.0]),)]
    dfA = spark.createDataFrame(dataA, ["id", "features"])

    dataB = [(3, Vectors.sparse(6, [1, 3, 5], [1.0, 1.0, 1.0]),),
             (4, Vectors.sparse(6, [2, 3, 5], [1.0, 1.0, 1.0]),),
             (5, Vectors.sparse(6, [1, 2, 4], [1.0, 1.0, 1.0]),)]
    dfB = spark.createDataFrame(dataB, ["id", "features"])

    key = Vectors.sparse(6, [1, 3], [1.0, 1.0])

    mh = MinHashLSH(inputCol="features", outputCol="hashes", numHashTables=5)
    model = mh.fit(dfA)

    # Feature Transformation
    print("The hashed dataset where hashed values are stored in the column 'values':")
    model.transform(dfA).show()

    # Cache the transformed columns
    transformedA = model.transform(dfA).cache()
    transformedB = model.transform(dfB).cache()

    # Approximate similarity join
    print("Approximately joining dfA and dfB on distance smaller than 0.6:")
    model.approxSimilarityJoin(dfA, dfB, 0.6)\
        .select("datasetA.id", "datasetB.id", "distCol").show()
    print("Joining cached datasets to avoid recomputing the hash values:")
    model.approxSimilarityJoin(transformedA, transformedB, 0.6)\
        .select("datasetA.id", "datasetB.id", "distCol").show()

    # Self Join
    print("Approximately self join of dfB on distance smaller than 0.6:")
    model.approxSimilarityJoin(dfA, dfA, 0.6).filter("datasetA.id < datasetB.id")\
        .select("datasetA.id", "datasetB.id", "distCol").show()

    # Approximate nearest neighbor search
    print("Approximately searching dfA for 2 nearest neighbors of the key:")
    model.approxNearestNeighbors(dfA, key, 2).show()
    print("Note: It may return less than 2 rows because of lack of elements in the hash buckets.")
    print("Searching cached dataset to avoid recomputing the hash values:")
    model.approxNearestNeighbors(transformedA, key, 2).show()

    # $example off$

    spark.stop()
