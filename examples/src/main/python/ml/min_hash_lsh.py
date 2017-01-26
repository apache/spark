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
  bin/spark-submit examples/src/main/python/ml/min_hash_lsh.py
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
    dfA = spark.createDataFrame(dataA, ["id", "keys"])

    dataB = [(3, Vectors.sparse(6, [1, 3, 5], [1.0, 1.0, 1.0]),),
             (4, Vectors.sparse(6, [2, 3, 5], [1.0, 1.0, 1.0]),),
             (5, Vectors.sparse(6, [1, 2, 4], [1.0, 1.0, 1.0]),)]
    dfB = spark.createDataFrame(dataB, ["id", "keys"])

    key = Vectors.sparse(6, [1, 3], [1.0, 1.0])

    mh = MinHashLSH(inputCol="keys", outputCol="values", numHashTables=3)
    model = mh.fit(dfA)

    # Feature Transformation
    model.transform(dfA).show()

    # Cache the transformed columns
    transformedA = model.transform(dfA).cache()
    transformedB = model.transform(dfB).cache()

    # Approximate similarity join
    model.approxSimilarityJoin(dfA, dfB, 0.6).show()
    model.approxSimilarityJoin(transformedA, transformedB, 0.6).show()

    # Self Join
    model.approxSimilarityJoin(dfA, dfA, 0.6).filter("datasetA.id < datasetB.id").show()

    # Approximate nearest neighbor search
    model.approxNearestNeighbors(dfA, key, 2).show()
    model.approxNearestNeighbors(transformedA, key, 2).show()

    # $example off$

    spark.stop()
