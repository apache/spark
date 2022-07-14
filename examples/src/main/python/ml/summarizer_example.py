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
An example for summarizer.
Run with:
  bin/spark-submit examples/src/main/python/ml/summarizer_example.py
"""
from pyspark.sql import SparkSession
# $example on$
from pyspark.ml.stat import Summarizer
from pyspark.sql import Row
from pyspark.ml.linalg import Vectors
# $example off$

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("SummarizerExample") \
        .getOrCreate()
    sc = spark.sparkContext

    # $example on$
    df = sc.parallelize([Row(weight=1.0, features=Vectors.dense(1.0, 1.0, 1.0)),
                         Row(weight=0.0, features=Vectors.dense(1.0, 2.0, 3.0))]).toDF()

    # create summarizer for multiple metrics "mean" and "count"
    summarizer = Summarizer.metrics("mean", "count")

    # compute statistics for multiple metrics with weight
    df.select(summarizer.summary(df.features, df.weight)).show(truncate=False)

    # compute statistics for multiple metrics without weight
    df.select(summarizer.summary(df.features)).show(truncate=False)

    # compute statistics for single metric "mean" with weight
    df.select(Summarizer.mean(df.features, df.weight)).show(truncate=False)

    # compute statistics for single metric "mean" without weight
    df.select(Summarizer.mean(df.features)).show(truncate=False)
    # $example off$

    spark.stop()
