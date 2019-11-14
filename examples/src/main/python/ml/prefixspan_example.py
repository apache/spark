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
An example demonstrating PrefixSpan.
Run with:
  bin/spark-submit examples/src/main/python/ml/prefixspan_example.py
"""
# $example on$
from pyspark.ml.fpm import PrefixSpan
# $example off$
from pyspark.sql import Row, SparkSession

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("PrefixSpanExample")\
        .getOrCreate()
    sc = spark.sparkContext

    # $example on$
    df = sc.parallelize([Row(sequence=[[1, 2], [3]]),
                         Row(sequence=[[1], [3, 2], [1, 2]]),
                         Row(sequence=[[1, 2], [5]]),
                         Row(sequence=[[6]])]).toDF()

    prefixSpan = PrefixSpan(minSupport=0.5, maxPatternLength=5,
                            maxLocalProjDBSize=32000000)

    # Find frequent sequential patterns.
    prefixSpan.findFrequentSequentialPatterns(df).show()
    # $example off$

    spark.stop()
