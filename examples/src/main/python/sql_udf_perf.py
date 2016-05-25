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

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split
from pyspark.sql.types import *
import timeit


def legacy_word_count(rdd):
    wc = rdd.flatMap(lambda x: x.split(" ")).map(lambda w: (w, 1)).reduceByKey(lambda x, y: x + y)
    return wc.count()


def dataframe_udf_word_count(df):
    wc = df.select(tokenizeUDF(df['value']).alias("w")).select(explode("w").alias("words")) \
                                                       .groupBy("words").count()
    return wc.count()


def dataframe_jython_udf_word_count(df):
    wc = df.select(tokenizeJythonUDF(df['value']).alias("w")).select(explode("w").alias("words")) \
                                                             .groupBy("words").count()
    return wc.count()


def dataframe_scala_udf_word_count(df):
    wc = df.select(split(df['value'], " ").alias("w")).select(explode("w").alias("words")) \
                                                      .groupBy("words").count()
    return wc.count()


def benchmark(textInputPath, repeat, number):
    print("Benchmarking wordcount:")
    tokenize = lambda x: x.split(" ")
    returnUDFType = ArrayType(StringType())
    tokenizeUDF = session.catalog.registerFunction("split", tokenize, returnUDFType)
    tokenizeJythonUDF = session.catalog.registerJythonFunction("split", tokenize, returnUDFType)
    rdd = sc.textFile(args[0])
    rdd.cache()
    rdd.count()
    print("RDD:")
    print(timeit.repeat(lambda: legacy_word_count(df), repeat=10, number=500))
    df = session.read.text(args[0])
    df.cache()
    df.count()
    print("DataFrame Python UDF:")
    print(timeit.repeat(lambda: dataframe_udf_word_count(df), repeat=10, number=500))
    print("DataFrame Jython UDF:")
    print(timeit.repeat(lambda: dataframe_jython_udf_word_count(df), repeat=10, number=500))
    print("DataFrame Scala UDF:")
    print(timeit.repeat(lambda: dataframe_scala_udf_word_count(df), repeat=10, number=500))


if __name__ == "__main__":
    session = SparkSession\
        .builder\
        .appName("PythonSQL Per")\
        .getOrCreate()
    sc = session._sc
    spark = session
    textInputPath = args[0]
    repeat = args[1]
    number = args[3]
    benchmark(textInputPath, repeat, number)
