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


def dataframe_udf_word_count(tokenizeUDF, df):
    wc = df.select(tokenizeUDF(df['value']).alias("w")).select(explode("w").alias("words")) \
                                                       .groupBy("words").count()
    return wc.count()


def dataframe_scala_udf_word_count(df):
    wc = df.select(split(df['value'], " ").alias("w")).select(explode("w").alias("words")) \
                                                      .groupBy("words").count()
    return wc.count()


def benchmark(textInputPath, repeat, number):
    """
    Benchmark wordcount on a provided text input path with a given number
    of repeats each executed number times.

    :param textInputPath: The input path to perform wordcount on.
    :param repeat: Number of times to repeat the test
    :param number: Number of iterations to perform the wordcount per test
    """
    def benchmark_func(func):
        return timeit.repeat(func, repeat=repeat, number=number)

    print("Benchmarking wordcount:")
    tokenize = lambda x: x.split(" ")
    returnUDFType = ArrayType(StringType())
    tokenizeUDF = session.catalog.registerFunction("split", tokenize, returnUDFType)
    tokenizeJythonUDF = session.catalog.registerJythonFunction("split", tokenize, returnUDFType)
    rdd = sc.textFile(textInputPath)
    rdd.cache()
    rdd.count()
    print("RDD:")
    print(timeit.repeat(lambda: legacy_word_count(rdd), repeat=repeat, number=number))
    rdd.unpersist()
    df = session.read.text(textInputPath)
    df.cache()
    df.count()
    print("DataFrame Python UDF:")
    python_udf_test_lambda = lambda: dataframe_udf_word_count(tokenizeUDF, df)
    python_udf_times = timeit.repeat(python_udf_test_lambda, repeat=repeat, number=number)
    print(python_udf_times)
    print("DataFrame Jython UDF:")
    jython_udf_test_lambda = lambda: dataframe_udf_word_count(tokenizeJythonUDF, df)
    jython_udf_times = timeit.repeat(jython_udf_test_lambda, repeat=repeat, number=number)
    print(jython_udf_times)
    print("DataFrame Scala UDF:")
    scala_udf_test_lambda = lambda: dataframe_scala_udf_word_count(df)
    scala_udf_times = timeit.repeat(scala_udf_test_lambda, repeat=repeat, number=number)
    print(scala_udf_times)


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: sql_udf_perf <inputPath> <repeat> <number>", file=sys.stderr)
        sys.exit(-1)

    session = SparkSession\
        .builder\
        .appName("PythonSQL Per")\
        .getOrCreate()
    sc = session._sc
    spark = session
    textInputPath = sys.argv[1]
    repeat = int(sys.argv[2])
    number = int(sys.argv[3])
    benchmark(textInputPath, repeat, number)
