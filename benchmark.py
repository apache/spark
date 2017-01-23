import pyspark
import timeit
import random
import sys
from pyspark.sql import SparkSession
import numpy as np
import pandas as pd

numPartition = 8

def scala_object(jpkg, obj):
    return jpkg.__getattr__(obj + "$").__getattr__("MODULE$")

def time(spark, df, repeat, number):
    print("collect as internal rows")
    time = timeit.repeat(lambda: df._jdf.queryExecution().executedPlan().executeCollect(), repeat=repeat, number=number)
    time_df = pd.Series(time)
    print(time_df.describe())

    print("internal rows to arrow record batch")
    arrow = scala_object(spark._jvm.org.apache.spark.sql, "Arrow")
    root_allocator = spark._jvm.org.apache.arrow.memory.RootAllocator(sys.maxsize)
    internal_rows = df._jdf.queryExecution().executedPlan().executeCollect()
    jschema = df._jdf.schema()
    def internalRowsToArrowRecordBatch():
        rb = arrow.internalRowsToArrowRecordBatch(internal_rows, jschema, root_allocator)
        rb.close()

    time = timeit.repeat(internalRowsToArrowRecordBatch, repeat=repeat, number=number)
    root_allocator.close()
    time_df = pd.Series(time)
    print(time_df.describe())

    print("toPandas with arrow")
    time = timeit.repeat(lambda: df.toPandas(True), repeat=repeat, number=number)
    time_df = pd.Series(time)
    print(time_df.describe())

    print("toPandas without arrow")
    time = timeit.repeat(lambda: df.toPandas(False), repeat=repeat, number=number)
    time_df = pd.Series(time)
    print(time_df.describe())

def long():
    return random.randint(0, 10000)

def double():
    return random.random()

def genDataLocal(spark, size, columns):
    data = [list([fn() for fn in columns]) for x in range(0, size)]
    df = spark.createDataFrame(data)
    return df

def genData(spark, size, columns):
    rdd = spark.sparkContext\
        .parallelize(range(0, size), numPartition)\
        .map(lambda _: [fn() for fn in columns])
    df = spark.createDataFrame(rdd)
    return df

if __name__ == "__main__":
    spark = SparkSession.builder.appName("ArrowBenchmark").getOrCreate()
    df = genData(spark, 1000 * 1000, [double])
    df.cache()
    df.count()
    df.collect()

    time(spark, df, 50, 1)
    df.unpersist()
