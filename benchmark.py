import pyspark
import timeit
import random
from pyspark.sql import SparkSession

numPartition = 8

def time(df, repeat, number):
    print("toPandas with arrow")
    print(timeit.repeat(lambda: df.toPandas(True), repeat=repeat, number=number))

    print("toPandas without arrow")
    print(timeit.repeat(lambda: df.toPandas(False), repeat=repeat, number=number))

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
    df = genData(spark, 1000 * 1000, [long, double])
    df.cache()
    df.count()

    time(df, 10, 1)

    df.unpersist()
