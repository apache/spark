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

# $example on:init_session$
from pyspark.sql import SparkSession
# $example off:init_session$

# $example on:schema_inferring$
# spark is an existing SparkSession.
from pyspark.sql import Row
# $example off:schema_inferring$

# $example on:programmatic_schema$
# Import SparkSession and data types
from pyspark.sql.types import *
# $example off:programmatic_schema$

"""
A simple example demonstrating Spark SQL.
Run with:
  ./bin/spark-submit examples/src/main/python/SparkSQLExample.py
"""

def runBasicDataFrameExample(spark):
    # $example on:create_df$
    # spark is an existing SparkSession
    df = spark.read.json("examples/src/main/resources/people.json")
    # Displays the content of the DataFrame to stdout
    df.show()
    # +----+-------+
    # | age|   name|
    # +----+-------+
    # |null|Michael|
    # |  30|   Andy|
    # |  19| Justin|
    # +----+-------+

    # $example on:untyped_ops$
    # spark, df are from the previous example
    # Print the schema in a tree format
    df.printSchema()
    # root
    # |-- age: long (nullable = true)
    # |-- name: string (nullable = true)

    # Select only the "name" column
    df.select("name").show()
    # +-------+
    # |   name|
    # +-------+
    # |Michael|
    # |   Andy|
    # | Justin|
    # +-------+

    # Select everybody, but increment the age by 1
    df.select(df['name'], df['age'] + 1).show()
    # +-------+---------+
    # |   name|(age + 1)|
    # +-------+---------+
    # |Michael|     null|
    # |   Andy|       31|
    # | Justin|       20|
    # +-------+---------+

    # Select people older than 21
    df.filter(df['age'] > 21).show()
    # +---+----+
    # |age|name|
    # +---+----+
    # | 30|Andy|
    # +---+----+

    # Count people by age
    df.groupBy("age").count().show()
    # +----+-----+
    # | age|count|
    # +----+-----+
    # |  19|    1|
    # |null|    1|
    # |  30|    1|
    # +----+-----+
    # $example off:untyped_ops$

    # $example on:run_sql$
    # Register the DataFrame as a SQL temporary view
    df.createOrReplaceTempView("people")

    sqlDF = spark.sql("SELECT * FROM people")
    sqlDF.show()
    # +----+-------+
    # | age|   name|
    # +----+-------+
    # |null|Michael|
    # |  30|   Andy|
    # |  19| Justin|
    # +----+-------+
    # $example off:run_sql$

def runInferSchemaExample(spark):
    # $example on:schema_inferring$
    sc = spark.sparkContext

    # Load a text file and convert each line to a Row.
    lines = sc.textFile("examples/src/main/resources/people.txt")
    parts = lines.map(lambda l: l.split(","))
    people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))

    # Infer the schema, and register the DataFrame as a table.
    schemaPeople = spark.createDataFrame(people)
    schemaPeople.createOrReplaceTempView("people")

    # SQL can be run over DataFrames that have been registered as a table.
    teenagers = spark.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

    # The results of SQL queries are Dataframe objects.
    # rdd returns the content as an :class:`pyspark.RDD` of :class:`Row`.
    teenNames = teenagers.rdd.map(lambda p: "Name: " + p.name)
    for teenName in teenNames.collect():
        print(teenName)
    # Name: Justin
    # $example on:schema_inferring$

def runProgrammaticSchemaExample(spark):
    # $example on:programmatic_schema$
    sc = spark.sparkContext

    # Load a text file and convert each line to a Row.
    lines = sc.textFile("examples/src/main/resources/people.txt")
    parts = lines.map(lambda l: l.split(","))
    # Each line is converted to a tuple.
    people = parts.map(lambda p: (p[0], p[1].strip()))

    # The schema is encoded in a string.
    schemaString = "name age"

    fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
    schema = StructType(fields)

    # Apply the schema to the RDD.
    schemaPeople = spark.createDataFrame(people, schema)

    # Creates a temporary view using the DataFrame
    schemaPeople.createOrReplaceTempView("people")

    # Creates a temporary view using the DataFrame
    schemaPeople.createOrReplaceTempView("people")

    # SQL can be run over DataFrames that have been registered as a table.
    results = spark.sql("SELECT name FROM people")

    results.show()
    # +-------+
    # |   name|
    # +-------+
    # |Michael|
    # |   Andy|
    # | Justin|
    # +-------+
    # $example off:programmatic_schema$

if __name__ == "__main__":
    # $example on:init_session$
    spark = SparkSession\
      .builder\
      .appName("PythonSQL")\
      .config("spark.some.config.option", "some-value")\
      .getOrCreate()
    # $example off:init_session$

    runBasicDataFrameExample(spark)
    runInferSchemaExample(spark)
    runProgrammaticSchemaExample(spark)

    # $example on:schema_merge$
    # spark is from the previous example.
    # Create a simple DataFrame, stored into a partition directory
    df1 = spark.createDataFrame(sc.parallelize(range(1, 6))
                                .map(lambda i: Row(single=i, double=i * 2)))
    df1.write.parquet("data/test_table/key=1")

    # Create another DataFrame in a new partition directory,
    # adding a new column and dropping an existing column
    df2 = spark.createDataFrame(sc.parallelize(range(6, 11))
                                .map(lambda i: Row(single=i, triple=i * 3)))
    df2.write.parquet("data/test_table/key=2")

    # Read the partitioned table
    df3 = spark.read.option("mergeSchema", "true").parquet("data/test_table")
    df3.printSchema()

    # The final schema consists of all 3 columns in the Parquet files together
    # with the partitioning column appeared in the partition directory paths.
    # root
    # |-- single: int (nullable = true)
    # |-- double: int (nullable = true)
    # |-- triple: int (nullable = true)
    # |-- key : int (nullable = true)
    # $example off:schema_merge$

    spark.stop()
