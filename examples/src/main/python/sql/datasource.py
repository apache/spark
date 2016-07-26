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

from pyspark.sql import SparkSession
# $example on:schema_merging$
from pyspark.sql import Row
# $example off:schema_merging$

"""
A simple example demonstrating Spark SQL data sources.
Run with:
  ./bin/spark-submit examples/src/main/python/sql/datasource.py
"""


def basic_datasource_example(spark):
    # $example on:generic_load_save_functions$
    df = spark.read.load("examples/src/main/resources/users.parquet")
    df.select("name", "favorite_color").write.save("namesAndFavColors.parquet")
    # $example off:generic_load_save_functions$

    # $example on:manual_load_options$
    df = spark.read.load("examples/src/main/resources/people.json", format="json")
    df.select("name", "age").write.save("namesAndAges.parquet", format="parquet")
    # $example off:manual_load_options$

    # $example on:direct_sql$
    df = spark.sql("SELECT * FROM parquet.`examples/src/main/resources/users.parquet`")
    # $example off:direct_sql$


def parquet_example(spark):
    # $example on:basic_parquet_example$
    peopleDF = spark.read.json("examples/src/main/resources/people.json")

    # DataFrames can be saved as Parquet files, maintaining the schema information.
    peopleDF.write.parquet("people.parquet")

    # Read in the Parquet file created above.
    # Parquet files are self-describing so the schema is preserved.
    # The result of loading a parquet file is also a DataFrame.
    parquetFile = spark.read.parquet("people.parquet")

    # Parquet files can also be used to create a temporary view and then used in SQL statements.
    parquetFile.createOrReplaceTempView("parquetFile")
    teenagers = spark.sql("SELECT name FROM parquetFile WHERE age >= 13 AND age <= 19")
    teenagers.show()
    # +------+
    # |  name|
    # +------+
    # |Justin|
    # +------+
    # $example off:basic_parquet_example$


def parquet_schema_merging_example(spark):
    # $example on:schema_merging$
    # spark is from the previous example.
    # Create a simple DataFrame, stored into a partition directory
    sc = spark.sparkContext

    squaresDF = spark.createDataFrame(sc.parallelize(range(1, 6))
                                      .map(lambda i: Row(single=i, double=i ** 2)))
    squaresDF.write.parquet("data/test_table/key=1")

    # Create another DataFrame in a new partition directory,
    # adding a new column and dropping an existing column
    cubesDF = spark.createDataFrame(sc.parallelize(range(6, 11))
                                    .map(lambda i: Row(single=i, triple=i ** 3)))
    cubesDF.write.parquet("data/test_table/key=2")

    # Read the partitioned table
    mergedDF = spark.read.option("mergeSchema", "true").parquet("data/test_table")
    mergedDF.printSchema()

    # The final schema consists of all 3 columns in the Parquet files together
    # with the partitioning column appeared in the partition directory paths.
    # root
    # |-- double: long (nullable = true)
    # |-- single: long (nullable = true)
    # |-- triple: long (nullable = true)
    # |-- key: integer (nullable = true)
    # $example off:schema_merging$


def json_dataset_examplg(spark):
    # $example on:json_dataset$
    # spark is from the previous example.
    sc = spark.sparkContext

    # A JSON dataset is pointed to by path.
    # The path can be either a single text file or a directory storing text files
    path = "examples/src/main/resources/people.json"
    peopleDF = spark.read.json(path)

    # The inferred schema can be visualized using the printSchema() method
    peopleDF.printSchema()
    # root
    # |-- age: long (nullable = true)
    # |-- name: string (nullable = true)

    # Creates a temporary view using the DataFrame
    peopleDF.createOrReplaceTempView("people")

    # SQL statements can be run by using the sql methods provided by spark
    teenagerNamesDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19")
    teenagerNamesDF.show()
    # +------+
    # |  name|
    # +------+
    # |Justin|
    # +------+

    # Alternatively, a DataFrame can be created for a JSON dataset represented by
    # an RDD[String] storing one JSON object per string
    jsonStrings = ['{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}']
    otherPeopleRDD = sc.parallelize(jsonStrings)
    otherPeople = spark.read.json(otherPeopleRDD)
    otherPeople.show()
    # +---------------+----+
    # |        address|name|
    # +---------------+----+
    # |[Columbus,Ohio]| Yin|
    # +---------------+----+
    # $example off:json_dataset$

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("PythonSQL") \
        .getOrCreate()

    basic_datasource_example(spark)
    parquet_example(spark)
    parquet_schema_merging_example(spark)
    json_dataset_examplg(spark)

    spark.stop()
