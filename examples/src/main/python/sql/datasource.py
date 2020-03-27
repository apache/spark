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
A simple example demonstrating Spark SQL data sources.
Run with:
  ./bin/spark-submit examples/src/main/python/sql/datasource.py
"""
from __future__ import print_function

from pyspark.sql import SparkSession
# $example on:schema_merging$
from pyspark.sql import Row
# $example off:schema_merging$


def generic_file_source_options_example(spark):
    # $example on:ignore_corrupt_files$
    # enable ignore corrupt files
    spark.sql("set spark.sql.files.ignoreCorruptFiles=true")
    # dir1/file3.json is corrupt from parquet's view
    test_corrupt_df = spark.read.parquet("examples/src/main/resources/dir1/",
                                         "examples/src/main/resources/dir1/dir2/")
    test_corrupt_df.show()
    # +-------------+
    # |         file|
    # +-------------+
    # |file1.parquet|
    # |file2.parquet|
    # +-------------+
    # $example off:ignore_corrupt_files$

    # $example on:recursive_file_lookup$
    recursive_loaded_df = spark.read.format("parquet")\
        .option("recursiveFileLookup", "true")\
        .load("examples/src/main/resources/dir1")
    recursive_loaded_df.show()
    # +-------------+
    # |         file|
    # +-------------+
    # |file1.parquet|
    # |file2.parquet|
    # +-------------+
    # $example off:recursive_file_lookup$
    spark.sql("set spark.sql.files.ignoreCorruptFiles=false")

    # $example on:load_with_path_glob_filter$
    df = spark.read.load("examples/src/main/resources/dir1",
                         format="parquet", pathGlobFilter="*.parquet")
    df.show()
    # +-------------+
    # |         file|
    # +-------------+
    # |file1.parquet|
    # +-------------+
    # $example off:load_with_path_glob_filter$


def basic_datasource_example(spark):
    # $example on:generic_load_save_functions$
    df = spark.read.load("examples/src/main/resources/users.parquet")
    df.select("name", "favorite_color").write.save("namesAndFavColors.parquet")
    # $example off:generic_load_save_functions$

    # $example on:write_partitioning$
    df.write.partitionBy("favorite_color").format("parquet").save("namesPartByColor.parquet")
    # $example off:write_partitioning$

    # $example on:write_partition_and_bucket$
    df = spark.read.parquet("examples/src/main/resources/users.parquet")
    (df
        .write
        .partitionBy("favorite_color")
        .bucketBy(42, "name")
        .saveAsTable("people_partitioned_bucketed"))
    # $example off:write_partition_and_bucket$

    # $example on:manual_load_options$
    df = spark.read.load("examples/src/main/resources/people.json", format="json")
    df.select("name", "age").write.save("namesAndAges.parquet", format="parquet")
    # $example off:manual_load_options$

    # $example on:manual_load_options_csv$
    df = spark.read.load("examples/src/main/resources/people.csv",
                         format="csv", sep=":", inferSchema="true", header="true")
    # $example off:manual_load_options_csv$

    # $example on:manual_save_options_orc$
    df = spark.read.orc("examples/src/main/resources/users.orc")
    (df.write.format("orc")
        .option("orc.bloom.filter.columns", "favorite_color")
        .option("orc.dictionary.key.threshold", "1.0")
        .option("orc.column.encoding.direct", "name")
        .save("users_with_options.orc"))
    # $example off:manual_save_options_orc$

    # $example on:write_sorting_and_bucketing$
    df.write.bucketBy(42, "name").sortBy("age").saveAsTable("people_bucketed")
    # $example off:write_sorting_and_bucketing$

    # $example on:direct_sql$
    df = spark.sql("SELECT * FROM parquet.`examples/src/main/resources/users.parquet`")
    # $example off:direct_sql$

    spark.sql("DROP TABLE IF EXISTS people_bucketed")
    spark.sql("DROP TABLE IF EXISTS people_partitioned_bucketed")


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
    #  |-- double: long (nullable = true)
    #  |-- single: long (nullable = true)
    #  |-- triple: long (nullable = true)
    #  |-- key: integer (nullable = true)
    # $example off:schema_merging$


def json_dataset_example(spark):
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
    #  |-- age: long (nullable = true)
    #  |-- name: string (nullable = true)

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


def jdbc_dataset_example(spark):
    # $example on:jdbc_dataset$
    # Note: JDBC loading and saving can be achieved via either the load/save or jdbc methods
    # Loading data from a JDBC source
    jdbcDF = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql:dbserver") \
        .option("dbtable", "schema.tablename") \
        .option("user", "username") \
        .option("password", "password") \
        .load()

    jdbcDF2 = spark.read \
        .jdbc("jdbc:postgresql:dbserver", "schema.tablename",
              properties={"user": "username", "password": "password"})

    # Specifying dataframe column data types on read
    jdbcDF3 = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql:dbserver") \
        .option("dbtable", "schema.tablename") \
        .option("user", "username") \
        .option("password", "password") \
        .option("customSchema", "id DECIMAL(38, 0), name STRING") \
        .load()

    # Saving data to a JDBC source
    jdbcDF.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql:dbserver") \
        .option("dbtable", "schema.tablename") \
        .option("user", "username") \
        .option("password", "password") \
        .save()

    jdbcDF2.write \
        .jdbc("jdbc:postgresql:dbserver", "schema.tablename",
              properties={"user": "username", "password": "password"})

    # Specifying create table column data types on write
    jdbcDF.write \
        .option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)") \
        .jdbc("jdbc:postgresql:dbserver", "schema.tablename",
              properties={"user": "username", "password": "password"})
    # $example off:jdbc_dataset$


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL data source example") \
        .getOrCreate()

    basic_datasource_example(spark)
    generic_file_source_options_example(spark)
    parquet_example(spark)
    parquet_schema_merging_example(spark)
    json_dataset_example(spark)
    jdbc_dataset_example(spark)

    spark.stop()
