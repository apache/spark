/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.examples.sql

import java.util.Properties

import org.apache.spark.sql.SparkSession

object SQLDataSourceExample {

  case class Person(name: String, age: Long)

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL data sources example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    runBasicDataSourceExample(spark)
    runBasicParquetExample(spark)
    runParquetSchemaMergingExample(spark)
    runJsonDatasetExample(spark)
    runJdbcDatasetExample(spark)

    spark.stop()
  }

  private def runBasicDataSourceExample(spark: SparkSession): Unit = {
    // $example on:generic_load_save_functions$
    val usersDF = spark.read.load("examples/src/main/resources/users.parquet")
    usersDF.select("name", "favorite_color").write.save("namesAndFavColors.parquet")
    // $example off:generic_load_save_functions$
    // $example on:manual_load_options$
    val peopleDF = spark.read.format("json").load("examples/src/main/resources/people.json")
    peopleDF.select("name", "age").write.format("parquet").save("namesAndAges.parquet")
    // $example off:manual_load_options$
    // $example on:direct_sql$
    val sqlDF = spark.sql("SELECT * FROM parquet.`examples/src/main/resources/users.parquet`")
    // $example off:direct_sql$
    // $example on:write_sorting_and_bucketing$
    peopleDF.write.bucketBy(42, "name").sortBy("age").saveAsTable("people_bucketed")
    // $example off:write_sorting_and_bucketing$
    // $example on:write_partitioning$
    usersDF.write.partitionBy("favorite_color").format("parquet").save("namesPartByColor.parquet")
    // $example off:write_partitioning$
    // $example on:write_partition_and_bucket$
    peopleDF
      .write
      .partitionBy("favorite_color")
      .bucketBy(42, "name")
      .saveAsTable("people_partitioned_bucketed")
    // $example off:write_partition_and_bucket$

    spark.sql("DROP TABLE IF EXISTS people_bucketed")
    spark.sql("DROP TABLE IF EXISTS people_partitioned_bucketed")
  }

  private def runBasicParquetExample(spark: SparkSession): Unit = {
    // $example on:basic_parquet_example$
    // Encoders for most common types are automatically provided by importing spark.implicits._
    import spark.implicits._

    val peopleDF = spark.read.json("examples/src/main/resources/people.json")

    // DataFrames can be saved as Parquet files, maintaining the schema information
    peopleDF.write.parquet("people.parquet")

    // Read in the parquet file created above
    // Parquet files are self-describing so the schema is preserved
    // The result of loading a Parquet file is also a DataFrame
    val parquetFileDF = spark.read.parquet("people.parquet")

    // Parquet files can also be used to create a temporary view and then used in SQL statements
    parquetFileDF.createOrReplaceTempView("parquetFile")
    val namesDF = spark.sql("SELECT name FROM parquetFile WHERE age BETWEEN 13 AND 19")
    namesDF.map(attributes => "Name: " + attributes(0)).show()
    // +------------+
    // |       value|
    // +------------+
    // |Name: Justin|
    // +------------+
    // $example off:basic_parquet_example$
  }

  private def runParquetSchemaMergingExample(spark: SparkSession): Unit = {
    // $example on:schema_merging$
    // This is used to implicitly convert an RDD to a DataFrame.
    import spark.implicits._

    // Create a simple DataFrame, store into a partition directory
    val squaresDF = spark.sparkContext.makeRDD(1 to 5).map(i => (i, i * i)).toDF("value", "square")
    squaresDF.write.parquet("data/test_table/key=1")

    // Create another DataFrame in a new partition directory,
    // adding a new column and dropping an existing column
    val cubesDF = spark.sparkContext.makeRDD(6 to 10).map(i => (i, i * i * i)).toDF("value", "cube")
    cubesDF.write.parquet("data/test_table/key=2")

    // Read the partitioned table
    val mergedDF = spark.read.option("mergeSchema", "true").parquet("data/test_table")
    mergedDF.printSchema()

    // The final schema consists of all 3 columns in the Parquet files together
    // with the partitioning column appeared in the partition directory paths
    // root
    //  |-- value: int (nullable = true)
    //  |-- square: int (nullable = true)
    //  |-- cube: int (nullable = true)
    //  |-- key: int (nullable = true)
    // $example off:schema_merging$
  }

  private def runJsonDatasetExample(spark: SparkSession): Unit = {
    // $example on:json_dataset$
    // Primitive types (Int, String, etc) and Product types (case classes) encoders are
    // supported by importing this when creating a Dataset.
    import spark.implicits._

    // A JSON dataset is pointed to by path.
    // The path can be either a single text file or a directory storing text files
    val path = "examples/src/main/resources/people.json"
    val peopleDF = spark.read.json(path)

    // The inferred schema can be visualized using the printSchema() method
    peopleDF.printSchema()
    // root
    //  |-- age: long (nullable = true)
    //  |-- name: string (nullable = true)

    // Creates a temporary view using the DataFrame
    peopleDF.createOrReplaceTempView("people")

    // SQL statements can be run by using the sql methods provided by spark
    val teenagerNamesDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19")
    teenagerNamesDF.show()
    // +------+
    // |  name|
    // +------+
    // |Justin|
    // +------+

    // Alternatively, a DataFrame can be created for a JSON dataset represented by
    // a Dataset[String] storing one JSON object per string
    val otherPeopleDataset = spark.createDataset(
      """{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
    val otherPeople = spark.read.json(otherPeopleDataset)
    otherPeople.show()
    // +---------------+----+
    // |        address|name|
    // +---------------+----+
    // |[Columbus,Ohio]| Yin|
    // +---------------+----+
    // $example off:json_dataset$
  }

  private def runJdbcDatasetExample(spark: SparkSession): Unit = {
    // $example on:jdbc_dataset$
    // Note: JDBC loading and saving can be achieved via either the load/save or jdbc methods
    // Loading data from a JDBC source
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql:dbserver")
      .option("dbtable", "schema.tablename")
      .option("user", "username")
      .option("password", "password")
      .load()

    val connectionProperties = new Properties()
    connectionProperties.put("user", "username")
    connectionProperties.put("password", "password")
    val jdbcDF2 = spark.read
      .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)

    // Saving data to a JDBC source
    jdbcDF.write
      .format("jdbc")
      .option("url", "jdbc:postgresql:dbserver")
      .option("dbtable", "schema.tablename")
      .option("user", "username")
      .option("password", "password")
      .save()

    jdbcDF2.write
      .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)

    // Specifying create table column data types on write
    jdbcDF.write
      .option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)")
      .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)
    // $example off:jdbc_dataset$
  }
}
