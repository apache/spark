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

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL data sources example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    runBasicDataSourceExample(spark)
    runGenericFileSourceOptionsExample(spark)
    runBasicParquetExample(spark)
    runParquetSchemaMergingExample(spark)
    runJsonDatasetExample(spark)
    runCsvDatasetExample(spark)
    runTextDatasetExample(spark)
    runJdbcDatasetExample(spark)

    spark.stop()
  }

  private def runGenericFileSourceOptionsExample(spark: SparkSession): Unit = {
    // $example on:ignore_corrupt_files$
    // enable ignore corrupt files via the data source option
    // dir1/file3.json is corrupt from parquet's view
    val testCorruptDF0 = spark.read.option("ignoreCorruptFiles", "true").parquet(
      "examples/src/main/resources/dir1/",
      "examples/src/main/resources/dir1/dir2/")
    testCorruptDF0.show()
    // +-------------+
    // |         file|
    // +-------------+
    // |file1.parquet|
    // |file2.parquet|
    // +-------------+

    // enable ignore corrupt files via the configuration
    spark.sql("set spark.sql.files.ignoreCorruptFiles=true")
    // dir1/file3.json is corrupt from parquet's view
    val testCorruptDF1 = spark.read.parquet(
      "examples/src/main/resources/dir1/",
      "examples/src/main/resources/dir1/dir2/")
    testCorruptDF1.show()
    // +-------------+
    // |         file|
    // +-------------+
    // |file1.parquet|
    // |file2.parquet|
    // +-------------+
    // $example off:ignore_corrupt_files$
    // $example on:recursive_file_lookup$
    val recursiveLoadedDF = spark.read.format("parquet")
      .option("recursiveFileLookup", "true")
      .load("examples/src/main/resources/dir1")
    recursiveLoadedDF.show()
    // +-------------+
    // |         file|
    // +-------------+
    // |file1.parquet|
    // |file2.parquet|
    // +-------------+
    // $example off:recursive_file_lookup$
    spark.sql("set spark.sql.files.ignoreCorruptFiles=false")
    // $example on:load_with_path_glob_filter$
    val testGlobFilterDF = spark.read.format("parquet")
      .option("pathGlobFilter", "*.parquet") // json file should be filtered out
      .load("examples/src/main/resources/dir1")
    testGlobFilterDF.show()
    // +-------------+
    // |         file|
    // +-------------+
    // |file1.parquet|
    // +-------------+
    // $example off:load_with_path_glob_filter$
    // $example on:load_with_modified_time_filter$
    val beforeFilterDF = spark.read.format("parquet")
      // Files modified before 07/01/2020 at 05:30 are allowed
      .option("modifiedBefore", "2020-07-01T05:30:00")
      .load("examples/src/main/resources/dir1");
    beforeFilterDF.show();
    // +-------------+
    // |         file|
    // +-------------+
    // |file1.parquet|
    // +-------------+
    val afterFilterDF = spark.read.format("parquet")
       // Files modified after 06/01/2020 at 05:30 are allowed
      .option("modifiedAfter", "2020-06-01T05:30:00")
      .load("examples/src/main/resources/dir1");
    afterFilterDF.show();
    // +-------------+
    // |         file|
    // +-------------+
    // +-------------+
    // $example off:load_with_modified_time_filter$
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
    // $example on:manual_load_options_csv$
    val peopleDFCsv = spark.read.format("csv")
      .option("sep", ";")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("examples/src/main/resources/people.csv")
    // $example off:manual_load_options_csv$
    // $example on:manual_save_options_orc$
    usersDF.write.format("orc")
      .option("orc.bloom.filter.columns", "favorite_color")
      .option("orc.dictionary.key.threshold", "1.0")
      .option("orc.column.encoding.direct", "name")
      .save("users_with_options.orc")
    // $example off:manual_save_options_orc$
    // $example on:manual_save_options_parquet$
    usersDF.write.format("parquet")
      .option("parquet.bloom.filter.enabled#favorite_color", "true")
      .option("parquet.bloom.filter.expected.ndv#favorite_color", "1000000")
      .option("parquet.enable.dictionary", "true")
      .option("parquet.page.write-checksum.enabled", "false")
      .save("users_with_options.parquet")
    // $example off:manual_save_options_parquet$

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
    usersDF
      .write
      .partitionBy("favorite_color")
      .bucketBy(42, "name")
      .saveAsTable("users_partitioned_bucketed")
    // $example off:write_partition_and_bucket$

    spark.sql("DROP TABLE IF EXISTS people_bucketed")
    spark.sql("DROP TABLE IF EXISTS users_partitioned_bucketed")
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

  private def runCsvDatasetExample(spark: SparkSession): Unit = {
    // $example on:csv_dataset$
    // A CSV dataset is pointed to by path.
    // The path can be either a single CSV file or a directory of CSV files
    val path = "examples/src/main/resources/people.csv"

    val df = spark.read.csv(path)
    df.show()
    // +------------------+
    // |               _c0|
    // +------------------+
    // |      name;age;job|
    // |Jorge;30;Developer|
    // |  Bob;32;Developer|
    // +------------------+

    // Read a csv with delimiter, the default delimiter is ","
    val df2 = spark.read.option("delimiter", ";").csv(path)
    df2.show()
    // +-----+---+---------+
    // |  _c0|_c1|      _c2|
    // +-----+---+---------+
    // | name|age|      job|
    // |Jorge| 30|Developer|
    // |  Bob| 32|Developer|
    // +-----+---+---------+

    // Read a csv with delimiter and a header
    val df3 = spark.read.option("delimiter", ";").option("header", "true").csv(path)
    df3.show()
    // +-----+---+---------+
    // | name|age|      job|
    // +-----+---+---------+
    // |Jorge| 30|Developer|
    // |  Bob| 32|Developer|
    // +-----+---+---------+

    // You can also use options() to use multiple options
    val df4 = spark.read.options(Map("delimiter"->";", "header"->"true")).csv(path)

    // "output" is a folder which contains multiple csv files and a _SUCCESS file.
    df3.write.csv("output")

    // Read all files in a folder, please make sure only CSV files should present in the folder.
    val folderPath = "examples/src/main/resources";
    val df5 = spark.read.csv(folderPath);
    df5.show();
    // Wrong schema because non-CSV files are read
    // +-----------+
    // |        _c0|
    // +-----------+
    // |238val_238|
    // |  86val_86|
    // |311val_311|
    // |  27val_27|
    // |165val_165|
    // +-----------+

    // $example off:csv_dataset$
  }

  private def runTextDatasetExample(spark: SparkSession): Unit = {
    // $example on:text_dataset$
    // A text dataset is pointed to by path.
    // The path can be either a single text file or a directory of text files
    val path = "examples/src/main/resources/people.txt"

    val df1 = spark.read.text(path)
    df1.show()
    // +-----------+
    // |      value|
    // +-----------+
    // |Michael, 29|
    // |   Andy, 30|
    // | Justin, 19|
    // +-----------+

    // You can use 'lineSep' option to define the line separator.
    // The line separator handles all `\r`, `\r\n` and `\n` by default.
    val df2 = spark.read.option("lineSep", ",").text(path)
    df2.show()
    // +-----------+
    // |      value|
    // +-----------+
    // |    Michael|
    // |   29\nAndy|
    // | 30\nJustin|
    // |       19\n|
    // +-----------+

    // You can also use 'wholetext' option to read each input file as a single row.
    val df3 = spark.read.option("wholetext", true).text(path)
    df3.show()
    //  +--------------------+
    //  |               value|
    //  +--------------------+
    //  |Michael, 29\nAndy...|
    //  +--------------------+

    // "output" is a folder which contains multiple text files and a _SUCCESS file.
    df1.write.text("output")

    // You can specify the compression format using the 'compression' option.
    df1.write.option("compression", "gzip").text("output_compressed")

    // $example off:text_dataset$
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
    // Specifying the custom data types of the read schema
    connectionProperties.put("customSchema", "id DECIMAL(38, 0), name STRING")
    val jdbcDF3 = spark.read
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
