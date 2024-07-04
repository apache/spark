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
package org.apache.spark.examples.sql;

// $example on:schema_merging$
import com.google.common.collect.Lists;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
// $example off:schema_merging$
import java.util.Properties;

// $example on:basic_parquet_example$
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
// $example on:schema_merging$
// $example on:json_dataset$
// $example on:csv_dataset$
// $example on:text_dataset$
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
// $example off:text_dataset$
// $example off:csv_dataset$
// $example off:json_dataset$
// $example off:schema_merging$
// $example off:basic_parquet_example$
import org.apache.spark.sql.SparkSession;

public class JavaSQLDataSourceExample {

  // $example on:schema_merging$
  public static class Square implements Serializable {
    private int value;
    private int square;

    // Getters and setters...
    // $example off:schema_merging$
    public int getValue() {
      return value;
    }

    public void setValue(int value) {
      this.value = value;
    }

    public int getSquare() {
      return square;
    }

    public void setSquare(int square) {
      this.square = square;
    }
    // $example on:schema_merging$
  }
  // $example off:schema_merging$

  // $example on:schema_merging$
  public static class Cube implements Serializable {
    private int value;
    private int cube;

    // Getters and setters...
    // $example off:schema_merging$
    public int getValue() {
      return value;
    }

    public void setValue(int value) {
      this.value = value;
    }

    public int getCube() {
      return cube;
    }

    public void setCube(int cube) {
      this.cube = cube;
    }
    // $example on:schema_merging$
  }
  // $example off:schema_merging$

  public static void main(String[] args) {
    SparkSession spark = SparkSession
      .builder()
      .appName("Java Spark SQL data sources example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate();

    runBasicDataSourceExample(spark);
    runGenericFileSourceOptionsExample(spark);
    runBasicParquetExample(spark);
    runParquetSchemaMergingExample(spark);
    runJsonDatasetExample(spark);
    runCsvDatasetExample(spark);
    runTextDatasetExample(spark);
    runJdbcDatasetExample(spark);
    runXmlDatasetExample(spark);

    spark.stop();
  }

  private static void runGenericFileSourceOptionsExample(SparkSession spark) {
    // $example on:ignore_corrupt_files$
    // enable ignore corrupt files via the data source option
    // dir1/file3.json is corrupt from parquet's view
    Dataset<Row> testCorruptDF0 = spark.read().option("ignoreCorruptFiles", "true").parquet(
        "examples/src/main/resources/dir1/",
        "examples/src/main/resources/dir1/dir2/");
    testCorruptDF0.show();
    // +-------------+
    // |         file|
    // +-------------+
    // |file1.parquet|
    // |file2.parquet|
    // +-------------+

    // enable ignore corrupt files via the configuration
    spark.sql("set spark.sql.files.ignoreCorruptFiles=true");
    // dir1/file3.json is corrupt from parquet's view
    Dataset<Row> testCorruptDF1 = spark.read().parquet(
            "examples/src/main/resources/dir1/",
            "examples/src/main/resources/dir1/dir2/");
    testCorruptDF1.show();
    // +-------------+
    // |         file|
    // +-------------+
    // |file1.parquet|
    // |file2.parquet|
    // +-------------+
    // $example off:ignore_corrupt_files$
    // $example on:recursive_file_lookup$
    Dataset<Row> recursiveLoadedDF = spark.read().format("parquet")
            .option("recursiveFileLookup", "true")
            .load("examples/src/main/resources/dir1");
    recursiveLoadedDF.show();
    // +-------------+
    // |         file|
    // +-------------+
    // |file1.parquet|
    // |file2.parquet|
    // +-------------+
    // $example off:recursive_file_lookup$
    spark.sql("set spark.sql.files.ignoreCorruptFiles=false");
    // $example on:load_with_path_glob_filter$
    Dataset<Row> testGlobFilterDF = spark.read().format("parquet")
            .option("pathGlobFilter", "*.parquet") // json file should be filtered out
            .load("examples/src/main/resources/dir1");
    testGlobFilterDF.show();
    // +-------------+
    // |         file|
    // +-------------+
    // |file1.parquet|
    // +-------------+
    // $example off:load_with_path_glob_filter$
    // $example on:load_with_modified_time_filter$
    Dataset<Row> beforeFilterDF = spark.read().format("parquet")
            // Only load files modified before 7/1/2020 at 05:30
            .option("modifiedBefore", "2020-07-01T05:30:00")
            // Only load files modified after 6/1/2020 at 05:30
            .option("modifiedAfter", "2020-06-01T05:30:00")
            // Interpret both times above relative to CST timezone
            .option("timeZone", "CST")
            .load("examples/src/main/resources/dir1");
    beforeFilterDF.show();
    // +-------------+
    // |         file|
    // +-------------+
    // |file1.parquet|
    // +-------------+
    // $example off:load_with_modified_time_filter$
  }

  private static void runBasicDataSourceExample(SparkSession spark) {
    // $example on:generic_load_save_functions$
    Dataset<Row> usersDF = spark.read().load("examples/src/main/resources/users.parquet");
    usersDF.select("name", "favorite_color").write().save("namesAndFavColors.parquet");
    // $example off:generic_load_save_functions$
    // $example on:manual_load_options$
    Dataset<Row> peopleDF =
      spark.read().format("json").load("examples/src/main/resources/people.json");
    peopleDF.select("name", "age").write().format("parquet").save("namesAndAges.parquet");
    // $example off:manual_load_options$
    // $example on:manual_load_options_csv$
    Dataset<Row> peopleDFCsv = spark.read().format("csv")
      .option("sep", ";")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("examples/src/main/resources/people.csv");
    // $example off:manual_load_options_csv$
    // $example on:manual_save_options_orc$
    usersDF.write().format("orc")
      .option("orc.bloom.filter.columns", "favorite_color")
      .option("orc.dictionary.key.threshold", "1.0")
      .option("orc.column.encoding.direct", "name")
      .save("users_with_options.orc");
    // $example off:manual_save_options_orc$
    // $example on:manual_save_options_parquet$
    usersDF.write().format("parquet")
        .option("parquet.bloom.filter.enabled#favorite_color", "true")
        .option("parquet.bloom.filter.expected.ndv#favorite_color", "1000000")
        .option("parquet.enable.dictionary", "true")
        .option("parquet.page.write-checksum.enabled", "false")
        .save("users_with_options.parquet");
    // $example off:manual_save_options_parquet$
    // $example on:direct_sql$
    Dataset<Row> sqlDF =
      spark.sql("SELECT * FROM parquet.`examples/src/main/resources/users.parquet`");
    // $example off:direct_sql$
    // $example on:write_sorting_and_bucketing$
    peopleDF.write().bucketBy(42, "name").sortBy("age").saveAsTable("people_bucketed");
    // $example off:write_sorting_and_bucketing$
    // $example on:write_partitioning$
    usersDF
      .write()
      .partitionBy("favorite_color")
      .format("parquet")
      .save("namesPartByColor.parquet");
    // $example off:write_partitioning$
    // $example on:write_partition_and_bucket$
    usersDF
      .write()
      .partitionBy("favorite_color")
      .bucketBy(42, "name")
      .saveAsTable("users_partitioned_bucketed");
    // $example off:write_partition_and_bucket$

    spark.sql("DROP TABLE IF EXISTS people_bucketed");
    spark.sql("DROP TABLE IF EXISTS users_partitioned_bucketed");
  }

  private static void runBasicParquetExample(SparkSession spark) {
    // $example on:basic_parquet_example$
    Dataset<Row> peopleDF = spark.read().json("examples/src/main/resources/people.json");

    // DataFrames can be saved as Parquet files, maintaining the schema information
    peopleDF.write().parquet("people.parquet");

    // Read in the Parquet file created above.
    // Parquet files are self-describing so the schema is preserved
    // The result of loading a parquet file is also a DataFrame
    Dataset<Row> parquetFileDF = spark.read().parquet("people.parquet");

    // Parquet files can also be used to create a temporary view and then used in SQL statements
    parquetFileDF.createOrReplaceTempView("parquetFile");
    Dataset<Row> namesDF = spark.sql("SELECT name FROM parquetFile WHERE age BETWEEN 13 AND 19");
    Dataset<String> namesDS = namesDF.map(
        (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
        Encoders.STRING());
    namesDS.show();
    // +------------+
    // |       value|
    // +------------+
    // |Name: Justin|
    // +------------+
    // $example off:basic_parquet_example$
  }

  private static void runParquetSchemaMergingExample(SparkSession spark) {
    // $example on:schema_merging$
    List<Square> squares = new ArrayList<>();
    for (int value = 1; value <= 5; value++) {
      Square square = new Square();
      square.setValue(value);
      square.setSquare(value * value);
      squares.add(square);
    }

    // Create a simple DataFrame, store into a partition directory
    Dataset<Row> squaresDF = spark.createDataFrame(squares, Square.class);
    squaresDF.write().parquet("data/test_table/key=1");

    List<Cube> cubes = new ArrayList<>();
    for (int value = 6; value <= 10; value++) {
      Cube cube = new Cube();
      cube.setValue(value);
      cube.setCube(value * value * value);
      cubes.add(cube);
    }

    // Create another DataFrame in a new partition directory,
    // adding a new column and dropping an existing column
    Dataset<Row> cubesDF = spark.createDataFrame(cubes, Cube.class);
    cubesDF.write().parquet("data/test_table/key=2");

    // Read the partitioned table
    Dataset<Row> mergedDF = spark.read().option("mergeSchema", true).parquet("data/test_table");
    mergedDF.printSchema();

    // The final schema consists of all 3 columns in the Parquet files together
    // with the partitioning column appeared in the partition directory paths
    // root
    //  |-- value: int (nullable = true)
    //  |-- square: int (nullable = true)
    //  |-- cube: int (nullable = true)
    //  |-- key: int (nullable = true)
    // $example off:schema_merging$
  }

  private static void runJsonDatasetExample(SparkSession spark) {
    // $example on:json_dataset$
    // A JSON dataset is pointed to by path.
    // The path can be either a single text file or a directory storing text files
    Dataset<Row> people = spark.read().json("examples/src/main/resources/people.json");

    // The inferred schema can be visualized using the printSchema() method
    people.printSchema();
    // root
    //  |-- age: long (nullable = true)
    //  |-- name: string (nullable = true)

    // Creates a temporary view using the DataFrame
    people.createOrReplaceTempView("people");

    // SQL statements can be run by using the sql methods provided by spark
    Dataset<Row> namesDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19");
    namesDF.show();
    // +------+
    // |  name|
    // +------+
    // |Justin|
    // +------+

    // Alternatively, a DataFrame can be created for a JSON dataset represented by
    // a Dataset<String> storing one JSON object per string.
    List<String> jsonData = Arrays.asList(
            "{\"name\":\"Yin\",\"address\":{\"city\":\"Columbus\",\"state\":\"Ohio\"}}");
    Dataset<String> anotherPeopleDataset = spark.createDataset(jsonData, Encoders.STRING());
    Dataset<Row> anotherPeople = spark.read().json(anotherPeopleDataset);
    anotherPeople.show();
    // +---------------+----+
    // |        address|name|
    // +---------------+----+
    // |[Columbus,Ohio]| Yin|
    // +---------------+----+
    // $example off:json_dataset$
  }

  private static void runCsvDatasetExample(SparkSession spark) {
    // $example on:csv_dataset$
    // A CSV dataset is pointed to by path.
    // The path can be either a single CSV file or a directory of CSV files
    String path = "examples/src/main/resources/people.csv";

    Dataset<Row> df = spark.read().csv(path);
    df.show();
    // +------------------+
    // |               _c0|
    // +------------------+
    // |      name;age;job|
    // |Jorge;30;Developer|
    // |  Bob;32;Developer|
    // +------------------+

    // Read a csv with delimiter, the default delimiter is ","
    Dataset<Row> df2 = spark.read().option("delimiter", ";").csv(path);
    df2.show();
    // +-----+---+---------+
    // |  _c0|_c1|      _c2|
    // +-----+---+---------+
    // | name|age|      job|
    // |Jorge| 30|Developer|
    // |  Bob| 32|Developer|
    // +-----+---+---------+

    // Read a csv with delimiter and a header
    Dataset<Row> df3 = spark.read().option("delimiter", ";").option("header", "true").csv(path);
    df3.show();
    // +-----+---+---------+
    // | name|age|      job|
    // +-----+---+---------+
    // |Jorge| 30|Developer|
    // |  Bob| 32|Developer|
    // +-----+---+---------+

    // You can also use options() to use multiple options
    java.util.Map<String, String> optionsMap = new java.util.HashMap<String, String>();
    optionsMap.put("delimiter",";");
    optionsMap.put("header","true");
    Dataset<Row> df4 = spark.read().options(optionsMap).csv(path);

    // "output" is a folder which contains multiple csv files and a _SUCCESS file.
    df3.write().csv("output");

    // Read all files in a folder, please make sure only CSV files should present in the folder.
    String folderPath = "examples/src/main/resources";
    Dataset<Row> df5 = spark.read().csv(folderPath);
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

  private static void runTextDatasetExample(SparkSession spark) {
    // $example on:text_dataset$
    // A text dataset is pointed to by path.
    // The path can be either a single text file or a directory of text files
    String path = "examples/src/main/resources/people.txt";

    Dataset<Row> df1 = spark.read().text(path);
    df1.show();
    // +-----------+
    // |      value|
    // +-----------+
    // |Michael, 29|
    // |   Andy, 30|
    // | Justin, 19|
    // +-----------+

    // You can use 'lineSep' option to define the line separator.
    // The line separator handles all `\r`, `\r\n` and `\n` by default.
    Dataset<Row> df2 = spark.read().option("lineSep", ",").text(path);
    df2.show();
    // +-----------+
    // |      value|
    // +-----------+
    // |    Michael|
    // |   29\nAndy|
    // | 30\nJustin|
    // |       19\n|
    // +-----------+

    // You can also use 'wholetext' option to read each input file as a single row.
    Dataset<Row> df3 = spark.read().option("wholetext", "true").text(path);
    df3.show();
    //  +--------------------+
    //  |               value|
    //  +--------------------+
    //  |Michael, 29\nAndy...|
    //  +--------------------+

    // "output" is a folder which contains multiple text files and a _SUCCESS file.
    df1.write().text("output");

    // You can specify the compression format using the 'compression' option.
    df1.write().option("compression", "gzip").text("output_compressed");

    // $example off:text_dataset$
  }

  private static void runJdbcDatasetExample(SparkSession spark) {
    // $example on:jdbc_dataset$
    // Note: JDBC loading and saving can be achieved via either the load/save or jdbc methods
    // Loading data from a JDBC source
    Dataset<Row> jdbcDF = spark.read()
      .format("jdbc")
      .option("url", "jdbc:postgresql:dbserver")
      .option("dbtable", "schema.tablename")
      .option("user", "username")
      .option("password", "password")
      .load();

    Properties connectionProperties = new Properties();
    connectionProperties.put("user", "username");
    connectionProperties.put("password", "password");
    Dataset<Row> jdbcDF2 = spark.read()
      .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties);

    // Saving data to a JDBC source
    jdbcDF.write()
      .format("jdbc")
      .option("url", "jdbc:postgresql:dbserver")
      .option("dbtable", "schema.tablename")
      .option("user", "username")
      .option("password", "password")
      .save();

    jdbcDF2.write()
      .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties);

    // Specifying create table column data types on write
    jdbcDF.write()
      .option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)")
      .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties);
    // $example off:jdbc_dataset$
  }

  private static void runXmlDatasetExample(SparkSession spark) {
    // $example on:xml_dataset$
    // Primitive types (Int, String, etc) and Product types (case classes) encoders are
    // supported by importing this when creating a Dataset.

    // An XML dataset is pointed to by path.
    // The path can be either a single xml file or more xml files
    String path = "examples/src/main/resources/people.xml";
    Dataset<Row> peopleDF = spark.read().option("rowTag", "person").xml(path);

    // The inferred schema can be visualized using the printSchema() method
    peopleDF.printSchema();
    // root
    //  |-- age: long (nullable = true)
    //  |-- name: string (nullable = true)

    // Creates a temporary view using the DataFrame
    peopleDF.createOrReplaceTempView("people");

    // SQL statements can be run by using the sql methods provided by spark
    Dataset<Row> teenagerNamesDF = spark.sql(
            "SELECT name FROM people WHERE age BETWEEN 13 AND 19");
    teenagerNamesDF.show();
    // +------+
    // |  name|
    // +------+
    // |Justin|
    // +------+

    // Alternatively, a DataFrame can be created for an XML dataset represented by a Dataset[String]
    List<String> xmlData = Collections.singletonList(
            "<person>" +
            "<name>laglangyue</name><job>Developer</job><age>28</age>" +
            "</person>");
    Dataset<String> otherPeopleDataset = spark.createDataset(Lists.newArrayList(xmlData),
            Encoders.STRING());

    Dataset<Row> otherPeople = spark.read()
        .option("rowTag", "person")
        .xml(otherPeopleDataset);
    otherPeople.show();
    // +---+---------+----------+
    // |age|      job|      name|
    // +---+---------+----------+
    // | 28|Developer|laglangyue|
    // +---+---------+----------+
    // $example off:xml_dataset$

  }
}
