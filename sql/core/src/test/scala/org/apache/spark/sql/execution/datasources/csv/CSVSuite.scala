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

package org.apache.spark.sql.execution.datasources.csv

import java.io.File
import java.nio.charset.UnsupportedCharsetException
import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.compress.GzipCodec

import org.apache.spark.SparkException
import org.apache.spark.sql.{DataFrame, QueryTest, Row, UDT}
import org.apache.spark.sql.test.{SharedSQLContext, SQLTestUtils}
import org.apache.spark.sql.types._

class CSVSuite extends QueryTest with SharedSQLContext with SQLTestUtils {
  import testImplicits._

  private val carsFile = "test-data/cars.csv"
  private val carsMalformedFile = "test-data/cars-malformed.csv"
  private val carsFile8859 = "test-data/cars_iso-8859-1.csv"
  private val carsTsvFile = "test-data/cars.tsv"
  private val carsAltFile = "test-data/cars-alternative.csv"
  private val carsUnbalancedQuotesFile = "test-data/cars-unbalanced-quotes.csv"
  private val carsNullFile = "test-data/cars-null.csv"
  private val carsBlankColName = "test-data/cars-blank-column-name.csv"
  private val emptyFile = "test-data/empty.csv"
  private val commentsFile = "test-data/comments.csv"
  private val disableCommentsFile = "test-data/disable_comments.csv"
  private val boolFile = "test-data/bool.csv"
  private val decimalFile = "test-data/decimal.csv"
  private val simpleSparseFile = "test-data/simple_sparse.csv"
  private val numbersFile = "test-data/numbers.csv"
  private val datesFile = "test-data/dates.csv"
  private val unescapedQuotesFile = "test-data/unescaped-quotes.csv"

  private def testFile(fileName: String): String = {
    Thread.currentThread().getContextClassLoader.getResource(fileName).toString
  }

  /** Verifies data and schema. */
  private def verifyCars(
      df: DataFrame,
      withHeader: Boolean,
      numCars: Int = 3,
      numFields: Int = 5,
      checkHeader: Boolean = true,
      checkValues: Boolean = true,
      checkTypes: Boolean = false): Unit = {

    val numColumns = numFields
    val numRows = if (withHeader) numCars else numCars + 1
    // schema
    assert(df.schema.fieldNames.length === numColumns)
    assert(df.count === numRows)

    if (checkHeader) {
      if (withHeader) {
        assert(df.schema.fieldNames === Array("year", "make", "model", "comment", "blank"))
      } else {
        assert(df.schema.fieldNames === Array("_c0", "_c1", "_c2", "_c3", "_c4"))
      }
    }

    if (checkValues) {
      val yearValues = List("2012", "1997", "2015")
      val actualYears = if (!withHeader) "year" :: yearValues else yearValues
      val years = if (withHeader) df.select("year").collect() else df.select("_c0").collect()

      years.zipWithIndex.foreach { case (year, index) =>
        if (checkTypes) {
          assert(year === Row(actualYears(index).toInt))
        } else {
          assert(year === Row(actualYears(index)))
        }
      }
    }
  }

  test("simple csv test") {
    val cars = spark
      .read
      .format("csv")
      .option("header", "false")
      .load(testFile(carsFile))

    verifyCars(cars, withHeader = false, checkTypes = false)
  }

  test("simple csv test with calling another function to load") {
    val cars = spark
      .read
      .option("header", "false")
      .csv(testFile(carsFile))

    verifyCars(cars, withHeader = false, checkTypes = false)
  }

  test("simple csv test with type inference") {
    val cars = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(testFile(carsFile))

    verifyCars(cars, withHeader = true, checkTypes = true)
  }

  test("test inferring booleans") {
    val result = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(testFile(boolFile))

    val expectedSchema = StructType(List(
      StructField("bool", BooleanType, nullable = true)))
    assert(result.schema === expectedSchema)
  }

  test("test inferring decimals") {
    val result = spark.read
      .format("csv")
      .option("comment", "~")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(testFile(decimalFile))
    val expectedSchema = StructType(List(
      StructField("decimal", DecimalType(20, 0), nullable = true),
      StructField("long", LongType, nullable = true),
      StructField("double", DoubleType, nullable = true)))
    assert(result.schema === expectedSchema)
  }

  test("test with alternative delimiter and quote") {
    val cars = spark.read
      .format("csv")
      .options(Map("quote" -> "\'", "delimiter" -> "|", "header" -> "true"))
      .load(testFile(carsAltFile))

    verifyCars(cars, withHeader = true)
  }

  test("parse unescaped quotes with maxCharsPerColumn") {
    val rows = spark.read
      .format("csv")
      .option("maxCharsPerColumn", "4")
      .load(testFile(unescapedQuotesFile))

    val expectedRows = Seq(Row("\"a\"b", "ccc", "ddd"), Row("ab", "cc\"c", "ddd\""))

    checkAnswer(rows, expectedRows)
  }

  test("bad encoding name") {
    val exception = intercept[UnsupportedCharsetException] {
      spark
        .read
        .format("csv")
        .option("charset", "1-9588-osi")
        .load(testFile(carsFile8859))
    }

    assert(exception.getMessage.contains("1-9588-osi"))
  }

  test("test different encoding") {
    // scalastyle:off
    spark.sql(
      s"""
         |CREATE TEMPORARY TABLE carsTable USING csv
         |OPTIONS (path "${testFile(carsFile8859)}", header "true",
         |charset "iso-8859-1", delimiter "þ")
      """.stripMargin.replaceAll("\n", " "))
    // scalastyle:on

    verifyCars(spark.table("carsTable"), withHeader = true)
  }

  test("test aliases sep and encoding for delimiter and charset") {
    // scalastyle:off
    val cars = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("encoding", "iso-8859-1")
      .option("sep", "þ")
      .load(testFile(carsFile8859))
    // scalastyle:on

    verifyCars(cars, withHeader = true)
  }

  test("DDL test with tab separated file") {
    spark.sql(
      s"""
         |CREATE TEMPORARY TABLE carsTable USING csv
         |OPTIONS (path "${testFile(carsTsvFile)}", header "true", delimiter "\t")
      """.stripMargin.replaceAll("\n", " "))

    verifyCars(spark.table("carsTable"), numFields = 6, withHeader = true, checkHeader = false)
  }

  test("DDL test parsing decimal type") {
    spark.sql(
      s"""
         |CREATE TEMPORARY TABLE carsTable
         |(yearMade double, makeName string, modelName string, priceTag decimal,
         | comments string, grp string)
         |USING csv
         |OPTIONS (path "${testFile(carsTsvFile)}", header "true", delimiter "\t")
      """.stripMargin.replaceAll("\n", " "))

    assert(
      spark.sql("SELECT makeName FROM carsTable where priceTag > 60000").collect().size === 1)
  }

  test("test for DROPMALFORMED parsing mode") {
    val cars = spark.read
      .format("csv")
      .options(Map("header" -> "true", "mode" -> "dropmalformed"))
      .load(testFile(carsFile))

    assert(cars.select("year").collect().size === 2)
  }

  test("test for blank column names on read and select columns") {
    val cars = spark.read
      .format("csv")
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .load(testFile(carsBlankColName))

    assert(cars.select("customer").collect().size == 2)
    assert(cars.select("_c0").collect().size == 2)
    assert(cars.select("_c1").collect().size == 2)
  }

  test("test for FAILFAST parsing mode") {
    val exception = intercept[SparkException]{
      spark.read
      .format("csv")
      .options(Map("header" -> "true", "mode" -> "failfast"))
      .load(testFile(carsFile)).collect()
    }

    assert(exception.getMessage.contains("Malformed line in FAILFAST mode: 2015,Chevy,Volt"))
  }

  test("test for tokens more than the fields in the schema") {
    val cars = spark
      .read
      .format("csv")
      .option("header", "false")
      .option("comment", "~")
      .load(testFile(carsMalformedFile))

    verifyCars(cars, withHeader = false, checkTypes = false)
  }

  test("test with null quote character") {
    val cars = spark.read
      .format("csv")
      .option("header", "true")
      .option("quote", "")
      .load(testFile(carsUnbalancedQuotesFile))

    verifyCars(cars, withHeader = true, checkValues = false)

  }

  test("test with empty file and known schema") {
    val result = spark.read
      .format("csv")
      .schema(StructType(List(StructField("column", StringType, false))))
      .load(testFile(emptyFile))

    assert(result.collect.size === 0)
    assert(result.schema.fieldNames.size === 1)
  }

  test("DDL test with empty file") {
    spark.sql(s"""
           |CREATE TEMPORARY TABLE carsTable
           |(yearMade double, makeName string, modelName string, comments string, grp string)
           |USING csv
           |OPTIONS (path "${testFile(emptyFile)}", header "false")
      """.stripMargin.replaceAll("\n", " "))

    assert(spark.sql("SELECT count(*) FROM carsTable").collect().head(0) === 0)
  }

  test("DDL test with schema") {
    spark.sql(s"""
           |CREATE TEMPORARY TABLE carsTable
           |(yearMade double, makeName string, modelName string, comments string, blank string)
           |USING csv
           |OPTIONS (path "${testFile(carsFile)}", header "true")
      """.stripMargin.replaceAll("\n", " "))

    val cars = spark.table("carsTable")
    verifyCars(cars, withHeader = true, checkHeader = false, checkValues = false)
    assert(
      cars.schema.fieldNames === Array("yearMade", "makeName", "modelName", "comments", "blank"))
  }

  test("save csv") {
    withTempDir { dir =>
      val csvDir = new File(dir, "csv").getCanonicalPath
      val cars = spark.read
        .format("csv")
        .option("header", "true")
        .load(testFile(carsFile))

      cars.coalesce(1).write
        .option("header", "true")
        .csv(csvDir)

      val carsCopy = spark.read
        .format("csv")
        .option("header", "true")
        .load(csvDir)

      verifyCars(carsCopy, withHeader = true)
    }
  }

  test("save csv with quote") {
    withTempDir { dir =>
      val csvDir = new File(dir, "csv").getCanonicalPath
      val cars = spark.read
        .format("csv")
        .option("header", "true")
        .load(testFile(carsFile))

      cars.coalesce(1).write
        .format("csv")
        .option("header", "true")
        .option("quote", "\"")
        .save(csvDir)

      val carsCopy = spark.read
        .format("csv")
        .option("header", "true")
        .option("quote", "\"")
        .load(csvDir)

      verifyCars(carsCopy, withHeader = true)
    }
  }

  test("save csv with quoteAll enabled") {
    withTempDir { dir =>
      val csvDir = new File(dir, "csv").getCanonicalPath

      val data = Seq(("test \"quote\"", 123, "it \"works\"!", "\"very\" well"))
      val df = spark.createDataFrame(data)

      // escapeQuotes should be true by default
      df.coalesce(1).write
        .format("csv")
        .option("quote", "\"")
        .option("escape", "\"")
        .option("quoteAll", "true")
        .save(csvDir)

      val results = spark.read
        .format("text")
        .load(csvDir)
        .collect()

      val expected = "\"test \"\"quote\"\"\",\"123\",\"it \"\"works\"\"!\",\"\"\"very\"\" well\""

      assert(results.toSeq.map(_.toSeq) === Seq(Seq(expected)))
    }
  }

  test("save csv with quote escaping enabled") {
    withTempDir { dir =>
      val csvDir = new File(dir, "csv").getCanonicalPath

      val data = Seq(("test \"quote\"", 123, "it \"works\"!", "\"very\" well"))
      val df = spark.createDataFrame(data)

      // escapeQuotes should be true by default
      df.coalesce(1).write
        .format("csv")
        .option("quote", "\"")
        .option("escape", "\"")
        .save(csvDir)

      val results = spark.read
        .format("text")
        .load(csvDir)
        .collect()

      val expected = "\"test \"\"quote\"\"\",123,\"it \"\"works\"\"!\",\"\"\"very\"\" well\""

      assert(results.toSeq.map(_.toSeq) === Seq(Seq(expected)))
    }
  }

  test("save csv with quote escaping disabled") {
    withTempDir { dir =>
      val csvDir = new File(dir, "csv").getCanonicalPath

      val data = Seq(("test \"quote\"", 123, "it \"works\"!", "\"very\" well"))
      val df = spark.createDataFrame(data)

      // escapeQuotes should be true by default
      df.coalesce(1).write
        .format("csv")
        .option("quote", "\"")
        .option("escapeQuotes", "false")
        .option("escape", "\"")
        .save(csvDir)

      val results = spark.read
        .format("text")
        .load(csvDir)
        .collect()

      val expected = "test \"quote\",123,it \"works\"!,\"\"\"very\"\" well\""

      assert(results.toSeq.map(_.toSeq) === Seq(Seq(expected)))
    }
  }

  test("commented lines in CSV data") {
    val results = spark.read
      .format("csv")
      .options(Map("comment" -> "~", "header" -> "false"))
      .load(testFile(commentsFile))
      .collect()

    val expected =
      Seq(Seq("1", "2", "3", "4", "5.01", "2015-08-20 15:57:00"),
        Seq("6", "7", "8", "9", "0", "2015-08-21 16:58:01"),
        Seq("1", "2", "3", "4", "5", "2015-08-23 18:00:42"))

    assert(results.toSeq.map(_.toSeq) === expected)
  }

  test("inferring schema with commented lines in CSV data") {
    val results = spark.read
      .format("csv")
      .options(Map("comment" -> "~", "header" -> "false", "inferSchema" -> "true"))
      .load(testFile(commentsFile))
      .collect()

    val expected =
      Seq(Seq(1, 2, 3, 4, 5.01D, Timestamp.valueOf("2015-08-20 15:57:00")),
          Seq(6, 7, 8, 9, 0, Timestamp.valueOf("2015-08-21 16:58:01")),
          Seq(1, 2, 3, 4, 5, Timestamp.valueOf("2015-08-23 18:00:42")))

    assert(results.toSeq.map(_.toSeq) === expected)
  }

  test("inferring timestamp types via custom date format") {
    val options = Map(
      "header" -> "true",
      "inferSchema" -> "true",
      "timestampFormat" -> "dd/MM/yyyy HH:mm")
    val results = spark.read
      .format("csv")
      .options(options)
      .load(testFile(datesFile))
      .select("date")
      .collect()

    val dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm")
    val expected =
      Seq(Seq(new Timestamp(dateFormat.parse("26/08/2015 18:00").getTime)),
        Seq(new Timestamp(dateFormat.parse("27/10/2014 18:30").getTime)),
        Seq(new Timestamp(dateFormat.parse("28/01/2016 20:00").getTime)))
    assert(results.toSeq.map(_.toSeq) === expected)
  }

  test("load date types via custom date format") {
    val customSchema = new StructType(Array(StructField("date", DateType, true)))
    val options = Map(
      "header" -> "true",
      "inferSchema" -> "false",
      "dateFormat" -> "dd/MM/yyyy hh:mm")
    val results = spark.read
      .format("csv")
      .options(options)
      .schema(customSchema)
      .load(testFile(datesFile))
      .select("date")
      .collect()

    val dateFormat = new SimpleDateFormat("dd/MM/yyyy hh:mm")
    val expected = Seq(
      new Date(dateFormat.parse("26/08/2015 18:00").getTime),
      new Date(dateFormat.parse("27/10/2014 18:30").getTime),
      new Date(dateFormat.parse("28/01/2016 20:00").getTime))
    val dates = results.toSeq.map(_.toSeq.head)
    expected.zip(dates).foreach {
      case (expectedDate, date) =>
        // As it truncates the hours, minutes and etc., we only check
        // if the dates (days, months and years) are the same via `toString()`.
        assert(expectedDate.toString === date.toString)
    }
  }

  test("setting comment to null disables comment support") {
    val results = spark.read
      .format("csv")
      .options(Map("comment" -> "", "header" -> "false"))
      .load(testFile(disableCommentsFile))
      .collect()

    val expected =
      Seq(
        Seq("#1", "2", "3"),
        Seq("4", "5", "6"))

    assert(results.toSeq.map(_.toSeq) === expected)
  }

  test("nullable fields with user defined null value of \"null\"") {

    // year,make,model,comment,blank
    val dataSchema = StructType(List(
      StructField("year", IntegerType, nullable = true),
      StructField("make", StringType, nullable = false),
      StructField("model", StringType, nullable = false),
      StructField("comment", StringType, nullable = true),
      StructField("blank", StringType, nullable = true)))
    val cars = spark.read
      .format("csv")
      .schema(dataSchema)
      .options(Map("header" -> "true", "nullValue" -> "null"))
      .load(testFile(carsNullFile))

    verifyCars(cars, withHeader = true, checkValues = false)
    val results = cars.collect()
    assert(results(0).toSeq === Array(2012, "Tesla", "S", "null", "null"))
    assert(results(2).toSeq === Array(null, "Chevy", "Volt", null, null))
  }

  test("RDD[String] to DataFrame: nullable fields with user defined null value of \"null\"") {
    val testFilePath = testFile(carsNullFile)
    val options = Map("header" -> "true", "nullValue" -> "null")
    val csvRDD = CSVRelation.baseRdd(
      spark,
      new CSVOptions(options),
      Seq(testFilePath)
    )
    // year,make,model,comment,blank
    val dataSchema = StructType(List(
      StructField("year", IntegerType, nullable = true),
      StructField("make", StringType, nullable = false),
      StructField("model", StringType, nullable = false),
      StructField("comment", StringType, nullable = true),
      StructField("blank", StringType, nullable = true)))

    val cars = spark.read.schema(dataSchema).options(options).csv(csvRDD)
    verifyCars(cars, withHeader = true, checkValues = false)
    val results = cars.collect()
    cars.printSchema()
    assert(results(0).toSeq === Array(2012, "Tesla", "S", "null", "null"))
    assert(results(2).toSeq === Array(null, "Chevy", "Volt", null, null))
  }

  test("save csv with compression codec option") {
    withTempDir { dir =>
      val csvDir = new File(dir, "csv").getCanonicalPath
      val cars = spark.read
        .format("csv")
        .option("header", "true")
        .load(testFile(carsFile))

      cars.coalesce(1).write
        .format("csv")
        .option("header", "true")
        .option("compression", "gZiP")
        .save(csvDir)

      val compressedFiles = new File(csvDir).listFiles()
      assert(compressedFiles.exists(_.getName.endsWith(".csv.gz")))

      val carsCopy = spark.read
        .format("csv")
        .option("header", "true")
        .load(csvDir)

      verifyCars(carsCopy, withHeader = true)
    }
  }

  test("SPARK-13543 Write the output as uncompressed via option()") {
    val extraOptions = Map(
      "mapreduce.output.fileoutputformat.compress" -> "true",
      "mapreduce.output.fileoutputformat.compress.type" -> CompressionType.BLOCK.toString,
      "mapreduce.map.output.compress" -> "true",
      "mapreduce.map.output.compress.codec" -> classOf[GzipCodec].getName
    )
    withTempDir { dir =>
      val csvDir = new File(dir, "csv").getCanonicalPath
      val cars = spark.read
        .format("csv")
        .option("header", "true")
        .options(extraOptions)
        .load(testFile(carsFile))

      cars.coalesce(1).write
        .format("csv")
        .option("header", "true")
        .option("compression", "none")
        .options(extraOptions)
        .save(csvDir)

      val compressedFiles = new File(csvDir).listFiles()
      assert(compressedFiles.exists(!_.getName.endsWith(".csv.gz")))

      val carsCopy = spark.read
        .format("csv")
        .option("header", "true")
        .options(extraOptions)
        .load(csvDir)

      verifyCars(carsCopy, withHeader = true)
    }
  }

  test("Schema inference correctly identifies the datatype when data is sparse.") {
    val df = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(testFile(simpleSparseFile))

    assert(
      df.schema.fields.map(field => field.dataType).deep ==
      Array(IntegerType, IntegerType, IntegerType, IntegerType).deep)
  }

  test("DataSet[String] to DataFrame -- Schema inference correctly identifies the datatype") {
    val testFilePath = testFile(simpleSparseFile)
    val options = Map("header" -> "true", "inferSchema" -> "true")
    val csvRDD = CSVRelation.baseRdd(
      spark,
      new CSVOptions(options),
      Seq(testFilePath)
    )
    val df = spark.read.options(options).csv(csvRDD)
    assert(
      df.schema.fields.map(field => field.dataType).deep ==
      Array(IntegerType, IntegerType, IntegerType, IntegerType).deep)
  }

  test("old csv data source name works") {
    val cars = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .load(testFile(carsFile))

    verifyCars(cars, withHeader = false, checkTypes = false)
  }

  test("nulls, NaNs and Infinity values can be parsed") {
    val numbers = spark
      .read
      .format("csv")
      .schema(StructType(List(
        StructField("int", IntegerType, true),
        StructField("long", LongType, true),
        StructField("float", FloatType, true),
        StructField("double", DoubleType, true)
      )))
      .options(Map(
        "header" -> "true",
        "mode" -> "DROPMALFORMED",
        "nullValue" -> "--",
        "nanValue" -> "NAN",
        "negativeInf" -> "-INF",
        "positiveInf" -> "INF"))
      .load(testFile(numbersFile))

    assert(numbers.count() == 8)
  }

  test("Create DataFrame out of RDD[String]") {
    val testFilePath = testFile(numbersFile)
    val options = Map(
      "header" -> "true",
      "mode" -> "DROPMALFORMED",
      "nullValue" -> "--",
      "nanValue" -> "NAN",
      "negativeInf" -> "-INF",
      "positiveInf" -> "INF")
    val csvRDD = CSVRelation.baseRdd(
      spark,
      new CSVOptions(options),
      Seq(testFilePath)
    )
    val df = spark.read.options(options).csv(csvRDD)

    assert(df.count() == 8)
  }

  test("error handling for unsupported data types.") {
    withTempDir { dir =>
      val csvDir = new File(dir, "csv").getCanonicalPath
      var msg = intercept[UnsupportedOperationException] {
        Seq((1, "Tesla")).toDF("a", "b").selectExpr("struct(a, b)").write.csv(csvDir)
      }.getMessage
      assert(msg.contains("CSV data source does not support struct<a:int,b:string> data type"))

      msg = intercept[UnsupportedOperationException] {
        Seq((1, Map("Tesla" -> 3))).toDF("id", "cars").write.csv(csvDir)
      }.getMessage
      assert(msg.contains("CSV data source does not support map<string,int> data type"))

      msg = intercept[UnsupportedOperationException] {
        Seq((1, Array("Tesla", "Chevy", "Ford"))).toDF("id", "brands").write.csv(csvDir)
      }.getMessage
      assert(msg.contains("CSV data source does not support array<string> data type"))

      msg = intercept[UnsupportedOperationException] {
        Seq((1, new UDT.MyDenseVector(Array(0.25, 2.25, 4.25)))).toDF("id", "vectors")
          .write.csv(csvDir)
      }.getMessage
      assert(msg.contains("CSV data source does not support array<double> data type"))

      msg = intercept[SparkException] {
        val schema = StructType(StructField("a", new UDT.MyDenseVectorUDT(), true) :: Nil)
        spark.range(1).write.csv(csvDir)
        spark.read.schema(schema).csv(csvDir).collect()
      }.getCause.getMessage
      assert(msg.contains("Unsupported type: array"))
    }
  }

  test("SPARK-15585 turn off quotations") {
    val cars = spark.read
      .format("csv")
      .option("header", "true")
      .option("quote", "")
      .load(testFile(carsUnbalancedQuotesFile))

    verifyCars(cars, withHeader = true, checkValues = false)
  }

  test("Write timestamps correctly in ISO8601 format by default") {
    withTempDir { dir =>
      val iso8601timestampsPath = s"${dir.getCanonicalPath}/iso8601timestamps.csv"
      val timestamps = spark.read
        .format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .option("timestampFormat", "dd/MM/yyyy HH:mm")
        .load(testFile(datesFile))
      timestamps.write
        .format("csv")
        .option("header", "true")
        .save(iso8601timestampsPath)

      // This will load back the timestamps as string.
      val iso8601Timestamps = spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .load(iso8601timestampsPath)

      val iso8501 = FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ss.SSSZZ")
      val expectedTimestamps = timestamps.collect().map { r =>
        // This should be ISO8601 formatted string.
        Row(iso8501.format(r.toSeq.head))
      }

      checkAnswer(iso8601Timestamps, expectedTimestamps)
    }
  }

  test("Write dates correctly in ISO8601 format by default") {
    withTempDir { dir =>
      val customSchema = new StructType(Array(StructField("date", DateType, true)))
      val iso8601datesPath = s"${dir.getCanonicalPath}/iso8601dates.csv"
      val dates = spark.read
        .format("csv")
        .schema(customSchema)
        .option("header", "true")
        .option("inferSchema", "false")
        .option("dateFormat", "dd/MM/yyyy HH:mm")
        .load(testFile(datesFile))
      dates.write
        .format("csv")
        .option("header", "true")
        .save(iso8601datesPath)

      // This will load back the dates as string.
      val iso8601dates = spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .load(iso8601datesPath)

      val iso8501 = FastDateFormat.getInstance("yyyy-MM-dd")
      val expectedDates = dates.collect().map { r =>
        // This should be ISO8601 formatted string.
        Row(iso8501.format(r.toSeq.head))
      }

      checkAnswer(iso8601dates, expectedDates)
    }
  }

  test("Roundtrip in reading and writing timestamps") {
    withTempDir { dir =>
      val iso8601timestampsPath = s"${dir.getCanonicalPath}/iso8601timestamps.csv"
      val timestamps = spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(testFile(datesFile))

      timestamps.write
        .format("csv")
        .option("header", "true")
        .save(iso8601timestampsPath)

      val iso8601timestamps = spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(iso8601timestampsPath)

      checkAnswer(iso8601timestamps, timestamps)
    }
  }

  test("Write dates correctly with dateFormat option") {
    val customSchema = new StructType(Array(StructField("date", DateType, true)))
    withTempDir { dir =>
      // With dateFormat option.
      val datesWithFormatPath = s"${dir.getCanonicalPath}/datesWithFormat.csv"
      val datesWithFormat = spark.read
        .format("csv")
        .schema(customSchema)
        .option("header", "true")
        .option("dateFormat", "dd/MM/yyyy HH:mm")
        .load(testFile(datesFile))
      datesWithFormat.write
        .format("csv")
        .option("header", "true")
        .option("dateFormat", "yyyy/MM/dd")
        .save(datesWithFormatPath)

      // This will load back the dates as string.
      val stringDatesWithFormat = spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .load(datesWithFormatPath)
      val expectedStringDatesWithFormat = Seq(
        Row("2015/08/26"),
        Row("2014/10/27"),
        Row("2016/01/28"))

      checkAnswer(stringDatesWithFormat, expectedStringDatesWithFormat)
    }
  }

  test("Write timestamps correctly with dateFormat option") {
    withTempDir { dir =>
      // With dateFormat option.
      val timestampsWithFormatPath = s"${dir.getCanonicalPath}/timestampsWithFormat.csv"
      val timestampsWithFormat = spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("timestampFormat", "dd/MM/yyyy HH:mm")
        .load(testFile(datesFile))
      timestampsWithFormat.write
        .format("csv")
        .option("header", "true")
        .option("timestampFormat", "yyyy/MM/dd HH:mm")
        .save(timestampsWithFormatPath)

      // This will load back the timestamps as string.
      val stringTimestampsWithFormat = spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .load(timestampsWithFormatPath)
      val expectedStringTimestampsWithFormat = Seq(
        Row("2015/08/26 18:00"),
        Row("2014/10/27 18:30"),
        Row("2016/01/28 20:00"))

      checkAnswer(stringTimestampsWithFormat, expectedStringTimestampsWithFormat)
    }
  }
}
