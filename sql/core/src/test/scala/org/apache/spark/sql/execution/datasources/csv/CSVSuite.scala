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
import java.nio.charset.{Charset, UnsupportedCharsetException}
import java.nio.file.Files
import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import java.util.Locale

import scala.collection.JavaConverters._
import scala.util.Properties

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.log4j.{AppenderSkeleton, LogManager}
import org.apache.log4j.spi.LoggingEvent

import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SharedSQLContext, SQLTestUtils}
import org.apache.spark.sql.types._

class CSVSuite extends QueryTest with SharedSQLContext with SQLTestUtils with TestCsvData {
  import testImplicits._

  private val carsFile = "test-data/cars.csv"
  private val carsMalformedFile = "test-data/cars-malformed.csv"
  private val carsFile8859 = "test-data/cars_iso-8859-1.csv"
  private val carsTsvFile = "test-data/cars.tsv"
  private val carsAltFile = "test-data/cars-alternative.csv"
  private val carsUnbalancedQuotesFile = "test-data/cars-unbalanced-quotes.csv"
  private val carsNullFile = "test-data/cars-null.csv"
  private val carsEmptyValueFile = "test-data/cars-empty-value.csv"
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
  private val valueMalformedFile = "test-data/value-malformed.csv"
  private val malformedRowFile = "test-data/malformedRow.csv"

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

  test("simple csv test with string dataset") {
    val csvDataset = spark.read.text(testFile(carsFile)).as[String]
    val cars = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvDataset)

    verifyCars(cars, withHeader = true, checkTypes = true)

    val carsWithoutHeader = spark.read
      .option("header", "false")
      .csv(csvDataset)

    verifyCars(carsWithoutHeader, withHeader = false, checkTypes = false)
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
    withView("carsTable") {
      // scalastyle:off
      spark.sql(
        s"""
          |CREATE TEMPORARY VIEW carsTable USING csv
          |OPTIONS (path "${testFile(carsFile8859)}", header "true",
          |charset "iso-8859-1", delimiter "þ")
         """.stripMargin.replaceAll("\n", " "))
      // scalastyle:on
      verifyCars(spark.table("carsTable"), withHeader = true)
    }
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
    withView("carsTable") {
      spark.sql(
        s"""
          |CREATE TEMPORARY VIEW carsTable USING csv
          |OPTIONS (path "${testFile(carsTsvFile)}", header "true", delimiter "\t")
         """.stripMargin.replaceAll("\n", " "))

      verifyCars(spark.table("carsTable"), numFields = 6, withHeader = true, checkHeader = false)
    }
  }

  test("DDL test parsing decimal type") {
    withView("carsTable") {
      spark.sql(
        s"""
          |CREATE TEMPORARY VIEW carsTable
          |(yearMade double, makeName string, modelName string, priceTag decimal,
          | comments string, grp string)
          |USING csv
          |OPTIONS (path "${testFile(carsTsvFile)}", header "true", delimiter "\t")
         """.stripMargin.replaceAll("\n", " "))

      assert(
        spark.sql("SELECT makeName FROM carsTable where priceTag > 60000").collect().size === 1)
    }
  }

  test("test for DROPMALFORMED parsing mode") {
    withSQLConf(SQLConf.CSV_PARSER_COLUMN_PRUNING.key -> "false") {
      Seq(false, true).foreach { multiLine =>
        val cars = spark.read
          .format("csv")
          .option("multiLine", multiLine)
          .options(Map("header" -> "true", "mode" -> "dropmalformed"))
          .load(testFile(carsFile))

        assert(cars.select("year").collect().size === 2)
      }
    }
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
    Seq(false, true).foreach { multiLine =>
      val exception = intercept[SparkException] {
        spark.read
          .format("csv")
          .option("multiLine", multiLine)
          .options(Map("header" -> "true", "mode" -> "failfast"))
          .load(testFile(carsFile)).collect()
      }

      assert(exception.getMessage.contains("Malformed CSV record"))
    }
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
    withView("carsTable") {
      spark.sql(
        s"""
          |CREATE TEMPORARY VIEW carsTable
          |(yearMade double, makeName string, modelName string, comments string, grp string)
          |USING csv
          |OPTIONS (path "${testFile(emptyFile)}", header "false")
         """.stripMargin.replaceAll("\n", " "))

      assert(spark.sql("SELECT count(*) FROM carsTable").collect().head(0) === 0)
    }
  }

  test("DDL test with schema") {
    withView("carsTable") {
      spark.sql(
        s"""
          |CREATE TEMPORARY VIEW carsTable
          |(yearMade double, makeName string, modelName string, comments string, blank string)
          |USING csv
          |OPTIONS (path "${testFile(carsFile)}", header "true")
         """.stripMargin.replaceAll("\n", " "))

      val cars = spark.table("carsTable")
      verifyCars(cars, withHeader = true, checkHeader = false, checkValues = false)
      assert(
        cars.schema.fieldNames === Array("yearMade", "makeName", "modelName", "comments", "blank"))
    }
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

  test("save csv with quote escaping, using charToEscapeQuoteEscaping option") {
    withTempPath { path =>

      // original text
      val df1 = Seq(
        """You are "beautiful"""",
        """Yes, \"in the inside"\"""
      ).toDF()

      // text written in CSV with following options:
      // quote character: "
      // escape character: \
      // character to escape quote escaping: #
      val df2 = Seq(
        """"You are \"beautiful\""""",
        """"Yes, #\\"in the inside\"#\""""
      ).toDF()

      df2.coalesce(1).write.text(path.getAbsolutePath)

      val df3 = spark.read
        .format("csv")
        .option("quote", "\"")
        .option("escape", "\\")
        .option("charToEscapeQuoteEscaping", "#")
        .load(path.getAbsolutePath)

      checkAnswer(df1, df3)
    }
  }

  test("SPARK-19018: Save csv with custom charset") {

    // scalastyle:off nonascii
    val content = "µß áâä ÁÂÄ"
    // scalastyle:on nonascii

    Seq("iso-8859-1", "utf-8", "utf-16", "utf-32", "windows-1250").foreach { encoding =>
      withTempPath { path =>
        val csvDir = new File(path, "csv")
        Seq(content).toDF().write
          .option("encoding", encoding)
          .csv(csvDir.getCanonicalPath)

        csvDir.listFiles().filter(_.getName.endsWith("csv")).foreach({ csvFile =>
          val readback = Files.readAllBytes(csvFile.toPath)
          val expected = (content + Properties.lineSeparator).getBytes(Charset.forName(encoding))
          assert(readback === expected)
        })
      }
    }
  }

  test("SPARK-19018: error handling for unsupported charsets") {
    val exception = intercept[SparkException] {
      withTempPath { path =>
        val csvDir = new File(path, "csv").getCanonicalPath
        Seq("a,A,c,A,b,B").toDF().write
          .option("encoding", "1-9588-osi")
          .csv(csvDir)
      }
    }

    assert(exception.getCause.getMessage.contains("1-9588-osi"))
  }

  test("commented lines in CSV data") {
    Seq("false", "true").foreach { multiLine =>

      val results = spark.read
        .format("csv")
        .options(Map("comment" -> "~", "header" -> "false", "multiLine" -> multiLine))
        .load(testFile(commentsFile))
        .collect()

      val expected =
        Seq(Seq("1", "2", "3", "4", "5.01", "2015-08-20 15:57:00"),
          Seq("6", "7", "8", "9", "0", "2015-08-21 16:58:01"),
          Seq("1", "2", "3", "4", "5", "2015-08-23 18:00:42"))

      assert(results.toSeq.map(_.toSeq) === expected)
    }
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

    val dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm", Locale.US)
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

    val dateFormat = new SimpleDateFormat("dd/MM/yyyy hh:mm", Locale.US)
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
    assert(results(0).toSeq === Array(2012, "Tesla", "S", null, null))
    assert(results(2).toSeq === Array(null, "Chevy", "Volt", null, null))
  }

  test("empty fields with user defined empty values") {

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
      .option("header", "true")
      .option("emptyValue", "empty")
      .load(testFile(carsEmptyValueFile))

    verifyCars(cars, withHeader = true, checkValues = false)
    val results = cars.collect()
    assert(results(0).toSeq === Array(2012, "Tesla", "S", "empty", "empty"))
    assert(results(1).toSeq ===
      Array(1997, "Ford", "E350", "Go get one now they are going fast", null))
    assert(results(2).toSeq === Array(2015, "Chevy", "Volt", null, "empty"))
  }

  test("save csv with empty fields with user defined empty values") {
    withTempDir { dir =>
      val csvDir = new File(dir, "csv").getCanonicalPath

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
        .option("header", "true")
        .option("nullValue", "NULL")
        .load(testFile(carsEmptyValueFile))

      cars.coalesce(1).write
        .format("csv")
        .option("header", "true")
        .option("emptyValue", "empty")
        .option("nullValue", null)
        .save(csvDir)

      val carsCopy = spark.read
        .format("csv")
        .schema(dataSchema)
        .option("header", "true")
        .load(csvDir)

      verifyCars(carsCopy, withHeader = true, checkValues = false)
      val results = carsCopy.collect()
      assert(results(0).toSeq === Array(2012, "Tesla", "S", "empty", "empty"))
      assert(results(1).toSeq ===
        Array(1997, "Ford", "E350", "Go get one now they are going fast", null))
      assert(results(2).toSeq === Array(2015, "Chevy", "Volt", null, "empty"))
    }
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
      val stringSchema = StructType(StructField("date", StringType, true) :: Nil)
      val iso8601Timestamps = spark.read
        .format("csv")
        .schema(stringSchema)
        .option("header", "true")
        .load(iso8601timestampsPath)

      val iso8501 = FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ss.SSSXXX", Locale.US)
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
      val stringSchema = StructType(StructField("date", StringType, true) :: Nil)
      val iso8601dates = spark.read
        .format("csv")
        .schema(stringSchema)
        .option("header", "true")
        .load(iso8601datesPath)

      val iso8501 = FastDateFormat.getInstance("yyyy-MM-dd", Locale.US)
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
      val stringSchema = StructType(StructField("date", StringType, true) :: Nil)
      val stringDatesWithFormat = spark.read
        .format("csv")
        .schema(stringSchema)
        .option("header", "true")
        .load(datesWithFormatPath)
      val expectedStringDatesWithFormat = Seq(
        Row("2015/08/26"),
        Row("2014/10/27"),
        Row("2016/01/28"))

      checkAnswer(stringDatesWithFormat, expectedStringDatesWithFormat)
    }
  }

  test("Write timestamps correctly with timestampFormat option") {
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
      val stringSchema = StructType(StructField("date", StringType, true) :: Nil)
      val stringTimestampsWithFormat = spark.read
        .format("csv")
        .schema(stringSchema)
        .option("header", "true")
        .load(timestampsWithFormatPath)
      val expectedStringTimestampsWithFormat = Seq(
        Row("2015/08/26 18:00"),
        Row("2014/10/27 18:30"),
        Row("2016/01/28 20:00"))

      checkAnswer(stringTimestampsWithFormat, expectedStringTimestampsWithFormat)
    }
  }

  test("Write timestamps correctly with timestampFormat option and timeZone option") {
    withTempDir { dir =>
      // With dateFormat option and timeZone option.
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
        .option(DateTimeUtils.TIMEZONE_OPTION, "GMT")
        .save(timestampsWithFormatPath)

      // This will load back the timestamps as string.
      val stringSchema = StructType(StructField("date", StringType, true) :: Nil)
      val stringTimestampsWithFormat = spark.read
        .format("csv")
        .schema(stringSchema)
        .option("header", "true")
        .load(timestampsWithFormatPath)
      val expectedStringTimestampsWithFormat = Seq(
        Row("2015/08/27 01:00"),
        Row("2014/10/28 01:30"),
        Row("2016/01/29 04:00"))

      checkAnswer(stringTimestampsWithFormat, expectedStringTimestampsWithFormat)

      val readBack = spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("timestampFormat", "yyyy/MM/dd HH:mm")
        .option(DateTimeUtils.TIMEZONE_OPTION, "GMT")
        .load(timestampsWithFormatPath)

      checkAnswer(readBack, timestampsWithFormat)
    }
  }

  test("load duplicated field names consistently with null or empty strings - case sensitive") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      withTempPath { path =>
        Seq("a,a,c,A,b,B").toDF().write.text(path.getAbsolutePath)
        val actualSchema = spark.read
          .format("csv")
          .option("header", true)
          .load(path.getAbsolutePath)
          .schema
        val fields = Seq("a0", "a1", "c", "A", "b", "B").map(StructField(_, StringType, true))
        val expectedSchema = StructType(fields)
        assert(actualSchema == expectedSchema)
      }
    }
  }

  test("load duplicated field names consistently with null or empty strings - case insensitive") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      withTempPath { path =>
        Seq("a,A,c,A,b,B").toDF().write.text(path.getAbsolutePath)
        val actualSchema = spark.read
          .format("csv")
          .option("header", true)
          .load(path.getAbsolutePath)
          .schema
        val fields = Seq("a0", "A1", "c", "A3", "b4", "B5").map(StructField(_, StringType, true))
        val expectedSchema = StructType(fields)
        assert(actualSchema == expectedSchema)
      }
    }
  }

  test("load null when the schema is larger than parsed tokens ") {
    withTempPath { path =>
      Seq("1").toDF().write.text(path.getAbsolutePath)
      val schema = StructType(
        StructField("a", IntegerType, true) ::
        StructField("b", IntegerType, true) :: Nil)
      val df = spark.read
        .schema(schema)
        .option("header", "false")
        .csv(path.getAbsolutePath)

      checkAnswer(df, Row(1, null))
    }
  }

  test("SPARK-18699 put malformed records in a `columnNameOfCorruptRecord` field") {
    Seq(false, true).foreach { multiLine =>
      val schema = new StructType().add("a", IntegerType).add("b", TimestampType)
      // We use `PERMISSIVE` mode by default if invalid string is given.
      val df1 = spark
        .read
        .option("mode", "abcd")
        .option("multiLine", multiLine)
        .schema(schema)
        .csv(testFile(valueMalformedFile))
      checkAnswer(df1,
        Row(null, null) ::
        Row(1, java.sql.Date.valueOf("1983-08-04")) ::
        Nil)

      // If `schema` has `columnNameOfCorruptRecord`, it should handle corrupt records
      val columnNameOfCorruptRecord = "_unparsed"
      val schemaWithCorrField1 = schema.add(columnNameOfCorruptRecord, StringType)
      val df2 = spark
        .read
        .option("mode", "Permissive")
        .option("columnNameOfCorruptRecord", columnNameOfCorruptRecord)
        .option("multiLine", multiLine)
        .schema(schemaWithCorrField1)
        .csv(testFile(valueMalformedFile))
      checkAnswer(df2,
        Row(null, null, "0,2013-111-11 12:13:14") ::
        Row(1, java.sql.Date.valueOf("1983-08-04"), null) ::
        Nil)

      // We put a `columnNameOfCorruptRecord` field in the middle of a schema
      val schemaWithCorrField2 = new StructType()
        .add("a", IntegerType)
        .add(columnNameOfCorruptRecord, StringType)
        .add("b", TimestampType)
      val df3 = spark
        .read
        .option("mode", "permissive")
        .option("columnNameOfCorruptRecord", columnNameOfCorruptRecord)
        .option("multiLine", multiLine)
        .schema(schemaWithCorrField2)
        .csv(testFile(valueMalformedFile))
      checkAnswer(df3,
        Row(null, "0,2013-111-11 12:13:14", null) ::
        Row(1, null, java.sql.Date.valueOf("1983-08-04")) ::
        Nil)

      val errMsg = intercept[AnalysisException] {
        spark
          .read
          .option("mode", "PERMISSIVE")
          .option("columnNameOfCorruptRecord", columnNameOfCorruptRecord)
          .option("multiLine", multiLine)
          .schema(schema.add(columnNameOfCorruptRecord, IntegerType))
          .csv(testFile(valueMalformedFile))
          .collect
      }.getMessage
      assert(errMsg.startsWith("The field for corrupt records must be string type and nullable"))
    }
  }

  test("SPARK-19610: Parse normal multi-line CSV files") {
    val primitiveFieldAndType = Seq(
      """"
        |string","integer
        |
        |
        |","long
        |
        |","bigInteger",double,boolean,null""".stripMargin,
      """"this is a
        |simple
        |string.","
        |
        |10","
        |21474836470","92233720368547758070","
        |
        |1.7976931348623157E308",true,""".stripMargin)

    withTempPath { path =>
      primitiveFieldAndType.toDF("value").coalesce(1).write.text(path.getAbsolutePath)

      val df = spark.read
        .option("header", true)
        .option("multiLine", true)
        .csv(path.getAbsolutePath)

      // Check if headers have new lines in the names.
      val actualFields = df.schema.fieldNames.toSeq
      val expectedFields =
        Seq("\nstring", "integer\n\n\n", "long\n\n", "bigInteger", "double", "boolean", "null")
      assert(actualFields === expectedFields)

      // Check if the rows have new lines in the values.
      val expected = Row(
        "this is a\nsimple\nstring.",
        "\n\n10",
        "\n21474836470",
        "92233720368547758070",
        "\n\n1.7976931348623157E308",
        "true",
         null)
      checkAnswer(df, expected)
    }
  }

  test("Empty file produces empty dataframe with empty schema") {
    Seq(false, true).foreach { multiLine =>
      val df = spark.read.format("csv")
        .option("header", true)
        .option("multiLine", multiLine)
        .load(testFile(emptyFile))

      assert(df.schema === spark.emptyDataFrame.schema)
      checkAnswer(df, spark.emptyDataFrame)
    }
  }

  test("Empty string dataset produces empty dataframe and keep user-defined schema") {
    val df1 = spark.read.csv(spark.emptyDataset[String])
    assert(df1.schema === spark.emptyDataFrame.schema)
    checkAnswer(df1, spark.emptyDataFrame)

    val schema = StructType(StructField("a", StringType) :: Nil)
    val df2 = spark.read.schema(schema).csv(spark.emptyDataset[String])
    assert(df2.schema === schema)
  }

  test("ignoreLeadingWhiteSpace and ignoreTrailingWhiteSpace options - read") {
    val input = " a,b  , c "

    // For reading, default of both `ignoreLeadingWhiteSpace` and`ignoreTrailingWhiteSpace`
    // are `false`. So, these are excluded.
    val combinations = Seq(
      (true, true),
      (false, true),
      (true, false))

    // Check if read rows ignore whitespaces as configured.
    val expectedRows = Seq(
      Row("a", "b", "c"),
      Row(" a", "b", " c"),
      Row("a", "b  ", "c "))

    combinations.zip(expectedRows)
      .foreach { case ((ignoreLeadingWhiteSpace, ignoreTrailingWhiteSpace), expected) =>
        val df = spark.read
          .option("ignoreLeadingWhiteSpace", ignoreLeadingWhiteSpace)
          .option("ignoreTrailingWhiteSpace", ignoreTrailingWhiteSpace)
          .csv(Seq(input).toDS())

        checkAnswer(df, expected)
      }
  }

  test("SPARK-18579: ignoreLeadingWhiteSpace and ignoreTrailingWhiteSpace options - write") {
    val df = Seq((" a", "b  ", " c ")).toDF()

    // For writing, default of both `ignoreLeadingWhiteSpace` and `ignoreTrailingWhiteSpace`
    // are `true`. So, these are excluded.
    val combinations = Seq(
      (false, false),
      (false, true),
      (true, false))

    // Check if written lines ignore each whitespaces as configured.
    val expectedLines = Seq(
      " a,b  , c ",
      " a,b, c",
      "a,b  ,c ")

    combinations.zip(expectedLines)
      .foreach { case ((ignoreLeadingWhiteSpace, ignoreTrailingWhiteSpace), expected) =>
        withTempPath { path =>
          df.write
            .option("ignoreLeadingWhiteSpace", ignoreLeadingWhiteSpace)
            .option("ignoreTrailingWhiteSpace", ignoreTrailingWhiteSpace)
            .csv(path.getAbsolutePath)

          // Read back the written lines.
          val readBack = spark.read.text(path.getAbsolutePath)
          checkAnswer(readBack, Row(expected))
        }
      }
  }

  test("SPARK-21263: Invalid float and double are handled correctly in different modes") {
    val exception = intercept[SparkException] {
      spark.read.schema("a DOUBLE")
        .option("mode", "FAILFAST")
        .csv(Seq("10u12").toDS())
        .collect()
    }
    assert(exception.getMessage.contains("""input string: "10u12""""))

    val count = spark.read.schema("a FLOAT")
      .option("mode", "DROPMALFORMED")
      .csv(Seq("10u12").toDS())
      .count()
    assert(count == 0)

    val results = spark.read.schema("a FLOAT")
      .option("mode", "PERMISSIVE")
      .csv(Seq("10u12").toDS())
    checkAnswer(results, Row(null))
  }

  test("SPARK-20978: Fill the malformed column when the number of tokens is less than schema") {
    val df = spark.read
      .schema("a string, b string, unparsed string")
      .option("columnNameOfCorruptRecord", "unparsed")
      .csv(Seq("a").toDS())
    checkAnswer(df, Row("a", null, "a"))
  }

  test("SPARK-21610: Corrupt records are not handled properly when creating a dataframe " +
    "from a file") {
    val columnNameOfCorruptRecord = "_corrupt_record"
    val schema = new StructType()
      .add("a", IntegerType)
      .add("b", TimestampType)
      .add(columnNameOfCorruptRecord, StringType)
    // negative cases
    val msg = intercept[AnalysisException] {
      spark
        .read
        .option("columnNameOfCorruptRecord", columnNameOfCorruptRecord)
        .schema(schema)
        .csv(testFile(valueMalformedFile))
        .select(columnNameOfCorruptRecord)
        .collect()
    }.getMessage
    assert(msg.contains("only include the internal corrupt record column"))
    intercept[org.apache.spark.sql.catalyst.errors.TreeNodeException[_]] {
      spark
        .read
        .option("columnNameOfCorruptRecord", columnNameOfCorruptRecord)
        .schema(schema)
        .csv(testFile(valueMalformedFile))
        .filter($"_corrupt_record".isNotNull)
        .count()
    }
    // workaround
    val df = spark
      .read
      .option("columnNameOfCorruptRecord", columnNameOfCorruptRecord)
      .schema(schema)
      .csv(testFile(valueMalformedFile))
      .cache()
    assert(df.filter($"_corrupt_record".isNotNull).count() == 1)
    assert(df.filter($"_corrupt_record".isNull).count() == 1)
    checkAnswer(
      df.select(columnNameOfCorruptRecord),
      Row("0,2013-111-11 12:13:14") :: Row(null) :: Nil
    )
  }

  test("SPARK-23846: schema inferring touches less data if samplingRatio < 1.0") {
    // Set default values for the DataSource parameters to make sure
    // that whole test file is mapped to only one partition. This will guarantee
    // reliable sampling of the input file.
    withSQLConf(
      "spark.sql.files.maxPartitionBytes" -> (128 * 1024 * 1024).toString,
      "spark.sql.files.openCostInBytes" -> (4 * 1024 * 1024).toString
    )(withTempPath { path =>
      val ds = sampledTestData.coalesce(1)
      ds.write.text(path.getAbsolutePath)

      val readback = spark.read
        .option("inferSchema", true).option("samplingRatio", 0.1)
        .csv(path.getCanonicalPath)
      assert(readback.schema == new StructType().add("_c0", IntegerType))
    })
  }

  test("SPARK-23846: usage of samplingRatio while parsing a dataset of strings") {
    val ds = sampledTestData.coalesce(1)
    val readback = spark.read
      .option("inferSchema", true).option("samplingRatio", 0.1)
      .csv(ds)

    assert(readback.schema == new StructType().add("_c0", IntegerType))
  }

  test("SPARK-23846: samplingRatio is out of the range (0, 1.0]") {
    val ds = spark.range(0, 100, 1, 1).map(_.toString)

    val errorMsg0 = intercept[IllegalArgumentException] {
      spark.read.option("inferSchema", true).option("samplingRatio", -1).csv(ds)
    }.getMessage
    assert(errorMsg0.contains("samplingRatio (-1.0) should be greater than 0"))

    val errorMsg1 = intercept[IllegalArgumentException] {
      spark.read.option("inferSchema", true).option("samplingRatio", 0).csv(ds)
    }.getMessage
    assert(errorMsg1.contains("samplingRatio (0.0) should be greater than 0"))

    val sampled = spark.read.option("inferSchema", true).option("samplingRatio", 1.0).csv(ds)
    assert(sampled.count() == ds.count())
  }

  test("SPARK-17916: An empty string should not be coerced to null when nullValue is passed.") {
    val litNull: String = null
    val df = Seq(
      (1, "John Doe"),
      (2, ""),
      (3, "-"),
      (4, litNull)
    ).toDF("id", "name")

    // Checks for new behavior where an empty string is not coerced to null when `nullValue` is
    // set to anything but an empty string literal.
    withTempPath { path =>
      df.write
        .option("nullValue", "-")
        .csv(path.getAbsolutePath)
      val computed = spark.read
        .option("nullValue", "-")
        .schema(df.schema)
        .csv(path.getAbsolutePath)
      val expected = Seq(
        (1, "John Doe"),
        (2, ""),
        (3, litNull),
        (4, litNull)
      ).toDF("id", "name")

      checkAnswer(computed, expected)
    }
    // Keeps the old behavior where empty string us coerced to nullValue is not passed.
    withTempPath { path =>
      df.write
        .csv(path.getAbsolutePath)
      val computed = spark.read
        .schema(df.schema)
        .csv(path.getAbsolutePath)
      val expected = Seq(
        (1, "John Doe"),
        (2, litNull),
        (3, "-"),
        (4, litNull)
      ).toDF("id", "name")

      checkAnswer(computed, expected)
    }
  }

  test("SPARK-25241: An empty string should not be coerced to null when emptyValue is passed.") {
    val litNull: String = null
    val df = Seq(
      (1, "John Doe"),
      (2, ""),
      (3, "-"),
      (4, litNull)
    ).toDF("id", "name")

    // Checks for new behavior where a null is not coerced to an empty string when `emptyValue` is
    // set to anything but an empty string literal.
    withTempPath { path =>
      df.write
        .option("emptyValue", "-")
        .csv(path.getAbsolutePath)
      val computed = spark.read
        .option("emptyValue", "-")
        .schema(df.schema)
        .csv(path.getAbsolutePath)
      val expected = Seq(
        (1, "John Doe"),
        (2, "-"),
        (3, "-"),
        (4, "-")
      ).toDF("id", "name")

      checkAnswer(computed, expected)
    }
    // Keeps the old behavior where empty string us coerced to emptyValue is not passed.
    withTempPath { path =>
      df.write
        .csv(path.getAbsolutePath)
      val computed = spark.read
        .schema(df.schema)
        .csv(path.getAbsolutePath)
      val expected = Seq(
        (1, "John Doe"),
        (2, litNull),
        (3, "-"),
        (4, litNull)
      ).toDF("id", "name")

      checkAnswer(computed, expected)
    }
  }

  test("SPARK-24329: skip lines with comments, and one or multiple whitespaces") {
    val schema = new StructType().add("colA", StringType)
    val ds = spark
      .read
      .schema(schema)
      .option("multiLine", false)
      .option("header", true)
      .option("comment", "#")
      .option("ignoreLeadingWhiteSpace", false)
      .option("ignoreTrailingWhiteSpace", false)
      .csv(testFile("test-data/comments-whitespaces.csv"))

    checkAnswer(ds, Seq(Row(""" "a" """)))
  }

  test("SPARK-24244: Select a subset of all columns") {
    withTempPath { path =>
      import collection.JavaConverters._
      val schema = new StructType()
        .add("f1", IntegerType).add("f2", IntegerType).add("f3", IntegerType)
        .add("f4", IntegerType).add("f5", IntegerType).add("f6", IntegerType)
        .add("f7", IntegerType).add("f8", IntegerType).add("f9", IntegerType)
        .add("f10", IntegerType).add("f11", IntegerType).add("f12", IntegerType)
        .add("f13", IntegerType).add("f14", IntegerType).add("f15", IntegerType)

      val odf = spark.createDataFrame(List(
        Row(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15),
        Row(-1, -2, -3, -4, -5, -6, -7, -8, -9, -10, -11, -12, -13, -14, -15)
      ).asJava, schema)
      odf.write.csv(path.getCanonicalPath)
      val idf = spark.read
        .schema(schema)
        .csv(path.getCanonicalPath)
        .select('f15, 'f10, 'f5)

      assert(idf.count() == 2)
      checkAnswer(idf, List(Row(15, 10, 5), Row(-15, -10, -5)))
    }
  }

  def checkHeader(multiLine: Boolean): Unit = {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      withTempPath { path =>
        val oschema = new StructType().add("f1", DoubleType).add("f2", DoubleType)
        val odf = spark.createDataFrame(List(Row(1.0, 1234.5)).asJava, oschema)
        odf.write.option("header", true).csv(path.getCanonicalPath)
        val ischema = new StructType().add("f2", DoubleType).add("f1", DoubleType)
        val exception = intercept[SparkException] {
          spark.read
            .schema(ischema)
            .option("multiLine", multiLine)
            .option("header", true)
            .option("enforceSchema", false)
            .csv(path.getCanonicalPath)
            .collect()
        }
        assert(exception.getMessage.contains("CSV header does not conform to the schema"))

        val shortSchema = new StructType().add("f1", DoubleType)
        val exceptionForShortSchema = intercept[SparkException] {
          spark.read
            .schema(shortSchema)
            .option("multiLine", multiLine)
            .option("header", true)
            .option("enforceSchema", false)
            .csv(path.getCanonicalPath)
            .collect()
        }
        assert(exceptionForShortSchema.getMessage.contains(
          "Number of column in CSV header is not equal to number of fields in the schema"))

        val longSchema = new StructType()
          .add("f1", DoubleType)
          .add("f2", DoubleType)
          .add("f3", DoubleType)

        val exceptionForLongSchema = intercept[SparkException] {
          spark.read
            .schema(longSchema)
            .option("multiLine", multiLine)
            .option("header", true)
            .option("enforceSchema", false)
            .csv(path.getCanonicalPath)
            .collect()
        }
        assert(exceptionForLongSchema.getMessage.contains("Header length: 2, schema size: 3"))

        val caseSensitiveSchema = new StructType().add("F1", DoubleType).add("f2", DoubleType)
        val caseSensitiveException = intercept[SparkException] {
          spark.read
            .schema(caseSensitiveSchema)
            .option("multiLine", multiLine)
            .option("header", true)
            .option("enforceSchema", false)
            .csv(path.getCanonicalPath)
            .collect()
        }
        assert(caseSensitiveException.getMessage.contains(
          "CSV header does not conform to the schema"))
      }
    }
  }

  test(s"SPARK-23786: Checking column names against schema in the multiline mode") {
    checkHeader(multiLine = true)
  }

  test(s"SPARK-23786: Checking column names against schema in the per-line mode") {
    checkHeader(multiLine = false)
  }

  test("SPARK-23786: CSV header must not be checked if it doesn't exist") {
    withTempPath { path =>
      val oschema = new StructType().add("f1", DoubleType).add("f2", DoubleType)
      val odf = spark.createDataFrame(List(Row(1.0, 1234.5)).asJava, oschema)
      odf.write.option("header", false).csv(path.getCanonicalPath)
      val ischema = new StructType().add("f2", DoubleType).add("f1", DoubleType)
      val idf = spark.read
          .schema(ischema)
          .option("header", false)
          .option("enforceSchema", false)
          .csv(path.getCanonicalPath)

      checkAnswer(idf, odf)
    }
  }

  test("SPARK-23786: Ignore column name case if spark.sql.caseSensitive is false") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      withTempPath { path =>
        val oschema = new StructType().add("A", StringType)
        val odf = spark.createDataFrame(List(Row("0")).asJava, oschema)
        odf.write.option("header", true).csv(path.getCanonicalPath)
        val ischema = new StructType().add("a", StringType)
        val idf = spark.read.schema(ischema)
          .option("header", true)
          .option("enforceSchema", false)
          .csv(path.getCanonicalPath)
        checkAnswer(idf, odf)
      }
    }
  }

  test("SPARK-23786: check header on parsing of dataset of strings") {
    val ds = Seq("columnA,columnB", "1.0,1000.0").toDS()
    val ischema = new StructType().add("columnB", DoubleType).add("columnA", DoubleType)
    val exception = intercept[IllegalArgumentException] {
      spark.read.schema(ischema).option("header", true).option("enforceSchema", false).csv(ds)
    }

    assert(exception.getMessage.contains("CSV header does not conform to the schema"))
  }

  test("SPARK-23786: enforce inferred schema") {
    val expectedSchema = new StructType().add("_c0", DoubleType).add("_c1", StringType)
    val withHeader = spark.read
      .option("inferSchema", true)
      .option("enforceSchema", false)
      .option("header", true)
      .csv(Seq("_c0,_c1", "1.0,a").toDS())
    assert(withHeader.schema == expectedSchema)
    checkAnswer(withHeader, Seq(Row(1.0, "a")))

    // Ignore the inferSchema flag if an user sets a schema
    val schema = new StructType().add("colA", DoubleType).add("colB", StringType)
    val ds = spark.read
      .option("inferSchema", true)
      .option("enforceSchema", false)
      .option("header", true)
      .schema(schema)
      .csv(Seq("colA,colB", "1.0,a").toDS())
    assert(ds.schema == schema)
    checkAnswer(ds, Seq(Row(1.0, "a")))

    val exception = intercept[IllegalArgumentException] {
      spark.read
        .option("inferSchema", true)
        .option("enforceSchema", false)
        .option("header", true)
        .schema(schema)
        .csv(Seq("col1,col2", "1.0,a").toDS())
    }
    assert(exception.getMessage.contains("CSV header does not conform to the schema"))
  }

  test("SPARK-23786: warning should be printed if CSV header doesn't conform to schema") {
    class TestAppender extends AppenderSkeleton {
      var events = new java.util.ArrayList[LoggingEvent]
      override def close(): Unit = {}
      override def requiresLayout: Boolean = false
      protected def append(event: LoggingEvent): Unit = events.add(event)
    }

    val testAppender1 = new TestAppender
    LogManager.getRootLogger.addAppender(testAppender1)
    try {
      val ds = Seq("columnA,columnB", "1.0,1000.0").toDS()
      val ischema = new StructType().add("columnB", DoubleType).add("columnA", DoubleType)

      spark.read.schema(ischema).option("header", true).option("enforceSchema", true).csv(ds)
    } finally {
      LogManager.getRootLogger.removeAppender(testAppender1)
    }
    assert(testAppender1.events.asScala
      .exists(msg => msg.getRenderedMessage.contains("CSV header does not conform to the schema")))

    val testAppender2 = new TestAppender
    LogManager.getRootLogger.addAppender(testAppender2)
    try {
      withTempPath { path =>
        val oschema = new StructType().add("f1", DoubleType).add("f2", DoubleType)
        val odf = spark.createDataFrame(List(Row(1.0, 1234.5)).asJava, oschema)
        odf.write.option("header", true).csv(path.getCanonicalPath)
        val ischema = new StructType().add("f2", DoubleType).add("f1", DoubleType)
        spark.read
          .schema(ischema)
          .option("header", true)
          .option("enforceSchema", true)
          .csv(path.getCanonicalPath)
          .collect()
      }
    } finally {
      LogManager.getRootLogger.removeAppender(testAppender2)
    }
    assert(testAppender2.events.asScala
      .exists(msg => msg.getRenderedMessage.contains("CSV header does not conform to the schema")))
  }

  test("SPARK-25134: check header on parsing of dataset with projection and column pruning") {
    withSQLConf(SQLConf.CSV_PARSER_COLUMN_PRUNING.key -> "true") {
      Seq(false, true).foreach { multiLine =>
        withTempPath { path =>
          val dir = path.getAbsolutePath
          Seq(("a", "b")).toDF("columnA", "columnB").write
            .format("csv")
            .option("header", true)
            .save(dir)

          // schema with one column
          checkAnswer(spark.read
            .format("csv")
            .option("header", true)
            .option("enforceSchema", false)
            .option("multiLine", multiLine)
            .load(dir)
            .select("columnA"),
            Row("a"))

          // empty schema
          assert(spark.read
            .format("csv")
            .option("header", true)
            .option("enforceSchema", false)
            .option("multiLine", multiLine)
            .load(dir)
            .count() === 1L)
        }
      }
    }
  }

  test("SPARK-24645 skip parsing when columnPruning enabled and partitions scanned only") {
    withSQLConf(SQLConf.CSV_PARSER_COLUMN_PRUNING.key -> "true") {
      withTempPath { path =>
        val dir = path.getAbsolutePath
        spark.range(10).selectExpr("id % 2 AS p", "id").write.partitionBy("p").csv(dir)
        checkAnswer(spark.read.csv(dir).selectExpr("sum(p)"), Row(5))
      }
    }
  }

  test("SPARK-24676 project required data from parsed data when columnPruning disabled") {
    withSQLConf(SQLConf.CSV_PARSER_COLUMN_PRUNING.key -> "false") {
      withTempPath { path =>
        val dir = path.getAbsolutePath
        spark.range(10).selectExpr("id % 2 AS p", "id AS c0", "id AS c1").write.partitionBy("p")
          .option("header", "true").csv(dir)
        val df1 = spark.read.option("header", true).csv(dir).selectExpr("sum(p)", "count(c0)")
        checkAnswer(df1, Row(5, 10))

        // empty required column case
        val df2 = spark.read.option("header", true).csv(dir).selectExpr("sum(p)")
        checkAnswer(df2, Row(5))
      }

      // the case where tokens length != parsedSchema length
      withTempPath { path =>
        val dir = path.getAbsolutePath
        Seq("1,2").toDF().write.text(dir)
        // more tokens
        val df1 = spark.read.schema("c0 int").format("csv").option("mode", "permissive").load(dir)
        checkAnswer(df1, Row(1))
        // less tokens
        val df2 = spark.read.schema("c0 int, c1 int, c2 int").format("csv")
          .option("mode", "permissive").load(dir)
        checkAnswer(df2, Row(1, 2, null))
      }
    }
  }

  test("count() for malformed input") {
    def countForMalformedCSV(expected: Long, input: Seq[String]): Unit = {
      val schema = new StructType().add("a", IntegerType)
      val strings = spark.createDataset(input)
      val df = spark.read.schema(schema).option("header", false).csv(strings)

      assert(df.count() == expected)
    }
    def checkCount(expected: Long): Unit = {
      val validRec = "1"
      val inputs = Seq(
        Seq("{-}", validRec),
        Seq(validRec, "?"),
        Seq("0xAC", validRec),
        Seq(validRec, "0.314"),
        Seq("\\\\\\", validRec)
      )
      inputs.foreach { input =>
        countForMalformedCSV(expected, input)
      }
    }

    checkCount(2)
    countForMalformedCSV(0, Seq(""))
  }

  test("SPARK-25387: bad input should not cause NPE") {
    val schema = StructType(StructField("a", IntegerType) :: Nil)
    val input = spark.createDataset(Seq("\u0000\u0000\u0001234"))

    checkAnswer(spark.read.schema(schema).csv(input), Row(null))
    checkAnswer(spark.read.option("multiLine", true).schema(schema).csv(input), Row(null))
    assert(spark.read.csv(input).collect().toSet == Set(Row()))
  }

  test("SPARK-31261: bad csv input with `columnNameCorruptRecord` should not cause NPE") {
    val schema = StructType(
      StructField("a", IntegerType) :: StructField("_corrupt_record", StringType) :: Nil)
    val input = spark.createDataset(Seq("\u0000\u0000\u0001234"))

    checkAnswer(
      spark.read
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(schema)
        .csv(input),
      Row(null, null))
    assert(spark.read.csv(input).collect().toSet == Set(Row()))
  }

  test("field names of inferred schema shouldn't compare to the first row") {
    val input = Seq("1,2").toDS()
    val df = spark.read.option("enforceSchema", false).csv(input)
    checkAnswer(df, Row("1", "2"))
  }

  test("using the backward slash as the delimiter") {
    val input = Seq("""abc\1""").toDS()
    val delimiter = """\\"""
    checkAnswer(spark.read.option("delimiter", delimiter).csv(input), Row("abc", "1"))
    checkAnswer(spark.read.option("inferSchema", true).option("delimiter", delimiter).csv(input),
      Row("abc", 1))
    val schema = new StructType().add("a", StringType).add("b", IntegerType)
    checkAnswer(spark.read.schema(schema).option("delimiter", delimiter).csv(input), Row("abc", 1))
  }

  test("SPARK-27873: disabling enforceSchema should not fail columnNameOfCorruptRecord") {
    Seq(false, true).foreach { multiLine =>
      withTempPath { path =>
        val df = Seq(("0", "2013-abc-11")).toDF("a", "b")
        df.write
          .option("header", "true")
          .csv(path.getAbsolutePath)

        val schema = StructType.fromDDL("a int, b date")
        val columnNameOfCorruptRecord = "_unparsed"
        val schemaWithCorrField = schema.add(columnNameOfCorruptRecord, StringType)
        val readDF = spark
          .read
          .option("mode", "Permissive")
          .option("header", "true")
          .option("enforceSchema", false)
          .option("multiLine", multiLine)
          .option("columnNameOfCorruptRecord", columnNameOfCorruptRecord)
          .schema(schemaWithCorrField)
          .csv(path.getAbsoluteFile.toString)
        checkAnswer(readDF, Row(null, null, "0,2013-abc-11") :: Nil)
      }
    }
  }

  test("SPARK-29101 test count with DROPMALFORMED mode") {
    Seq((true, 4), (false, 3)).foreach { case (csvColumnPruning, expectedCount) =>
      withSQLConf(SQLConf.CSV_PARSER_COLUMN_PRUNING.key -> csvColumnPruning.toString) {
        val count = spark.read
          .option("header", "true")
          .option("mode", "DROPMALFORMED")
          .csv(testFile(malformedRowFile))
          .count()
        assert(expectedCount == count)
      }
    }
  }

  test("parse timestamp in microsecond precision") {
    withTempPath { path =>
      val t = "2019-11-14 20:35:30.123456"
      Seq(t).toDF("t").write.text(path.getAbsolutePath)
      val readback = spark.read
        .schema("t timestamp")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss.SSSSSS")
        .csv(path.getAbsolutePath)
      checkAnswer(readback, Row(Timestamp.valueOf(t)))
    }
  }

  test("Roundtrip in reading and writing timestamps in microsecond precision") {
    withTempPath { path =>
      val timestamp = Timestamp.valueOf("2019-11-18 11:56:00.123456")
      Seq(timestamp).toDF("t")
        .write
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss.SSSSSS")
        .csv(path.getAbsolutePath)
      val readback = spark.read
        .schema("t timestamp")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss.SSSSSS")
        .csv(path.getAbsolutePath)
      checkAnswer(readback, Row(timestamp))
    }
  }
}
