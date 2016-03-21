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
import java.sql.Timestamp

import org.apache.spark.SparkException
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.test.{SharedSQLContext, SQLTestUtils}
import org.apache.spark.sql.types._

class CSVSuite extends QueryTest with SharedSQLContext with SQLTestUtils {
  private val carsFile = "cars.csv"
  private val carsMalformedFile = "cars-malformed.csv"
  private val carsFile8859 = "cars_iso-8859-1.csv"
  private val carsTsvFile = "cars.tsv"
  private val carsAltFile = "cars-alternative.csv"
  private val carsUnbalancedQuotesFile = "cars-unbalanced-quotes.csv"
  private val carsNullFile = "cars-null.csv"
  private val emptyFile = "empty.csv"
  private val commentsFile = "comments.csv"
  private val disableCommentsFile = "disable_comments.csv"

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
        assert(df.schema.fieldNames === Array("C0", "C1", "C2", "C3", "C4"))
      }
    }

    if (checkValues) {
      val yearValues = List("2012", "1997", "2015")
      val actualYears = if (!withHeader) "year" :: yearValues else yearValues
      val years = if (withHeader) df.select("year").collect() else df.select("C0").collect()

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
    val cars = sqlContext
      .read
      .format("csv")
      .option("header", "false")
      .load(testFile(carsFile))

    verifyCars(cars, withHeader = false, checkTypes = false)
  }

  test("simple csv test with type inference") {
    val cars = sqlContext
      .read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(testFile(carsFile))

    verifyCars(cars, withHeader = true, checkTypes = true)
  }

  test("test with alternative delimiter and quote") {
    val cars = sqlContext.read
      .format("csv")
      .options(Map("quote" -> "\'", "delimiter" -> "|", "header" -> "true"))
      .load(testFile(carsAltFile))

    verifyCars(cars, withHeader = true)
  }

  test("bad encoding name") {
    val exception = intercept[UnsupportedCharsetException] {
      sqlContext
        .read
        .format("csv")
        .option("charset", "1-9588-osi")
        .load(testFile(carsFile8859))
    }

    assert(exception.getMessage.contains("1-9588-osi"))
  }

  test("test different encoding") {
    // scalastyle:off
    sqlContext.sql(
      s"""
         |CREATE TEMPORARY TABLE carsTable USING csv
         |OPTIONS (path "${testFile(carsFile8859)}", header "true",
         |charset "iso-8859-1", delimiter "þ")
      """.stripMargin.replaceAll("\n", " "))
    //scalstyle:on

    verifyCars(sqlContext.table("carsTable"), withHeader = true)
  }

  test("test aliases sep and encoding for delimiter and charset") {
    val cars = sqlContext
      .read
      .format("csv")
      .option("header", "true")
      .option("encoding", "iso-8859-1")
      .option("sep", "þ")
      .load(testFile(carsFile8859))

    verifyCars(cars, withHeader = true)
  }

  test("DDL test with tab separated file") {
    sqlContext.sql(
      s"""
         |CREATE TEMPORARY TABLE carsTable USING csv
         |OPTIONS (path "${testFile(carsTsvFile)}", header "true", delimiter "\t")
      """.stripMargin.replaceAll("\n", " "))

    verifyCars(sqlContext.table("carsTable"), numFields = 6, withHeader = true, checkHeader = false)
  }

  test("DDL test parsing decimal type") {
    sqlContext.sql(
      s"""
         |CREATE TEMPORARY TABLE carsTable
         |(yearMade double, makeName string, modelName string, priceTag decimal,
         | comments string, grp string)
         |USING csv
         |OPTIONS (path "${testFile(carsTsvFile)}", header "true", delimiter "\t")
      """.stripMargin.replaceAll("\n", " "))

    assert(
      sqlContext.sql("SELECT makeName FROM carsTable where priceTag > 60000").collect().size === 1)
  }

  test("test for DROPMALFORMED parsing mode") {
    val cars = sqlContext.read
      .format("csv")
      .options(Map("header" -> "true", "mode" -> "dropmalformed"))
      .load(testFile(carsFile))

    assert(cars.select("year").collect().size === 2)
  }

  test("test for FAILFAST parsing mode") {
    val exception = intercept[SparkException]{
      sqlContext.read
      .format("csv")
      .options(Map("header" -> "true", "mode" -> "failfast"))
      .load(testFile(carsFile)).collect()
    }

    assert(exception.getMessage.contains("Malformed line in FAILFAST mode: 2015,Chevy,Volt"))
  }

  test("test for tokens more than the fields in the schema") {
    val cars = sqlContext
      .read
      .format("csv")
      .option("header", "false")
      .option("comment", "~")
      .load(testFile(carsMalformedFile))

    verifyCars(cars, withHeader = false, checkTypes = false)
  }

  test("test with null quote character") {
    val cars = sqlContext.read
      .format("csv")
      .option("header", "true")
      .option("quote", "")
      .load(testFile(carsUnbalancedQuotesFile))

    verifyCars(cars, withHeader = true, checkValues = false)

  }

  test("test with empty file and known schema") {
    val result = sqlContext.read
      .format("csv")
      .schema(StructType(List(StructField("column", StringType, false))))
      .load(testFile(emptyFile))

    assert(result.collect.size === 0)
    assert(result.schema.fieldNames.size === 1)
  }


  test("DDL test with empty file") {
    sqlContext.sql(s"""
           |CREATE TEMPORARY TABLE carsTable
           |(yearMade double, makeName string, modelName string, comments string, grp string)
           |USING csv
           |OPTIONS (path "${testFile(emptyFile)}", header "false")
      """.stripMargin.replaceAll("\n", " "))

    assert(sqlContext.sql("SELECT count(*) FROM carsTable").collect().head(0) === 0)
  }

  test("DDL test with schema") {
    sqlContext.sql(s"""
           |CREATE TEMPORARY TABLE carsTable
           |(yearMade double, makeName string, modelName string, comments string, blank string)
           |USING csv
           |OPTIONS (path "${testFile(carsFile)}", header "true")
      """.stripMargin.replaceAll("\n", " "))

    val cars = sqlContext.table("carsTable")
    verifyCars(cars, withHeader = true, checkHeader = false, checkValues = false)
    assert(
      cars.schema.fieldNames === Array("yearMade", "makeName", "modelName", "comments", "blank"))
  }

  test("save csv") {
    withTempDir { dir =>
      val csvDir = new File(dir, "csv").getCanonicalPath
      val cars = sqlContext.read
        .format("csv")
        .option("header", "true")
        .load(testFile(carsFile))

      cars.coalesce(1).write
        .format("csv")
        .option("header", "true")
        .save(csvDir)

      val carsCopy = sqlContext.read
        .format("csv")
        .option("header", "true")
        .load(csvDir)

      verifyCars(carsCopy, withHeader = true)
    }
  }

  test("save csv with quote") {
    withTempDir { dir =>
      val csvDir = new File(dir, "csv").getCanonicalPath
      val cars = sqlContext.read
        .format("csv")
        .option("header", "true")
        .load(testFile(carsFile))

      cars.coalesce(1).write
        .format("csv")
        .option("header", "true")
        .option("quote", "\"")
        .save(csvDir)

      val carsCopy = sqlContext.read
        .format("csv")
        .option("header", "true")
        .option("quote", "\"")
        .load(csvDir)

      verifyCars(carsCopy, withHeader = true)
    }
  }

  test("commented lines in CSV data") {
    val results = sqlContext.read
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
    val results = sqlContext.read
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

  test("setting comment to null disables comment support") {
    val results = sqlContext.read
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
    val cars = sqlContext.read
      .format("csv")
      .schema(dataSchema)
      .options(Map("header" -> "true", "nullValue" -> "null"))
      .load(testFile(carsNullFile))

    verifyCars(cars, withHeader = true, checkValues = false)
    val results = cars.collect()
    assert(results(0).toSeq === Array(2012, "Tesla", "S", "null", "null"))
    assert(results(2).toSeq === Array(null, "Chevy", "Volt", null, null))
  }

  test("save csv with compression codec option") {
    withTempDir { dir =>
      val csvDir = new File(dir, "csv").getCanonicalPath
      val cars = sqlContext.read
        .format("csv")
        .option("header", "true")
        .load(testFile(carsFile))

      cars.coalesce(1).write
        .format("csv")
        .option("header", "true")
        .option("compression", "gZiP")
        .save(csvDir)

      val compressedFiles = new File(csvDir).listFiles()
      assert(compressedFiles.exists(_.getName.endsWith(".gz")))

      val carsCopy = sqlContext.read
        .format("csv")
        .option("header", "true")
        .load(csvDir)

      verifyCars(carsCopy, withHeader = true)
    }
  }
}
