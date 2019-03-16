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

package org.apache.spark.sql.catalyst.expressions

import java.text.SimpleDateFormat
import java.util.{Calendar, Locale}

import org.scalatest.exceptions.TestFailedException

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.PlanTestBase
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class CsvExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper with PlanTestBase {
  val badCsv = "\u0000\u0000\u0000A\u0001AAA"

  val gmtId = Option(DateTimeUtils.TimeZoneGMT.getID)

  test("from_csv") {
    val csvData = "1"
    val schema = StructType(StructField("a", IntegerType) :: Nil)
    checkEvaluation(
      CsvToStructs(schema, Map.empty, Literal(csvData), gmtId),
      InternalRow(1)
    )
  }

  test("from_csv - invalid data") {
    val csvData = "---"
    val schema = StructType(StructField("a", DoubleType) :: Nil)
    checkEvaluation(
      CsvToStructs(schema, Map("mode" -> PermissiveMode.name), Literal(csvData), gmtId),
      InternalRow(null))

    // Default mode is Permissive
    checkEvaluation(CsvToStructs(schema, Map.empty, Literal(csvData), gmtId), InternalRow(null))
  }

  test("from_csv null input column") {
    val schema = StructType(StructField("a", IntegerType) :: Nil)
    checkEvaluation(
      CsvToStructs(schema, Map.empty, Literal.create(null, StringType), gmtId),
      null
    )
  }

  test("from_csv bad UTF-8") {
    val schema = StructType(StructField("a", IntegerType) :: Nil)
    checkEvaluation(
      CsvToStructs(schema, Map.empty, Literal(badCsv), gmtId),
      InternalRow(null))
  }

  test("from_csv with timestamp") {
    val schema = StructType(StructField("t", TimestampType) :: Nil)

    val csvData1 = "2016-01-01T00:00:00.123Z"
    var c = Calendar.getInstance(DateTimeUtils.TimeZoneGMT)
    c.set(2016, 0, 1, 0, 0, 0)
    c.set(Calendar.MILLISECOND, 123)
    checkEvaluation(
      CsvToStructs(schema, Map.empty, Literal(csvData1), gmtId),
      InternalRow(c.getTimeInMillis * 1000L)
    )
    // The result doesn't change because the CSV string includes timezone string ("Z" here),
    // which means the string represents the timestamp string in the timezone regardless of
    // the timeZoneId parameter.
    checkEvaluation(
      CsvToStructs(schema, Map.empty, Literal(csvData1), Option("PST")),
      InternalRow(c.getTimeInMillis * 1000L)
    )

    val csvData2 = "2016-01-01T00:00:00"
    for (tz <- DateTimeTestUtils.outstandingTimezones) {
      c = Calendar.getInstance(tz)
      c.set(2016, 0, 1, 0, 0, 0)
      c.set(Calendar.MILLISECOND, 0)
      checkEvaluation(
        CsvToStructs(
          schema,
          Map("timestampFormat" -> "yyyy-MM-dd'T'HH:mm:ss"),
          Literal(csvData2),
          Option(tz.getID)),
        InternalRow(c.getTimeInMillis * 1000L)
      )
      checkEvaluation(
        CsvToStructs(
          schema,
          Map("timestampFormat" -> "yyyy-MM-dd'T'HH:mm:ss",
            DateTimeUtils.TIMEZONE_OPTION -> tz.getID),
          Literal(csvData2),
          gmtId),
        InternalRow(c.getTimeInMillis * 1000L)
      )
    }
  }

  test("from_csv empty input column") {
    val schema = StructType(StructField("a", IntegerType) :: Nil)
    checkEvaluation(
      CsvToStructs(schema, Map.empty, Literal.create(" ", StringType), gmtId),
      InternalRow(null)
    )
  }

  test("forcing schema nullability") {
    val input = """1,,"foo""""
    val csvSchema = new StructType()
      .add("a", LongType, nullable = false)
      .add("b", StringType, nullable = false)
      .add("c", StringType, nullable = false)
    val output = InternalRow(1L, null, UTF8String.fromString("foo"))
    val expr = CsvToStructs(csvSchema, Map.empty, Literal.create(input, StringType), gmtId)
    checkEvaluation(expr, output)
    val schema = expr.dataType
    val schemaToCompare = csvSchema.asNullable
    assert(schemaToCompare == schema)
  }


  test("from_csv missing columns") {
    val schema = new StructType()
      .add("a", IntegerType)
      .add("b", IntegerType)
    checkEvaluation(
      CsvToStructs(schema, Map.empty, Literal.create("1"), gmtId),
      InternalRow(1, null)
    )
  }

  test("unsupported mode") {
    val csvData = "---"
    val schema = StructType(StructField("a", DoubleType) :: Nil)
    val exception = intercept[TestFailedException] {
      checkEvaluation(
        CsvToStructs(schema, Map("mode" -> DropMalformedMode.name), Literal(csvData), gmtId),
        InternalRow(null))
    }.getCause
    assert(exception.getMessage.contains("from_csv() doesn't support the DROPMALFORMED mode"))
  }

  test("infer schema of CSV strings") {
    checkEvaluation(new SchemaOfCsv(Literal.create("1,abc")), "struct<_c0:int,_c1:string>")
  }

  test("infer schema of CSV strings by using options") {
    checkEvaluation(
      new SchemaOfCsv(Literal.create("1|abc"), Map("delimiter" -> "|")),
      "struct<_c0:int,_c1:string>")
  }

  test("to_csv - struct") {
    val schema = StructType(StructField("a", IntegerType) :: Nil)
    val struct = Literal.create(create_row(1), schema)
    checkEvaluation(StructsToCsv(Map.empty, struct, gmtId), "1")
  }

  test("to_csv null input column") {
    val schema = StructType(StructField("a", IntegerType) :: Nil)
    val struct = Literal.create(null, schema)
    checkEvaluation(
      StructsToCsv(Map.empty, struct, gmtId),
      null
    )
  }

  test("to_csv with timestamp") {
    val schema = StructType(StructField("t", TimestampType) :: Nil)
    val c = Calendar.getInstance(DateTimeUtils.TimeZoneGMT)
    c.set(2016, 0, 1, 0, 0, 0)
    c.set(Calendar.MILLISECOND, 0)
    val struct = Literal.create(create_row(c.getTimeInMillis * 1000L), schema)

    checkEvaluation(StructsToCsv(Map.empty, struct, gmtId), "2016-01-01T00:00:00.000Z")
    checkEvaluation(
      StructsToCsv(Map.empty, struct, Option("PST")), "2015-12-31T16:00:00.000-08:00")

    checkEvaluation(
      StructsToCsv(
        Map("timestampFormat" -> "yyyy-MM-dd'T'HH:mm:ss",
          DateTimeUtils.TIMEZONE_OPTION -> gmtId.get),
        struct,
        gmtId),
      "2016-01-01T00:00:00"
    )
    checkEvaluation(
      StructsToCsv(
        Map("timestampFormat" -> "yyyy-MM-dd'T'HH:mm:ss",
          DateTimeUtils.TIMEZONE_OPTION -> "PST"),
        struct,
        gmtId),
      "2015-12-31T16:00:00"
    )
  }

  test("parse date with locale") {
    Seq("en-US", "ru-RU").foreach { langTag =>
      val locale = Locale.forLanguageTag(langTag)
      val date = new SimpleDateFormat("yyyy-MM-dd").parse("2018-11-05")
      val schema = new StructType().add("d", DateType)
      val dateFormat = "MMM yyyy"
      val sdf = new SimpleDateFormat(dateFormat, locale)
      val dateStr = sdf.format(date)
      val options = Map("dateFormat" -> dateFormat, "locale" -> langTag)

      checkEvaluation(
        CsvToStructs(schema, options, Literal.create(dateStr), gmtId),
        InternalRow(17836)) // number of days from 1970-01-01
    }
  }

  test("verify corrupt column") {
    checkExceptionInExpression[AnalysisException](
      CsvToStructs(
        schema = StructType.fromDDL("i int, _unparsed boolean"),
        options = Map("columnNameOfCorruptRecord" -> "_unparsed"),
        child = Literal.create("a"),
        timeZoneId = gmtId),
      expectedErrMsg = "The field for corrupt records must be string type and nullable")
  }
}
