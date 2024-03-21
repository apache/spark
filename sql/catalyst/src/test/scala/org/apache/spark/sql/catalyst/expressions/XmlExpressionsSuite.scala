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

import java.text.{DecimalFormat, DecimalFormatSymbols, SimpleDateFormat}
import java.util.{Calendar, Locale, TimeZone}

import org.scalatest.exceptions.TestFailedException

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.plans.PlanTestBase
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.{PST, UTC, UTC_OPT}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class XmlExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper with PlanTestBase {
  test("from_xml escaping") {
    val schema = StructType(StructField("\"quote", IntegerType) :: Nil)
    GenerateUnsafeProjection.generate(
      XmlToStructs(schema, Map.empty, Literal("\"quote"), UTC_OPT) :: Nil)
  }

  test("from_xml") {
    val xmlData = """<row><a>1</a></row>"""
    val schema = StructType(StructField("a", IntegerType) :: Nil)
    checkEvaluation(
      XmlToStructs(schema, Map.empty, Literal(xmlData), UTC_OPT),
      InternalRow(1)
    )
  }

  test("from_xml- invalid data") {
    val xmlData = """<row><a>1</row>"""
    val schema = StructType(StructField("a", IntegerType) :: Nil)
    checkEvaluation(
      XmlToStructs(schema, Map.empty, Literal(xmlData), UTC_OPT),
      InternalRow(null)
    )

    val exception = intercept[TestFailedException] {
      checkEvaluation(
        XmlToStructs(schema, Map("mode" -> FailFastMode.name), Literal(xmlData), UTC_OPT),
        InternalRow(null)
      )
    }.getCause
    checkError(
      exception = exception.asInstanceOf[SparkException],
      errorClass = "MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION",
      parameters = Map("badRecord" -> "[null]", "failFastMode" -> "FAILFAST")
    )
  }

  test("from_xml - input=array, schema=array, output=row of array") {
    val input = s"""
            |<row>
            |   <a>1</a>
            |   <a>2</a>
            |</row>""".stripMargin
    val schema = StructType(StructField("a", ArrayType(IntegerType)) :: Nil)
    val output = Row(Array(1, 2))
    checkEvaluation(XmlToStructs(schema, Map.empty, Literal(input), UTC_OPT), output)
  }

  test("from_xml - input=single element, schema=array, output=array of single element") {
    val input = s"""
            |<row>
            |   <a>1</a>
            |</row>""".stripMargin
    val schema = StructType(StructField("a", ArrayType(IntegerType)) :: Nil)
    val output = Row(Array(1))
    checkEvaluation(XmlToStructs(schema, Map.empty, Literal(input), UTC_OPT), output)
  }

  test("from_xml - input=empty row, schema=array, output=empty") {
    val input = "<row> </row>"
    val schema = StructType(StructField("a", ArrayType(IntegerType)) :: Nil)
    val output = Row(null)
    checkEvaluation(XmlToStructs(schema, Map.empty, Literal(input), UTC_OPT), output)
  }

  test("from_xml - input=empty, schema=array, output=empty") {
    val input = """<?xml version="1.0"?>"""
    val schema = StructType(StructField("a", ArrayType(IntegerType)) :: Nil)
    val output = Row(null)
    checkEvaluation(XmlToStructs(schema, Map.empty, Literal(input), UTC_OPT), output)
  }

  test("from_xml - input=array, schema=struct, output=single row") {
    val input = s"""
            |<row>
            |   <a>1</a>
            |   <a>2</a>
            |</row>""".stripMargin
    val corrupted = "corrupted"
    val schema = new StructType().add("a", IntegerType).add(corrupted, StringType)
    val output = InternalRow(2, null)
    val options = Map("columnNameOfCorruptRecord" -> corrupted)
    checkEvaluation(XmlToStructs(schema, options, Literal(input), UTC_OPT), output)
  }

  test("from_xml - input=empty array, schema=struct, output=single row with null") {
    val input = s"""
            |<row>
            |   <a></a>
            |   <a></a>
            |</row>""".stripMargin
    val schema = StructType(StructField("a", IntegerType) :: Nil)
    val output = InternalRow(null)
    checkEvaluation(XmlToStructs(schema, Map.empty, Literal(input), UTC_OPT), output)
  }

  test("from_xml - input=empty object, schema=struct, output=single row with null") {
    val input = s"""
            |<row>
            |   <a></a>
            |   <a></a>
            |</row>""".stripMargin
    val schema = StructType(StructField("a", IntegerType) :: Nil)
    val output = InternalRow(null)
    checkEvaluation(XmlToStructs(schema, Map.empty, Literal(input), UTC_OPT), output)
  }

  test("from_xml null input column") {
    val schema = StructType(StructField("a", IntegerType) :: Nil)
    checkEvaluation(
      XmlToStructs(schema, Map.empty, Literal.create(null, StringType), UTC_OPT),
      null
    )
  }

  test("from_xml with timestamp") {
    val schema = StructType(StructField("t", TimestampType) :: Nil)
    val xmlData1 = s"""
                     |<row>
                     |  <t>2016-01-01T00:00:00.123Z</t>
                     |</row>""".stripMargin
    var c = Calendar.getInstance(TimeZone.getTimeZone(UTC))
    c.set(2016, 0, 1, 0, 0, 0)
    c.set(Calendar.MILLISECOND, 123)


    // The result doesn't change because the xml string includes timezone string ("Z" here),
    // which means the string represents the timestamp string in the timezone regardless of
    // the timeZoneId parameter.
    checkEvaluation(
      XmlToStructs(schema, Map.empty, Literal.create(xmlData1, StringType), UTC_OPT),
      InternalRow(c.getTimeInMillis * 1000L)
    )

    val xmlData2 = s"""
                     |<row>
                     |  <t>2016-01-01T00:00:00</t>
                     |</row>""".stripMargin
    for (zid <- DateTimeTestUtils.outstandingZoneIds) {
      c = Calendar.getInstance(TimeZone.getTimeZone(zid))
      c.set(2016, 0, 1, 0, 0, 0)
      c.set(Calendar.MILLISECOND, 0)

      checkEvaluation(
        XmlToStructs(
          schema,
          Map("timestampFormat" -> "yyyy-MM-dd'T'HH:mm:ss"),
          Literal.create(xmlData2, StringType),
          Option(zid.getId)),
        InternalRow(c.getTimeInMillis * 1000L)
      )
      checkEvaluation(
        XmlToStructs(
          schema,
          Map("timestampFormat" -> "yyyy-MM-dd'T'HH:mm:ss",
            DateTimeUtils.TIMEZONE_OPTION -> zid.getId),
          Literal.create(xmlData2, StringType),
          UTC_OPT),
        InternalRow(c.getTimeInMillis * 1000L)
      )
    }
  }

  test("from_xml empty input column") {
    val schema = StructType(StructField("a", IntegerType) :: Nil)
    checkEvaluation(
      XmlToStructs(schema, Map.empty, Literal.create(" ", StringType), UTC_OPT),
      InternalRow(null)
    )
  }

  test("to_xml escaping") {
    val schema = StructType(StructField("\"quote", IntegerType) :: Nil)
    val struct = Literal.create(create_row(1), schema)
    GenerateUnsafeProjection.generate(
      StructsToXml(Map.empty, struct, UTC_OPT) :: Nil)
  }

  test("to_xml - struct") {
    val schema = StructType(StructField("a", IntegerType) :: Nil)
    val struct = Literal.create(create_row(1), schema)
    checkEvaluation(
      StructsToXml(Map.empty, struct, UTC_OPT),
      s"""|<ROW>
          |    <a>1</a>
          |</ROW>""".stripMargin
    )
  }

  test("to_xml - array") {
    val inputSchema = StructType(StructField("a", ArrayType(IntegerType)) :: Nil)
    val input = Row(Array(1, 2))
    val output = s"""|<ROW>
                     |    <a>1</a>
                     |    <a>2</a>
                     |</ROW>""".stripMargin
    checkEvaluation(
      StructsToXml(Map.empty, Literal.create(input, inputSchema), UTC_OPT),
      output)
  }

  test("to_xml - row with empty array") {
    val inputSchema = StructType(StructField("a", ArrayType(IntegerType)) :: Nil)
    val input = Row(Array(null))
    val output = """<ROW/>"""
    checkEvaluation(
      StructsToXml(Map.empty, Literal.create(input, inputSchema), UTC_OPT),
      output)
  }

  test("to_xml - empty row") {
    val inputSchema = StructType(StructField("a", ArrayType(IntegerType)) :: Nil)
    val input = Row(null)
    val output = """<ROW/>"""
    checkEvaluation(
      StructsToXml(Map.empty, Literal.create(input, inputSchema), UTC_OPT),
      output)
  }

  test("to_xml null input column") {
    val schema = StructType(StructField("a", IntegerType) :: Nil)
    val struct = Literal.create(null, schema)
    checkEvaluation(
      StructsToXml(Map.empty, struct, UTC_OPT),
      null
    )
  }

  test("to_xml with timestamp") {
    val schema = StructType(StructField("t", TimestampType) :: Nil)
    val c = Calendar.getInstance(TimeZone.getTimeZone(UTC))
    c.set(2016, 0, 1, 0, 0, 0)
    c.set(Calendar.MILLISECOND, 0)
    val struct = Literal.create(create_row(c.getTimeInMillis * 1000L), schema)

    checkEvaluation(
      StructsToXml(Map.empty, struct, UTC_OPT),
      s"""|<ROW>
          |    <t>2016-01-01T00:00:00.000Z</t>
          |</ROW>""".stripMargin
    )
    checkEvaluation(
      StructsToXml(Map.empty, struct, Option(PST.getId)),
      s"""|<ROW>
          |    <t>2015-12-31T16:00:00.000-08:00</t>
          |</ROW>""".stripMargin
    )

    checkEvaluation(
      StructsToXml(
        Map("timestampFormat" -> "yyyy-MM-dd'T'HH:mm:ss",
          DateTimeUtils.TIMEZONE_OPTION -> UTC_OPT.get),
        struct,
        UTC_OPT),
        s"""|<ROW>
            |    <t>2016-01-01T00:00:00</t>
            |</ROW>""".stripMargin
    )
    checkEvaluation(
      StructsToXml(
        Map("timestampFormat" -> "yyyy-MM-dd'T'HH:mm:ss",
          DateTimeUtils.TIMEZONE_OPTION -> PST.getId),
        struct,
        UTC_OPT),
        s"""|<ROW>
            |    <t>2015-12-31T16:00:00</t>
            |</ROW>""".stripMargin
    )
  }

  test("to_xml - row with array of maps") {
    val inputSchema = StructType(
      StructField("f", ArrayType(MapType(StringType, IntegerType))) :: Nil)
    val input = Row(Array(Map("a" -> 1), Map("a" -> 2)))
    val output = s"""|<ROW>
                     |    <f>
                     |        <a>1</a>
                     |    </f>
                     |    <f>
                     |        <a>2</a>
                     |    </f>
                     |</ROW>""".stripMargin
    checkEvaluation(
      StructsToXml(Map.empty, Literal.create(input, inputSchema), UTC_OPT),
      output)
  }

  test("to_xml - row with single map") {
    val inputSchema = StructType(StructField("f", MapType(StringType, IntegerType)) :: Nil)
    val input = Row(Map("a" -> 1))
    val output = s"""|<ROW>
                     |    <f>
                     |        <a>1</a>
                     |    </f>
                     |</ROW>""".stripMargin
    checkEvaluation(
      StructsToXml(Map.empty, Literal.create(input, inputSchema), UTC_OPT),
      output)
  }

  test("from_xml missing fields") {
    val input = s"""
            |<row>
            |   <a>1</a>
            |   <c>foo</c>
            |</row>""".stripMargin
    val xmlSchema = new StructType()
      .add("a", LongType, nullable = false)
      .add("b", StringType, nullable = false)
      .add("c", StringType, nullable = false)
    val output = InternalRow(1L, null, UTF8String.fromString("foo"))
    val expr = XmlToStructs(xmlSchema, Map.empty, Literal.create(input, StringType), UTC_OPT)
    checkEvaluation(expr, output)
    val schema = expr.dataType
    val schemaToCompare = xmlSchema.asNullable
    assert(schemaToCompare == schema)
  }

  test("infer schema of xml strings") {
    checkEvaluation(new SchemaOfXml(Literal.create("""<ROW><col>0</col></ROW>""")),
      "STRUCT<col: BIGINT>")
    val input = s"""|<ROW>
                    |    <col0>a</col0>
                    |    <col0>b</col0>
                    |    <col1>
                    |        <col2>b</col2>
                    |    </col1>
                    |</ROW>""".stripMargin
    checkEvaluation(
      new SchemaOfXml(Literal.create(input)),
      "STRUCT<col0: ARRAY<STRING>, col1: STRUCT<col2: STRING>>")
  }

  test("infer schema of Xml strings by using options") {
    checkEvaluation(
      new SchemaOfXml(Literal.create("""<ROW><col>01</col></ROW>"""),
        CreateMap(Seq(Literal.create("allowNumericLeadingZeros"), Literal.create("true")))),
      "STRUCT<col: BIGINT>")
  }

  test("parse date with locale") {
    Seq("en-US", "ru-RU").foreach { langTag =>
      val locale = Locale.forLanguageTag(langTag)
      val date = new SimpleDateFormat("yyyy-MM-dd").parse("2018-11-05")
      val schema = new StructType().add("d", DateType)
      val dateFormat = "MMM yyyy"
      val sdf = new SimpleDateFormat(dateFormat, locale)
      val dateStr = s"""|<ROW>
                      |    <d>${sdf.format(date)}</d>
                      |</ROW>""".stripMargin
      val options = Map("dateFormat" -> dateFormat, "locale" -> langTag)

      checkEvaluation(
        XmlToStructs(schema, options, Literal.create(dateStr), UTC_OPT),
        InternalRow(17836)) // number of days from 1970-01-01
    }
  }

  test("verify corrupt column") {
    checkExceptionInExpression[AnalysisException](
      XmlToStructs(
        schema = StructType.fromDDL("i int, _unparsed boolean"),
        options = Map("columnNameOfCorruptRecord" -> "_unparsed"),
        child = Literal.create("""{"i":"a"}"""),
        timeZoneId = UTC_OPT),
      expectedErrMsg = "The field for corrupt records must be string type and nullable")
  }

  def decimalInput(langTag: String): (Decimal, String) = {
    val decimalVal = new java.math.BigDecimal("1000.001")
    val decimalType = new DecimalType(10, 5)
    val expected = Decimal(decimalVal, decimalType.precision, decimalType.scale)
    val decimalFormat = new DecimalFormat("",
      new DecimalFormatSymbols(Locale.forLanguageTag(langTag)))
    val input = s"""|<ROW>
                    |    <d>${decimalFormat.format(expected.toBigDecimal)}</d>
                    |</ROW>""".stripMargin

    (expected, input)
  }

  test("parse decimals using locale") {
    def checkDecimalParsing(langTag: String): Unit = {
      val schema = new StructType().add("d", DecimalType(10, 5))
      val options = Map("locale" -> langTag)
      val (expected, input) = decimalInput(langTag)

      checkEvaluation(
        XmlToStructs(schema, options, Literal.create(input), UTC_OPT),
        InternalRow(expected))
    }

    Seq("en-US", "ko-KR", "ru-RU", "de-DE").foreach(checkDecimalParsing)
  }

  test("inferring the decimal type using locale") {
    def checkDecimalInfer(langTag: String, expectedType: String): Unit = {
      val options = Map("locale" -> langTag, "prefersDecimal" -> "true")
      val (_, input) = decimalInput(langTag)

      checkEvaluation(
        SchemaOfXml(Literal.create(input), options),
        expectedType)
    }

    Seq("en-US", "ko-KR", "ru-RU", "de-DE").foreach {
        checkDecimalInfer(_, """STRUCT<d: DECIMAL(7,3)>""")
    }
  }

}
