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

import java.math.BigDecimal
import java.util.Locale

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class UnivocityParserSuite extends SparkFunSuite {
  private val parser =
    new UnivocityParser(StructType(Seq.empty), new CSVOptions(Map.empty[String, String]))

  private def assertNull(v: Any) = assert(v == null)

  test("Can parse decimal type values") {
    val stringValues = Seq("10.05", "1,000.01", "158,058,049.001")
    val decimalValues = Seq(10.05, 1000.01, 158058049.001)
    val decimalType = new DecimalType()

    stringValues.zip(decimalValues).foreach { case (strVal, decimalVal) =>
      val decimalValue = new BigDecimal(decimalVal.toString)
      assert(parser.makeConverter("_1", decimalType).apply(strVal) ===
        Decimal(decimalValue, decimalType.precision, decimalType.scale))
    }
  }

  test("Nullable types are handled") {
    val types = Seq(ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType,
      BooleanType, DecimalType.DoubleDecimal, TimestampType, DateType, StringType)

    // Nullable field with nullValue option.
    types.foreach { t =>
      // Tests that a custom nullValue.
      val converter =
        parser.makeConverter("_1", t, nullable = true, CSVOptions("nullValue", "-"))
      assertNull(converter.apply("-"))
      assertNull(converter.apply(null))

      // Tests that the default nullValue is empty string.
      assertNull(parser.makeConverter("_1", t, nullable = true).apply(""))
    }

    // Not nullable field with nullValue option.
    types.foreach { t =>
      // Casts a null to not nullable field should throw an exception.
      val converter =
        parser.makeConverter("_1", t, nullable = false, CSVOptions("nullValue", "-"))
      var message = intercept[RuntimeException] {
        converter.apply("-")
      }.getMessage
      assert(message.contains("null value found but field _1 is not nullable."))
      message = intercept[RuntimeException] {
        converter.apply(null)
      }.getMessage
      assert(message.contains("null value found but field _1 is not nullable."))
    }

    // If nullValue is different with empty string, then, empty string should not be casted into
    // null.
    Seq(true, false).foreach { b =>
      val converter =
        parser.makeConverter("_1", StringType, nullable = b, CSVOptions("nullValue", "null"))
      assert(converter.apply("") == UTF8String.fromString(""))
    }
  }

  test("Throws exception for empty string with non null type") {
    val exception = intercept[RuntimeException]{
      parser.makeConverter("_1", IntegerType, nullable = false, CSVOptions()).apply("")
    }
    assert(exception.getMessage.contains("null value found but field _1 is not nullable."))
  }

  test("Types are cast correctly") {
    assert(parser.makeConverter("_1", ByteType).apply("10") == 10)
    assert(parser.makeConverter("_1", ShortType).apply("10") == 10)
    assert(parser.makeConverter("_1", IntegerType).apply("10") == 10)
    assert(parser.makeConverter("_1", LongType).apply("10") == 10)
    assert(parser.makeConverter("_1", FloatType).apply("1.00") == 1.0)
    assert(parser.makeConverter("_1", DoubleType).apply("1.00") == 1.0)
    assert(parser.makeConverter("_1", BooleanType).apply("true") == true)

    val timestampsOptions = CSVOptions("timestampFormat", "dd/MM/yyyy hh:mm")
    val customTimestamp = "31/01/2015 00:00"
    val expectedTime = timestampsOptions.timestampFormat.parse(customTimestamp).getTime
    val castedTimestamp =
      parser.makeConverter("_1", TimestampType, nullable = true, timestampsOptions)
        .apply(customTimestamp)
    assert(castedTimestamp == expectedTime * 1000L)

    val customDate = "31/01/2015"
    val dateOptions = CSVOptions("dateFormat", "dd/MM/yyyy")
    val expectedDate = dateOptions.dateFormat.parse(customDate).getTime
    val castedDate =
      parser.makeConverter("_1", DateType, nullable = true, dateOptions)
        .apply(customTimestamp)
    assert(castedDate == DateTimeUtils.millisToDays(expectedDate))

    val timestamp = "2015-01-01 00:00:00"
    assert(parser.makeConverter("_1", TimestampType).apply(timestamp) ==
      DateTimeUtils.stringToTime(timestamp).getTime  * 1000L)
    assert(parser.makeConverter("_1", DateType).apply("2015-01-01") ==
      DateTimeUtils.millisToDays(DateTimeUtils.stringToTime("2015-01-01").getTime))
  }

  test("Float and Double Types are cast without respect to platform default Locale") {
    val originalLocale = Locale.getDefault
    try {
      Locale.setDefault(new Locale("fr", "FR"))
      // Would parse as 1.0 in fr-FR
      assert(parser.makeConverter("_1", FloatType).apply("1,00") == 100.0)
      assert(parser.makeConverter("_1", DoubleType).apply("1,00") == 100.0)
    } finally {
      Locale.setDefault(originalLocale)
    }
  }

  test("Float NaN values are parsed correctly") {
    val floatVal: Float = parser.makeConverter(
      "_1", FloatType, nullable = true, CSVOptions("nanValue", "nn")
    ).apply("nn").asInstanceOf[Float]

    // Java implements the IEEE-754 floating point standard which guarantees that any comparison
    // against NaN will return false (except != which returns true)
    assert(floatVal != floatVal)
  }

  test("Double NaN values are parsed correctly") {
    val doubleVal: Double = parser.makeConverter(
      "_1", DoubleType, nullable = true, CSVOptions("nanValue", "-")
    ).apply("-").asInstanceOf[Double]

    assert(doubleVal.isNaN)
  }

  test("Float infinite values can be parsed") {
    val floatVal1 = parser.makeConverter(
      "_1", FloatType, nullable = true, CSVOptions("negativeInf", "max")
    ).apply("max").asInstanceOf[Float]

    assert(floatVal1 == Float.NegativeInfinity)

    val floatVal2 = parser.makeConverter(
      "_1", FloatType, nullable = true, CSVOptions("positiveInf", "max")
    ).apply("max").asInstanceOf[Float]

    assert(floatVal2 == Float.PositiveInfinity)
  }

  test("Double infinite values can be parsed") {
    val doubleVal1 = parser.makeConverter(
      "_1", DoubleType, nullable = true, CSVOptions("negativeInf", "max")
    ).apply("max").asInstanceOf[Double]

    assert(doubleVal1 == Double.NegativeInfinity)

    val doubleVal2 = parser.makeConverter(
      "_1", DoubleType, nullable = true, CSVOptions("positiveInf", "max")
    ).apply("max").asInstanceOf[Double]

    assert(doubleVal2 == Double.PositiveInfinity)
  }

}
