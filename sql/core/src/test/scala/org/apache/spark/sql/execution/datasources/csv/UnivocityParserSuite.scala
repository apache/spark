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
  private def getParser(options: CSVOptions) = {
    new UnivocityParser(StructType(Seq.empty), options)
  }

  private def assertNull(v: Any) = assert(v == null)

  test("Can parse decimal type values") {
    val stringValues = Seq("10.05", "1,000.01", "158,058,049.001")
    val decimalValues = Seq(10.05, 1000.01, 158058049.001)
    val decimalType = new DecimalType()

    stringValues.zip(decimalValues).foreach { case (strVal, decimalVal) =>
      val decimalValue = new BigDecimal(decimalVal.toString)
      val options = new CSVOptions(Map.empty[String, String], false, "GMT")
      assert(
        getParser(options)
          .makeConverter("_1", decimalType, options = options)
          .apply(strVal) === Decimal(decimalValue, decimalType.precision, decimalType.scale))
    }
  }

  test("Nullable types are handled") {
    val types = Seq(ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType,
      BooleanType, DecimalType.DoubleDecimal, TimestampType, DateType, StringType)

    // Nullable field with nullValue option.
    types.foreach { t =>
      // Tests that a custom nullValue.
      val nullValueOptions = new CSVOptions(Map("nullValue" -> "-"), false, "GMT")
      val converter = getParser(nullValueOptions)
        .makeConverter("_1", t, nullable = true, options = nullValueOptions)
      assertNull(converter.apply("-"))
      assertNull(converter.apply(null))

      // Tests that the default nullValue is empty string.
      val options = new CSVOptions(Map.empty[String, String], false, "GMT")
      val parser = getParser(options)
      assertNull(parser.makeConverter("_1", t, nullable = true, options = options).apply(""))
    }

    // Not nullable field with nullValue option.
    types.foreach { t =>
      // Casts a null to not nullable field should throw an exception.
      val options = new CSVOptions(Map("nullValue" -> "-"), false, "GMT")
      val converter = getParser(options)
        .makeConverter("_1", t, nullable = false, options = options)
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
      val options = new CSVOptions(Map("nullValue" -> "null"), false, "GMT")
      val converter = getParser(options)
        .makeConverter("_1", StringType, nullable = b, options = options)
      assert(converter.apply("") == UTF8String.fromString(""))
    }
  }

  test("Throws exception for empty string with non null type") {
    val options = new CSVOptions(Map.empty[String, String], false, "GMT")
    val exception = intercept[RuntimeException]{
      getParser(options)
        .makeConverter("_1", IntegerType, nullable = false, options = options)
        .apply("")
    }
    assert(exception.getMessage.contains("null value found but field _1 is not nullable."))
  }

  test("Types are cast correctly") {
    val options = new CSVOptions(Map.empty[String, String], false, "GMT")
    val parser = getParser(options)
    assert(parser.makeConverter("_1", ByteType, options = options).apply("10") == 10)
    assert(parser.makeConverter("_1", ShortType, options = options).apply("10") == 10)
    assert(parser.makeConverter("_1", IntegerType, options = options).apply("10") == 10)
    assert(parser.makeConverter("_1", LongType, options = options).apply("10") == 10)
    assert(parser.makeConverter("_1", FloatType, options = options).apply("1.00") == 1.0)
    assert(parser.makeConverter("_1", DoubleType, options = options).apply("1.00") == 1.0)
    assert(parser.makeConverter("_1", BooleanType, options = options).apply("true") == true)

    val timestampsOptions =
      new CSVOptions(Map("timestampFormat" -> "dd/MM/yyyy hh:mm"), false, "GMT")
    val customTimestamp = "31/01/2015 00:00"
    val expectedTime = timestampsOptions.timestampFormat.parse(customTimestamp).getTime
    val castedTimestamp = getParser(timestampsOptions)
      .makeConverter("_1", TimestampType, nullable = true, options = timestampsOptions)
      .apply(customTimestamp)
    assert(castedTimestamp == expectedTime * 1000L)

    val customDate = "31/01/2015"
    val dateOptions = new CSVOptions(Map("dateFormat" -> "dd/MM/yyyy"), false, "GMT")
    val expectedDate = dateOptions.dateFormat.parse(customDate).getTime
    val castedDate = getParser(dateOptions)
      .makeConverter("_1", DateType, nullable = true, options = dateOptions)
      .apply(customTimestamp)
    assert(castedDate == DateTimeUtils.millisToDays(expectedDate))

    val timestamp = "2015-01-01 00:00:00"
    assert(parser.makeConverter("_1", TimestampType, options = options).apply(timestamp) ==
      DateTimeUtils.stringToTime(timestamp).getTime  * 1000L)
    assert(parser.makeConverter("_1", DateType, options = options).apply("2015-01-01") ==
      DateTimeUtils.millisToDays(DateTimeUtils.stringToTime("2015-01-01").getTime))
  }

  test("Throws exception for casting an invalid string to Float and Double Types") {
    val options = new CSVOptions(Map.empty[String, String], false, "GMT")
    val types = Seq(DoubleType, FloatType)
    val input = Seq("10u000", "abc", "1 2/3")
    types.foreach { dt =>
      input.foreach { v =>
        val message = intercept[NumberFormatException] {
          getParser(options).makeConverter("_1", dt, options = options).apply(v)
        }.getMessage
        assert(message.contains(v))
      }
    }
  }

  test("Float NaN values are parsed correctly") {
    val options = new CSVOptions(Map("nanValue" -> "nn"), false, "GMT")
    val floatVal: Float = getParser(options).makeConverter(
      "_1", FloatType, nullable = true, options = options
    ).apply("nn").asInstanceOf[Float]

    // Java implements the IEEE-754 floating point standard which guarantees that any comparison
    // against NaN will return false (except != which returns true)
    assert(floatVal != floatVal)
  }

  test("Double NaN values are parsed correctly") {
    val options = new CSVOptions(Map("nanValue" -> "-"), false, "GMT")
    val doubleVal: Double = getParser(options).makeConverter(
      "_1", DoubleType, nullable = true, options = options
    ).apply("-").asInstanceOf[Double]

    assert(doubleVal.isNaN)
  }

  test("Float infinite values can be parsed") {
    val negativeInfOptions = new CSVOptions(Map("negativeInf" -> "max"), false, "GMT")
    val floatVal1 = getParser(negativeInfOptions).makeConverter(
      "_1", FloatType, nullable = true, options = negativeInfOptions
    ).apply("max").asInstanceOf[Float]

    assert(floatVal1 == Float.NegativeInfinity)

    val positiveInfOptions = new CSVOptions(Map("positiveInf" -> "max"), false, "GMT")
    val floatVal2 = getParser(positiveInfOptions).makeConverter(
      "_1", FloatType, nullable = true, options = positiveInfOptions
    ).apply("max").asInstanceOf[Float]

    assert(floatVal2 == Float.PositiveInfinity)
  }

  test("Double infinite values can be parsed") {
    val negativeInfOptions = new CSVOptions(Map("negativeInf" -> "max"), false, "GMT")
    val doubleVal1 = getParser(negativeInfOptions).makeConverter(
      "_1", DoubleType, nullable = true, options = negativeInfOptions
    ).apply("max").asInstanceOf[Double]

    assert(doubleVal1 == Double.NegativeInfinity)

    val positiveInfOptions = new CSVOptions(Map("positiveInf" -> "max"), false, "GMT")
    val doubleVal2 = getParser(positiveInfOptions).makeConverter(
      "_1", DoubleType, nullable = true, options = positiveInfOptions
    ).apply("max").asInstanceOf[Double]

    assert(doubleVal2 == Double.PositiveInfinity)
  }
}
