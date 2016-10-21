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

class CSVTypeCastSuite extends SparkFunSuite {

  private def assertNull(v: Any) = assert(v == null)

  test("Can parse decimal type values") {
    val stringValues = Seq("10.05", "1,000.01", "158,058,049.001")
    val decimalValues = Seq(10.05, 1000.01, 158058049.001)
    val decimalType = new DecimalType()

    stringValues.zip(decimalValues).foreach { case (strVal, decimalVal) =>
      val decimalValue = new BigDecimal(decimalVal.toString)
      assert(CSVTypeCast.castTo(strVal, decimalType) ===
        Decimal(decimalValue, decimalType.precision, decimalType.scale))
    }
  }

  test("Can parse escaped characters") {
    assert(CSVTypeCast.toChar("""\t""") === '\t')
    assert(CSVTypeCast.toChar("""\r""") === '\r')
    assert(CSVTypeCast.toChar("""\b""") === '\b')
    assert(CSVTypeCast.toChar("""\f""") === '\f')
    assert(CSVTypeCast.toChar("""\"""") === '\"')
    assert(CSVTypeCast.toChar("""\'""") === '\'')
    assert(CSVTypeCast.toChar("""\u0000""") === '\u0000')
  }

  test("Does not accept delimiter larger than one character") {
    val exception = intercept[IllegalArgumentException]{
      CSVTypeCast.toChar("ab")
    }
    assert(exception.getMessage.contains("cannot be more than one character"))
  }

  test("Throws exception for unsupported escaped characters") {
    val exception = intercept[IllegalArgumentException]{
      CSVTypeCast.toChar("""\1""")
    }
    assert(exception.getMessage.contains("Unsupported special character for delimiter"))
  }

  test("Nullable types are handled") {
    assertNull(
      CSVTypeCast.castTo("-", ByteType, nullable = true, CSVOptions("nullValue", "-")))
    assertNull(
      CSVTypeCast.castTo("-", ShortType, nullable = true, CSVOptions("nullValue", "-")))
    assertNull(
      CSVTypeCast.castTo("-", IntegerType, nullable = true, CSVOptions("nullValue", "-")))
    assertNull(
      CSVTypeCast.castTo("-", LongType, nullable = true, CSVOptions("nullValue", "-")))
    assertNull(
      CSVTypeCast.castTo("-", FloatType, nullable = true, CSVOptions("nullValue", "-")))
    assertNull(
      CSVTypeCast.castTo("-", DoubleType, nullable = true, CSVOptions("nullValue", "-")))
    assertNull(
      CSVTypeCast.castTo("-", BooleanType, nullable = true, CSVOptions("nullValue", "-")))
    assertNull(
      CSVTypeCast.castTo("-", DecimalType.DoubleDecimal, true, CSVOptions("nullValue", "-")))
    assertNull(
      CSVTypeCast.castTo("-", TimestampType, nullable = true, CSVOptions("nullValue", "-")))
    assertNull(
      CSVTypeCast.castTo("-", DateType, nullable = true, CSVOptions("nullValue", "-")))
    assertNull(
      CSVTypeCast.castTo("-", StringType, nullable = true, CSVOptions("nullValue", "-")))
  }

  test("String type should also respect `nullValue`") {
    assertNull(
      CSVTypeCast.castTo("", StringType, nullable = true, CSVOptions()))
    assert(
      CSVTypeCast.castTo("", StringType, nullable = false, CSVOptions()) ==
        UTF8String.fromString(""))

    assert(
      CSVTypeCast.castTo("", StringType, nullable = true, CSVOptions("nullValue", "null")) ==
        UTF8String.fromString(""))
    assert(
      CSVTypeCast.castTo("", StringType, nullable = false, CSVOptions("nullValue", "null")) ==
        UTF8String.fromString(""))

    assertNull(
      CSVTypeCast.castTo(null, StringType, nullable = true, CSVOptions("nullValue", "null")))
  }

  test("Throws exception for empty string with non null type") {
    val exception = intercept[NumberFormatException]{
      CSVTypeCast.castTo("", IntegerType, nullable = false, CSVOptions())
    }
    assert(exception.getMessage.contains("For input string: \"\""))
  }

  test("Types are cast correctly") {
    assert(CSVTypeCast.castTo("10", ByteType) == 10)
    assert(CSVTypeCast.castTo("10", ShortType) == 10)
    assert(CSVTypeCast.castTo("10", IntegerType) == 10)
    assert(CSVTypeCast.castTo("10", LongType) == 10)
    assert(CSVTypeCast.castTo("1.00", FloatType) == 1.0)
    assert(CSVTypeCast.castTo("1.00", DoubleType) == 1.0)
    assert(CSVTypeCast.castTo("true", BooleanType) == true)

    val timestampsOptions = CSVOptions("timestampFormat", "dd/MM/yyyy hh:mm")
    val customTimestamp = "31/01/2015 00:00"
    val expectedTime = timestampsOptions.timestampFormat.parse(customTimestamp).getTime
    val castedTimestamp =
      CSVTypeCast.castTo(customTimestamp, TimestampType, nullable = true, timestampsOptions)
    assert(castedTimestamp == expectedTime * 1000L)

    val customDate = "31/01/2015"
    val dateOptions = CSVOptions("dateFormat", "dd/MM/yyyy")
    val expectedDate = dateOptions.dateFormat.parse(customDate).getTime
    val castedDate = CSVTypeCast.castTo(customTimestamp, DateType, nullable = true, dateOptions)
    assert(castedDate == DateTimeUtils.millisToDays(expectedDate))

    val timestamp = "2015-01-01 00:00:00"
    assert(CSVTypeCast.castTo(timestamp, TimestampType) ==
      DateTimeUtils.stringToTime(timestamp).getTime  * 1000L)
    assert(CSVTypeCast.castTo("2015-01-01", DateType) ==
      DateTimeUtils.millisToDays(DateTimeUtils.stringToTime("2015-01-01").getTime))
  }

  test("Float and Double Types are cast correctly with Locale") {
    val originalLocale = Locale.getDefault
    try {
      val locale : Locale = new Locale("fr", "FR")
      Locale.setDefault(locale)
      assert(CSVTypeCast.castTo("1,00", FloatType) == 1.0)
      assert(CSVTypeCast.castTo("1,00", DoubleType) == 1.0)
    } finally {
      Locale.setDefault(originalLocale)
    }
  }

  test("Float NaN values are parsed correctly") {
    val floatVal: Float = CSVTypeCast.castTo(
      "nn", FloatType, nullable = true, CSVOptions("nanValue", "nn")).asInstanceOf[Float]

    // Java implements the IEEE-754 floating point standard which guarantees that any comparison
    // against NaN will return false (except != which returns true)
    assert(floatVal != floatVal)
  }

  test("Double NaN values are parsed correctly") {
    val doubleVal: Double = CSVTypeCast.castTo(
      "-", DoubleType, nullable = true, CSVOptions("nanValue", "-")).asInstanceOf[Double]

    assert(doubleVal.isNaN)
  }

  test("Float infinite values can be parsed") {
    val floatVal1 = CSVTypeCast.castTo(
      "max", FloatType, nullable = true, CSVOptions("negativeInf", "max")).asInstanceOf[Float]

    assert(floatVal1 == Float.NegativeInfinity)

    val floatVal2 = CSVTypeCast.castTo(
      "max", FloatType, nullable = true, CSVOptions("positiveInf", "max")).asInstanceOf[Float]

    assert(floatVal2 == Float.PositiveInfinity)
  }

  test("Double infinite values can be parsed") {
    val doubleVal1 = CSVTypeCast.castTo(
      "max", DoubleType, nullable = true, CSVOptions("negativeInf", "max")
    ).asInstanceOf[Double]

    assert(doubleVal1 == Double.NegativeInfinity)

    val doubleVal2 = CSVTypeCast.castTo(
      "max", DoubleType, nullable = true, CSVOptions("positiveInf", "max")
    ).asInstanceOf[Double]

    assert(doubleVal2 == Double.PositiveInfinity)
  }

}
