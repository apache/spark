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
      assert(CSVTypeCast.castTo(strVal, "_1", decimalType) ===
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
      CSVTypeCast.castTo("-", "_1", ByteType, nullable = true, CSVOptions("nullValue", "-")))
    assertNull(
      CSVTypeCast.castTo("-", "_1", ShortType, nullable = true, CSVOptions("nullValue", "-")))
    assertNull(
      CSVTypeCast.castTo("-", "_1", IntegerType, nullable = true, CSVOptions("nullValue", "-")))
    assertNull(
      CSVTypeCast.castTo("-", "_1", LongType, nullable = true, CSVOptions("nullValue", "-")))
    assertNull(
      CSVTypeCast.castTo("-", "_1", FloatType, nullable = true, CSVOptions("nullValue", "-")))
    assertNull(
      CSVTypeCast.castTo("-", "_1", DoubleType, nullable = true, CSVOptions("nullValue", "-")))
    assertNull(
      CSVTypeCast.castTo("-", "_1", BooleanType, nullable = true, CSVOptions("nullValue", "-")))
    assertNull(
      CSVTypeCast.castTo("-", "_1", DecimalType.DoubleDecimal, true, CSVOptions("nullValue", "-")))
    assertNull(
      CSVTypeCast.castTo("-", "_1", TimestampType, nullable = true, CSVOptions("nullValue", "-")))
    assertNull(
      CSVTypeCast.castTo("-", "_1", DateType, nullable = true, CSVOptions("nullValue", "-")))
    assertNull(
      CSVTypeCast.castTo("-", "_1", StringType, nullable = true, CSVOptions("nullValue", "-")))
    assertNull(
      CSVTypeCast.castTo(null, "_1", IntegerType, nullable = true, CSVOptions("nullValue", "-")))

    // casting a null to not nullable field should throw an exception.
    var message = intercept[RuntimeException] {
      CSVTypeCast.castTo(null, "_1", IntegerType, nullable = false, CSVOptions("nullValue", "-"))
    }.getMessage
    assert(message.contains("null value found but field _1 is not nullable."))

    message = intercept[RuntimeException] {
      CSVTypeCast.castTo("-", "_1", StringType, nullable = false, CSVOptions("nullValue", "-"))
    }.getMessage
    assert(message.contains("null value found but field _1 is not nullable."))
  }

  test("String type should also respect `nullValue`") {
    assertNull(
      CSVTypeCast.castTo("", "_1", StringType, nullable = true, CSVOptions()))

    assert(
      CSVTypeCast.castTo("", "_1", StringType, nullable = true, CSVOptions("nullValue", "null")) ==
        UTF8String.fromString(""))
    assert(
      CSVTypeCast.castTo("", "_1", StringType, nullable = false, CSVOptions("nullValue", "null")) ==
        UTF8String.fromString(""))

    assertNull(
      CSVTypeCast.castTo(null, "_1", StringType, nullable = true, CSVOptions("nullValue", "null")))
  }

  test("Throws exception for empty string with non null type") {
    val exception = intercept[RuntimeException]{
      CSVTypeCast.castTo("", "_1", IntegerType, nullable = false, CSVOptions())
    }
    assert(exception.getMessage.contains("null value found but field _1 is not nullable."))
  }

  test("Types are cast correctly") {
    assert(CSVTypeCast.castTo("10", "_1", ByteType) == 10)
    assert(CSVTypeCast.castTo("10", "_1", ShortType) == 10)
    assert(CSVTypeCast.castTo("10", "_1", IntegerType) == 10)
    assert(CSVTypeCast.castTo("10", "_1", LongType) == 10)
    assert(CSVTypeCast.castTo("1.00", "_1", FloatType) == 1.0)
    assert(CSVTypeCast.castTo("1.00", "_1", DoubleType) == 1.0)
    assert(CSVTypeCast.castTo("true", "_1", BooleanType) == true)

    val timestampsOptions = CSVOptions("timestampFormat", "dd/MM/yyyy hh:mm")
    val customTimestamp = "31/01/2015 00:00"
    val expectedTime = timestampsOptions.timestampFormat.parse(customTimestamp).getTime
    val castedTimestamp =
      CSVTypeCast.castTo(customTimestamp, "_1", TimestampType, nullable = true, timestampsOptions)
    assert(castedTimestamp == expectedTime * 1000L)

    val customDate = "31/01/2015"
    val dateOptions = CSVOptions("dateFormat", "dd/MM/yyyy")
    val expectedDate = dateOptions.dateFormat.parse(customDate).getTime
    val castedDate =
      CSVTypeCast.castTo(customTimestamp, "_1", DateType, nullable = true, dateOptions)
    assert(castedDate == DateTimeUtils.millisToDays(expectedDate))

    val timestamp = "2015-01-01 00:00:00"
    assert(CSVTypeCast.castTo(timestamp, "_1", TimestampType) ==
      DateTimeUtils.stringToTime(timestamp).getTime  * 1000L)
    assert(CSVTypeCast.castTo("2015-01-01", "_1", DateType) ==
      DateTimeUtils.millisToDays(DateTimeUtils.stringToTime("2015-01-01").getTime))
  }

  test("Float and Double Types are cast without respect to platform default Locale") {
    val originalLocale = Locale.getDefault
    try {
      Locale.setDefault(new Locale("fr", "FR"))
      assert(CSVTypeCast.castTo("1,00", "_1", FloatType) == 100.0) // Would parse as 1.0 in fr-FR
      assert(CSVTypeCast.castTo("1,00", "_1", DoubleType) == 100.0)
    } finally {
      Locale.setDefault(originalLocale)
    }
  }

  test("Float NaN values are parsed correctly") {
    val floatVal: Float = CSVTypeCast.castTo(
      "nn", "_1", FloatType, nullable = true, CSVOptions("nanValue", "nn")).asInstanceOf[Float]

    // Java implements the IEEE-754 floating point standard which guarantees that any comparison
    // against NaN will return false (except != which returns true)
    assert(floatVal != floatVal)
  }

  test("Double NaN values are parsed correctly") {
    val doubleVal: Double = CSVTypeCast.castTo(
      "-", "_1", DoubleType, nullable = true, CSVOptions("nanValue", "-")).asInstanceOf[Double]

    assert(doubleVal.isNaN)
  }

  test("Float infinite values can be parsed") {
    val floatVal1 = CSVTypeCast.castTo(
      "max", "_1", FloatType, nullable = true, CSVOptions("negativeInf", "max")).asInstanceOf[Float]

    assert(floatVal1 == Float.NegativeInfinity)

    val floatVal2 = CSVTypeCast.castTo(
      "max", "_1", FloatType, nullable = true, CSVOptions("positiveInf", "max")).asInstanceOf[Float]

    assert(floatVal2 == Float.PositiveInfinity)
  }

  test("Double infinite values can be parsed") {
    val doubleVal1 = CSVTypeCast.castTo(
      "max", "_1", DoubleType, nullable = true, CSVOptions("negativeInf", "max")
    ).asInstanceOf[Double]

    assert(doubleVal1 == Double.NegativeInfinity)

    val doubleVal2 = CSVTypeCast.castTo(
      "max", "_1", DoubleType, nullable = true, CSVOptions("positiveInf", "max")
    ).asInstanceOf[Double]

    assert(doubleVal2 == Double.PositiveInfinity)
  }

}
