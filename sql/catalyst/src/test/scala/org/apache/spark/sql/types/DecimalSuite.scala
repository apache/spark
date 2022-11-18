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

package org.apache.spark.sql.types

import org.scalatest.PrivateMethodTester

import org.apache.spark.{SparkArithmeticException, SparkFunSuite, SparkNumberFormatException}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.Decimal._
import org.apache.spark.unsafe.types.UTF8String

class DecimalSuite extends SparkFunSuite with PrivateMethodTester with SQLHelper {
  /** Check that a Decimal has the given string representation, precision and scale */
  private def checkDecimal(d: Decimal, string: String, precision: Int, scale: Int): Unit = {
    assert(d.toString === string)
    assert(d.precision === precision)
    assert(d.scale === scale)
  }

  test("creating decimals") {
    checkDecimal(new Decimal(), "0", 1, 0)
    checkDecimal(Decimal(BigDecimal("0.09")), "0.09", 2, 2)
    checkDecimal(Decimal(BigDecimal("0.9")), "0.9", 1, 1)
    checkDecimal(Decimal(BigDecimal("0.90")), "0.90", 2, 2)
    checkDecimal(Decimal(BigDecimal("0.0")), "0.0", 1, 1)
    checkDecimal(Decimal(BigDecimal("0")), "0", 1, 0)
    checkDecimal(Decimal(BigDecimal("1.0")), "1.0", 2, 1)
    checkDecimal(Decimal(BigDecimal("-0.09")), "-0.09", 2, 2)
    checkDecimal(Decimal(BigDecimal("-0.9")), "-0.9", 1, 1)
    checkDecimal(Decimal(BigDecimal("-0.90")), "-0.90", 2, 2)
    checkDecimal(Decimal(BigDecimal("-1.0")), "-1.0", 2, 1)
    checkDecimal(Decimal(BigDecimal("10.030")), "10.030", 5, 3)
    checkDecimal(Decimal(BigDecimal("10.030"), 4, 1), "10.0", 4, 1)
    checkDecimal(Decimal(BigDecimal("-9.95"), 4, 1), "-10.0", 4, 1)
    checkDecimal(Decimal("10.030"), "10.030", 5, 3)
    checkDecimal(Decimal(10.03), "10.03", 4, 2)
    checkDecimal(Decimal(17L), "17", 20, 0)
    checkDecimal(Decimal(17), "17", 10, 0)
    checkDecimal(Decimal(17L, 2, 1), "1.7", 2, 1)
    checkDecimal(Decimal(170L, 4, 2), "1.70", 4, 2)
    checkDecimal(Decimal(17L, 24, 1), "1.7", 24, 1)
    checkDecimal(Decimal(1e17.toLong, 18, 0), 1e17.toLong.toString, 18, 0)
    checkDecimal(Decimal(1000000000000000000L, 20, 2), "10000000000000000.00", 20, 2)
    checkDecimal(Decimal(Long.MaxValue), Long.MaxValue.toString, 20, 0)
    checkDecimal(Decimal(Long.MinValue), Long.MinValue.toString, 20, 0)

    checkError(
      exception = intercept[SparkArithmeticException](Decimal(170L, 2, 1)),
      errorClass = "NUMERIC_VALUE_OUT_OF_RANGE",
      parameters = Map(
        "value" -> "0",
        "precision" -> "2",
        "scale" -> "1",
        "config" -> "\"spark.sql.ansi.enabled\""))
    checkError(
      exception = intercept[SparkArithmeticException](Decimal(170L, 2, 0)),
      errorClass = "NUMERIC_VALUE_OUT_OF_RANGE",
      parameters = Map(
        "value" -> "0",
        "precision" -> "2",
        "scale" -> "0",
        "config" -> "\"spark.sql.ansi.enabled\""))
    checkError(
      exception = intercept[SparkArithmeticException](Decimal(BigDecimal("10.030"), 2, 1)),
      errorClass = "DECIMAL_PRECISION_EXCEEDS_MAX_PRECISION",
      parameters = Map("precision" -> "3", "maxPrecision" -> "2"))
    checkError(
      exception = intercept[SparkArithmeticException](Decimal(BigDecimal("-9.95"), 2, 1)),
      errorClass = "DECIMAL_PRECISION_EXCEEDS_MAX_PRECISION",
      parameters = Map("precision" -> "3", "maxPrecision" -> "2"))
    checkError(
      exception = intercept[SparkArithmeticException](Decimal(1e17.toLong, 17, 0)),
      errorClass = "NUMERIC_VALUE_OUT_OF_RANGE",
      parameters = Map(
        "value" -> "0",
        "precision" -> "17",
        "scale" -> "0",
        "config" -> "\"spark.sql.ansi.enabled\""))
  }

  test("creating decimals with negative scale under legacy mode") {
    withSQLConf(SQLConf.LEGACY_ALLOW_NEGATIVE_SCALE_OF_DECIMAL_ENABLED.key -> "true") {
      checkDecimal(Decimal(BigDecimal("98765"), 5, -3), "9.9E+4", 5, -3)
      checkDecimal(Decimal(BigDecimal("314.159"), 6, -2), "3E+2", 6, -2)
      checkDecimal(Decimal(BigDecimal(1.579e12), 4, -9), "1.579E+12", 4, -9)
      checkDecimal(Decimal(BigDecimal(1.579e12), 4, -10), "1.58E+12", 4, -10)
      checkDecimal(Decimal(103050709L, 9, -10), "1.03050709E+18", 9, -10)
      checkDecimal(Decimal(1e8.toLong, 10, -10), "1.00000000E+18", 10, -10)
    }
  }

  test("SPARK-30252: Negative scale is not allowed by default") {
    def checkNegativeScaleDecimal(d: => Decimal): Unit = {
      intercept[AnalysisException](d)
        .getMessage
        .contains("Negative scale is not allowed under ansi mode")
    }
    checkNegativeScaleDecimal(Decimal(BigDecimal("98765"), 5, -3))
    checkNegativeScaleDecimal(Decimal(BigDecimal("98765").underlying(), 5, -3))
    checkNegativeScaleDecimal(Decimal(98765L, 5, -3))
    checkNegativeScaleDecimal(Decimal.createUnsafe(98765L, 5, -3))
  }

  test("double and long values") {
    /** Check that a Decimal converts to the given double and long values */
    def checkValues(d: Decimal, doubleValue: Double, longValue: Long): Unit = {
      assert(d.toDouble === doubleValue)
      assert(d.toLong === longValue)
    }

    checkValues(new Decimal(), 0.0, 0L)
    checkValues(Decimal(BigDecimal("10.030")), 10.03, 10L)
    checkValues(Decimal(BigDecimal("10.030"), 4, 1), 10.0, 10L)
    checkValues(Decimal(BigDecimal("-9.95"), 4, 1), -10.0, -10L)
    checkValues(Decimal(10.03), 10.03, 10L)
    checkValues(Decimal(17L), 17.0, 17L)
    checkValues(Decimal(17), 17.0, 17L)
    checkValues(Decimal(17L, 2, 1), 1.7, 1L)
    checkValues(Decimal(170L, 4, 2), 1.7, 1L)
    checkValues(Decimal(1e16.toLong), 1e16, 1e16.toLong)
    checkValues(Decimal(1e17.toLong), 1e17, 1e17.toLong)
    checkValues(Decimal(1e18.toLong), 1e18, 1e18.toLong)
    checkValues(Decimal(2e18.toLong), 2e18, 2e18.toLong)
    checkValues(Decimal(Long.MaxValue), Long.MaxValue.toDouble, Long.MaxValue)
    checkValues(Decimal(Long.MinValue), Long.MinValue.toDouble, Long.MinValue)
    checkValues(Decimal(Double.MaxValue), Double.MaxValue, 0L)
    checkValues(Decimal(Double.MinValue), Double.MinValue, 0L)
  }

  // Accessor for the BigDecimal value of a Decimal, which will be null if it's using Longs
  private val decimalVal = PrivateMethod[BigDecimal](Symbol("decimalVal"))

  /** Check whether a decimal is represented compactly (passing whether we expect it to be) */
  private def checkCompact(d: Decimal, expected: Boolean): Unit = {
    val isCompact = d.invokePrivate(decimalVal()).eq(null)
    assert(isCompact == expected, s"$d ${if (expected) "was not" else "was"} compact")
  }

  test("small decimals represented as unscaled long") {
    checkCompact(new Decimal(), true)
    checkCompact(Decimal(BigDecimal("10.03")), false)
    checkCompact(Decimal(BigDecimal("100000000000000000000")), false)
    checkCompact(Decimal(17L), true)
    checkCompact(Decimal(17), true)
    checkCompact(Decimal(17L, 2, 1), true)
    checkCompact(Decimal(170L, 4, 2), true)
    checkCompact(Decimal(17L, 24, 1), true)
    checkCompact(Decimal(1e16.toLong), true)
    checkCompact(Decimal(1e17.toLong), true)
    checkCompact(Decimal(1e18.toLong - 1), true)
    checkCompact(Decimal(- 1e18.toLong + 1), true)
    checkCompact(Decimal(1e18.toLong - 1, 30, 10), true)
    checkCompact(Decimal(- 1e18.toLong + 1, 30, 10), true)
    checkCompact(Decimal(1e18.toLong), false)
    checkCompact(Decimal(-1e18.toLong), false)
    checkCompact(Decimal(1e18.toLong, 30, 10), false)
    checkCompact(Decimal(-1e18.toLong, 30, 10), false)
    checkCompact(Decimal(Long.MaxValue), false)
    checkCompact(Decimal(Long.MinValue), false)
  }

  test("hash code") {
    assert(Decimal(123).hashCode() === (123).##)
    assert(Decimal(-123).hashCode() === (-123).##)
    assert(Decimal(Int.MaxValue).hashCode() === Int.MaxValue.##)
    assert(Decimal(Long.MaxValue).hashCode() === Long.MaxValue.##)
    assert(Decimal(BigDecimal(123)).hashCode() === (123).##)

    val reallyBig = BigDecimal("123182312312313232112312312123.1231231231")
    assert(Decimal(reallyBig).hashCode() === reallyBig.hashCode)
  }

  test("equals") {
    // The decimals on the left are stored compactly, while the ones on the right aren't
    checkCompact(Decimal(123), true)
    checkCompact(Decimal(BigDecimal(123)), false)
    checkCompact(Decimal("123"), false)
    assert(Decimal(123) === Decimal(BigDecimal(123)))
    assert(Decimal(123) === Decimal(BigDecimal("123.00")))
    assert(Decimal(-123) === Decimal(BigDecimal(-123)))
    assert(Decimal(-123) === Decimal(BigDecimal("-123.00")))
  }

  test("isZero") {
    assert(Decimal(0).isZero)
    assert(Decimal(0, 4, 2).isZero)
    assert(Decimal("0").isZero)
    assert(Decimal("0.000").isZero)
    assert(!Decimal(1).isZero)
    assert(!Decimal(1, 4, 2).isZero)
    assert(!Decimal("1").isZero)
    assert(!Decimal("0.001").isZero)
  }

  test("arithmetic") {
    assert(Decimal(100) + Decimal(-100) === Decimal(0))
    assert(Decimal(100) + Decimal(-100) === Decimal(0))
    assert(Decimal(100) * Decimal(-100) === Decimal(-10000))
    assert(Decimal(1e13) * Decimal(1e13) === Decimal(1e26))
    assert(Decimal(100) / Decimal(-100) === Decimal(-1))
    assert(Decimal(100) / Decimal(0) === null)
    assert(Decimal(100) % Decimal(-100) === Decimal(0))
    assert(Decimal(100) % Decimal(3) === Decimal(1))
    assert(Decimal(-100) % Decimal(3) === Decimal(-1))
    assert(Decimal(100) % Decimal(0) === null)
  }

  test("longVal arithmetic") {
    assert(Decimal(10, 2, 0) + Decimal(10, 2, 0) === Decimal(20, 3, 0))
    assert(Decimal(10, 2, 0) + Decimal(90, 2, 0) === Decimal(100, 3, 0))
    assert(Decimal(10, 2, 0) - Decimal(-10, 2, 0) === Decimal(20, 3, 0))
    assert(Decimal(10, 2, 0) - Decimal(-90, 2, 0) === Decimal(100, 3, 0))
  }

  test("quot") {
    assert(Decimal(100).quot(Decimal(100)) === Decimal(BigDecimal("1")))
    assert(Decimal(100).quot(Decimal(33)) === Decimal(BigDecimal("3")))
    assert(Decimal(100).quot(Decimal(-100)) === Decimal(BigDecimal("-1")))
    assert(Decimal(100).quot(Decimal(-33)) === Decimal(BigDecimal("-3")))
  }

  test("negate & abs") {
    assert(-Decimal(100) === Decimal(BigDecimal("-100")))
    assert(-Decimal(-100) === Decimal(BigDecimal("100")))
    assert(Decimal(100).abs === Decimal(BigDecimal("100")))
    assert(Decimal(-100).abs === Decimal(BigDecimal("100")))
  }

  test("floor & ceil") {
    assert(Decimal("10.03").floor === Decimal(BigDecimal("10")))
    assert(Decimal("10.03").ceil === Decimal(BigDecimal("11")))
    assert(Decimal("-10.03").floor === Decimal(BigDecimal("-11")))
    assert(Decimal("-10.03").ceil === Decimal(BigDecimal("-10")))
  }

  // regression test for SPARK-8359
  test("accurate precision after multiplication") {
    val decimal = (Decimal(Long.MaxValue, 38, 0) * Decimal(Long.MaxValue, 38, 0)).toJavaBigDecimal
    assert(decimal.unscaledValue.toString === "85070591730234615847396907784232501249")
  }

  // regression test for SPARK-8677
  test("fix non-terminating decimal expansion problem") {
    val decimal = Decimal(1.0, 10, 3) / Decimal(3.0, 10, 3)
    // The difference between decimal should not be more than 0.001.
    assert(decimal.toDouble - 0.333 < 0.001)
  }

  // regression test for SPARK-8800
  test("fix loss of precision/scale when doing division operation") {
    val a = Decimal(2) / Decimal(3)
    assert(a.toDouble < 1.0 && a.toDouble > 0.6)
    val b = Decimal(1) / Decimal(8)
    assert(b.toDouble === 0.125)
  }

  test("set/setOrNull") {
    assert(new Decimal().set(10L, 10, 0).toUnscaledLong === 10L)
    assert(new Decimal().set(100L, 10, 0).toUnscaledLong === 100L)
    assert(Decimal(Long.MaxValue, 100, 0).toUnscaledLong === Long.MaxValue)
  }

  test("changePrecision/toPrecision on compact decimal should respect rounding mode") {
    Seq(ROUND_FLOOR, ROUND_CEILING, ROUND_HALF_UP, ROUND_HALF_EVEN).foreach { mode =>
      Seq("0.4", "0.5", "0.6", "1.0", "1.1", "1.6", "2.5", "5.5").foreach { n =>
        Seq("", "-").foreach { sign =>
          val bd = BigDecimal(sign + n)
          val unscaled = (bd * 10).toLongExact
          val d = Decimal(unscaled, 8, 1)
          assert(d.changePrecision(10, 0, mode))
          assert(d.toString === bd.setScale(0, mode).toString(), s"num: $sign$n, mode: $mode")

          val copy = d.toPrecision(10, 0, mode)
          assert(copy !== null)
          assert(d.ne(copy))
          assert(d === copy)
          assert(copy.toString === bd.setScale(0, mode).toString(), s"num: $sign$n, mode: $mode")
        }
      }
    }
  }

  test("SPARK-20341: support BigInt's value does not fit in long value range") {
    val bigInt = scala.math.BigInt("9223372036854775808")
    val decimal = Decimal.apply(bigInt)
    assert(decimal.toJavaBigDecimal.unscaledValue.toString === "9223372036854775808")
  }

  test("SPARK-26038: toScalaBigInt/toJavaBigInteger") {
    // not fitting long
    val decimal = Decimal("1234568790123456789012348790.1234879012345678901234568790")
    assert(decimal.toScalaBigInt == scala.math.BigInt("1234568790123456789012348790"))
    assert(decimal.toJavaBigInteger == new java.math.BigInteger("1234568790123456789012348790"))
    // fitting long
    val decimalLong = Decimal(123456789123456789L, 18, 9)
    assert(decimalLong.toScalaBigInt == scala.math.BigInt("123456789"))
    assert(decimalLong.toJavaBigInteger == new java.math.BigInteger("123456789"))
  }

  test("UTF8String to Decimal") {
    def checkFromString(string: String): Unit = {
      assert(Decimal.fromString(UTF8String.fromString(string)) === Decimal(string))
      assert(Decimal.fromStringANSI(UTF8String.fromString(string)) === Decimal(string))
    }

    def checkOutOfRangeFromString(string: String): Unit = {
      assert(Decimal.fromString(UTF8String.fromString(string)) === null)
      checkError(
        exception = intercept[SparkArithmeticException](
          Decimal.fromStringANSI(UTF8String.fromString(string))),
        errorClass = "NUMERIC_OUT_OF_SUPPORTED_RANGE",
        parameters = Map("value" -> string))
    }

    checkFromString("12345678901234567890123456789012345678")
    checkOutOfRangeFromString("123456789012345678901234567890123456789")

    checkFromString("0.00000000000000000000000000000000000001")
    checkFromString("0.000000000000000000000000000000000000000000000001")

    checkFromString("6E-640")

    checkFromString("6E+37")
    checkOutOfRangeFromString("6E+38")
    checkOutOfRangeFromString("6.0790316E+25569151")

    assert(Decimal.fromString(UTF8String.fromString("str")) === null)
    checkError(
      exception = intercept[SparkNumberFormatException](
        Decimal.fromStringANSI(UTF8String.fromString("str"))),
      errorClass = "CAST_INVALID_INPUT",
      parameters = Map(
        "expression" -> "'str'",
        "sourceType" -> "\"STRING\"",
        "targetType" -> "\"DECIMAL(10,0)\"",
        "ansiConfig" -> "\"spark.sql.ansi.enabled\""))
  }

  test("SPARK-35841: Casting string to decimal type doesn't work " +
    "if the sum of the digits is greater than 38") {
    val values = Array(
      "28.9259999999999983799625624669715762138",
      "28.925999999999998379962562466971576213",
      "2.9259999999999983799625624669715762138"
    )
    for (string <- values) {
      assert(Decimal.fromString(UTF8String.fromString(string)) === Decimal(string))
      assert(Decimal.fromStringANSI(UTF8String.fromString(string)) === Decimal(string))
    }
  }

  test("SPARK-37451: Performance improvement regressed String to Decimal cast") {
    val values = Array("7.836725755512218E38")
    for (string <- values) {
      assert(Decimal.fromString(UTF8String.fromString(string)) === null)
      checkError(
        exception = intercept[SparkArithmeticException](
          Decimal.fromStringANSI(UTF8String.fromString(string))),
        errorClass = "NUMERIC_OUT_OF_SUPPORTED_RANGE",
        parameters = Map("value" -> string))
    }

    withSQLConf(SQLConf.LEGACY_ALLOW_NEGATIVE_SCALE_OF_DECIMAL_ENABLED.key -> "true") {
      for (string <- values) {
        assert(Decimal.fromString(UTF8String.fromString(string)) === Decimal(string))
        assert(Decimal.fromStringANSI(UTF8String.fromString(string)) === Decimal(string))
      }
    }
  }
}
