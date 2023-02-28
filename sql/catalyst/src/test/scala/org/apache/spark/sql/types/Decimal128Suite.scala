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

import org.apache.spark.{SparkArithmeticException, SparkException, SparkFunSuite, SparkNumberFormatException}
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.Decimal.{ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_EVEN, ROUND_HALF_UP}
import org.apache.spark.unsafe.types.UTF8String

class Decimal128Suite extends SparkFunSuite with PrivateMethodTester with SQLHelper{

  val allSupportedRoundModes = Seq(ROUND_HALF_UP, ROUND_HALF_EVEN, ROUND_CEILING, ROUND_FLOOR)

  /** Check that a Decimal128 has the given string representation, precision and scale */
  private def checkDecimal128(d: Decimal128, string: String, precision: Int, scale: Int): Unit = {
    assert(d.toString === string)
    assert(d.precision === precision)
    assert(d.scale === scale)
  }

  test("creating decimals") {
    checkDecimal128(new Decimal128(), "0", 1, 0)
    checkDecimal128(Decimal128(BigDecimal("0.09")), "0.09", 2, 2)
    checkDecimal128(Decimal128(BigDecimal("0.9")), "0.9", 1, 1)
    checkDecimal128(Decimal128(BigDecimal("0.90")), "0.90", 2, 2)
    checkDecimal128(Decimal128(BigDecimal("0.0")), "0.0", 1, 1)
    checkDecimal128(Decimal128(BigDecimal("0")), "0", 1, 0)
    checkDecimal128(Decimal128(BigDecimal("1.0")), "1.0", 2, 1)
    checkDecimal128(Decimal128(BigDecimal("-0.09")), "-0.09", 2, 2)
    checkDecimal128(Decimal128(BigDecimal("-0.9")), "-0.9", 1, 1)
    checkDecimal128(Decimal128(BigDecimal("-0.90")), "-0.90", 2, 2)
    checkDecimal128(Decimal128(BigDecimal("-1.0")), "-1.0", 2, 1)
    checkDecimal128(Decimal128(BigDecimal("10.030")), "10.030", 5, 3)
    checkDecimal128(Decimal128(BigDecimal("10.030"), 4, 1), "10.0", 4, 1)
    checkDecimal128(Decimal128(BigDecimal("-9.95"), 4, 1), "-10.0", 4, 1)
    checkDecimal128(Decimal128("10.030"), "10.030", 5, 3)
    checkDecimal128(Decimal128(10.03), "10.03", 4, 2)
    checkDecimal128(Decimal128(17L), "17", 20, 0)
    checkDecimal128(Decimal128(17), "17", 10, 0)
    checkDecimal128(Decimal128(17L, 2, 1), "1.7", 2, 1)
    checkDecimal128(Decimal128(170L, 4, 2), "1.70", 4, 2)
    checkDecimal128(Decimal128(17L, 24, 1), "1.7", 24, 1)
    checkDecimal128(Decimal128(1e17.toLong, 18, 0), 1e17.toLong.toString, 18, 0)
    checkDecimal128(Decimal128(1000000000000000000L, 20, 2), "10000000000000000.00", 20, 2)
    checkDecimal128(Decimal128(Long.MaxValue), Long.MaxValue.toString, 20, 0)
    checkDecimal128(Decimal128(Long.MinValue), Long.MinValue.toString, 20, 0)
    checkDecimal128(Decimal128(Int128.MAX_VALUE, 38, 0),
      "170141183460469231731687303715884105727", 38, 0)
    checkDecimal128(Decimal128(Int128.MIN_VALUE, 38, 0),
      "-170141183460469231731687303715884105728", 38, 0)

    checkError(
      exception = intercept[SparkArithmeticException](Decimal128(170L, 2, 1)),
      errorClass = "NUMERIC_VALUE_OUT_OF_RANGE",
      parameters = Map(
        "value" -> "0",
        "precision" -> "2",
        "scale" -> "1",
        "config" -> "\"spark.sql.ansi.enabled\""))
    checkError(
      exception = intercept[SparkArithmeticException](Decimal128(170L, 2, 0)),
      errorClass = "NUMERIC_VALUE_OUT_OF_RANGE",
      parameters = Map(
        "value" -> "0",
        "precision" -> "2",
        "scale" -> "0",
        "config" -> "\"spark.sql.ansi.enabled\""))
    checkError(
      exception = intercept[SparkArithmeticException](Decimal128(BigDecimal("10.030"), 2, 1)),
      errorClass = "DECIMAL_PRECISION_EXCEEDS_MAX_PRECISION",
      parameters = Map("precision" -> "3", "maxPrecision" -> "2"))
    checkError(
      exception = intercept[SparkArithmeticException](Decimal128(BigDecimal("-9.95"), 2, 1)),
      errorClass = "DECIMAL_PRECISION_EXCEEDS_MAX_PRECISION",
      parameters = Map("precision" -> "3", "maxPrecision" -> "2"))
    checkError(
      exception = intercept[SparkArithmeticException](Decimal128(1e17.toLong, 17, 0)),
      errorClass = "NUMERIC_VALUE_OUT_OF_RANGE",
      parameters = Map(
        "value" -> "0",
        "precision" -> "17",
        "scale" -> "0",
        "config" -> "\"spark.sql.ansi.enabled\""))

    // Int128 overflow.
    val e =
      intercept[ArithmeticException](Decimal128(0x8000000000000000L, 0x0000000000000000L, 38, 0))
    assert(e.getMessage.contains(
      "Decimal overflow: Construct Int128(-9223372036854775808, 0) instance."))
  }

  test("creating decimals with negative scale under legacy mode") {
    withSQLConf(SQLConf.LEGACY_ALLOW_NEGATIVE_SCALE_OF_DECIMAL_ENABLED.key -> "true") {
      checkDecimal128(Decimal128(BigDecimal("98765"), 5, -3), "9.9E+4", 5, -3)
      checkDecimal128(Decimal128(BigDecimal("314.159"), 6, -2), "3E+2", 6, -2)
      checkDecimal128(Decimal128(BigDecimal(1.579e12), 4, -9), "1.579E+12", 4, -9)
      checkDecimal128(Decimal128(BigDecimal(1.579e12), 4, -10), "1.58E+12", 4, -10)
      checkDecimal128(Decimal128(103050709L, 9, -10), "1.03050709E+18", 9, -10)
      checkDecimal128(Decimal128(1e8.toLong, 10, -10), "1.00000000E+18", 10, -10)
    }
  }

  test("SPARK-30252: Negative scale is not allowed by default") {
    def checkNegativeScaleDecimal(d: => Decimal128): Unit = {
      checkError(
        exception = intercept[SparkException] (d),
        errorClass = "INTERNAL_ERROR",
        parameters = Map("message" -> ("Negative scale is not allowed: -3. " +
          "Set the config \"spark.sql.legacy.allowNegativeScaleOfDecimal\" " +
          "to \"true\" to allow it."))
      )
    }
    checkNegativeScaleDecimal(Decimal128(BigDecimal("98765"), 5, -3))
    checkNegativeScaleDecimal(Decimal128(BigDecimal("98765").underlying(), 5, -3))
    checkNegativeScaleDecimal(Decimal128(98765L, 5, -3))
    checkNegativeScaleDecimal(Decimal128.createUnsafe(98765L, 5, -3))
  }

  test("double and long values") {
    /** Check that a Decimal128 converts to the given double and long values */
    def checkValues(d: Decimal128, doubleValue: Double, longValue: Long): Unit = {
      assert(d.toDouble === doubleValue)
      assert(d.toLong === longValue)
    }

    checkValues(new Decimal128(), 0.0, 0L)
    checkValues(Decimal128(BigDecimal("10.030")), 10.03, 10L)
    checkValues(Decimal128(BigDecimal("10.030"), 4, 1), 10.0, 10L)
    checkValues(Decimal128(BigDecimal("-9.95"), 4, 1), -10.0, -10L)
    checkValues(Decimal128(10.03), 10.03, 10L)
    checkValues(Decimal128(17L), 17.0, 17L)
    checkValues(Decimal128(17), 17.0, 17L)
    checkValues(Decimal128(17L, 2, 1), 1.7, 1L)
    checkValues(Decimal128(170L, 4, 2), 1.7, 1L)
    checkValues(Decimal128(1e16.toLong), 1e16, 1e16.toLong)
    checkValues(Decimal128(1e17.toLong), 1e17, 1e17.toLong)
    checkValues(Decimal128(1e18.toLong), 1e18, 1e18.toLong)
    checkValues(Decimal128(2e18.toLong), 2e18, 2e18.toLong)
    checkValues(Decimal128(Long.MaxValue), Long.MaxValue.toDouble, Long.MaxValue)
    checkValues(Decimal128(Long.MinValue), Long.MinValue.toDouble, Long.MinValue)

    val e1 = intercept[ArithmeticException](Decimal128(Double.MaxValue))
    assert(e1.getMessage.contains("BigInteger out of Int128 range"))
    val e2 = intercept[ArithmeticException](Decimal128(Double.MinValue))
    assert(e2.getMessage.contains("BigInteger out of Int128 range"))
  }

  // Accessor for the BigDecimal value of a Decimal, which will be null if it's using Longs
  private val int128Val = PrivateMethod[Int128](Symbol("int128Val"))

  /** Check whether a decimal is represented compactly (passing whether we expect it to be) */
  private def checkCompact(d: Decimal128, expected: Boolean): Unit = {
    val isCompact = d.invokePrivate(int128Val()).eq(null)
    assert(isCompact == expected, s"$d ${if (expected) "was not" else "was"} compact")
  }

  test("small decimals represented as unscaled long") {
    checkCompact(new Decimal128(), true)
    checkCompact(Decimal128(BigDecimal("10.03")), false)
    checkCompact(Decimal128(BigDecimal("100000000000000000000")), false)
    checkCompact(Decimal128(17L), true)
    checkCompact(Decimal128(17), true)
    checkCompact(Decimal128(17L, 2, 1), true)
    checkCompact(Decimal128(170L, 4, 2), true)
    checkCompact(Decimal128(17L, 24, 1), true)
    checkCompact(Decimal128(1e16.toLong), true)
    checkCompact(Decimal128(1e17.toLong), true)
    checkCompact(Decimal128(1e18.toLong - 1), true)
    checkCompact(Decimal128(- 1e18.toLong + 1), true)
    checkCompact(Decimal128(1e18.toLong - 1, 30, 10), true)
    checkCompact(Decimal128(- 1e18.toLong + 1, 30, 10), true)
    checkCompact(Decimal128(1e18.toLong), false)
    checkCompact(Decimal128(-1e18.toLong), false)
    checkCompact(Decimal128(1e18.toLong, 30, 10), false)
    checkCompact(Decimal128(-1e18.toLong, 30, 10), false)
    checkCompact(Decimal128(Long.MaxValue), false)
    checkCompact(Decimal128(Long.MinValue), false)
  }

  test("hash code") {
    assert(Decimal128(123).hashCode() === (123).##)
    assert(Decimal128(-123).hashCode() === (-123).##)
    assert(Decimal128(Int.MaxValue).hashCode() === Int.MaxValue.##)
    assert(Decimal128(Long.MaxValue).hashCode() === Long.MaxValue.##)
    assert(Decimal128(BigDecimal(123)).hashCode() === (123).##)
  }

  test("equals") {
    // The decimals on the left are stored compactly, while the ones on the right aren't
    checkCompact(Decimal128(123), true)
    checkCompact(Decimal128(BigDecimal(123)), false)
    checkCompact(Decimal128("123"), false)
    assert(Decimal128(123) === Decimal128(BigDecimal(123)))
    assert(Decimal128(123) === Decimal128(BigDecimal("123.00")))
    assert(Decimal128(-123) === Decimal128(BigDecimal(-123)))
    assert(Decimal128(-123) === Decimal128(BigDecimal("-123.00")))

    assert(Decimal128(123, 3, 1) === Decimal128(BigDecimal("12.3")))
    assert(Decimal128(-123, 3, 2) === Decimal128(BigDecimal("-1.23")))
  }

  test("isZero") {
    assert(Decimal128(0).isZero)
    assert(Decimal128(0, 0, 2).isZero)
    assert(Decimal128("0").isZero)
    assert(Decimal128("0.000").isZero)
    assert(!Decimal128(1).isZero)
    assert(!Decimal128(1, 4, 2).isZero)
    assert(!Decimal128("1").isZero)
    assert(!Decimal128("0.001").isZero)
  }

  test("arithmetic") {
    assert(Decimal128(100) + Decimal128(-100) === Decimal128(0))
    assert(Decimal128(100) + Decimal128(-100) === Decimal128(0))
    assert(Decimal128(100) * Decimal128(-100) === Decimal128(-10000))
    assert(Decimal128(1e13) * Decimal128(1e13) === Decimal128(1e26))
    assert(Decimal128(100) / Decimal128(-100) === Decimal128(-1))
    assert(Decimal128(100) / Decimal128(0) === null)
    assert(Decimal128(100) % Decimal128(-100) === Decimal128(0))
    assert(Decimal128(100) % Decimal128(3) === Decimal128(1))
    assert(Decimal128(-100) % Decimal128(3) === Decimal128(-1))
    assert(Decimal128(100) % Decimal128(0) === null)
  }

  test("longVal arithmetic") {
    assert(Decimal128(10, 2, 0) + Decimal128(10, 2, 0) === Decimal128(20, 3, 0))
    assert(Decimal128(10, 2, 0) + Decimal128(90, 2, 0) === Decimal128(100, 3, 0))
    assert(Decimal128(10, 2, 0) - Decimal128(-10, 2, 0) === Decimal128(20, 3, 0))
    assert(Decimal128(10, 2, 0) - Decimal128(-90, 2, 0) === Decimal128(100, 3, 0))
  }

  test("other arithmetic") {
    // test +
    assert(Decimal128(100, 3, 1) + Decimal128(-100, 3, 1) === Decimal128(0))
    assert(Decimal128(100, 3, 1) + Decimal128(-100, 3, 2) === Decimal128(900, 3, 2))
    assert(Decimal128(100, 3, 2) + Decimal128(-100, 3, 1) === Decimal128(-900, 3, 2))
    assert(Decimal128(100, 3, 1) + Decimal128(-100, 3, 2) === Decimal128("9.00"))
    assert(Decimal128("10.0") + Decimal128("-1.00") === Decimal128(BigDecimal("9.00")))
    assert(Decimal128("15432.21543600787131") + Decimal128("57832.21543600787313") ===
      Decimal128(BigDecimal("73264.43087201574444")))
    val e1 = intercept[ArithmeticException](
      Decimal128("170141183460469231731687303715884105727") + Decimal128(1))
    assert(e1.getMessage.contains(
      "Int128 Overflow. Int128(9223372036854775807, -1) add Int128(0, 1)."))

    // test -
    assert(Decimal128(100) - Decimal128(-100) === Decimal128(200))
    assert(Decimal128(100, 3, 1) - Decimal128(-100, 3, 1) === Decimal128(200, 3, 1))
    assert(Decimal128(100, 3, 1) - Decimal128(-100, 3, 2) === Decimal128(1100, 4, 2))
    assert(Decimal128(100, 3, 2) - Decimal128(-100, 3, 1) === Decimal128(1100, 4, 2))
    assert(Decimal128(100, 3, 1) - Decimal128(-100, 3, 2) === Decimal128("11.00"))
    assert(Decimal128("10.0") - Decimal128("-1.00") === Decimal128(BigDecimal("11.00")))
    assert(Decimal128("15432.21543600787131") - Decimal128("57832.21543600787313") ===
      Decimal128(BigDecimal("-42400.00000000000182")))
    val e2 = intercept[ArithmeticException](
      Decimal128("-170141183460469231731687303715884105728") - Decimal128(1))
    assert(e2.getMessage.contains(
      "Int128 Overflow. Int128(-9223372036854775808, 0) subtract Int128(0, 1)."))

    // test *
    assert(Decimal128(100, 3, 1) * Decimal128(-100, 3, 1) === Decimal128(-10000, 5, 2))
    assert(Decimal128(100, 3, 1) * Decimal128(-100, 3, 2) === Decimal128(-10000, 5, 3))
    assert(Decimal128(100, 3, 2) * Decimal128(-100, 3, 1) === Decimal128(-10000, 5, 3))
    assert(Decimal128(100, 3, 1) * Decimal128(-100, 3, 2) === Decimal128("-10.00"))
    assert(Decimal128("10.0") * Decimal128("-1.00") === Decimal128(BigDecimal("-10.00")))
    assert(Decimal128("15432.21543600787131") * Decimal128("57832.21543600787313") ===
      Decimal128(BigDecimal("892479207.7500933852299992378118469003")))
    assert(Decimal128(1e13) * Decimal128(1e13) === Decimal128(1e26))
    val e3 = intercept[ArithmeticException](
      Decimal128("12345678901234567890123456789012345678") * Decimal128(9))
    assert(e3.getMessage.contains("Decimal overflow: Decimal128 multiply."))

    // test /
    assert(Decimal128(100, 3, 1) / Decimal128(-100, 3, 1) === Decimal128(-1, 1, 0))
    assert(Decimal128(100, 3, 1) / Decimal128(-100, 3, 2) === Decimal128(-10, 2, 0))
    assert(Decimal128(100, 3, 2) / Decimal128(-100, 3, 1) === Decimal128(-10, 2, 2))
    assert(Decimal128(100, 3, 1) / Decimal128(-100, 3, 2) === Decimal128("-10.00"))
    assert(Decimal128("10.0") / Decimal128("-1.00") === Decimal128(BigDecimal("-10.00")))
    assert(Decimal128("15432.21543600") / Decimal128("57832.21543") ===
      Decimal128(BigDecimal("0.26684462")))
    assert(Decimal128(100) / Decimal128(0) === null)
    val e4 = intercept[ArithmeticException](
      Decimal128("12345678901234567890123456789012345678") / Decimal128(1, 1, 1))
    assert(e4.getMessage.contains("Decimal overflow: Decimal128 division."))

    // test %
    assert(Decimal128(100, 3, 1) % Decimal128(-100, 3, 1) === Decimal128(0))
    assert(Decimal128(100, 3, 1) % Decimal128(-100, 3, 2) === Decimal128(0))
    assert(Decimal128(100, 3, 2) % Decimal128(-100, 3, 1) === Decimal128(100, 3, 2))
    assert(Decimal128(100, 3, 1) % Decimal128(-100, 3, 2) === Decimal128("0.0"))
    assert(Decimal128("10.0") % Decimal128("-1.00") === Decimal128(BigDecimal("0.0")))
    assert(Decimal128("15432.21543600") % Decimal128("57832.21543") ===
      Decimal128(BigDecimal("15432.21543600")))
    assert(Decimal128(100) % Decimal128(3) === Decimal128(1))
    assert(Decimal128(-100) % Decimal128(3) === Decimal128(-1))
    assert(Decimal128(100) % Decimal128(0) === null)
  }

  test("quot") {
    assert(Decimal128(100).quot(Decimal128(100)) === Decimal128(BigDecimal("1")))
    assert(Decimal128(100).quot(Decimal128(33)) === Decimal128(BigDecimal("3")))
    assert(Decimal128(100).quot(Decimal128(-100)) === Decimal128(BigDecimal("-1")))
    assert(Decimal128(100).quot(Decimal128(-33)) === Decimal128(BigDecimal("-3")))

    assert(Decimal128("15432.21543600").quot(Decimal128("57832.21543")) ===
      Decimal128(BigDecimal("0")))
    assert(Decimal128("57832.21543").quot(Decimal128("15432.21543600")) ===
      Decimal128(BigDecimal("3")))
  }

  test("negate & abs") {
    assert(-Decimal128(100) === Decimal128(BigDecimal("-100")))
    assert(-Decimal128(-100) === Decimal128(BigDecimal("100")))
    assert(Decimal128(100).abs === Decimal128(BigDecimal("100")))
    assert(Decimal128(-100).abs === Decimal128(BigDecimal("100")))
  }

  test("negate") {
    assert(-Decimal128("15432.21543600") === Decimal128(BigDecimal("-15432.21543600")))
    assert(-Decimal128("-57832.21543") === Decimal128(BigDecimal("57832.21543")))
  }

  test("floor & ceil") {
    assert(Decimal128("10.03").floor === Decimal128(BigDecimal("10")))
    assert(Decimal128("10.03").ceil === Decimal128(BigDecimal("11")))
    assert(Decimal128("-10.03").floor === Decimal128(BigDecimal("-11")))
    assert(Decimal128("-10.03").ceil === Decimal128(BigDecimal("-10")))
  }

  test("accurate precision after multiplication") {
    val decimal =
      (Decimal128(Long.MaxValue, 38, 0) * Decimal128(Long.MaxValue, 38, 0)).toJavaBigDecimal
    assert(decimal.unscaledValue.toString === "85070591730234615847396907784232501249")
  }

  test("fix non-terminating decimal expansion problem") {
    val decimal = Decimal128(1.0, 10, 3) / Decimal128(3.0, 10, 3)
    // The difference between decimal should not be more than 0.001.
    assert(decimal.toDouble - 0.333 < 0.001)
  }

  test("fix loss of precision/scale when doing division operation") {
    val a = Decimal128(2) / Decimal128(3)
    // Different from Decimal: assert(a.toDouble < 1.0 && a.toDouble > 0.6)
    assert(a.toDouble == 1.0)
    val b = Decimal128(1) / Decimal128(8)
    // Different from Decimal: assert(b.toDouble === 0.125)
    assert(b.toDouble === 0.0)
  }

  test("set/setOrNull") {
    assert(new Decimal128().set(10L, 10, 0).toUnscaledLong === 10L)
    assert(new Decimal128().set(100L, 10, 0).toUnscaledLong === 100L)
    assert(Decimal128(Long.MaxValue, 100, 0).toUnscaledLong === Long.MaxValue)
  }

  test("changePrecision/toPrecision on compact decimal should respect rounding mode") {
    allSupportedRoundModes.foreach { mode =>
      Seq("0.4", "0.5", "0.6", "1.0", "1.1", "1.6", "2.5", "5.5").foreach { n =>
        Seq("", "-").foreach { sign =>
          val bd = BigDecimal(sign + n)
          val unscaled = (bd * 10).toLongExact
          val d = Decimal128(unscaled, 8, 1)
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
    val decimal = Decimal128.apply(bigInt)
    assert(decimal.toJavaBigDecimal.unscaledValue.toString === "9223372036854775808")
  }

  test("SPARK-26038: toScalaBigInt/toJavaBigInteger") {
    // not fitting long
    val decimal = Decimal128("1234568790123456789.1234879012345678901")
    assert(decimal.toScalaBigInt == scala.math.BigInt("1234568790123456789"))
    assert(decimal.toJavaBigInteger == new java.math.BigInteger("1234568790123456789"))
    // fitting long
    val decimalLong = Decimal128(123456789123456789L, 18, 9)
    assert(decimalLong.toScalaBigInt == scala.math.BigInt("123456789"))
    assert(decimalLong.toJavaBigInteger == new java.math.BigInteger("123456789"))
  }

  test("UTF8String to Decimal128") {
    def checkFromString(string: String): Unit = {
      assert(Decimal128.fromString(UTF8String.fromString(string)) === Decimal128(string))
      assert(Decimal128.fromStringANSI(UTF8String.fromString(string)) === Decimal128(string))
    }

    def checkOutOfRangeFromString(string: String): Unit = {
      assert(Decimal128.fromString(UTF8String.fromString(string)) === null)
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

    assert(Decimal128.fromString(UTF8String.fromString("str")) === null)
    checkError(
      exception = intercept[SparkNumberFormatException](
        Decimal128.fromStringANSI(UTF8String.fromString("str"))),
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
      "28.925999999999998379962562466971576213",
      "2.9259999999999983799625624669715762138"
    )
    for (string <- values) {
      assert(Decimal128.fromString(UTF8String.fromString(string)) === Decimal128(string))
      assert(Decimal128.fromStringANSI(UTF8String.fromString(string)) === Decimal128(string))
    }
  }

  test("SPARK-37451: Performance improvement regressed String to Decimal cast") {
    val values = Array("7.836725755512218E38")
    for (string <- values) {
      assert(Decimal128.fromString(UTF8String.fromString(string)) === null)
      checkError(
        exception = intercept[SparkArithmeticException](
          Decimal128.fromStringANSI(UTF8String.fromString(string))),
        errorClass = "NUMERIC_OUT_OF_SUPPORTED_RANGE",
        parameters = Map("value" -> string))
    }

    withSQLConf(SQLConf.LEGACY_ALLOW_NEGATIVE_SCALE_OF_DECIMAL_ENABLED.key -> "true") {
      for (string <- values) {
        assert(Decimal128.fromString(UTF8String.fromString(string)) === Decimal128(string))
        assert(Decimal128.fromStringANSI(UTF8String.fromString(string)) === Decimal128(string))
      }
    }
  }

  // 18 is a max number of digits in Decimal's compact long
  test("SPARK-41554: decrease/increase scale by 18 and more on compact decimal") {
    val unscaledNums = Seq(
      0L, 1L, 10L, 51L, 123L, 523L,
      // 18 digits
      912345678901234567L,
      112345678901234567L,
      512345678901234567L
    )
    val precision = 38
    // generate some (from, to) scale pairs, e.g. (38, 18), (-20, -2), etc
    val scalePairs = for {
      scale <- Seq(38, 20, 19, 18)
      delta <- Seq(38, 20, 19, 18)
      a = scale
      b = scale - delta
    } yield {
      Seq((a, b), (-a, -b), (b, a), (-b, -a))
    }

    for {
      unscaled <- unscaledNums
      mode <- allSupportedRoundModes
      (scaleFrom, scaleTo) <- scalePairs.flatten
      sign <- Seq(1L, -1L)
    } {
      val unscaledWithSign = unscaled * sign
      if (scaleFrom < 0 || scaleTo < 0) {
        withSQLConf(SQLConf.LEGACY_ALLOW_NEGATIVE_SCALE_OF_DECIMAL_ENABLED.key -> "true") {
          checkScaleChange(unscaledWithSign, scaleFrom, scaleTo, mode)
        }
      } else {
        checkScaleChange(unscaledWithSign, scaleFrom, scaleTo, mode)
      }
    }

    def checkScaleChange(unscaled: Long, scaleFrom: Int, scaleTo: Int,
                         roundMode: BigDecimal.RoundingMode.Value): Unit = {
      val decimal = Decimal128(unscaled, precision, scaleFrom)
      checkCompact(decimal, true)
      decimal.changePrecision(precision, scaleTo, roundMode)
      val bd = BigDecimal(unscaled, scaleFrom).setScale(scaleTo, roundMode)
      assert(decimal.toBigDecimal === bd,
        s"unscaled: $unscaled, scaleFrom: $scaleFrom, scaleTo: $scaleTo, mode: $roundMode")
    }
  }

}
