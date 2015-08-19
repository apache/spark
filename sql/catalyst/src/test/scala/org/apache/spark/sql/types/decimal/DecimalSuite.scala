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

package org.apache.spark.sql.types.decimal

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types.Decimal
import org.scalatest.PrivateMethodTester

import scala.language.postfixOps

class DecimalSuite extends SparkFunSuite with PrivateMethodTester {
  /** Check that a Decimal has the given string representation, precision and scale */
  private def checkDecimal(d: Decimal, string: String, precision: Int, scale: Int): Unit = {
    assert(d.toString === string)
    assert(d.precision === precision)
    assert(d.scale === scale)
  }

  test("creating decimals") {
    checkDecimal(new Decimal(), "0", 1, 0)
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
    checkDecimal(Decimal(Long.MaxValue), Long.MaxValue.toString, 20, 0)
    checkDecimal(Decimal(Long.MinValue), Long.MinValue.toString, 20, 0)
    intercept[IllegalArgumentException](Decimal(170L, 2, 1))
    intercept[IllegalArgumentException](Decimal(170L, 2, 0))
    intercept[IllegalArgumentException](Decimal(BigDecimal("10.030"), 2, 1))
    intercept[IllegalArgumentException](Decimal(BigDecimal("-9.95"), 2, 1))
    intercept[IllegalArgumentException](Decimal(1e17.toLong, 17, 0))
  }

  test("creating decimals with negative scale") {
    checkDecimal(Decimal(BigDecimal("98765"), 5, -3), "9.9E+4", 5, -3)
    checkDecimal(Decimal(BigDecimal("314.159"), 6, -2), "3E+2", 6, -2)
    checkDecimal(Decimal(BigDecimal(1.579e12), 4, -9), "1.579E+12", 4, -9)
    checkDecimal(Decimal(BigDecimal(1.579e12), 4, -10), "1.58E+12", 4, -10)
    checkDecimal(Decimal(103050709L, 9, -10), "1.03050709E+18", 9, -10)
    checkDecimal(Decimal(1e8.toLong, 10, -10), "1.00000000E+18", 10, -10)
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
  private val decimalVal = PrivateMethod[BigDecimal]('decimalVal)

  /** Check whether a decimal is represented compactly (passing whether we expect it to be) */
  private def checkCompact(d: Decimal, expected: Boolean): Unit = {
    val isCompact = d.invokePrivate(decimalVal()).eq(null)
    assert(isCompact == expected, s"$d ${if (expected) "was not" else "was"} compact")
  }

  test("small decimals represented as unscaled long") {
    checkCompact(new Decimal(), true)
    checkCompact(Decimal(BigDecimal(10.03)), false)
    checkCompact(Decimal(BigDecimal(1e20)), false)
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
}
