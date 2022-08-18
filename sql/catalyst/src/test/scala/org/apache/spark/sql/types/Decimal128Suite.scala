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

import org.apache.spark.SparkFunSuite

class Decimal128Suite extends SparkFunSuite {

  /** Check that a Decimal128 has the given string representation and scale */
  private def checkDecimal128(d: Decimal128, string: String, scale: Int): Unit = {
    assert(d.toString === string)
    assert(d.scale === scale)
  }

  test("creating decimals") {
    checkDecimal128(new Decimal128(), "0", 0)
    checkDecimal128(Decimal128(BigDecimal("0.09")), "0.09", 2)
    checkDecimal128(Decimal128(BigDecimal("0.9")), "0.9", 1)
    checkDecimal128(Decimal128(BigDecimal("0.90")), "0.90", 2)
    checkDecimal128(Decimal128(BigDecimal("0.0")), "0.0", 1)
    checkDecimal128(Decimal128(BigDecimal("0")), "0", 0)
    checkDecimal128(Decimal128(BigDecimal("1.0")), "1.0", 1)
    checkDecimal128(Decimal128(BigDecimal("-0.09")), "-0.09", 2)
    checkDecimal128(Decimal128(BigDecimal("-0.9")), "-0.9", 1)
    checkDecimal128(Decimal128(BigDecimal("-0.90")), "-0.90", 2)
    checkDecimal128(Decimal128(BigDecimal("-1.0")), "-1.0", 1)
    checkDecimal128(Decimal128(BigDecimal("10.030")), "10.030", 3)
    checkDecimal128(Decimal128(BigDecimal("10.030"), 1), "10.0", 1)
    checkDecimal128(Decimal128(BigDecimal("-9.95"), 1), "-10.0", 1)
    checkDecimal128(Decimal128("10.030"), "10.030", 3)
    checkDecimal128(Decimal128(10.03), "10.03", 2)
    checkDecimal128(Decimal128(17L), "17", 0)
    checkDecimal128(Decimal128(17), "17", 0)
    checkDecimal128(Decimal128(17L, 2, 1), "31359464925306237747.4", 1)
    checkDecimal128(
      Decimal128(BigDecimal("31359464925306237747.4"), 1), "31359464925306237747.4", 1)
    checkDecimal128(Decimal128(170L, 4, 2), "31359464925306237747.24", 2)
    checkDecimal128(Decimal128(17L, 24, 1), "31359464925306237749.6", 1)
    checkDecimal128(Decimal128(1e17.toLong, 18, 0), "1844674407370955161600000000000000018", 0)
    checkDecimal128(Decimal128(1000000000L, 20, 2), "184467440737095516160000000.20", 2)
    checkDecimal128(Decimal128(Long.MaxValue), Long.MaxValue.toString, 0)
    checkDecimal128(Decimal128(Long.MinValue), Long.MinValue.toString, 0)

    val reallyBig = BigDecimal("123182312312313232112312312123.1231231231")
    val e = intercept[ArithmeticException](Decimal128(reallyBig))
    assert(e.getMessage.contains("BigInteger out of Int128 range"))
  }

  test("hash code") {
    assert(Decimal128(123).hashCode() === (-460565894).##)
    assert(Decimal128(-123).hashCode() === (1655510596).##)
    assert(Decimal128(Int.MaxValue).hashCode() === (-217723243).##)
    assert(Decimal128(Long.MaxValue).hashCode() === (-237151917).##)
    assert(Decimal128(BigDecimal(123)).hashCode() === (-460565894).##)
  }

  test("equals") {
    assert(Decimal128(123) === Decimal128(BigDecimal(123)))
    assert(Decimal128(123) === Decimal128(BigDecimal("123.00")))
    assert(Decimal128(-123) === Decimal128(BigDecimal(-123)))
    assert(Decimal128(-123) === Decimal128(BigDecimal("-123.00")))
    assert(Decimal128(123, 1) === Decimal128(BigDecimal("12.3")))
    assert(Decimal128(-123, 2) === Decimal128(BigDecimal("-1.23")))
    assert(Decimal128(17L, 2, 1) === Decimal128(BigDecimal("31359464925306237747.4")))
    assert(Decimal128(17L, 2, 1) === Decimal128(BigDecimal("31359464925306237747.4"), 1))
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
    assert(Decimal128(100, 0) + Decimal128(-100, 0) === Decimal128(0))
    assert(Decimal128(100, 1) + Decimal128(-100, 1) === Decimal128(0))
    assert(Decimal128(100, 1) + Decimal128(-100, 2) === Decimal128(900, 2))
    assert(Decimal128(100, 1) + Decimal128(-100, 2) === Decimal128("9.00"))
    assert(Decimal128("10.0") + Decimal128("-1.00") === Decimal128(BigDecimal("9.00")))
//    assert(Decimal(100) * Decimal(-100) === Decimal(-10000))
//    assert(Decimal(1e13) * Decimal(1e13) === Decimal(1e26))
//    assert(Decimal(100) / Decimal(-100) === Decimal(-1))
//    assert(Decimal(100) / Decimal(0) === null)
//    assert(Decimal(100) % Decimal(-100) === Decimal(0))
//    assert(Decimal(100) % Decimal(3) === Decimal(1))
//    assert(Decimal(-100) % Decimal(3) === Decimal(-1))
//    assert(Decimal(100) % Decimal(0) === null)
  }

}
