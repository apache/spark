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

import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import org.apache.spark.SparkFunSuite
import org.apache.spark.SparkIllegalArgumentException
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.types._


class BitwiseExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper
    with ScalaCheckPropertyChecks {

  import IntegralLiteralTestUtils._

  test("BitwiseNOT") {
    def check(input: Any, expected: Any): Unit = {
      val expr = BitwiseNot(Literal(input))
      assert(expr.dataType === Literal(input).dataType)
      checkEvaluation(expr, expected)
    }

    // Need the extra toByte even though IntelliJ thought it's not needed.
    check(1.toByte, (~1.toByte).toByte)
    check(1000.toShort, (~1000.toShort).toShort)
    check(1000000, ~1000000)
    check(123456789123L, ~123456789123L)

    checkEvaluation(BitwiseNot(Literal.create(null, IntegerType)), null)
    checkEvaluation(BitwiseNot(positiveShortLit), (~positiveShort).toShort)
    checkEvaluation(BitwiseNot(negativeShortLit), (~negativeShort).toShort)
    checkEvaluation(BitwiseNot(positiveIntLit), ~positiveInt)
    checkEvaluation(BitwiseNot(negativeIntLit), ~negativeInt)
    checkEvaluation(BitwiseNot(positiveLongLit), ~positiveLong)
    checkEvaluation(BitwiseNot(negativeLongLit), ~negativeLong)

    DataTypeTestUtils.integralType.foreach { dt =>
      checkConsistencyBetweenInterpretedAndCodegen(BitwiseNot, dt)
    }
  }

  test("BitwiseAnd") {
    def check(input1: Any, input2: Any, expected: Any): Unit = {
      val expr = BitwiseAnd(Literal(input1), Literal(input2))
      assert(expr.dataType === Literal(input1).dataType)
      checkEvaluation(expr, expected)
    }

    // Need the extra toByte even though IntelliJ thought it's not needed.
    check(1.toByte, 2.toByte, (1.toByte & 2.toByte).toByte)
    check(1000.toShort, 2.toShort, (1000.toShort & 2.toShort).toShort)
    check(1000000, 4, 1000000 & 4)
    check(123456789123L, 5L, 123456789123L & 5L)

    val nullLit = Literal.create(null, IntegerType)
    checkEvaluation(BitwiseAnd(nullLit, Literal(1)), null)
    checkEvaluation(BitwiseAnd(Literal(1), nullLit), null)
    checkEvaluation(BitwiseAnd(nullLit, nullLit), null)
    checkEvaluation(BitwiseAnd(positiveShortLit, negativeShortLit),
      (positiveShort & negativeShort).toShort)
    checkEvaluation(BitwiseAnd(positiveIntLit, negativeIntLit), positiveInt & negativeInt)
    checkEvaluation(BitwiseAnd(positiveLongLit, negativeLongLit), positiveLong & negativeLong)

    DataTypeTestUtils.integralType.foreach { dt =>
      checkConsistencyBetweenInterpretedAndCodegen(BitwiseAnd, dt, dt)
    }
  }

  test("BitwiseOr") {
    def check(input1: Any, input2: Any, expected: Any): Unit = {
      val expr = BitwiseOr(Literal(input1), Literal(input2))
      assert(expr.dataType === Literal(input1).dataType)
      checkEvaluation(expr, expected)
    }

    // Need the extra toByte even though IntelliJ thought it's not needed.
    check(1.toByte, 2.toByte, (1.toByte | 2.toByte).toByte)
    check(1000.toShort, 2.toShort, (1000.toShort | 2.toShort).toShort)
    check(1000000, 4, 1000000 | 4)
    check(123456789123L, 5L, 123456789123L | 5L)

    val nullLit = Literal.create(null, IntegerType)
    checkEvaluation(BitwiseOr(nullLit, Literal(1)), null)
    checkEvaluation(BitwiseOr(Literal(1), nullLit), null)
    checkEvaluation(BitwiseOr(nullLit, nullLit), null)
    checkEvaluation(BitwiseOr(positiveShortLit, negativeShortLit),
      (positiveShort | negativeShort).toShort)
    checkEvaluation(BitwiseOr(positiveIntLit, negativeIntLit), positiveInt | negativeInt)
    checkEvaluation(BitwiseOr(positiveLongLit, negativeLongLit), positiveLong | negativeLong)

    DataTypeTestUtils.integralType.foreach { dt =>
      checkConsistencyBetweenInterpretedAndCodegen(BitwiseOr, dt, dt)
    }
  }

  test("BitwiseXor") {
    def check(input1: Any, input2: Any, expected: Any): Unit = {
      val expr = BitwiseXor(Literal(input1), Literal(input2))
      assert(expr.dataType === Literal(input1).dataType)
      checkEvaluation(expr, expected)
    }

    // Need the extra toByte even though IntelliJ thought it's not needed.
    check(1.toByte, 2.toByte, (1.toByte ^ 2.toByte).toByte)
    check(1000.toShort, 2.toShort, (1000.toShort ^ 2.toShort).toShort)
    check(1000000, 4, 1000000 ^ 4)
    check(123456789123L, 5L, 123456789123L ^ 5L)

    val nullLit = Literal.create(null, IntegerType)
    checkEvaluation(BitwiseXor(nullLit, Literal(1)), null)
    checkEvaluation(BitwiseXor(Literal(1), nullLit), null)
    checkEvaluation(BitwiseXor(nullLit, nullLit), null)
    checkEvaluation(BitwiseXor(positiveShortLit, negativeShortLit),
      (positiveShort ^ negativeShort).toShort)
    checkEvaluation(BitwiseXor(positiveIntLit, negativeIntLit), positiveInt ^ negativeInt)
    checkEvaluation(BitwiseXor(positiveLongLit, negativeLongLit), positiveLong ^ negativeLong)

    DataTypeTestUtils.integralType.foreach { dt =>
      checkConsistencyBetweenInterpretedAndCodegen(BitwiseXor, dt, dt)
    }
  }

  test("BitCount") {
    // null
    val nullLongLiteral = Literal.create(null, LongType)
    val nullIntLiteral = Literal.create(null, IntegerType)
    val nullBooleanLiteral = Literal.create(null, BooleanType)
    checkEvaluation(new BitwiseCount(nullLongLiteral), null)
    checkEvaluation(new BitwiseCount(nullIntLiteral), null)
    checkEvaluation(new BitwiseCount(nullBooleanLiteral), null)

    // boolean
    checkEvaluation(new BitwiseCount(Literal(true)), 1)
    checkEvaluation(new BitwiseCount(Literal(false)), 0)

    // byte/tinyint
    checkEvaluation(new BitwiseCount(Literal(1.toByte)), 1)
    checkEvaluation(new BitwiseCount(Literal(2.toByte)), 1)
    checkEvaluation(new BitwiseCount(Literal(3.toByte)), 2)

    // short/smallint
    checkEvaluation(new BitwiseCount(Literal(1.toShort)), 1)
    checkEvaluation(new BitwiseCount(Literal(2.toShort)), 1)
    checkEvaluation(new BitwiseCount(Literal(3.toShort)), 2)

    // int
    checkEvaluation(new BitwiseCount(Literal(1)), 1)
    checkEvaluation(new BitwiseCount(Literal(2)), 1)
    checkEvaluation(new BitwiseCount(Literal(3)), 2)

    // long/bigint
    checkEvaluation(new BitwiseCount(Literal(1L)), 1)
    checkEvaluation(new BitwiseCount(Literal(2L)), 1)
    checkEvaluation(new BitwiseCount(Literal(3L)), 2)

    // negative num
    checkEvaluation(new BitwiseCount(Literal(-1L)), 64)

    // edge value
    checkEvaluation(new BitwiseCount(Literal(9223372036854775807L)), 63)
    checkEvaluation(new BitwiseCount(Literal(-9223372036854775808L)), 1)
  }

  test("BitCount should respect input integer type bit width") {
    // BIT_COUNT(-1) should return the number of bits in the type, not always 64
    checkEvaluation(new BitwiseCount(Literal((-1).toByte)), 8)
    checkEvaluation(new BitwiseCount(Literal((-1).toShort)), 16)
    checkEvaluation(new BitwiseCount(Literal(-1)), 32)
    checkEvaluation(new BitwiseCount(Literal(-1L)), 64)

    // Additional cases with specific negative values
    checkEvaluation(new BitwiseCount(Literal(Int.MinValue)), 1)
    checkEvaluation(new BitwiseCount(Literal(-65536)), 16)
    checkEvaluation(new BitwiseCount(Literal(-256)), 24)
    checkEvaluation(new BitwiseCount(Literal(Byte.MinValue)), 1)
    checkEvaluation(new BitwiseCount(Literal((-256).toShort)), 8)
    checkEvaluation(new BitwiseCount(Literal(Short.MinValue)), 1)
    checkEvaluation(new BitwiseCount(Literal(Long.MinValue)), 1)

    DataTypeTestUtils.integralType.foreach { dt =>
      checkConsistencyBetweenInterpretedAndCodegen((e: Expression) => new BitwiseCount(e), dt)
    }
  }

  test("BitCount with explicit bits parameter") {
    // Trino-compatible examples
    checkEvaluation(BitwiseCount(Literal(9), 64), 2)
    checkEvaluation(BitwiseCount(Literal(9), 8), 2)
    checkEvaluation(BitwiseCount(Literal(-7), 64), 62)
    checkEvaluation(BitwiseCount(Literal(-7), 8), 6)
    checkEvaluation(BitwiseCount(Literal(0), 8), 0)
    checkEvaluation(BitwiseCount(Literal(-1), 8), 8)
    checkEvaluation(BitwiseCount(Literal(-1), 64), 64)
    checkEvaluation(BitwiseCount(Literal(-1), 1), 1)
    checkEvaluation(BitwiseCount(Literal(0), 1), 0)

    // null first argument
    checkEvaluation(BitwiseCount(Literal.create(null, IntegerType), 8), null)
    checkEvaluation(BitwiseCount(Literal.create(null, LongType), 64), null)

    // boolean with bits
    checkEvaluation(BitwiseCount(Literal(true), 8), 1)
    checkEvaluation(BitwiseCount(Literal(false), 8), 0)

    // codegen consistency for two-argument form
    // Use bits=64 for all types to avoid range errors with random values
    DataTypeTestUtils.integralType.foreach { dt =>
      checkConsistencyBetweenInterpretedAndCodegen(
        (e: Expression) => BitwiseCount(e, 64), dt)
    }

    // range validation: value out of bits-bit signed integer range
    checkErrorInExpression[SparkIllegalArgumentException](
      BitwiseCount(Literal(200), 8),
      "INVALID_PARAMETER_VALUE.BITS_VALUE_OUT_OF_RANGE",
      Map("parameter" -> "`expr`",
        "functionName" -> "`bit_count`",
        "bits" -> "8",
        "lower" -> "-128",
        "upper" -> "127",
        "invalidValue" -> "200"))
    checkErrorInExpression[SparkIllegalArgumentException](
      BitwiseCount(Literal(-129), 8),
      "INVALID_PARAMETER_VALUE.BITS_VALUE_OUT_OF_RANGE",
      Map("parameter" -> "`expr`",
        "functionName" -> "`bit_count`",
        "bits" -> "8",
        "lower" -> "-128",
        "upper" -> "127",
        "invalidValue" -> "-129"))
    // boundary values should succeed
    checkEvaluation(BitwiseCount(Literal(127), 8), 7)
    checkEvaluation(BitwiseCount(Literal(-128), 8), 1)
  }

  test("BitCount builder validation") {
    // non-foldable bits
    val ex1 = intercept[Exception] {
      BitCountExpressionBuilder.build("bit_count",
        Seq(Literal(1), $"col".int))
    }
    assert(ex1.getMessage.contains("NON_FOLDABLE_ARGUMENT") ||
      ex1.getMessage.contains("foldable"))

    // null bits
    val ex2 = intercept[Exception] {
      BitCountExpressionBuilder.build("bit_count",
        Seq(Literal(1), Literal.create(null, IntegerType)))
    }
    assert(ex2.getMessage.contains("NON_FOLDABLE_ARGUMENT") ||
      ex2.getMessage.contains("foldable"))

    // bits out of range
    val ex3 = intercept[Exception] {
      BitCountExpressionBuilder.build("bit_count",
        Seq(Literal(1), Literal(0)))
    }
    assert(ex3.getMessage.contains("BITS_RANGE") ||
      ex3.getMessage.contains("[1, 64]"))

    val ex4 = intercept[Exception] {
      BitCountExpressionBuilder.build("bit_count",
        Seq(Literal(1), Literal(65)))
    }
    assert(ex4.getMessage.contains("BITS_RANGE") ||
      ex4.getMessage.contains("[1, 64]"))

    // wrong number of arguments
    val ex5 = intercept[Exception] {
      BitCountExpressionBuilder.build("bit_count",
        Seq(Literal(1), Literal(8), Literal(3)))
    }
    assert(ex5.getMessage.contains("WRONG_NUM_ARGS") ||
      ex5.getMessage.contains("number"))

    // direct construction with invalid bits triggers require
    intercept[IllegalArgumentException] { BitwiseCount(Literal(1), -1) }
    intercept[IllegalArgumentException] { BitwiseCount(Literal(1), 0) }
    intercept[IllegalArgumentException] { BitwiseCount(Literal(1), 65) }
    intercept[IllegalArgumentException] { BitwiseCount(Literal(1), 100) }
  }

  test("BitCount property: two-argument matches Long.bitCount with mask") {
    val genBits = Gen.choose(1, 64)
    forAll(genBits, minSuccessful(200)) { (bits: Int) =>
      val lower = -(1L << (bits - 1))
      val upper = (1L << (bits - 1)) - 1
      val genValue = Gen.choose(lower, upper)
      forAll(genValue, minSuccessful(10)) { (value: Long) =>
        val mask = if (bits == 64) -1L else (1L << bits) - 1
        val expected = java.lang.Long.bitCount(value & mask)
        val result = BitwiseCount(Literal(value), bits).eval(null)
        assert(result === expected,
          s"bit_count($value, $bits): expected $expected but got $result")
      }
    }
  }

  test("BitCount property: single-argument matches natural bit width") {
    // Long
    forAll(Gen.choose(Long.MinValue, Long.MaxValue), minSuccessful(100)) { (value: Long) =>
      val expected = java.lang.Long.bitCount(value)
      assert(new BitwiseCount(Literal(value)).eval(null) === expected)
    }
    // Int
    forAll(Gen.choose(Int.MinValue, Int.MaxValue), minSuccessful(100)) { (value: Int) =>
      val expected = java.lang.Integer.bitCount(value)
      assert(new BitwiseCount(Literal(value)).eval(null) === expected)
    }
    // Short
    forAll(Gen.choose(Short.MinValue, Short.MaxValue), minSuccessful(100)) { (value: Short) =>
      val expected = java.lang.Integer.bitCount(java.lang.Short.toUnsignedInt(value))
      assert(new BitwiseCount(Literal(value)).eval(null) === expected)
    }
    // Byte
    forAll(Gen.choose(Byte.MinValue, Byte.MaxValue), minSuccessful(100)) { (value: Byte) =>
      val expected = java.lang.Integer.bitCount(java.lang.Byte.toUnsignedInt(value))
      assert(new BitwiseCount(Literal(value)).eval(null) === expected)
    }
  }

  test("BitGet") {
    val nullLongLiteral = Literal.create(null, LongType)
    val nullIntLiteral = Literal.create(null, IntegerType)
    checkEvaluation(BitwiseGet(nullLongLiteral, Literal(1)), null)
    checkEvaluation(BitwiseGet(Literal(11L), nullIntLiteral), null)
    checkEvaluation(BitwiseGet(nullLongLiteral, nullIntLiteral), null)
    checkEvaluation(BitwiseGet(Literal(11L), Literal(3)), 1.toByte)
    checkEvaluation(BitwiseGet(Literal(11L), Literal(2)), 0.toByte)
    checkEvaluation(BitwiseGet(Literal(11L), Literal(1)), 1.toByte)
    checkEvaluation(BitwiseGet(Literal(11L), Literal(0)), 1.toByte)
    checkEvaluation(BitwiseGet(Literal(11L), Literal(63)), 0.toByte)

    val row1 = create_row(11L, -1)
    val row2 = create_row(11L, 64)
    val row3 = create_row(11, 32)
    val row4 = create_row(11.toShort, 16)
    val row5 = create_row(11.toByte, 16)

    val tl = $"t".long.at(0)
    val ti = $"t".int.at(0)
    val ts = $"t".short.at(0)
    val tb = $"t".byte.at(0)
    val p = $"p".int.at(1)

    val expr = BitwiseGet(tl, p)
    checkErrorInExpression[SparkIllegalArgumentException](
      expr, row1, "INVALID_PARAMETER_VALUE.BIT_POSITION_RANGE",
      Map("parameter" -> "`pos`",
        "functionName" -> "`bit_get`",
        "upper" -> "64",
        "invalidValue" -> "-1"))
    checkErrorInExpression[SparkIllegalArgumentException](
      expr, row2, "INVALID_PARAMETER_VALUE.BIT_POSITION_RANGE",
      Map("parameter" -> "`pos`",
        "functionName" -> "`bit_get`",
        "upper" -> "64",
        "invalidValue" -> "64"))
    checkErrorInExpression[SparkIllegalArgumentException](
      BitwiseGet(ti, p), row3, "INVALID_PARAMETER_VALUE.BIT_POSITION_RANGE",
      Map("parameter" -> "`pos`",
        "functionName" -> "`bit_get`",
        "upper" -> "32",
        "invalidValue" -> "32"))
    checkErrorInExpression[SparkIllegalArgumentException](
      BitwiseGet(ts, p), row4, "INVALID_PARAMETER_VALUE.BIT_POSITION_RANGE",
      Map("parameter" -> "`pos`",
        "functionName" -> "`bit_get`",
        "upper" -> "16",
        "invalidValue" -> "16"))
    checkErrorInExpression[SparkIllegalArgumentException](
      BitwiseGet(tb, p), row5, "INVALID_PARAMETER_VALUE.BIT_POSITION_RANGE",
      Map("parameter" -> "`pos`",
        "functionName" -> "`bit_get`",
        "upper" -> "8",
        "invalidValue" -> "16"))
    DataTypeTestUtils.integralType.foreach { dt =>
      checkConsistencyBetweenInterpretedAndCodegenAllowingException(BitwiseGet, dt, IntegerType)
    }
  }
}
