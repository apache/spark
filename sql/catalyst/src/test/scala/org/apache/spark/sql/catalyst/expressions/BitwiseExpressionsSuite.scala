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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.types._


class BitwiseExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper {

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
    checkExceptionInExpression[IllegalArgumentException](
      expr, row1, "Invalid bit position: -1 is less than zero")
    checkExceptionInExpression[IllegalArgumentException](
      expr, row2, "Invalid bit position: 64 exceeds the bit upper limit")
    checkExceptionInExpression[IllegalArgumentException](
      BitwiseGet(ti, p), row3, "Invalid bit position: 32 exceeds the bit upper limit")
    checkExceptionInExpression[IllegalArgumentException](
      BitwiseGet(ts, p), row4, "Invalid bit position: 16 exceeds the bit upper limit")
    checkExceptionInExpression[IllegalArgumentException](
      BitwiseGet(tb, p), row5, "Invalid bit position: 16 exceeds the bit upper limit")

    DataTypeTestUtils.integralType.foreach { dt =>
      checkConsistencyBetweenInterpretedAndCodegenAllowingException(BitwiseGet, dt, IntegerType)
    }
  }
}
