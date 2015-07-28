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
import org.apache.spark.sql.types._


class BitwiseFunctionsSuite extends SparkFunSuite with ExpressionEvalHelper {

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
  }
}
