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

import org.scalatest.Matchers._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}


class ArithmeticExpressionSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("arithmetic") {
    val row = create_row(1, 2, 3, null)
    val c1 = 'a.int.at(0)
    val c2 = 'a.int.at(1)
    val c3 = 'a.int.at(2)
    val c4 = 'a.int.at(3)

    checkEvaluation(UnaryMinus(c1), -1, row)
    checkEvaluation(UnaryMinus(Literal.create(100, IntegerType)), -100)

    checkEvaluation(Add(c1, c4), null, row)
    checkEvaluation(Add(c1, c2), 3, row)
    checkEvaluation(Add(c1, Literal.create(null, IntegerType)), null, row)
    checkEvaluation(Add(Literal.create(null, IntegerType), c2), null, row)
    checkEvaluation(
      Add(Literal.create(null, IntegerType), Literal.create(null, IntegerType)), null, row)

    checkEvaluation(-c1, -1, row)
    checkEvaluation(c1 + c2, 3, row)
    checkEvaluation(c1 - c2, -1, row)
    checkEvaluation(c1 * c2, 2, row)
    checkEvaluation(c1 / c2, 0, row)
    checkEvaluation(c1 % c2, 1, row)
  }

  test("fractional arithmetic") {
    val row = create_row(1.1, 2.0, 3.1, null)
    val c1 = 'a.double.at(0)
    val c2 = 'a.double.at(1)
    val c3 = 'a.double.at(2)
    val c4 = 'a.double.at(3)

    checkEvaluation(UnaryMinus(c1), -1.1, row)
    checkEvaluation(UnaryMinus(Literal.create(100.0, DoubleType)), -100.0)
    checkEvaluation(Add(c1, c4), null, row)
    checkEvaluation(Add(c1, c2), 3.1, row)
    checkEvaluation(Add(c1, Literal.create(null, DoubleType)), null, row)
    checkEvaluation(Add(Literal.create(null, DoubleType), c2), null, row)
    checkEvaluation(
      Add(Literal.create(null, DoubleType), Literal.create(null, DoubleType)), null, row)

    checkEvaluation(-c1, -1.1, row)
    checkEvaluation(c1 + c2, 3.1, row)
    checkDoubleEvaluation(c1 - c2, (-0.9 +- 0.001), row)
    checkDoubleEvaluation(c1 * c2, (2.2 +- 0.001), row)
    checkDoubleEvaluation(c1 / c2, (0.55 +- 0.001), row)
    checkDoubleEvaluation(c3 % c2, (1.1 +- 0.001), row)
  }

  test("Divide") {
    checkEvaluation(Divide(Literal(2), Literal(1)), 2)
    checkEvaluation(Divide(Literal(1.0), Literal(2.0)), 0.5)
    checkEvaluation(Divide(Literal(1), Literal(2)), 0)
    checkEvaluation(Divide(Literal(1), Literal(0)), null)
    checkEvaluation(Divide(Literal(1.0), Literal(0.0)), null)
    checkEvaluation(Divide(Literal(0.0), Literal(0.0)), null)
    checkEvaluation(Divide(Literal(0), Literal.create(null, IntegerType)), null)
    checkEvaluation(Divide(Literal(1), Literal.create(null, IntegerType)), null)
    checkEvaluation(Divide(Literal.create(null, IntegerType), Literal(0)), null)
    checkEvaluation(Divide(Literal.create(null, DoubleType), Literal(0.0)), null)
    checkEvaluation(Divide(Literal.create(null, IntegerType), Literal(1)), null)
    checkEvaluation(Divide(Literal.create(null, IntegerType), Literal.create(null, IntegerType)),
      null)
  }

  test("Remainder") {
    checkEvaluation(Remainder(Literal(2), Literal(1)), 0)
    checkEvaluation(Remainder(Literal(1.0), Literal(2.0)), 1.0)
    checkEvaluation(Remainder(Literal(1), Literal(2)), 1)
    checkEvaluation(Remainder(Literal(1), Literal(0)), null)
    checkEvaluation(Remainder(Literal(1.0), Literal(0.0)), null)
    checkEvaluation(Remainder(Literal(0.0), Literal(0.0)), null)
    checkEvaluation(Remainder(Literal(0), Literal.create(null, IntegerType)), null)
    checkEvaluation(Remainder(Literal(1), Literal.create(null, IntegerType)), null)
    checkEvaluation(Remainder(Literal.create(null, IntegerType), Literal(0)), null)
    checkEvaluation(Remainder(Literal.create(null, DoubleType), Literal(0.0)), null)
    checkEvaluation(Remainder(Literal.create(null, IntegerType), Literal(1)), null)
    checkEvaluation(Remainder(Literal.create(null, IntegerType), Literal.create(null, IntegerType)),
      null)
  }

  test("MaxOf") {
    checkEvaluation(MaxOf(1, 2), 2)
    checkEvaluation(MaxOf(2, 1), 2)
    checkEvaluation(MaxOf(1L, 2L), 2L)
    checkEvaluation(MaxOf(2L, 1L), 2L)

    checkEvaluation(MaxOf(Literal.create(null, IntegerType), 2), 2)
    checkEvaluation(MaxOf(2, Literal.create(null, IntegerType)), 2)
  }

  test("MinOf") {
    checkEvaluation(MinOf(1, 2), 1)
    checkEvaluation(MinOf(2, 1), 1)
    checkEvaluation(MinOf(1L, 2L), 1L)
    checkEvaluation(MinOf(2L, 1L), 1L)

    checkEvaluation(MinOf(Literal.create(null, IntegerType), 1), 1)
    checkEvaluation(MinOf(1, Literal.create(null, IntegerType)), 1)
  }

  test("SQRT") {
    val inputSequence = (1 to (1<<24) by 511).map(_ * (1L<<24))
    val expectedResults = inputSequence.map(l => math.sqrt(l.toDouble))
    val rowSequence = inputSequence.map(l => create_row(l.toDouble))
    val d = 'a.double.at(0)

    for ((row, expected) <- rowSequence zip expectedResults) {
      checkEvaluation(Sqrt(d), expected, row)
    }

    checkEvaluation(Sqrt(Literal.create(null, DoubleType)), null, create_row(null))
    checkEvaluation(Sqrt(-1), null, EmptyRow)
    checkEvaluation(Sqrt(-1.5), null, EmptyRow)
  }
}
