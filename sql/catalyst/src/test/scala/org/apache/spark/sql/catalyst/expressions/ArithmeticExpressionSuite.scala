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
import org.apache.spark.sql.types.{Decimal, DoubleType, IntegerType}


class ArithmeticExpressionSuite extends SparkFunSuite with ExpressionEvalHelper {

  /**
   * Runs through the testFunc for all numeric data types.
   *
   * @param testFunc a test function that accepts a conversion function to convert an integer
   *                 into another data type.
   */
  private def testNumericDataTypes(testFunc: (Int => Any) => Unit): Unit = {
    testFunc(_.toByte)
    testFunc(_.toShort)
    testFunc(identity)
    testFunc(_.toLong)
    testFunc(_.toFloat)
    testFunc(_.toDouble)
    testFunc(Decimal(_))
  }

  test("+ (Add)") {
    testNumericDataTypes { convert =>
      val left = Literal(convert(1))
      val right = Literal(convert(2))
      checkEvaluation(Add(left, right), convert(3))
      checkEvaluation(Add(Literal.create(null, left.dataType), right), null)
      checkEvaluation(Add(left, Literal.create(null, right.dataType)), null)
    }
  }

  test("- (UnaryMinus)") {
    testNumericDataTypes { convert =>
      val input = Literal(convert(1))
      val dataType = input.dataType
      checkEvaluation(UnaryMinus(input), convert(-1))
      checkEvaluation(UnaryMinus(Literal.create(null, dataType)), null)
    }
  }

  test("- (Minus)") {
    testNumericDataTypes { convert =>
      val left = Literal(convert(1))
      val right = Literal(convert(2))
      checkEvaluation(Subtract(left, right), convert(-1))
      checkEvaluation(Subtract(Literal.create(null, left.dataType), right), null)
      checkEvaluation(Subtract(left, Literal.create(null, right.dataType)), null)
    }
  }

  test("* (Multiply)") {
    testNumericDataTypes { convert =>
      val left = Literal(convert(1))
      val right = Literal(convert(2))
      checkEvaluation(Multiply(left, right), convert(2))
      checkEvaluation(Multiply(Literal.create(null, left.dataType), right), null)
      checkEvaluation(Multiply(left, Literal.create(null, right.dataType)), null)
    }
  }

  test("/ (Divide) basic") {
    testNumericDataTypes { convert =>
      val left = Literal(convert(2))
      val right = Literal(convert(1))
      val dataType = left.dataType
      checkEvaluation(Divide(left, right), convert(2))
      checkEvaluation(Divide(Literal.create(null, dataType), right), null)
      checkEvaluation(Divide(left, Literal.create(null, right.dataType)), null)
      checkEvaluation(Divide(left, Literal(convert(0))), null)  // divide by zero
    }
  }

  test("/ (Divide) for integral type") {
    checkEvaluation(Divide(Literal(1.toByte), Literal(2.toByte)), 0.toByte)
    checkEvaluation(Divide(Literal(1.toShort), Literal(2.toShort)), 0.toShort)
    checkEvaluation(Divide(Literal(1), Literal(2)), 0)
    checkEvaluation(Divide(Literal(1.toLong), Literal(2.toLong)), 0.toLong)
  }

  test("/ (Divide) for floating point") {
    checkEvaluation(Divide(Literal(1.0f), Literal(2.0f)), 0.5f)
    checkEvaluation(Divide(Literal(1.0), Literal(2.0)), 0.5)
    checkEvaluation(Divide(Literal(Decimal(1.0)), Literal(Decimal(2.0))), Decimal(0.5))
  }

  test("Abs") {
    testNumericDataTypes { convert =>
      checkEvaluation(Abs(Literal(convert(0))), convert(0))
      checkEvaluation(Abs(Literal(convert(1))), convert(1))
      checkEvaluation(Abs(Literal(convert(-1))), convert(1))
    }
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
