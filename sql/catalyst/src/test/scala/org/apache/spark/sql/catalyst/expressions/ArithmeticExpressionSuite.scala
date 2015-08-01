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
import org.apache.spark.sql.types.Decimal

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

  test("% (Remainder)") {
    testNumericDataTypes { convert =>
      val left = Literal(convert(1))
      val right = Literal(convert(2))
      checkEvaluation(Remainder(left, right), convert(1))
      checkEvaluation(Remainder(Literal.create(null, left.dataType), right), null)
      checkEvaluation(Remainder(left, Literal.create(null, right.dataType)), null)
      checkEvaluation(Remainder(left, Literal(convert(0))), null)  // mod by 0
    }
  }

  test("Abs") {
    testNumericDataTypes { convert =>
      val input = Literal(convert(1))
      val dataType = input.dataType
      checkEvaluation(Abs(Literal(convert(0))), convert(0))
      checkEvaluation(Abs(Literal(convert(1))), convert(1))
      checkEvaluation(Abs(Literal(convert(-1))), convert(1))
      checkEvaluation(Abs(Literal.create(null, dataType)), null)
    }
  }

  test("MaxOf basic") {
    testNumericDataTypes { convert =>
      val small = Literal(convert(1))
      val large = Literal(convert(2))
      checkEvaluation(MaxOf(small, large), convert(2))
      checkEvaluation(MaxOf(large, small), convert(2))
      checkEvaluation(MaxOf(Literal.create(null, small.dataType), large), convert(2))
      checkEvaluation(MaxOf(large, Literal.create(null, small.dataType)), convert(2))
    }
  }

  test("MaxOf for atomic type") {
    checkEvaluation(MaxOf(true, false), true)
    checkEvaluation(MaxOf("abc", "bcd"), "bcd")
    checkEvaluation(MaxOf(Array(1.toByte, 2.toByte), Array(1.toByte, 3.toByte)),
      Array(1.toByte, 3.toByte))
  }

  test("MinOf basic") {
    testNumericDataTypes { convert =>
      val small = Literal(convert(1))
      val large = Literal(convert(2))
      checkEvaluation(MinOf(small, large), convert(1))
      checkEvaluation(MinOf(large, small), convert(1))
      checkEvaluation(MinOf(Literal.create(null, small.dataType), large), convert(2))
      checkEvaluation(MinOf(small, Literal.create(null, small.dataType)), convert(1))
    }
  }

  test("MinOf for atomic type") {
    checkEvaluation(MinOf(true, false), false)
    checkEvaluation(MinOf("abc", "bcd"), "abc")
    checkEvaluation(MinOf(Array(1.toByte, 2.toByte), Array(1.toByte, 3.toByte)),
      Array(1.toByte, 2.toByte))
  }

  test("pmod") {
    testNumericDataTypes { convert =>
      val left = Literal(convert(7))
      val right = Literal(convert(3))
      checkEvaluation(Pmod(left, right), convert(1))
      checkEvaluation(Pmod(Literal.create(null, left.dataType), right), null)
      checkEvaluation(Pmod(left, Literal.create(null, right.dataType)), null)
      checkEvaluation(Remainder(left, Literal(convert(0))), null)  // mod by 0
    }
    checkEvaluation(Pmod(-7, 3), 2)
    checkEvaluation(Pmod(7.2D, 4.1D), 3.1000000000000005)
    checkEvaluation(Pmod(Decimal(0.7), Decimal(0.2)), Decimal(0.1))
    checkEvaluation(Pmod(2L, Long.MaxValue), 2L)
  }
}
