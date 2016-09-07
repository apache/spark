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

import java.sql.{Date, Timestamp}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.TypeCheckFailure
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.types._

class ArithmeticExpressionSuite extends SparkFunSuite with ExpressionEvalHelper {

  import IntegralLiteralTestUtils._

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
    checkEvaluation(Add(positiveShortLit, negativeShortLit), -1.toShort)
    checkEvaluation(Add(positiveIntLit, negativeIntLit), -1)
    checkEvaluation(Add(positiveLongLit, negativeLongLit), -1L)

    DataTypeTestUtils.numericAndInterval.foreach { tpe =>
      checkConsistencyBetweenInterpretedAndCodegen(Add, tpe, tpe)
    }
  }

  test("- (UnaryMinus)") {
    testNumericDataTypes { convert =>
      val input = Literal(convert(1))
      val dataType = input.dataType
      checkEvaluation(UnaryMinus(input), convert(-1))
      checkEvaluation(UnaryMinus(Literal.create(null, dataType)), null)
    }
    checkEvaluation(UnaryMinus(Literal(Long.MinValue)), Long.MinValue)
    checkEvaluation(UnaryMinus(Literal(Int.MinValue)), Int.MinValue)
    checkEvaluation(UnaryMinus(Literal(Short.MinValue)), Short.MinValue)
    checkEvaluation(UnaryMinus(Literal(Byte.MinValue)), Byte.MinValue)
    checkEvaluation(UnaryMinus(positiveShortLit), (- positiveShort).toShort)
    checkEvaluation(UnaryMinus(negativeShortLit), (- negativeShort).toShort)
    checkEvaluation(UnaryMinus(positiveIntLit), - positiveInt)
    checkEvaluation(UnaryMinus(negativeIntLit), - negativeInt)
    checkEvaluation(UnaryMinus(positiveLongLit), - positiveLong)
    checkEvaluation(UnaryMinus(negativeLongLit), - negativeLong)

    DataTypeTestUtils.numericAndInterval.foreach { tpe =>
      checkConsistencyBetweenInterpretedAndCodegen(UnaryMinus, tpe)
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
    checkEvaluation(Subtract(positiveShortLit, negativeShortLit),
      (positiveShort - negativeShort).toShort)
    checkEvaluation(Subtract(positiveIntLit, negativeIntLit), positiveInt - negativeInt)
    checkEvaluation(Subtract(positiveLongLit, negativeLongLit), positiveLong - negativeLong)

    DataTypeTestUtils.numericAndInterval.foreach { tpe =>
      checkConsistencyBetweenInterpretedAndCodegen(Subtract, tpe, tpe)
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
    checkEvaluation(Multiply(positiveShortLit, negativeShortLit),
      (positiveShort * negativeShort).toShort)
    checkEvaluation(Multiply(positiveIntLit, negativeIntLit), positiveInt * negativeInt)
    checkEvaluation(Multiply(positiveLongLit, negativeLongLit), positiveLong * negativeLong)

    DataTypeTestUtils.numericTypeWithoutDecimal.foreach { tpe =>
      checkConsistencyBetweenInterpretedAndCodegen(Multiply, tpe, tpe)
    }
  }

  private def testDecimalAndDoubleType(testFunc: (Int => Any) => Unit): Unit = {
    testFunc(_.toDouble)
    testFunc(Decimal(_))
  }

  test("/ (Divide) basic") {
    testDecimalAndDoubleType { convert =>
      val left = Literal(convert(2))
      val right = Literal(convert(1))
      val dataType = left.dataType
      checkEvaluation(Divide(left, right), convert(2))
      checkEvaluation(Divide(Literal.create(null, dataType), right), null)
      checkEvaluation(Divide(left, Literal.create(null, right.dataType)), null)
      checkEvaluation(Divide(left, Literal(convert(0))), null)  // divide by zero
    }

    Seq(DoubleType, DecimalType.SYSTEM_DEFAULT).foreach { tpe =>
      checkConsistencyBetweenInterpretedAndCodegen(Divide, tpe, tpe)
    }
  }

  // By fixing SPARK-15776, Divide's inputType is required to be DoubleType of DecimalType.
  // TODO: in future release, we should add a IntegerDivide to support integral types.
  ignore("/ (Divide) for integral type") {
    checkEvaluation(Divide(Literal(1.toByte), Literal(2.toByte)), 0.toByte)
    checkEvaluation(Divide(Literal(1.toShort), Literal(2.toShort)), 0.toShort)
    checkEvaluation(Divide(Literal(1), Literal(2)), 0)
    checkEvaluation(Divide(Literal(1.toLong), Literal(2.toLong)), 0.toLong)
    checkEvaluation(Divide(positiveShortLit, negativeShortLit), 0.toShort)
    checkEvaluation(Divide(positiveIntLit, negativeIntLit), 0)
    checkEvaluation(Divide(positiveLongLit, negativeLongLit), 0L)
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
    checkEvaluation(Remainder(positiveShortLit, positiveShortLit), 0.toShort)
    checkEvaluation(Remainder(negativeShortLit, negativeShortLit), 0.toShort)
    checkEvaluation(Remainder(positiveIntLit, positiveIntLit), 0)
    checkEvaluation(Remainder(negativeIntLit, negativeIntLit), 0)
    checkEvaluation(Remainder(positiveLongLit, positiveLongLit), 0L)
    checkEvaluation(Remainder(negativeLongLit, negativeLongLit), 0L)

    // TODO: the following lines would fail the test due to inconsistency result of interpret
    // and codegen for remainder between giant values, seems like a numeric stability issue
    // DataTypeTestUtils.numericTypeWithoutDecimal.foreach { tpe =>
    //  checkConsistencyBetweenInterpretedAndCodegen(Remainder, tpe, tpe)
    // }
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
    checkEvaluation(Abs(positiveShortLit), positiveShort)
    checkEvaluation(Abs(negativeShortLit), (- negativeShort).toShort)
    checkEvaluation(Abs(positiveIntLit), positiveInt)
    checkEvaluation(Abs(negativeIntLit), - negativeInt)
    checkEvaluation(Abs(positiveLongLit), positiveLong)
    checkEvaluation(Abs(negativeLongLit), - negativeLong)

    DataTypeTestUtils.numericTypeWithoutDecimal.foreach { tpe =>
      checkConsistencyBetweenInterpretedAndCodegen(Abs, tpe)
    }
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
    checkEvaluation(Pmod(Literal(-7), Literal(3)), 2)
    checkEvaluation(Pmod(Literal(7.2D), Literal(4.1D)), 3.1000000000000005)
    checkEvaluation(Pmod(Literal(Decimal(0.7)), Literal(Decimal(0.2))), Decimal(0.1))
    checkEvaluation(Pmod(Literal(2L), Literal(Long.MaxValue)), 2L)
    checkEvaluation(Pmod(positiveShort, negativeShort), positiveShort.toShort)
    checkEvaluation(Pmod(positiveInt, negativeInt), positiveInt)
    checkEvaluation(Pmod(positiveLong, negativeLong), positiveLong)
  }

  test("function least") {
    val row = create_row(1, 2, "a", "b", "c")
    val c1 = 'a.int.at(0)
    val c2 = 'a.int.at(1)
    val c3 = 'a.string.at(2)
    val c4 = 'a.string.at(3)
    val c5 = 'a.string.at(4)
    checkEvaluation(Least(Seq(c4, c3, c5)), "a", row)
    checkEvaluation(Least(Seq(c1, c2)), 1, row)
    checkEvaluation(Least(Seq(c1, c2, Literal(-1))), -1, row)
    checkEvaluation(Least(Seq(c4, c5, c3, c3, Literal("a"))), "a", row)

    val nullLiteral = Literal.create(null, IntegerType)
    checkEvaluation(Least(Seq(nullLiteral, nullLiteral)), null)
    checkEvaluation(Least(Seq(Literal(null), Literal(null))), null, InternalRow.empty)
    checkEvaluation(Least(Seq(Literal(-1.0), Literal(2.5))), -1.0, InternalRow.empty)
    checkEvaluation(Least(Seq(Literal(-1), Literal(2))), -1, InternalRow.empty)
    checkEvaluation(
      Least(Seq(Literal((-1.0).toFloat), Literal(2.5.toFloat))), (-1.0).toFloat, InternalRow.empty)
    checkEvaluation(
      Least(Seq(Literal(Long.MaxValue), Literal(Long.MinValue))), Long.MinValue, InternalRow.empty)
    checkEvaluation(Least(Seq(Literal(1.toByte), Literal(2.toByte))), 1.toByte, InternalRow.empty)
    checkEvaluation(
      Least(Seq(Literal(1.toShort), Literal(2.toByte.toShort))), 1.toShort, InternalRow.empty)
    checkEvaluation(Least(Seq(Literal("abc"), Literal("aaaa"))), "aaaa", InternalRow.empty)
    checkEvaluation(Least(Seq(Literal(true), Literal(false))), false, InternalRow.empty)
    checkEvaluation(
      Least(Seq(
        Literal(BigDecimal("1234567890987654321123456")),
        Literal(BigDecimal("1234567890987654321123458")))),
      BigDecimal("1234567890987654321123456"), InternalRow.empty)
    checkEvaluation(
      Least(Seq(Literal(Date.valueOf("2015-01-01")), Literal(Date.valueOf("2015-07-01")))),
      Date.valueOf("2015-01-01"), InternalRow.empty)
    checkEvaluation(
      Least(Seq(
        Literal(Timestamp.valueOf("2015-07-01 08:00:00")),
        Literal(Timestamp.valueOf("2015-07-01 10:00:00")))),
      Timestamp.valueOf("2015-07-01 08:00:00"), InternalRow.empty)

    // Type checking error
    assert(
      Least(Seq(Literal(1), Literal("1"))).checkInputDataTypes() ==
        TypeCheckFailure("The expressions should all have the same type, " +
          "got LEAST(int, string)."))

    DataTypeTestUtils.ordered.foreach { dt =>
      checkConsistencyBetweenInterpretedAndCodegen(Least, dt, 2)
    }
  }

  test("function greatest") {
    val row = create_row(1, 2, "a", "b", "c")
    val c1 = 'a.int.at(0)
    val c2 = 'a.int.at(1)
    val c3 = 'a.string.at(2)
    val c4 = 'a.string.at(3)
    val c5 = 'a.string.at(4)
    checkEvaluation(Greatest(Seq(c4, c5, c3)), "c", row)
    checkEvaluation(Greatest(Seq(c2, c1)), 2, row)
    checkEvaluation(Greatest(Seq(c1, c2, Literal(2))), 2, row)
    checkEvaluation(Greatest(Seq(c4, c5, c3, Literal("ccc"))), "ccc", row)

    val nullLiteral = Literal.create(null, IntegerType)
    checkEvaluation(Greatest(Seq(nullLiteral, nullLiteral)), null)
    checkEvaluation(Greatest(Seq(Literal(null), Literal(null))), null, InternalRow.empty)
    checkEvaluation(Greatest(Seq(Literal(-1.0), Literal(2.5))), 2.5, InternalRow.empty)
    checkEvaluation(Greatest(Seq(Literal(-1), Literal(2))), 2, InternalRow.empty)
    checkEvaluation(
      Greatest(Seq(Literal((-1.0).toFloat), Literal(2.5.toFloat))), 2.5.toFloat, InternalRow.empty)
    checkEvaluation(Greatest(
      Seq(Literal(Long.MaxValue), Literal(Long.MinValue))), Long.MaxValue, InternalRow.empty)
    checkEvaluation(
      Greatest(Seq(Literal(1.toByte), Literal(2.toByte))), 2.toByte, InternalRow.empty)
    checkEvaluation(
      Greatest(Seq(Literal(1.toShort), Literal(2.toByte.toShort))), 2.toShort, InternalRow.empty)
    checkEvaluation(Greatest(Seq(Literal("abc"), Literal("aaaa"))), "abc", InternalRow.empty)
    checkEvaluation(Greatest(Seq(Literal(true), Literal(false))), true, InternalRow.empty)
    checkEvaluation(
      Greatest(Seq(
        Literal(BigDecimal("1234567890987654321123456")),
        Literal(BigDecimal("1234567890987654321123458")))),
      BigDecimal("1234567890987654321123458"), InternalRow.empty)
    checkEvaluation(Greatest(
      Seq(Literal(Date.valueOf("2015-01-01")), Literal(Date.valueOf("2015-07-01")))),
      Date.valueOf("2015-07-01"), InternalRow.empty)
    checkEvaluation(
      Greatest(Seq(
        Literal(Timestamp.valueOf("2015-07-01 08:00:00")),
        Literal(Timestamp.valueOf("2015-07-01 10:00:00")))),
      Timestamp.valueOf("2015-07-01 10:00:00"), InternalRow.empty)

    // Type checking error
    assert(
      Greatest(Seq(Literal(1), Literal("1"))).checkInputDataTypes() ==
        TypeCheckFailure("The expressions should all have the same type, " +
          "got GREATEST(int, string)."))

    DataTypeTestUtils.ordered.foreach { dt =>
      checkConsistencyBetweenInterpretedAndCodegen(Greatest, dt, 2)
    }
  }
}
