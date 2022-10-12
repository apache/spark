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
import java.time.{Duration, Period}
import java.time.temporal.ChronoUnit

import org.apache.spark.{SparkArithmeticException, SparkFunSuite}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.trees.CurrentOrigin.withOrigin
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.internal.SQLConf
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

    Seq("true", "false").foreach { failOnError =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> failOnError) {
        DataTypeTestUtils.numericAndInterval.foreach { tpe =>
          checkConsistencyBetweenInterpretedAndCodegenAllowingException(Add(_, _), tpe, tpe)
        }
      }
    }
  }

  private def getMaxValue(dt: DataType): Literal = dt match {
    case IntegerType => Literal.create(Int.MaxValue, IntegerType)
    case LongType => Literal.create(Long.MaxValue, LongType)
    case _ => s"Fail to find the max value of $dt"
  }

  private def getMinValue(dt: DataType): Literal = dt match {
    case IntegerType => Literal.create(Int.MinValue, IntegerType)
    case LongType => Literal.create(Long.MinValue, LongType)
    case _ => s"Fail to find the min value of $dt"
  }

  test("Add: Overflow exception should contain SQL text context") {
    Seq(IntegerType, LongType).foreach { dt =>
      val maxValue = getMaxValue(dt)
      val query = s"${maxValue.sql} + ${maxValue.sql}"
      val o = Origin(
        line = Some(1),
        startPosition = Some(7),
        startIndex = Some(7),
        stopIndex = Some(7 + query.length -1),
        sqlText = Some(s"select $query"))
      withOrigin(o) {
        val expr = Add(maxValue, maxValue, EvalMode.ANSI)
        checkExceptionInExpression[ArithmeticException](expr, EmptyRow, query)
      }
    }
  }

  test("- (UnaryMinus)") {
    testNumericDataTypes { convert =>
      val input = Literal(convert(1))
      val dataType = input.dataType
      checkEvaluation(UnaryMinus(input), convert(-1))
      checkEvaluation(UnaryMinus(Literal.create(null, dataType)), null)
    }
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      checkEvaluation(UnaryMinus(Literal(Long.MinValue)), Long.MinValue)
      checkEvaluation(UnaryMinus(Literal(Int.MinValue)), Int.MinValue)
      checkEvaluation(UnaryMinus(Literal(Short.MinValue)), Short.MinValue)
      checkEvaluation(UnaryMinus(Literal(Byte.MinValue)), Byte.MinValue)
    }
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      checkExceptionInExpression[ArithmeticException](
        UnaryMinus(Literal(Long.MinValue)), "overflow")
      checkExceptionInExpression[ArithmeticException](
        UnaryMinus(Literal(Int.MinValue)), "overflow")
      checkExceptionInExpression[ArithmeticException](
        UnaryMinus(Literal(Short.MinValue)), "overflow")
      checkExceptionInExpression[ArithmeticException](
        UnaryMinus(Literal(Byte.MinValue)), "overflow")
      checkEvaluation(UnaryMinus(positiveShortLit), (- positiveShort).toShort)
      checkEvaluation(UnaryMinus(negativeShortLit), (- negativeShort).toShort)
      checkEvaluation(UnaryMinus(positiveIntLit), - positiveInt)
      checkEvaluation(UnaryMinus(negativeIntLit), - negativeInt)
      checkEvaluation(UnaryMinus(positiveLongLit), - positiveLong)
      checkEvaluation(UnaryMinus(negativeLongLit), - negativeLong)
    }
    checkEvaluation(UnaryMinus(positiveShortLit), (- positiveShort).toShort)
    checkEvaluation(UnaryMinus(negativeShortLit), (- negativeShort).toShort)
    checkEvaluation(UnaryMinus(positiveIntLit), - positiveInt)
    checkEvaluation(UnaryMinus(negativeIntLit), - negativeInt)
    checkEvaluation(UnaryMinus(positiveLongLit), - positiveLong)
    checkEvaluation(UnaryMinus(negativeLongLit), - negativeLong)

    Seq("true", "false").foreach { failOnError =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> failOnError) {
        DataTypeTestUtils.numericAndInterval.foreach { tpe =>
          checkConsistencyBetweenInterpretedAndCodegenAllowingException(UnaryMinus(_), tpe)
        }
      }
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

    Seq("true", "false").foreach { failOnError =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> failOnError) {
        DataTypeTestUtils.numericAndInterval.foreach { tpe =>
          checkConsistencyBetweenInterpretedAndCodegenAllowingException(Subtract(_, _), tpe, tpe)
        }
      }
    }
  }

  test("Minus: Overflow exception should contain SQL text context") {
    Seq(IntegerType, LongType).foreach { dt =>
      val minValue = getMinValue(dt)
      val maxValue = getMaxValue(dt)
      val query = s"${minValue.sql} - ${maxValue.sql}"
      val o = Origin(
        line = Some(1),
        startPosition = Some(7),
        startIndex = Some(7),
        stopIndex = Some(7 + query.length -1),
        sqlText = Some(s"select $query"))
      withOrigin(o) {
        val expr = Subtract(minValue, maxValue, EvalMode.ANSI)
        checkExceptionInExpression[ArithmeticException](expr, EmptyRow, query)
      }
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

    Seq("true", "false").foreach { failOnError =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> failOnError) {
        DataTypeTestUtils.numericTypeWithoutDecimal.foreach { tpe =>
          checkConsistencyBetweenInterpretedAndCodegenAllowingException(Multiply(_, _), tpe, tpe)
        }
      }
    }
  }

  test("Multiply: Overflow exception should contain SQL text context") {
    Seq(IntegerType, LongType).foreach { dt =>
      val maxValue = getMaxValue(dt)
      val query = s"${maxValue.sql} * ${maxValue.sql}"
      val o = Origin(
        line = Some(1),
        startPosition = Some(7),
        startIndex = Some(7),
        stopIndex = Some(7 + query.length -1),
        sqlText = Some(s"select $query"))
      withOrigin(o) {
        val expr = Multiply(maxValue, maxValue, EvalMode.ANSI)
        checkExceptionInExpression[ArithmeticException](expr, EmptyRow, query)
      }
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
      checkEvaluation(Divide(left, right), convert(2))
      checkEvaluation(Divide(Literal.create(null, left.dataType), right), null)
      checkEvaluation(Divide(left, Literal.create(null, right.dataType)), null)
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
        checkEvaluation(Divide(left, Literal(convert(0))), null) // divide by zero
      }
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
        checkExceptionInExpression[ArithmeticException](
          Divide(left, Literal(convert(0))), "Division by zero")
      }
    }

    Seq("true", "false").foreach { failOnError =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> failOnError) {
        Seq(DoubleType, DecimalType.SYSTEM_DEFAULT).foreach { tpe =>
          checkConsistencyBetweenInterpretedAndCodegenAllowingException(Divide(_, _), tpe, tpe)
        }
      }
    }
  }

  test("Divide: divide by 0 exception should contain SQL text context") {
    val query = "1234.5D / 0"
    val o = Origin(
      line = Some(1),
      startPosition = Some(7),
      startIndex = Some(7),
      stopIndex = Some(7 + query.length -1),
      sqlText = Some(s"select $query"))
    withOrigin(o) {
      val expr = Divide(Literal(1234.5, DoubleType), Literal(0.0, DoubleType), EvalMode.ANSI)
      checkExceptionInExpression[ArithmeticException](expr, EmptyRow, query)
    }
  }

  private def testDecimalAndLongType(testFunc: (Int => Any) => Unit): Unit = {
    testFunc(_.toLong)
    testFunc(Decimal(_))
  }

  test("/ (Divide) for Long and Decimal type") {
    testDecimalAndLongType { convert =>
      val left = Literal(convert(1))
      val right = Literal(convert(2))
      checkEvaluation(IntegralDivide(left, right), 0L)
      checkEvaluation(IntegralDivide(Literal.create(null, left.dataType), right), null)
      checkEvaluation(IntegralDivide(left, Literal.create(null, right.dataType)), null)
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
        checkEvaluation(IntegralDivide(left, Literal(convert(0))), null) // divide by zero
      }
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
        checkExceptionInExpression[ArithmeticException](
          IntegralDivide(left, Literal(convert(0))), "Division by zero")
      }
    }
    checkEvaluation(IntegralDivide(positiveLongLit, negativeLongLit), 0L)

    Seq("true", "false").foreach { failOnError =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> failOnError) {
        Seq(LongType, DecimalType.SYSTEM_DEFAULT).foreach { tpe =>
          checkConsistencyBetweenInterpretedAndCodegenAllowingException(
            IntegralDivide(_, _), tpe, tpe)
        }
      }
    }
  }

  test("IntegralDivide: throw exception on overflow under ANSI mode") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      checkExceptionInExpression[ArithmeticException](
        IntegralDivide(Literal(Long.MinValue), Literal(-1L)), "Overflow in integral divide.")
    }
  }

  test("IntegralDivide: exception should contain SQL text context") {
    Seq(-1L, 0L).foreach { right =>
      val query = s"${Long.MinValue} div right"
      val o = Origin(
        line = Some(1),
        startPosition = Some(7),
        startIndex = Some(7),
        stopIndex = Some(7 + query.length -1),
        sqlText = Some(s"select $query"))
      withOrigin(o) {
        val expr =
          IntegralDivide(
            Literal(Long.MinValue, LongType), Literal(right, LongType), EvalMode.ANSI)
        checkExceptionInExpression[ArithmeticException](expr, EmptyRow, query)
      }
    }
  }

  test("% (Remainder)") {
    testNumericDataTypes { convert =>
      val left = Literal(convert(1))
      val right = Literal(convert(2))
      checkEvaluation(Remainder(left, right), convert(1))
      checkEvaluation(Remainder(Literal.create(null, left.dataType), right), null)
      checkEvaluation(Remainder(left, Literal.create(null, right.dataType)), null)
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
        checkEvaluation(Remainder(left, Literal(convert(0))), null) // mod by 0
      }
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
        checkExceptionInExpression[ArithmeticException](
          Remainder(left, Literal(convert(0))), "Division by zero")
      }
    }
    checkEvaluation(Remainder(positiveShortLit, positiveShortLit), 0.toShort)
    checkEvaluation(Remainder(negativeShortLit, negativeShortLit), 0.toShort)
    checkEvaluation(Remainder(positiveIntLit, positiveIntLit), 0)
    checkEvaluation(Remainder(negativeIntLit, negativeIntLit), 0)
    checkEvaluation(Remainder(positiveLongLit, positiveLongLit), 0L)
    checkEvaluation(Remainder(negativeLongLit, negativeLongLit), 0L)

    Seq("true", "false").foreach { failOnError =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> failOnError) {
        DataTypeTestUtils.numericTypeWithoutDecimal.foreach { tpe =>
          checkConsistencyBetweenInterpretedAndCodegenAllowingException(Remainder(_, _), tpe, tpe)
        }
      }
    }
  }

  test("Remainder/Pmod: exception should contain SQL text context") {
    Seq(("%", Remainder), ("pmod", Pmod)).foreach { case (symbol, exprBuilder) =>
      val query = s"1L $symbol 0L"
      val o = Origin(
        line = Some(1),
        startPosition = Some(7),
        startIndex = Some(7),
        stopIndex = Some(7 + query.length -1),
        sqlText = Some(s"select $query"))
      withOrigin(o) {
        val expression = exprBuilder(Literal(1L, LongType), Literal(0L, LongType), EvalMode.ANSI)
        checkExceptionInExpression[ArithmeticException](expression, EmptyRow, query)
      }
    }
  }

  test("SPARK-17617: % (Remainder) double % double on super big double") {
    val leftDouble = Literal(-5083676433652386516D)
    val rightDouble = Literal(10D)
    checkEvaluation(Remainder(leftDouble, rightDouble), -6.0D)

    // Float has smaller precision
    val leftFloat = Literal(-5083676433652386516F)
    val rightFloat = Literal(10F)
    checkEvaluation(Remainder(leftFloat, rightFloat), -2.0F)
  }

  test("Abs") {
    // SPARK-34742: when the input is not MinValue of integral types, the results of function ABS
    //              should be the same with/without ANSI mode on.
    Seq("true", "false").foreach { ansiEnabled =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> ansiEnabled) {
        testNumericDataTypes { convert =>
          val input = Literal(convert(1))
          val dataType = input.dataType
          checkEvaluation(Abs(Literal(convert(0))), convert(0))
          checkEvaluation(Abs(Literal(convert(1))), convert(1))
          checkEvaluation(Abs(Literal(convert(-1))), convert(1))
          checkEvaluation(Abs(Literal.create(null, dataType)), null)
        }
      }
    }
    checkEvaluation(Abs(positiveShortLit), positiveShort)
    checkEvaluation(Abs(negativeShortLit), (- negativeShort).toShort)
    checkEvaluation(Abs(positiveIntLit), positiveInt)
    checkEvaluation(Abs(negativeIntLit), - negativeInt)
    checkEvaluation(Abs(positiveLongLit), positiveLong)
    checkEvaluation(Abs(negativeLongLit), - negativeLong)

    DataTypeTestUtils.numericTypeWithoutDecimal.foreach { tpe =>
      checkConsistencyBetweenInterpretedAndCodegen((e: Expression) => Abs(e, false), tpe)
    }
  }

  test("SPARK-34742: Abs throws exception when input is out of range in ANSI mode") {
    val minValues = Seq(
      Literal(Byte.MinValue, ByteType),
      Literal(Short.MinValue, ShortType),
      Literal(Int.MinValue),
      Literal(Long.MinValue)
    )
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      minValues.foreach { v =>
        checkExceptionInExpression[ArithmeticException](Abs(v), "overflow")
      }
    }
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      minValues.foreach { v =>
        checkEvaluation(Abs(v), v.value)
      }
    }
  }

  test("pmod") {
    testNumericDataTypes { convert =>
      val left = Literal(convert(7))
      val right = Literal(convert(3))
      checkEvaluation(Pmod(left, right), convert(1))
      checkEvaluation(Pmod(Literal.create(null, left.dataType), right), null)
      checkEvaluation(Pmod(left, Literal.create(null, right.dataType)), null)
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
        checkEvaluation(Pmod(left, Literal(convert(0))), null) // mod by 0
      }
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
        checkExceptionInExpression[ArithmeticException](
          Pmod(left, Literal(convert(0))), "Division by zero")
      }
    }
    checkEvaluation(Pmod(Literal(-7), Literal(3)), 2)
    checkEvaluation(Pmod(Literal(7.2D), Literal(4.1D)), 3.1000000000000005)
    checkEvaluation(Pmod(Literal(Decimal(0.7)), Literal(Decimal(0.2))), Decimal(0.1))
    checkEvaluation(Pmod(Literal(2L), Literal(Long.MaxValue)), 2L)
    checkEvaluation(Pmod(positiveShort, negativeShort), positiveShort.toShort)
    checkEvaluation(Pmod(positiveInt, negativeInt), positiveInt)
    checkEvaluation(Pmod(positiveLong, negativeLong), positiveLong)

    Seq("true", "false").foreach { failOnError =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> failOnError) {
        DataTypeTestUtils.numericTypeWithoutDecimal.foreach { tpe =>
          checkConsistencyBetweenInterpretedAndCodegenAllowingException(Pmod(_, _), tpe, tpe)
        }
      }
    }
  }

  test("function least") {
    val row = create_row(1, 2, "a", "b", "c")
    val c1 = $"a".int.at(0)
    val c2 = $"a".int.at(1)
    val c3 = $"a".string.at(2)
    val c4 = $"a".string.at(3)
    val c5 = $"a".string.at(4)
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
    Least(Seq(Literal(1), Literal("1"))).checkInputDataTypes() match {
      case TypeCheckResult.DataTypeMismatch(errorSubClass, messageParameters) =>
        assert(errorSubClass == "DATA_DIFF_TYPES")
        assert(messageParameters === Map(
          "functionName" -> "`least`",
          "dataType" -> "[\"INT\", \"STRING\"]"))
    }

    DataTypeTestUtils.ordered.foreach { dt =>
      checkConsistencyBetweenInterpretedAndCodegen(Least, dt, 2)
    }

    val least = Least(Seq(
      Literal.create(Seq(1, 2), ArrayType(IntegerType, containsNull = false)),
      Literal.create(Seq(1, 3, null), ArrayType(IntegerType, containsNull = true))))
    assert(least.dataType === ArrayType(IntegerType, containsNull = true))
    checkEvaluation(least, Seq(1, 2))
  }

  test("function greatest") {
    val row = create_row(1, 2, "a", "b", "c")
    val c1 = $"a".int.at(0)
    val c2 = $"a".int.at(1)
    val c3 = $"a".string.at(2)
    val c4 = $"a".string.at(3)
    val c5 = $"a".string.at(4)
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
    Greatest(Seq(Literal(1), Literal("1"))).checkInputDataTypes() match {
      case TypeCheckResult.DataTypeMismatch(errorSubClass, messageParameters) =>
        assert(errorSubClass == "DATA_DIFF_TYPES")
        assert(messageParameters === Map(
          "functionName" -> "`greatest`",
          "dataType" -> "[\"INT\", \"STRING\"]"))
    }

    DataTypeTestUtils.ordered.foreach { dt =>
      checkConsistencyBetweenInterpretedAndCodegen(Greatest, dt, 2)
    }

    val greatest = Greatest(Seq(
      Literal.create(Seq(1, 2), ArrayType(IntegerType, containsNull = false)),
      Literal.create(Seq(1, 3, null), ArrayType(IntegerType, containsNull = true))))
    assert(greatest.dataType === ArrayType(IntegerType, containsNull = true))
    checkEvaluation(greatest, Seq(1, 3, null))
  }

  test("SPARK-22499: Least and greatest should not generate codes beyond 64KB") {
    val N = 2000
    val strings = (1 to N).map(x => "s" * x)
    val inputsExpr = strings.map(Literal.create(_, StringType))

    checkEvaluation(Least(inputsExpr), "s" * 1, EmptyRow)
    checkEvaluation(Greatest(inputsExpr), "s" * N, EmptyRow)
  }

  test("SPARK-22704: Least and greatest use less global variables") {
    val ctx1 = new CodegenContext()
    Least(Seq(Literal(1), Literal(1))).genCode(ctx1)
    assert(ctx1.inlinedMutableStates.size == 1)

    val ctx2 = new CodegenContext()
    Greatest(Seq(Literal(1), Literal(1))).genCode(ctx2)
    assert(ctx2.inlinedMutableStates.size == 1)
  }

  test("SPARK-28322: IntegralDivide supports decimal type") {
    checkEvaluation(IntegralDivide(Literal(Decimal(1)), Literal(Decimal(2))), 0L)
    checkEvaluation(IntegralDivide(Literal(Decimal(2.4)), Literal(Decimal(1.1))), 2L)
    checkEvaluation(IntegralDivide(Literal(Decimal(1.2)), Literal(Decimal(1.1))), 1L)
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      checkEvaluation(
        IntegralDivide(Literal(Decimal(0.2)), Literal(Decimal(0.0))), null) // mod by 0
    }
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      checkExceptionInExpression[ArithmeticException](
        IntegralDivide(Literal(Decimal(0.2)), Literal(Decimal(0.0))), "Division by zero")
    }
    // overflows long and so returns a wrong result
    checkEvaluation(IntegralDivide(
      Literal(Decimal("99999999999999999999999999999999999")), Literal(Decimal(0.001))),
      687399551400672280L)
    // overflow during promote precision
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      checkEvaluation(IntegralDivide(
        Literal(Decimal("99999999999999999999999999999999999999")), Literal(Decimal(0.00001))),
        null)
    }
  }

  test("SPARK-24598: overflow on long returns wrong result") {
    val maxLongLiteral = Literal(Long.MaxValue)
    val minLongLiteral = Literal(Long.MinValue)
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      val e1 = Add(maxLongLiteral, Literal(1L))
      val e2 = Subtract(maxLongLiteral, Literal(-1L))
      val e3 = Multiply(maxLongLiteral, Literal(2L))
      val e4 = Add(minLongLiteral, minLongLiteral)
      val e5 = Subtract(minLongLiteral, maxLongLiteral)
      val e6 = Multiply(minLongLiteral, minLongLiteral)
      Seq(e1, e2, e3, e4, e5, e6).foreach { e =>
        checkExceptionInExpression[ArithmeticException](e, "overflow")
      }
    }
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      val e1 = Add(maxLongLiteral, Literal(1L))
      val e2 = Subtract(maxLongLiteral, Literal(-1L))
      val e3 = Multiply(maxLongLiteral, Literal(2L))
      val e4 = Add(minLongLiteral, minLongLiteral)
      val e5 = Subtract(minLongLiteral, maxLongLiteral)
      val e6 = Multiply(minLongLiteral, minLongLiteral)
      checkEvaluation(e1, Long.MinValue)
      checkEvaluation(e2, Long.MinValue)
      checkEvaluation(e3, -2L)
      checkEvaluation(e4, 0L)
      checkEvaluation(e5, 1L)
      checkEvaluation(e6, 0L)
    }
  }

  test("SPARK-24598: overflow on integer returns wrong result") {
    val maxIntLiteral = Literal(Int.MaxValue)
    val minIntLiteral = Literal(Int.MinValue)
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      val e1 = Add(maxIntLiteral, Literal(1))
      val e2 = Subtract(maxIntLiteral, Literal(-1))
      val e3 = Multiply(maxIntLiteral, Literal(2))
      val e4 = Add(minIntLiteral, minIntLiteral)
      val e5 = Subtract(minIntLiteral, maxIntLiteral)
      val e6 = Multiply(minIntLiteral, minIntLiteral)
      Seq(e1, e2, e3, e4, e5, e6).foreach { e =>
        checkExceptionInExpression[ArithmeticException](e, "overflow")
      }
    }
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      val e1 = Add(maxIntLiteral, Literal(1))
      val e2 = Subtract(maxIntLiteral, Literal(-1))
      val e3 = Multiply(maxIntLiteral, Literal(2))
      val e4 = Add(minIntLiteral, minIntLiteral)
      val e5 = Subtract(minIntLiteral, maxIntLiteral)
      val e6 = Multiply(minIntLiteral, minIntLiteral)
      checkEvaluation(e1, Int.MinValue)
      checkEvaluation(e2, Int.MinValue)
      checkEvaluation(e3, -2)
      checkEvaluation(e4, 0)
      checkEvaluation(e5, 1)
      checkEvaluation(e6, 0)
    }
  }

  test("SPARK-24598: overflow on short returns wrong result") {
    val maxShortLiteral = Literal(Short.MaxValue)
    val minShortLiteral = Literal(Short.MinValue)
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      val e1 = Add(maxShortLiteral, Literal(1.toShort))
      val e2 = Subtract(maxShortLiteral, Literal((-1).toShort))
      val e3 = Multiply(maxShortLiteral, Literal(2.toShort))
      val e4 = Add(minShortLiteral, minShortLiteral)
      val e5 = Subtract(minShortLiteral, maxShortLiteral)
      val e6 = Multiply(minShortLiteral, minShortLiteral)
      Seq(e1, e2, e3, e4, e5, e6).foreach { e =>
        checkExceptionInExpression[ArithmeticException](e, "overflow")
      }
    }
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      val e1 = Add(maxShortLiteral, Literal(1.toShort))
      val e2 = Subtract(maxShortLiteral, Literal((-1).toShort))
      val e3 = Multiply(maxShortLiteral, Literal(2.toShort))
      val e4 = Add(minShortLiteral, minShortLiteral)
      val e5 = Subtract(minShortLiteral, maxShortLiteral)
      val e6 = Multiply(minShortLiteral, minShortLiteral)
      checkEvaluation(e1, Short.MinValue)
      checkEvaluation(e2, Short.MinValue)
      checkEvaluation(e3, (-2).toShort)
      checkEvaluation(e4, 0.toShort)
      checkEvaluation(e5, 1.toShort)
      checkEvaluation(e6, 0.toShort)
    }
  }

  test("SPARK-24598: overflow on byte returns wrong result") {
    val maxByteLiteral = Literal(Byte.MaxValue)
    val minByteLiteral = Literal(Byte.MinValue)
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      val e1 = Add(maxByteLiteral, Literal(1.toByte))
      val e2 = Subtract(maxByteLiteral, Literal((-1).toByte))
      val e3 = Multiply(maxByteLiteral, Literal(2.toByte))
      val e4 = Add(minByteLiteral, minByteLiteral)
      val e5 = Subtract(minByteLiteral, maxByteLiteral)
      val e6 = Multiply(minByteLiteral, minByteLiteral)
      Seq(e1, e2, e3, e4, e5, e6).foreach { e =>
        checkExceptionInExpression[ArithmeticException](e, "overflow")
      }
    }
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      val e1 = Add(maxByteLiteral, Literal(1.toByte))
      val e2 = Subtract(maxByteLiteral, Literal((-1).toByte))
      val e3 = Multiply(maxByteLiteral, Literal(2.toByte))
      val e4 = Add(minByteLiteral, minByteLiteral)
      val e5 = Subtract(minByteLiteral, maxByteLiteral)
      val e6 = Multiply(minByteLiteral, minByteLiteral)
      checkEvaluation(e1, Byte.MinValue)
      checkEvaluation(e2, Byte.MinValue)
      checkEvaluation(e3, (-2).toByte)
      checkEvaluation(e4, 0.toByte)
      checkEvaluation(e5, 1.toByte)
      checkEvaluation(e6, 0.toByte)
    }
  }

  test("SPARK-33008: division by zero on divide-like operations returns incorrect result") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      val operators: Seq[((Expression, Expression) => Expression, ((Int => Any) => Unit) => Unit)] =
        Seq((Divide(_, _), testDecimalAndDoubleType),
          (IntegralDivide(_, _), testDecimalAndLongType),
          (Remainder(_, _), testNumericDataTypes),
          (Pmod(_, _), testNumericDataTypes))
      operators.foreach { case (operator, testTypesFn) =>
        testTypesFn { convert =>
          val one = Literal(convert(1))
          val zero = Literal(convert(0))
          checkEvaluation(operator(Literal.create(null, one.dataType), zero), null)
          checkEvaluation(operator(one, Literal.create(null, zero.dataType)), null)
          checkExceptionInExpression[ArithmeticException](operator(one, zero), "Division by zero")
        }
      }
    }
  }

  test("SPARK-34677: exact add and subtract of day-time and year-month intervals") {
    Seq(EvalMode.ANSI, EvalMode.LEGACY).foreach { evalMode =>
      checkExceptionInExpression[ArithmeticException](
        UnaryMinus(
          Literal.create(Period.ofMonths(Int.MinValue), YearMonthIntervalType()),
          evalMode == EvalMode.ANSI),
        "overflow")
      checkExceptionInExpression[ArithmeticException](
        Subtract(
          Literal.create(Period.ofMonths(Int.MinValue), YearMonthIntervalType()),
          Literal.create(Period.ofMonths(10), YearMonthIntervalType()),
          evalMode
        ),
        "overflow")
      checkExceptionInExpression[ArithmeticException](
        Add(
          Literal.create(Period.ofMonths(Int.MaxValue), YearMonthIntervalType()),
          Literal.create(Period.ofMonths(10), YearMonthIntervalType()),
          evalMode
        ),
        "overflow")

      checkExceptionInExpression[ArithmeticException](
        Subtract(
          Literal.create(Duration.ofDays(-106751991), DayTimeIntervalType()),
          Literal.create(Duration.ofDays(10), DayTimeIntervalType()),
          evalMode
        ),
        "overflow")
      checkExceptionInExpression[ArithmeticException](
        Add(
          Literal.create(Duration.ofDays(106751991), DayTimeIntervalType()),
          Literal.create(Duration.ofDays(10), DayTimeIntervalType()),
          evalMode
        ),
        "overflow")
    }
  }

  test("SPARK-34920: error class") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      val operators: Seq[((Expression, Expression) => Expression, ((Int => Any) => Unit) => Unit)] =
        Seq((Divide(_, _), testDecimalAndDoubleType),
          (IntegralDivide(_, _), testDecimalAndLongType),
          (Remainder(_, _), testNumericDataTypes),
          (Pmod(_, _), testNumericDataTypes))
      operators.foreach { case (operator, testTypesFn) =>
        testTypesFn { convert =>
          val one = Literal(convert(1))
          val zero = Literal(convert(0))
          checkEvaluation(operator(Literal.create(null, one.dataType), zero), null)
          checkEvaluation(operator(one, Literal.create(null, zero.dataType)), null)
          checkExceptionInExpression[SparkArithmeticException](operator(one, zero),
            "Division by zero")
        }
      }
    }
  }

  test("SPARK-36920: Support year-month intervals by ABS") {
    checkEvaluation(Abs(Literal(Period.ZERO)), Period.ZERO)
    checkEvaluation(Abs(Literal(Period.ofMonths(-1))), Period.ofMonths(1))
    checkEvaluation(Abs(Literal(Period.ofYears(-12345))), Period.ofYears(12345))
    checkEvaluation(Abs(Literal.create(null, YearMonthIntervalType())), null)
    checkExceptionInExpression[ArithmeticException](
      Abs(Literal(Period.ofMonths(Int.MinValue))),
      "overflow")

    DataTypeTestUtils.yearMonthIntervalTypes.foreach { tpe =>
      checkConsistencyBetweenInterpretedAndCodegen((e: Expression) => Abs(e, false), tpe)
    }
  }

  test("SPARK-36920: Support day-time intervals by ABS") {
    checkEvaluation(Abs(Literal(Duration.ZERO)), Duration.ZERO)
    checkEvaluation(
      Abs(Literal(Duration.of(-1, ChronoUnit.MICROS))),
      Duration.of(1, ChronoUnit.MICROS))
    checkEvaluation(Abs(Literal(Duration.ofDays(-12345))), Duration.ofDays(12345))
    checkEvaluation(Abs(Literal.create(null, DayTimeIntervalType())), null)
    checkExceptionInExpression[ArithmeticException](
      Abs(Literal(Duration.of(Long.MinValue, ChronoUnit.MICROS))),
      "overflow")

    DataTypeTestUtils.dayTimeIntervalTypes.foreach { tpe =>
      checkConsistencyBetweenInterpretedAndCodegen((e: Expression) => Abs(e, false), tpe)
    }
  }

  test("SPARK-36921: Support YearMonthIntervalType by div") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      checkEvaluation(IntegralDivide(Literal(Period.ZERO), Literal(Period.ZERO)), null)
      checkEvaluation(IntegralDivide(Literal(Period.ofYears(1)),
        Literal(Period.ZERO)), null)
      checkEvaluation(IntegralDivide(Period.ofMonths(Int.MinValue),
        Literal(Period.ZERO)), null)
      checkEvaluation(IntegralDivide(Period.ofMonths(Int.MaxValue),
        Literal(Period.ZERO)), null)
    }
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      checkExceptionInExpression[ArithmeticException](
        IntegralDivide(Literal(Period.ZERO), Literal(Period.ZERO)), "Division by zero")
      checkExceptionInExpression[ArithmeticException](
        IntegralDivide(Literal(Period.ofYears(1)), Literal(Period.ZERO)), "Division by zero")
      checkExceptionInExpression[ArithmeticException](
        IntegralDivide(Period.ofMonths(Int.MinValue), Literal(Period.ZERO)), "Division by zero")
      checkExceptionInExpression[ArithmeticException](
        IntegralDivide(Period.ofMonths(Int.MaxValue), Literal(Period.ZERO)), "Division by zero")
    }

    checkEvaluation(IntegralDivide(Literal.create(null, YearMonthIntervalType()),
      Literal.create(null, YearMonthIntervalType())), null)
    checkEvaluation(IntegralDivide(Literal.create(null, YearMonthIntervalType()),
      Literal(Period.ofYears(1))), null)
    checkEvaluation(IntegralDivide(Literal(Period.ofYears(1)),
      Literal.create(null, YearMonthIntervalType())), null)

    checkEvaluation(IntegralDivide(Period.ofMonths(Int.MaxValue),
      Period.ofMonths(Int.MaxValue)), 1L)
    checkEvaluation(IntegralDivide(Period.ofMonths(Int.MaxValue),
      Period.ofMonths(Int.MinValue)), 0L)
    checkEvaluation(IntegralDivide(Period.ofMonths(Int.MinValue),
      Period.ofMonths(Int.MinValue)), 1L)
    checkEvaluation(IntegralDivide(Period.ofMonths(Int.MinValue),
      Period.ofMonths(Int.MaxValue)), -1L)

    checkEvaluation(IntegralDivide(Literal(Period.ZERO),
      Literal(Period.ofYears(-1))), 0L)
    checkEvaluation(IntegralDivide(Literal(Period.ofYears(2)),
      Literal(Period.ofYears(1))), 2L)
    checkEvaluation(IntegralDivide(Literal(Period.ofYears(2)),
      Literal(Period.ofYears(-1))), -2L)
    checkEvaluation(IntegralDivide(Literal(Period.ofYears(1)),
      Literal(Period.ofMonths(3))), 4L)
    checkEvaluation(IntegralDivide(Literal(Period.ofYears(1)),
      Literal(Period.ofMonths(-3))), -4L)
    checkEvaluation(IntegralDivide(Literal(Period.ofYears(1)),
      Literal(Period.ofMonths(5))), 2L)
    checkEvaluation(IntegralDivide(Literal(Period.ofYears(1)),
      Literal(Period.ofMonths(-5))), -2L)
  }
  test("SPARK-36921: Support DayTimeIntervalType by div") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      checkEvaluation(IntegralDivide(Literal(Duration.ZERO), Literal(Duration.ZERO)), null)
      checkEvaluation(IntegralDivide(Literal(Duration.ofDays(1)),
        Literal(Duration.ZERO)), null)
      checkEvaluation(IntegralDivide(Literal(Duration.of(Long.MaxValue, ChronoUnit.MICROS)),
        Literal(Duration.ZERO)), null)
      checkEvaluation(IntegralDivide(Literal(Duration.of(Long.MinValue, ChronoUnit.MICROS)),
        Literal(Duration.ZERO)), null)
    }
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      checkExceptionInExpression[ArithmeticException](
        IntegralDivide(Literal(Duration.ZERO), Literal(Duration.ZERO)), "Division by zero")
      checkExceptionInExpression[ArithmeticException](
        IntegralDivide(Literal(Duration.ofDays(1)),
          Literal(Duration.ZERO)), "Division by zero")
      checkExceptionInExpression[ArithmeticException](
        IntegralDivide(Literal(Duration.of(Long.MaxValue, ChronoUnit.MICROS)),
          Literal(Duration.ZERO)), "Division by zero")
      checkExceptionInExpression[ArithmeticException](
        IntegralDivide(Literal(Duration.of(Long.MinValue, ChronoUnit.MICROS)),
          Literal(Duration.ZERO)), "Division by zero")
    }

    checkEvaluation(IntegralDivide(Literal.create(null, DayTimeIntervalType()),
      Literal.create(null, DayTimeIntervalType())), null)
    checkEvaluation(IntegralDivide(Literal.create(null, DayTimeIntervalType()),
      Literal(Duration.ofDays(1))), null)
    checkEvaluation(IntegralDivide(Literal(Duration.ofDays(1)),
      Literal.create(null, DayTimeIntervalType())), null)

    checkEvaluation(IntegralDivide(Literal(Duration.of(Long.MaxValue, ChronoUnit.MICROS)),
      Literal(Duration.of(Long.MaxValue, ChronoUnit.MICROS))), 1L)
    checkEvaluation(IntegralDivide(Literal(Duration.of(Long.MinValue, ChronoUnit.MICROS)),
      Literal(Duration.of(Long.MinValue, ChronoUnit.MICROS))), 1L)
    checkEvaluation(IntegralDivide(Literal(Duration.of(Long.MaxValue, ChronoUnit.MICROS)),
      Literal(Duration.of(Long.MinValue, ChronoUnit.MICROS))), 0L)
    checkEvaluation(IntegralDivide(Literal(Duration.of(Long.MinValue, ChronoUnit.MICROS)),
      Literal(Duration.of(Long.MaxValue, ChronoUnit.MICROS))), -1L)

    checkEvaluation(IntegralDivide(Literal(Duration.ZERO),
      Literal(Duration.ofDays(-1))), 0L)
    checkEvaluation(IntegralDivide(Literal(Duration.ofDays(2)),
      Literal(Duration.ofDays(1))), 2L)
    checkEvaluation(IntegralDivide(Literal(Duration.ofDays(2)),
      Literal(Duration.ofDays(-1))), -2L)
    checkEvaluation(IntegralDivide(Literal(Duration.ofDays(1)),
      Literal(Duration.ofHours(4))), 6L)
    checkEvaluation(IntegralDivide(Literal(Duration.ofDays(1)),
      Literal(Duration.ofHours(-4))), -6L)
    checkEvaluation(IntegralDivide(Literal(Duration.ofDays(1)),
      Literal(Duration.ofHours(5))), 4L)
    checkEvaluation(IntegralDivide(Literal(Duration.ofDays(1)),
      Literal(Duration.ofHours(-5))), -4L)
  }
}
