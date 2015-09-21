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

import java.sql.{Timestamp, Date}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.types._


class ConditionalExpressionSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("if") {
    val testcases = Seq[(java.lang.Boolean, Integer, Integer, Integer)](
      (true, 1, 2, 1),
      (false, 1, 2, 2),
      (null, 1, 2, 2),
      (true, null, 2, null),
      (false, 1, null, null),
      (null, null, 2, 2),
      (null, 1, null, null)
    )

    // dataType must match T.
    def testIf(convert: (Integer => Any), dataType: DataType): Unit = {
      for ((predicate, trueValue, falseValue, expected) <- testcases) {
        val trueValueConverted = if (trueValue == null) null else convert(trueValue)
        val falseValueConverted = if (falseValue == null) null else convert(falseValue)
        val expectedConverted = if (expected == null) null else convert(expected)

        checkEvaluation(
          If(Literal.create(predicate, BooleanType),
            Literal.create(trueValueConverted, dataType),
            Literal.create(falseValueConverted, dataType)),
          expectedConverted)
      }
    }

    testIf(_ == 1, BooleanType)
    testIf(_.toShort, ShortType)
    testIf(identity, IntegerType)
    testIf(_.toLong, LongType)

    testIf(_.toFloat, FloatType)
    testIf(_.toDouble, DoubleType)
    testIf(Decimal(_), DecimalType.USER_DEFAULT)

    testIf(identity, DateType)
    testIf(_.toLong, TimestampType)

    testIf(_.toString, StringType)

    DataTypeTestUtils.propertyCheckSupported.foreach { dt =>
      checkConsistencyBetweenInterpretedAndCodegen(If, BooleanType, dt, dt)
    }
  }

  test("case when") {
    val row = create_row(null, false, true, "a", "b", "c")
    val c1 = 'a.boolean.at(0)
    val c2 = 'a.boolean.at(1)
    val c3 = 'a.boolean.at(2)
    val c4 = 'a.string.at(3)
    val c5 = 'a.string.at(4)
    val c6 = 'a.string.at(5)

    checkEvaluation(CaseWhen(Seq(c1, c4, c6)), "c", row)
    checkEvaluation(CaseWhen(Seq(c2, c4, c6)), "c", row)
    checkEvaluation(CaseWhen(Seq(c3, c4, c6)), "a", row)
    checkEvaluation(CaseWhen(Seq(Literal.create(null, BooleanType), c4, c6)), "c", row)
    checkEvaluation(CaseWhen(Seq(Literal.create(false, BooleanType), c4, c6)), "c", row)
    checkEvaluation(CaseWhen(Seq(Literal.create(true, BooleanType), c4, c6)), "a", row)

    checkEvaluation(CaseWhen(Seq(c3, c4, c2, c5, c6)), "a", row)
    checkEvaluation(CaseWhen(Seq(c2, c4, c3, c5, c6)), "b", row)
    checkEvaluation(CaseWhen(Seq(c1, c4, c2, c5, c6)), "c", row)
    checkEvaluation(CaseWhen(Seq(c1, c4, c2, c5)), null, row)

    assert(CaseWhen(Seq(c2, c4, c6)).nullable === true)
    assert(CaseWhen(Seq(c2, c4, c3, c5, c6)).nullable === true)
    assert(CaseWhen(Seq(c2, c4, c3, c5)).nullable === true)

    val c4_notNull = 'a.boolean.notNull.at(3)
    val c5_notNull = 'a.boolean.notNull.at(4)
    val c6_notNull = 'a.boolean.notNull.at(5)

    assert(CaseWhen(Seq(c2, c4_notNull, c6_notNull)).nullable === false)
    assert(CaseWhen(Seq(c2, c4, c6_notNull)).nullable === true)
    assert(CaseWhen(Seq(c2, c4_notNull, c6)).nullable === true)

    assert(CaseWhen(Seq(c2, c4_notNull, c3, c5_notNull, c6_notNull)).nullable === false)
    assert(CaseWhen(Seq(c2, c4, c3, c5_notNull, c6_notNull)).nullable === true)
    assert(CaseWhen(Seq(c2, c4_notNull, c3, c5, c6_notNull)).nullable === true)
    assert(CaseWhen(Seq(c2, c4_notNull, c3, c5_notNull, c6)).nullable === true)

    assert(CaseWhen(Seq(c2, c4_notNull, c3, c5_notNull)).nullable === true)
    assert(CaseWhen(Seq(c2, c4, c3, c5_notNull)).nullable === true)
    assert(CaseWhen(Seq(c2, c4_notNull, c3, c5)).nullable === true)
  }

  test("case key when") {
    val row = create_row(null, 1, 2, "a", "b", "c")
    val c1 = 'a.int.at(0)
    val c2 = 'a.int.at(1)
    val c3 = 'a.int.at(2)
    val c4 = 'a.string.at(3)
    val c5 = 'a.string.at(4)
    val c6 = 'a.string.at(5)

    val literalNull = Literal.create(null, IntegerType)
    val literalInt = Literal(1)
    val literalString = Literal("a")

    checkEvaluation(CaseKeyWhen(c1, Seq(c2, c4, c5)), "b", row)
    checkEvaluation(CaseKeyWhen(c1, Seq(c2, c4, literalNull, c5, c6)), "c", row)
    checkEvaluation(CaseKeyWhen(c2, Seq(literalInt, c4, c5)), "a", row)
    checkEvaluation(CaseKeyWhen(c2, Seq(c1, c4, c5)), "b", row)
    checkEvaluation(CaseKeyWhen(c4, Seq(literalString, c2, c3)), 1, row)
    checkEvaluation(CaseKeyWhen(c4, Seq(c6, c3, c5, c2, Literal(3))), 3, row)

    checkEvaluation(CaseKeyWhen(literalInt, Seq(c2, c4, c5)), "a", row)
    checkEvaluation(CaseKeyWhen(literalString, Seq(c5, c2, c4, c3)), 2, row)
    checkEvaluation(CaseKeyWhen(c6, Seq(c5, c2, c4, c3)), null, row)
    checkEvaluation(CaseKeyWhen(literalNull, Seq(c2, c5, c1, c6)), null, row)
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

    DataTypeTestUtils.ordered.foreach { dt =>
      checkConsistencyBetweenInterpretedAndCodegen(Greatest, dt, 2)
    }
  }
}
