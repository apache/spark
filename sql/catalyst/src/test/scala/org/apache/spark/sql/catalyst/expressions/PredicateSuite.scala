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

import scala.collection.immutable.HashSet

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.RandomDataGenerator
import org.apache.spark.sql.types._


class PredicateSuite extends SparkFunSuite with ExpressionEvalHelper {

  private def booleanLogicTest(
    name: String,
    op: (Expression, Expression) => Expression,
    truthTable: Seq[(Any, Any, Any)]) {
    test(s"3VL $name") {
      truthTable.foreach {
        case (l, r, answer) =>
          val expr = op(Literal.create(l, BooleanType), Literal.create(r, BooleanType))
          checkEvaluation(expr, answer)
      }
    }
  }

  // scalastyle:off
  /**
   * Checks for three-valued-logic.  Based on:
   * http://en.wikipedia.org/wiki/Null_(SQL)#Comparisons_with_NULL_and_the_three-valued_logic_.283VL.29
   * I.e. in flat cpo "False -> Unknown -> True",
   *   OR is lowest upper bound,
   *   AND is greatest lower bound.
   * p       q       p OR q  p AND q  p = q
   * True    True    True    True     True
   * True    False   True    False    False
   * True    Unknown True    Unknown  Unknown
   * False   True    True    False    False
   * False   False   False   False    True
   * False   Unknown Unknown False    Unknown
   * Unknown True    True    Unknown  Unknown
   * Unknown False   Unknown False    Unknown
   * Unknown Unknown Unknown Unknown  Unknown
   *
   * p       NOT p
   * True    False
   * False   True
   * Unknown Unknown
   */
  // scalastyle:on

  test("3VL Not") {
    val notTrueTable =
      (true, false) ::
        (false, true) ::
        (null, null) :: Nil
    notTrueTable.foreach { case (v, answer) =>
      checkEvaluation(Not(Literal.create(v, BooleanType)), answer)
    }
    checkConsistencyBetweenInterpretedAndCodegen(Not, BooleanType)
  }

  test("AND, OR, EqualTo, EqualNullSafe consistency check") {
    checkConsistencyBetweenInterpretedAndCodegen(And, BooleanType, BooleanType)
    checkConsistencyBetweenInterpretedAndCodegen(Or, BooleanType, BooleanType)
    DataTypeTestUtils.propertyCheckSupported.foreach { dt =>
      checkConsistencyBetweenInterpretedAndCodegen(EqualTo, dt, dt)
      checkConsistencyBetweenInterpretedAndCodegen(EqualNullSafe, dt, dt)
    }
  }

  booleanLogicTest("AND", And,
    (true, true, true) ::
      (true, false, false) ::
      (true, null, null) ::
      (false, true, false) ::
      (false, false, false) ::
      (false, null, false) ::
      (null, true, null) ::
      (null, false, false) ::
      (null, null, null) :: Nil)

  booleanLogicTest("OR", Or,
    (true, true, true) ::
      (true, false, true) ::
      (true, null, true) ::
      (false, true, true) ::
      (false, false, false) ::
      (false, null, null) ::
      (null, true, true) ::
      (null, false, null) ::
      (null, null, null) :: Nil)

  booleanLogicTest("=", EqualTo,
    (true, true, true) ::
      (true, false, false) ::
      (true, null, null) ::
      (false, true, false) ::
      (false, false, true) ::
      (false, null, null) ::
      (null, true, null) ::
      (null, false, null) ::
      (null, null, null) :: Nil)

  test("IN") {
    checkEvaluation(In(Literal.create(null, IntegerType), Seq(Literal(1), Literal(2))), null)
    checkEvaluation(In(Literal.create(null, IntegerType), Seq(Literal.create(null, IntegerType))),
      null)
    checkEvaluation(In(Literal(1), Seq(Literal.create(null, IntegerType))), null)
    checkEvaluation(In(Literal(1), Seq(Literal(1), Literal.create(null, IntegerType))), true)
    checkEvaluation(In(Literal(2), Seq(Literal(1), Literal.create(null, IntegerType))), null)
    checkEvaluation(In(Literal(1), Seq(Literal(1), Literal(2))), true)
    checkEvaluation(In(Literal(2), Seq(Literal(1), Literal(2))), true)
    checkEvaluation(In(Literal(3), Seq(Literal(1), Literal(2))), false)
    checkEvaluation(
      And(In(Literal(1), Seq(Literal(1), Literal(2))), In(Literal(2), Seq(Literal(1), Literal(2)))),
      true)

    val ns = Literal.create(null, StringType)
    checkEvaluation(In(ns, Seq(Literal("1"), Literal("2"))), null)
    checkEvaluation(In(ns, Seq(ns)), null)
    checkEvaluation(In(Literal("a"), Seq(ns)), null)
    checkEvaluation(In(Literal("^Ba*n"), Seq(Literal("^Ba*n"), ns)), true)
    checkEvaluation(In(Literal("^Ba*n"), Seq(Literal("aa"), Literal("^Ba*n"))), true)
    checkEvaluation(In(Literal("^Ba*n"), Seq(Literal("aa"), Literal("^n"))), false)

    val primitiveTypes = Seq(IntegerType, FloatType, DoubleType, StringType, ByteType, ShortType,
      LongType, BinaryType, BooleanType, DecimalType.USER_DEFAULT, TimestampType)
    primitiveTypes.map { t =>
      val dataGen = RandomDataGenerator.forType(t, nullable = true).get
      val inputData = Seq.fill(10) {
        val value = dataGen.apply()
        value match {
          case d: Double if d.isNaN => 0.0d
          case f: Float if f.isNaN => 0.0f
          case _ => value
        }
      }
      val input = inputData.map(Literal.create(_, t))
      val expected = if (inputData(0) == null) {
        null
      } else if (inputData.slice(1, 10).contains(inputData(0))) {
        true
      } else if (inputData.slice(1, 10).contains(null)) {
        null
      } else {
        false
      }
      checkEvaluation(In(input(0), input.slice(1, 10)), expected)
    }
  }

  test("INSET") {
    val hS = HashSet[Any]() + 1 + 2
    val nS = HashSet[Any]() + 1 + 2 + null
    val one = Literal(1)
    val two = Literal(2)
    val three = Literal(3)
    val nl = Literal(null)
    checkEvaluation(InSet(one, hS), true)
    checkEvaluation(InSet(two, hS), true)
    checkEvaluation(InSet(two, nS), true)
    checkEvaluation(InSet(three, hS), false)
    checkEvaluation(InSet(three, nS), null)
    checkEvaluation(InSet(nl, hS), null)
    checkEvaluation(InSet(nl, nS), null)

    val primitiveTypes = Seq(IntegerType, FloatType, DoubleType, StringType, ByteType, ShortType,
      LongType, BinaryType, BooleanType, DecimalType.USER_DEFAULT, TimestampType)
    primitiveTypes.map { t =>
      val dataGen = RandomDataGenerator.forType(t, nullable = true).get
      val inputData = Seq.fill(10) {
        val value = dataGen.apply()
        value match {
          case d: Double if d.isNaN => 0.0d
          case f: Float if f.isNaN => 0.0f
          case _ => value
        }
      }
      val input = inputData.map(Literal(_))
      val expected = if (inputData(0) == null) {
        null
      } else if (inputData.slice(1, 10).contains(inputData(0))) {
        true
      } else if (inputData.slice(1, 10).contains(null)) {
        null
      } else {
        false
      }
      checkEvaluation(InSet(input(0), inputData.slice(1, 10).toSet), expected)
    }
  }

  private val smallValues = Seq(1, Decimal(1), Array(1.toByte), "a", 0f, 0d, false).map(Literal(_))
  private val largeValues =
    Seq(2, Decimal(2), Array(2.toByte), "b", Float.NaN, Double.NaN, true).map(Literal(_))

  private val equalValues1 =
    Seq(1, Decimal(1), Array(1.toByte), "a", Float.NaN, Double.NaN, true).map(Literal(_))
  private val equalValues2 =
    Seq(1, Decimal(1), Array(1.toByte), "a", Float.NaN, Double.NaN, true).map(Literal(_))

  test("BinaryComparison consistency check") {
    DataTypeTestUtils.ordered.foreach { dt =>
      checkConsistencyBetweenInterpretedAndCodegen(LessThan, dt, dt)
      checkConsistencyBetweenInterpretedAndCodegen(LessThanOrEqual, dt, dt)
      checkConsistencyBetweenInterpretedAndCodegen(GreaterThan, dt, dt)
      checkConsistencyBetweenInterpretedAndCodegen(GreaterThanOrEqual, dt, dt)
    }
  }

  test("BinaryComparison: lessThan") {
    for (i <- 0 until smallValues.length) {
      checkEvaluation(LessThan(smallValues(i), largeValues(i)), true)
      checkEvaluation(LessThan(equalValues1(i), equalValues2(i)), false)
      checkEvaluation(LessThan(largeValues(i), smallValues(i)), false)
    }
  }

  test("BinaryComparison: LessThanOrEqual") {
    for (i <- 0 until smallValues.length) {
      checkEvaluation(LessThanOrEqual(smallValues(i), largeValues(i)), true)
      checkEvaluation(LessThanOrEqual(equalValues1(i), equalValues2(i)), true)
      checkEvaluation(LessThanOrEqual(largeValues(i), smallValues(i)), false)
    }
  }

  test("BinaryComparison: GreaterThan") {
    for (i <- 0 until smallValues.length) {
      checkEvaluation(GreaterThan(smallValues(i), largeValues(i)), false)
      checkEvaluation(GreaterThan(equalValues1(i), equalValues2(i)), false)
      checkEvaluation(GreaterThan(largeValues(i), smallValues(i)), true)
    }
  }

  test("BinaryComparison: GreaterThanOrEqual") {
    for (i <- 0 until smallValues.length) {
      checkEvaluation(GreaterThanOrEqual(smallValues(i), largeValues(i)), false)
      checkEvaluation(GreaterThanOrEqual(equalValues1(i), equalValues2(i)), true)
      checkEvaluation(GreaterThanOrEqual(largeValues(i), smallValues(i)), true)
    }
  }

  test("BinaryComparison: EqualTo") {
    for (i <- 0 until smallValues.length) {
      checkEvaluation(EqualTo(smallValues(i), largeValues(i)), false)
      checkEvaluation(EqualTo(equalValues1(i), equalValues2(i)), true)
      checkEvaluation(EqualTo(largeValues(i), smallValues(i)), false)
    }
  }

  test("BinaryComparison: EqualNullSafe") {
    for (i <- 0 until smallValues.length) {
      checkEvaluation(EqualNullSafe(smallValues(i), largeValues(i)), false)
      checkEvaluation(EqualNullSafe(equalValues1(i), equalValues2(i)), true)
      checkEvaluation(EqualNullSafe(largeValues(i), smallValues(i)), false)
    }
  }

  test("BinaryComparison: null test") {
    val normalInt = Literal(1)
    val nullInt = Literal.create(null, IntegerType)

    def nullTest(op: (Expression, Expression) => Expression): Unit = {
      checkEvaluation(op(normalInt, nullInt), null)
      checkEvaluation(op(nullInt, normalInt), null)
      checkEvaluation(op(nullInt, nullInt), null)
    }

    nullTest(LessThan)
    nullTest(LessThanOrEqual)
    nullTest(GreaterThan)
    nullTest(GreaterThanOrEqual)
    nullTest(EqualTo)

    checkEvaluation(EqualNullSafe(normalInt, nullInt), false)
    checkEvaluation(EqualNullSafe(nullInt, normalInt), false)
    checkEvaluation(EqualNullSafe(nullInt, nullInt), true)
  }
}
