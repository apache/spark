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

import scala.collection.immutable.HashSet

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.{IntegerType, BooleanType}


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
  val notTrueTable =
    (true, false) ::
      (false, true) ::
      (null, null) :: Nil

  test("3VL Not") {
    notTrueTable.foreach { case (v, answer) =>
      checkEvaluation(Not(Literal.create(v, BooleanType)), answer)
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
    checkEvaluation(In(Literal(1), Seq(Literal(1), Literal(2))), true)
    checkEvaluation(In(Literal(2), Seq(Literal(1), Literal(2))), true)
    checkEvaluation(In(Literal(3), Seq(Literal(1), Literal(2))), false)
    checkEvaluation(
      And(In(Literal(1), Seq(Literal(1), Literal(2))), In(Literal(2), Seq(Literal(1), Literal(2)))),
      true)
  }

  test("INSET") {
    val hS = HashSet[Any]() + 1 + 2
    val nS = HashSet[Any]() + 1 + 2 + null
    val one = Literal(1)
    val two = Literal(2)
    val three = Literal(3)
    val nl = Literal(null)
    val s = Seq(one, two)
    val nullS = Seq(one, two, null)
    checkEvaluation(InSet(one, hS), true)
    checkEvaluation(InSet(two, hS), true)
    checkEvaluation(InSet(two, nS), true)
    checkEvaluation(InSet(nl, nS), true)
    checkEvaluation(InSet(three, hS), false)
    checkEvaluation(InSet(three, nS), false)
    checkEvaluation(And(InSet(one, hS), InSet(two, hS)), true)
  }


  test("BinaryComparison") {
    val row = create_row(1, 2, 3, null, 3, null)
    val c1 = 'a.int.at(0)
    val c2 = 'a.int.at(1)
    val c3 = 'a.int.at(2)
    val c4 = 'a.int.at(3)
    val c5 = 'a.int.at(4)
    val c6 = 'a.int.at(5)

    checkEvaluation(LessThan(c1, c4), null, row)
    checkEvaluation(LessThan(c1, c2), true, row)
    checkEvaluation(LessThan(c1, Literal.create(null, IntegerType)), null, row)
    checkEvaluation(LessThan(Literal.create(null, IntegerType), c2), null, row)
    checkEvaluation(
      LessThan(Literal.create(null, IntegerType), Literal.create(null, IntegerType)), null, row)

    checkEvaluation(c1 < c2, true, row)
    checkEvaluation(c1 <= c2, true, row)
    checkEvaluation(c1 > c2, false, row)
    checkEvaluation(c1 >= c2, false, row)
    checkEvaluation(c1 === c2, false, row)
    checkEvaluation(c1 !== c2, true, row)
    checkEvaluation(c4 <=> c1, false, row)
    checkEvaluation(c1 <=> c4, false, row)
    checkEvaluation(c4 <=> c6, true, row)
    checkEvaluation(c3 <=> c5, true, row)
    checkEvaluation(Literal(true) <=> Literal.create(null, BooleanType), false, row)
    checkEvaluation(Literal.create(null, BooleanType) <=> Literal(true), false, row)

    val d1 = DateTimeUtils.fromJavaDate(Date.valueOf("1970-01-01"))
    val d2 = DateTimeUtils.fromJavaDate(Date.valueOf("1970-01-02"))
    checkEvaluation(Literal(d1) < Literal(d2), true)

    val ts1 = new Timestamp(12)
    val ts2 = new Timestamp(123)
    checkEvaluation(Literal("ab") < Literal("abc"), true)
    checkEvaluation(Literal(ts1) < Literal(ts2), true)
  }
}
