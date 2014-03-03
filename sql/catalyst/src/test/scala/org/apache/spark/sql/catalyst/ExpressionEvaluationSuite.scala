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

package org.apache.spark.sql
package catalyst
package expressions

import org.scalatest.FunSuite

import types._
import expressions._

import dsl._

class ExpressionEvaluationSuite extends FunSuite {

  test("literals") {
    assert((Literal(1) + Literal(1)).apply(null) === 2)
  }

  /**
   * Checks for three-valued-logic.  Based on:
   * http://en.wikipedia.org/wiki/Null_(SQL)#Comparisons_with_NULL_and_the_three-valued_logic_.283VL.29
   *
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

  val notTrueTable =
    (true, false) ::
    (false, true) ::
    (null, null) :: Nil

  test("3VL Not") {
    notTrueTable.foreach {
      case (v, answer) =>
        val expr = Not(Literal(v, BooleanType))
        val result = expr.apply(null)
        if (result != answer)
          fail(s"$expr should not evaluate to $result, expected: $answer")    }
  }

  booleanLogicTest("AND", _ && _,
    (true,  true,  true) ::
    (true,  false, false) ::
    (true,  null,  null) ::
    (false, true,  false) ::
    (false, false, false) ::
    (false, null,  false) ::
    (null,  true,  null) ::
    (null,  false, false) ::
    (null,  null,  null) :: Nil)

  booleanLogicTest("OR", _ || _,
    (true,  true,  true) ::
    (true,  false, true) ::
    (true,  null,  true) ::
    (false, true,  true) ::
    (false, false, false) ::
    (false, null,  null) ::
    (null,  true,  true) ::
    (null,  false, null) ::
    (null,  null,  null) :: Nil)

  booleanLogicTest("=", _ === _,
    (true,  true,  true) ::
    (true,  false, false) ::
    (true,  null,  null) ::
    (false, true,  false) ::
    (false, false, true) ::
    (false, null,  null) ::
    (null,  true,  null) ::
    (null,  false, null) ::
    (null,  null,  null) :: Nil)

  def booleanLogicTest(name: String, op: (Expression, Expression) => Expression,  truthTable: Seq[(Any, Any, Any)]) {
    test(s"3VL $name") {
      truthTable.foreach {
        case (l,r,answer) =>
          val expr = op(Literal(l, BooleanType), Literal(r, BooleanType))
          val result = expr.apply(null)
          if (result != answer)
            fail(s"$expr should not evaluate to $result, expected: $answer")
      }
    }
  }
}