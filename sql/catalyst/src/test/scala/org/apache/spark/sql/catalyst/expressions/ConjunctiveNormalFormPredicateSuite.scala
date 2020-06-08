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
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.BooleanType

class ConjunctiveNormalFormPredicateSuite extends SparkFunSuite with PredicateHelper with PlanTest {
  private val a = AttributeReference("A", BooleanType)(exprId = ExprId(1)).withQualifier(Seq("ta"))
  private val b = AttributeReference("B", BooleanType)(exprId = ExprId(2)).withQualifier(Seq("tb"))
  private val c = AttributeReference("C", BooleanType)(exprId = ExprId(3)).withQualifier(Seq("tc"))
  private val d = AttributeReference("D", BooleanType)(exprId = ExprId(4)).withQualifier(Seq("td"))
  private val e = AttributeReference("E", BooleanType)(exprId = ExprId(5)).withQualifier(Seq("te"))
  private val f = AttributeReference("F", BooleanType)(exprId = ExprId(6)).withQualifier(Seq("tf"))
  private val g = AttributeReference("C", BooleanType)(exprId = ExprId(7)).withQualifier(Seq("tg"))
  private val h = AttributeReference("D", BooleanType)(exprId = ExprId(8)).withQualifier(Seq("th"))
  private val i = AttributeReference("E", BooleanType)(exprId = ExprId(9)).withQualifier(Seq("ti"))
  private val j = AttributeReference("F", BooleanType)(exprId = ExprId(10)).withQualifier(Seq("tj"))
  private val a1 =
    AttributeReference("a1", BooleanType)(exprId = ExprId(11)).withQualifier(Seq("ta"))
  private val a2 =
    AttributeReference("a2", BooleanType)(exprId = ExprId(12)).withQualifier(Seq("ta"))
  private val b1 =
    AttributeReference("b1", BooleanType)(exprId = ExprId(12)).withQualifier(Seq("tb"))

  // Check CNF conversion with expected expression, assuming the input has non-empty result.
  private def checkCondition(input: Expression, expected: Expression): Unit = {
    val cnf = conjunctiveNormalForm(input)
    assert(cnf.nonEmpty)
    val result = cnf.reduceLeft(And)
    assert(result.semanticEquals(expected))
  }

  test("Keep non-predicated expressions") {
    checkCondition(a, a)
    checkCondition(Literal(1), Literal(1))
  }

  test("Conversion of Not") {
    checkCondition(!a, !a)
    checkCondition(!(!a), a)
    checkCondition(!(!(a && b)), a && b)
    checkCondition(!(!(a || b)), a || b)
    checkCondition(!(a || b), !a && !b)
    checkCondition(!(a && b), !a || !b)
  }

  test("Conversion of And") {
    checkCondition(a && b, a && b)
    checkCondition(a && b && c, a && b && c)
    checkCondition(a && (b || c), a && (b || c))
    checkCondition((a || b) && c, (a || b) && c)
    checkCondition(a && b && c && d, a && b && c && d)
  }

  test("Conversion of Or") {
    checkCondition(a || b, a || b)
    checkCondition(a || b || c, a || b || c)
    checkCondition(a || b || c || d, a || b || c || d)
    checkCondition((a && b) || c, (a || c) && (b || c))
    checkCondition((a && b) || (c && d), (a || c) && (a || d) && (b || c) && (b || d))
  }

  test("More complex cases") {
    checkCondition(a && !(b || c), a && !b && !c)
    checkCondition((a && b) || !(c && d), (a || !c || !d) && (b || !c || !d))
    checkCondition(a || b || c && d, (a || b || c) && (a || b || d))
    checkCondition(a || (b && c || d), (a || b || d) && (a || c || d))
    checkCondition(a && !(b && c || d && e), a && (!b || !c) && (!d || !e))
    checkCondition(((a && b) || c) || (d || e), (a || c || d || e) && (b || c || d || e))

    checkCondition(
      (a && b && c) || (d && e && f),
      (a || d) && (a || e) && (a || f) && (b || d) && (b || e) && (b || f) &&
      (c || d) && (c || e) && (c || f)
    )
  }

  test("Aggregate predicate of same qualifiers to avoid expanding") {
    checkCondition(((a && b && a1) || c), ((a && a1) || c) && (b ||c))
    checkCondition(((a && a1 && b) || c), ((a && a1) || c) && (b ||c))
    checkCondition(((b && d && a && a1) || c), ((a && a1) || c) && (b ||c) && (d || c))
    checkCondition(((b && a2 && d && a && a1) || c), ((a2 && a && a1) || c) && (b ||c) && (d || c))
    checkCondition(((b && d && a && a1 && b1) || c),
      ((a && a1) || c) && ((b && b1) ||c) && (d || c))
    checkCondition((a && a1) || (b && b1), (a && a1) || (b && b1))
    checkCondition((a && a1 && c) || (b && b1), ((a && a1) || (b && b1)) && (c || (b && b1)))
  }

  test("Return None when exceeding MAX_CNF_NODE_COUNT") {
    // The following expression contains 36 conjunctive sub-expressions in CNF
    val input = (a && b && c) || (d && e && f) || (g && h && i && j)
    // The following expression contains 9 conjunctive sub-expressions in CNF
    val input2 = (a && b && c) || (d && e && f)
    Seq(8, 9, 10, 35, 36, 37).foreach { maxCount =>
      withSQLConf(SQLConf.MAX_CNF_NODE_COUNT.key -> maxCount.toString) {
        if (maxCount < 36) {
          assert(conjunctiveNormalForm(input).isEmpty)
        } else {
          assert(conjunctiveNormalForm(input).nonEmpty)
        }
        if (maxCount < 9) {
          assert(conjunctiveNormalForm(input2).isEmpty)
        } else {
          assert(conjunctiveNormalForm(input2).nonEmpty)
        }
      }
    }
  }
}
