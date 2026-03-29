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
import org.apache.spark.sql.types.BooleanType

class ExtractPredicatesWithinOutputSetSuite extends SparkFunSuite with PlanTest {
  private val a = AttributeReference("A", BooleanType)(exprId = ExprId(1))
  private val b = AttributeReference("B", BooleanType)(exprId = ExprId(2))
  private val c = AttributeReference("C", BooleanType)(exprId = ExprId(3))
  private val d = AttributeReference("D", BooleanType)(exprId = ExprId(4))
  private val e = AttributeReference("E", BooleanType)(exprId = ExprId(5))
  private val f = AttributeReference("F", BooleanType)(exprId = ExprId(6))
  private val g = AttributeReference("G", BooleanType)(exprId = ExprId(7))
  private val h = AttributeReference("H", BooleanType)(exprId = ExprId(8))
  private val i = AttributeReference("I", BooleanType)(exprId = ExprId(9))

  private def checkCondition(
      input: Expression,
      convertibleAttributes: Seq[Attribute],
      expected: Option[Expression]): Unit = {
    val result = extractPredicatesWithinOutputSet(input, AttributeSet(convertibleAttributes))
    if (expected.isEmpty) {
      assert(result.isEmpty)
    } else {
      assert(result.isDefined && result.get.semanticEquals(expected.get))
    }
  }

  test("Convertible conjunctive predicates") {
    checkCondition(a && b, Seq(a, b), Some(a && b))
    checkCondition(a && b, Seq(a), Some(a))
    checkCondition(a && b, Seq(b), Some(b))
    checkCondition(a && b && c, Seq(a, c), Some(a && c))
    checkCondition(a && b && c && d, Seq(b, c), Some(b && c))
  }

  test("Convertible disjunctive predicates") {
    checkCondition(a || b, Seq(a, b), Some(a || b))
    checkCondition(a || b, Seq(a), None)
    checkCondition(a ||  b, Seq(b), None)
    checkCondition(a || b || c, Seq(a, c), None)
    checkCondition(a || b || c || d, Seq(a, b, d), None)
    checkCondition(a || b || c || d, Seq(d, c, b, a), Some(a || b || c || d))
  }

  test("Convertible complex predicates") {
    checkCondition((a && b) || (c && d), Seq(a, c), Some(a || c))
    checkCondition((a && b) || (c && d), Seq(a, b), None)
    checkCondition((a && b) || (c && d), Seq(a, c, d), Some(a || (c && d)))
    checkCondition((a && b && c) || (d && e && f), Seq(a, c, d, f), Some((a && c) || (d && f)))
    checkCondition((a && b) || (c && d) || (e && f) || (g && h), Seq(a, c, e, g),
      Some(a || c || e || g))
    checkCondition((a && b) || (c && d) || (e && f) || (g && h), Seq(a, e, g), None)
    checkCondition((a || b) || (c && d) || (e && f) || (g && h), Seq(a, c, e, g), None)
    checkCondition((a || b) || (c && d) || (e && f) || (g && h), Seq(a, b, c, e, g),
      Some(a || b || c || e || g))
    checkCondition((a && b && c) || (d && e && f) || (g && h && i), Seq(b, e, h), Some(b || e || h))
    checkCondition((a && b && c) || (d && e && f) || (g && h && i), Seq(b, e, d), None)
  }
}
