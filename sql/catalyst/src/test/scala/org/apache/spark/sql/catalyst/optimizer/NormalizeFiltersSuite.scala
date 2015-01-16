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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.analysis.EliminateAnalysisOperators
import org.apache.spark.sql.catalyst.expressions.{And, Expression, Or}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

// For implicit conversions
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._

class NormalizeFiltersSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Seq(
      Batch("AnalysisNodes", Once,
        EliminateAnalysisOperators),
      Batch("NormalizeFilters", FixedPoint(100),
        BooleanSimplification,
        SimplifyFilters))
  }

  val relation = LocalRelation('a.int, 'b.int, 'c.string)

  def checkExpression(original: Expression, expected: Expression): Unit = {
    val actual = Optimize(relation.where(original)).collect { case f: Filter => f.condition }.head
    val result = (actual, expected) match {
      case (And(l1, r1), And(l2, r2)) => (l1 == l2 && r1 == r2) || (l1 == r2 && l2 == r1)
      case (Or (l1, r1), Or (l2, r2)) => (l1 == l2 && r1 == r2) || (l1 == r2 && l2 == r1)
      case (lhs, rhs) => lhs fastEquals rhs
    }

    assert(result, s"$actual isn't equivalent to $expected")
  }

  test("a && a => a") {
    checkExpression('a === 1 && 'a === 1, 'a === 1)
    checkExpression('a === 1 && 'a === 1 && 'a === 1, 'a === 1)
  }

  test("a || a => a") {
    checkExpression('a === 1 || 'a === 1, 'a === 1)
    checkExpression('a === 1 || 'a === 1 || 'a === 1, 'a === 1)
  }

  test("(a && b) || (a && c) => a && (b || c)") {
    checkExpression(
      ('a === 1 && 'a < 10) || ('a > 2 && 'a === 1),
      ('a === 1) && ('a < 10 || 'a > 2))

    checkExpression(
      ('a < 1 && 'b > 2 && 'c.isNull) || ('a < 1 && 'c === "hello" && 'b > 2),
      ('c.isNull || 'c === "hello") && 'a < 1 && 'b > 2)
  }
}
