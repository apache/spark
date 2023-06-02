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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{AttributeSet, InSubquery, ListQuery, ScalarSubquery}
import org.apache.spark.sql.catalyst.plans.{LeftSemi, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class AssignNewExprIdsSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("Assign Expr Ids", Once,
      AssignNewExprIds) :: Nil
  }

  val testRelation1 = LocalRelation($"a".int, $"b".int)
  val testRelation2 = LocalRelation($"c".int, $"d".int)

  private def collectAllReferences(plan: LogicalPlan): AttributeSet =
    AttributeSet(plan.collectWithSubqueries({
      case e: LogicalPlan => e.references ++ e.producedAttributes
    }).flatten)

  private def check(input: LogicalPlan): Unit = {
    val output = AssignNewExprIds(input)
    comparePlans(input, output)
    val oldRefs = collectAllReferences(input)
    val newRefs = collectAllReferences(output)
    assert(oldRefs.size == newRefs.size)
    assert(oldRefs.intersect(newRefs).isEmpty)
  }

  test("Reassign IDs in a simple plan") {
    val plan =
      testRelation1
        .sortBy($"a".asc, $"b".asc)
        .distribute($"a")(2)
        .where($"a" === 10)
        .groupBy($"a")(sum($"b")).analyze
    check(plan)
  }

  test("Plan with scalar subquery") {
    val testRelation3 = LocalRelation($"e".int, $"f".int)
    val subqPlan =
      testRelation3
        .groupBy($"e")(sum($"f").as("sum"))
        .where($"e" === $"a")
    val subqExpr = ScalarSubquery(subqPlan)
    val originalQuery =
      testRelation1
        .select(subqExpr.as("sum"))
        .join(testRelation2, joinType = LeftSemi, condition = Some($"sum" === $"d")).analyze
    check(originalQuery)
  }

  test("Plan with IN subquery") {
    val subPlan =
      testRelation2
        .where($"b" < $"d")
        .select($"c")
    val query =
      testRelation1
        .where(InSubquery(Seq($"a"), ListQuery(subPlan)))
        .select($"a").analyze
    check(query)
  }
}
