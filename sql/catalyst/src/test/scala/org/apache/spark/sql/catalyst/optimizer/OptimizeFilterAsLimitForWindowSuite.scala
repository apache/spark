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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{CurrentRow, DenseRank, Rank, RowFrame, RowNumber, SpecifiedWindowFrame, UnboundedPreceding}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation, LogicalPlan, Window}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class OptimizeFilterAsLimitForWindowSuite extends PlanTest {

  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Optimize filter as group limit for window", FixedPoint(100),
        CollapseProject,
        RemoveNoopOperators,
        PushDownPredicates,
        OptimizeFilterAsLimitForWindow) :: Nil
  }

  private object WithoutOptimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Optimize filter as group limit for window", FixedPoint(100),
        CollapseProject,
        RemoveNoopOperators,
        PushDownPredicates) :: Nil
  }

  private val testRelation = LocalRelation.fromExternalRows(
    Seq("a".attr.int, "b".attr.int, "c".attr.int),
    1.to(10).map(i => Row(i % 3, 2, i)))
  private val a = testRelation.output(0)
  private val b = testRelation.output(1)
  private val c = testRelation.output(2)
  private val rankFunctions = Seq(RowNumber(), Rank(c :: Nil), DenseRank(c :: Nil))
  private val conditions1 = Seq(
    $"rn" === 2,
    $"rn" < 3,
    $"rn" <= 2
  )
  private val conditions2 = Seq(
    $"rn" === 0,
    $"rn" < 1,
    $"rn" <= 0
  )
  private val windowFrame = SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow)

  test("window without filter") {
    for (rankFunction <- rankFunctions) {
      val originalQuery =
        testRelation
          .select(a, b, c,
            windowExpr(rankFunction,
              windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"))

      comparePlans(
        Optimize.execute(originalQuery.analyze),
        WithoutOptimize.execute(originalQuery.analyze))
    }
  }

  test("optimize filter as group limit for window filter: supported filter") {
    for (condition <- conditions1; rankFunction <- rankFunctions) {
      val originalQuery =
        testRelation
          .select(a, b, c,
            windowExpr(rankFunction,
              windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"))
          .where(condition)

      val optimizedQuery = Optimize.execute(originalQuery.analyze)
      assert(optimizedQuery.isInstanceOf[Filter])
      val filter = optimizedQuery.asInstanceOf[Filter]
      assert(filter.child.isInstanceOf[Window])
      val window = filter.child.asInstanceOf[Window]
      assert(window.groupLimitInfo == Some(2, rankFunction))
    }
  }

  test("optimize filter as group limit for window filter: unsupported filter") {
    rankFunctions.foreach { rankFunction =>
      val originalQuery =
        testRelation
          .select(a, b, c,
            windowExpr(rankFunction,
              windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"))
          .where('rn > 2)

      comparePlans(
        Optimize.execute(originalQuery.analyze),
        WithoutOptimize.execute(originalQuery.analyze))
    }
  }

  test("optimize filter as group limit for window filter: empty relation") {
    for (condition <- conditions2; rankFunction <- rankFunctions) {
      val originalQuery =
        testRelation
          .select(a, b, c,
            windowExpr(rankFunction,
              windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"))
          .where(condition)

      val correctAnswer = LocalRelation(originalQuery.output)

      comparePlans(
        Optimize.execute(originalQuery.analyze),
        correctAnswer)
    }
  }
}
