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
import org.apache.spark.sql.catalyst.expressions.{CurrentRow, Rank, RowFrame, RowNumber, SpecifiedWindowFrame, UnboundedPreceding}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.internal.SQLConf

class LimitPushdownThroughWindowSuite extends PlanTest {
  // CollapseProject and RemoveNoopOperators is needed because we need it to collapse project.
  private val limitPushdownRules = Seq(
    CollapseProject,
    RemoveNoopOperators,
    LimitPushDown,
    EliminateLimits,
    ConstantFolding,
    BooleanSimplification)

  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Limit pushdown", FixedPoint(100),
        limitPushdownRules: _*) :: Nil
  }

  private object WithoutOptimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Without Limit pushdown", FixedPoint(100),
        limitPushdownRules.filterNot(_.ruleName.equals(LimitPushDown.ruleName)): _*) :: Nil
  }

  private val testRelation = LocalRelation.fromExternalRows(
    Seq("a".attr.int, "b".attr.int, "c".attr.int),
    1.to(6).map(_ => Row(1, 2, 3)))

  private val a = testRelation.output(0)
  private val b = testRelation.output(1)
  private val c = testRelation.output(2)
  private val windowFrame = SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow)

  test("Push down limit through window when partitionSpec is empty") {
    val originalQuery = testRelation
      .select(a, b, c,
        windowExpr(RowNumber(), windowSpec(Nil, c.desc :: Nil, windowFrame)).as("rn"))
      .limit(2)
    val correctAnswer = testRelation
      .select(a, b, c)
      .orderBy(c.desc)
      .limit(2)
      .select(a, b, c,
        windowExpr(RowNumber(), windowSpec(Nil, c.desc :: Nil, windowFrame)).as("rn"))

    comparePlans(
      Optimize.execute(originalQuery.analyze),
      WithoutOptimize.execute(correctAnswer.analyze))
  }

  test("Should not push down if partitionSpec is not empty") {
    val originalQuery = testRelation
      .select(a, b, c,
        windowExpr(RowNumber(), windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"))
      .limit(20)

    comparePlans(
      Optimize.execute(originalQuery.analyze),
      WithoutOptimize.execute(originalQuery.analyze))
  }

  test("Should not push down when child's maxRows smaller than limit value") {
    val originalQuery = testRelation
      .select(a, b, c,
        windowExpr(RowNumber(), windowSpec(Nil, c.desc :: Nil, windowFrame)).as("rn"))
      .limit(20)

    comparePlans(
      Optimize.execute(originalQuery.analyze),
      WithoutOptimize.execute(originalQuery.analyze))
  }

  test("Should not push down if it is not RankLike/RowNumber window function") {
    val originalQuery = testRelation
      .select(a, b, c,
        windowExpr(count(b), windowSpec(Nil, c.desc :: Nil, windowFrame)).as("rn"))
      .limit(20)

    comparePlans(
      Optimize.execute(originalQuery.analyze),
      WithoutOptimize.execute(originalQuery.analyze))
  }

  test("Should not push down if more than one window function") {
    val originalQuery = testRelation
      .select(a, b, c,
        windowExpr(RowNumber(), windowSpec(Nil, c.desc :: Nil, windowFrame)).as("rn"),
        windowExpr(new Rank(), windowSpec(Nil, c.desc :: Nil, windowFrame)).as("rk"))
      .limit(20)

    comparePlans(
      Optimize.execute(originalQuery.analyze),
      WithoutOptimize.execute(originalQuery.analyze))
  }

  test("Push down limit through window respect spark.sql.execution.topKSortFallbackThreshold") {
    Seq(1, 100).foreach { threshold =>
      withSQLConf(SQLConf.TOP_K_SORT_FALLBACK_THRESHOLD.key -> threshold.toString) {
        val originalQuery = testRelation
          .select(a, b, c,
            windowExpr(RowNumber(), windowSpec(Nil, c.desc :: Nil, windowFrame)).as("rn"))
          .limit(2)
        val correctAnswer = if (threshold == 1) {
          originalQuery
        } else {
          testRelation
            .select(a, b, c)
            .orderBy(c.desc)
            .limit(2)
            .select(a, b, c,
              windowExpr(RowNumber(), windowSpec(Nil, c.desc :: Nil, windowFrame)).as("rn"))
        }

        comparePlans(
          Optimize.execute(originalQuery.analyze),
          WithoutOptimize.execute(correctAnswer.analyze))
      }
    }
  }
}
