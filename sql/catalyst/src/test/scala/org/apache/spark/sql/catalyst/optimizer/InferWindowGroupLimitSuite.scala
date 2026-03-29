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
import org.apache.spark.sql.catalyst.expressions.{CurrentRow, DenseRank, Literal, NthValue, NTile, PercentRank, Rank, RowFrame, RowNumber, SpecifiedWindowFrame, UnboundedPreceding}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf

class InferWindowGroupLimitSuite extends PlanTest {
  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Insert WindowGroupLimit", FixedPoint(10),
        CollapseProject,
        RemoveNoopOperators,
        PushDownPredicates,
        InferWindowGroupLimit,
        LimitPushDownThroughWindow) :: Nil
  }

  private object WithoutOptimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Insert WindowGroupLimit", FixedPoint(10),
        CollapseProject,
        RemoveNoopOperators,
        PushDownPredicates,
        LimitPushDownThroughWindow) :: Nil
  }

  private val testRelation = LocalRelation.fromExternalRows(
    Seq("a".attr.int, "b".attr.int, "c".attr.int),
    1.to(6).map(_ => Row(1, 2, 3)))
  private val a = testRelation.output(0)
  private val b = testRelation.output(1)
  private val c = testRelation.output(2)
  private val rankLikeFunctions = Seq(RowNumber(), Rank(c :: Nil), DenseRank(c :: Nil))
  private val unsupportedFunctions = Seq(new NthValue(c, Literal(1)), new NTile())
  private val windowFrame = SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow)
  private val supportedConditions = Seq($"rn" === 2, $"rn" < 3, $"rn" <= 2)
  private val unsupportedConditions = Seq($"rn" > 2, $"rn" === 1 || b < 2)

  test("window without filter") {
    for (function <- rankLikeFunctions) {
      val originalQuery =
        testRelation
          .select(a, b, c,
            windowExpr(function,
              windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"))

      comparePlans(
        Optimize.execute(originalQuery.analyze),
        WithoutOptimize.execute(originalQuery.analyze))
    }
  }

  test("spark.sql.window.group.limit.threshold = -1") {
    withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_THRESHOLD.key -> "-1") {
      for (condition <- supportedConditions; function <- rankLikeFunctions) {
        val originalQuery =
          testRelation
            .select(a, b, c,
              windowExpr(function,
                windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"))
            .where(condition)

        comparePlans(
          Optimize.execute(originalQuery.analyze),
          WithoutOptimize.execute(originalQuery.analyze))
      }
    }
  }

  test("Insert window group limit node for top-k computation") {
    for (condition <- supportedConditions; function <- rankLikeFunctions;
      moreCond <- Seq(true, false)) {
      val cond = if (moreCond) {
        condition && b > 0
      } else {
        condition
      }

      val originalQuery0 =
        testRelation
          .select(a, b, c,
            windowExpr(function,
              windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"))
          .where(cond)

      val correctAnswer0 =
        testRelation
          .windowGroupLimit(a :: Nil, c.desc :: Nil, function, 2)
          .select(a, b, c,
            windowExpr(function,
              windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"))
          .where(cond)

      comparePlans(
        Optimize.execute(originalQuery0.analyze),
        WithoutOptimize.execute(correctAnswer0.analyze))

      withSQLConf(SQLConf.TOP_K_SORT_FALLBACK_THRESHOLD.key -> "-1") {
        val originalQuery1 =
          testRelation
            .select(a, b, c,
              windowExpr(function,
                windowSpec(Nil, c.desc :: Nil, windowFrame)).as("rn"))
            .where(cond)

        val correctAnswer1 =
          testRelation
            .windowGroupLimit(Nil, c.desc :: Nil, function, 2)
            .select(a, b, c,
              windowExpr(function,
                windowSpec(Nil, c.desc :: Nil, windowFrame)).as("rn"))
            .where(cond)

        comparePlans(
          Optimize.execute(originalQuery1.analyze),
          WithoutOptimize.execute(correctAnswer1.analyze))
      }
    }
  }

  test("Unsupported conditions") {
    for (condition <- unsupportedConditions; function <- rankLikeFunctions) {
      val originalQuery =
        testRelation
          .select(a, b, c,
            windowExpr(function,
              windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"))
          .where(condition)

      comparePlans(
        Optimize.execute(originalQuery.analyze),
        WithoutOptimize.execute(originalQuery.analyze))
    }
  }

  test("Unsupported window functions") {
    for (condition <- supportedConditions; function <- unsupportedFunctions) {
      val originalQuery =
        testRelation
          .select(a, b, c,
            windowExpr(function,
              windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"))
          .where(condition)

      comparePlans(
        Optimize.execute(originalQuery.analyze),
        WithoutOptimize.execute(originalQuery.analyze))
    }
  }

  test("Insert window group limit node for top-k computation: multiple rank-like functions") {
    rankLikeFunctions.foreach { function =>
      val originalQuery =
        testRelation
          .select(a, b, c,
            windowExpr(function,
              windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"),
            windowExpr(function,
              windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn2"))
          .where(Symbol("rn") < 2 && Symbol("rn2") === 3)

      val correctAnswer =
        testRelation
          .windowGroupLimit(a :: Nil, c.desc :: Nil, function, 1)
          .select(a, b, c,
            windowExpr(function,
              windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"),
            windowExpr(function,
              windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn2"))
          .where(Symbol("rn") < 2 && Symbol("rn2") === 3)

      comparePlans(
        Optimize.execute(originalQuery.analyze),
        WithoutOptimize.execute(correctAnswer.analyze))
    }
  }

  test("multiple different rank-like window function and only one used in filter") {
    val originalQuery =
      testRelation
        .select(a, b, c,
          windowExpr(RowNumber(),
            windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"),
          windowExpr(Rank(c :: Nil),
            windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rank"))
        .where(Symbol("rn") < 2)

    val correctAnswer =
      testRelation
        .windowGroupLimit(a :: Nil, c.desc :: Nil, RowNumber(), 1)
        .select(a, b, c,
          windowExpr(RowNumber(),
            windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"),
          windowExpr(Rank(c :: Nil),
            windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rank"))
        .where(Symbol("rn") < 2)

    comparePlans(
      Optimize.execute(originalQuery.analyze),
      WithoutOptimize.execute(correctAnswer.analyze))
  }

  test("rank-like window function with unsupported window function") {
    val originalQuery =
      testRelation
        .select(a, b, c,
          windowExpr(RowNumber(),
            windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"),
          windowExpr(new NthValue(c, Literal(1)),
            windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rank"))
        .where(Symbol("rn") < 2)

    val correctAnswer =
      testRelation
        .windowGroupLimit(a :: Nil, c.desc :: Nil, RowNumber(), 1)
        .select(a, b, c,
          windowExpr(RowNumber(),
            windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"),
          windowExpr(new NthValue(c, Literal(1)),
            windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rank"))
        .where(Symbol("rn") < 2)

    comparePlans(
      Optimize.execute(originalQuery.analyze),
      WithoutOptimize.execute(correctAnswer.analyze))
  }

  test("multiple different rank-like window function with filter") {
    val originalQuery =
      testRelation
        .select(a, b, c,
          windowExpr(RowNumber(),
            windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"),
          windowExpr(Rank(c :: Nil),
            windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rank"))
        .where(Symbol("rn") < 2 && Symbol("rank") === 3)

    val correctAnswer =
      testRelation
        .windowGroupLimit(a :: Nil, c.desc :: Nil, RowNumber(), 1)
        .select(a, b, c,
          windowExpr(RowNumber(),
            windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"),
          windowExpr(Rank(c :: Nil),
            windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rank"))
        .where(Symbol("rn") < 2 && Symbol("rank") === 3)

    comparePlans(
      Optimize.execute(originalQuery.analyze),
      WithoutOptimize.execute(correctAnswer.analyze))
  }

  test("Insert window group limit node for top-k computation: empty relation") {
    Seq($"rn" === 0, $"rn" < 1, $"rn" <= 0).foreach { condition =>
      rankLikeFunctions.foreach { function =>
        val originalQuery =
          testRelation
            .select(a, b, c,
              windowExpr(function,
                windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"))
            .where(condition)

        val correctAnswer = LocalRelation(originalQuery.output)

        comparePlans(
          Optimize.execute(originalQuery.analyze),
          WithoutOptimize.execute(correctAnswer.analyze))
      }
    }
  }

  test("Insert limit node for top-k computation") {
    for (condition <- supportedConditions; moreCond <- Seq(true, false)) {
      val cond = if (moreCond) {
        condition && b > 0
      } else {
        condition
      }

      val originalQuery0 =
        testRelation
          .select(a, b, c,
            windowExpr(RowNumber(),
              windowSpec(Nil, c.desc :: Nil, windowFrame)).as("rn"))
          .where(cond)

      val correctAnswer0 =
        testRelation
          .select(a, b, c,
            windowExpr(RowNumber(),
              windowSpec(Nil, c.desc :: Nil, windowFrame)).as("rn"))
          .limit(2)
          .where(cond)

      comparePlans(
        Optimize.execute(originalQuery0.analyze),
        WithoutOptimize.execute(correctAnswer0.analyze))

      val originalQuery1 =
        testRelation
          .select(a, b, c,
            windowExpr(Rank(c :: Nil),
              windowSpec(Nil, c.desc :: Nil, windowFrame)).as("rn"))
          .where(cond)

      val correctAnswer1 =
        testRelation
          .windowGroupLimit(Nil, c.desc :: Nil, Rank(c :: Nil), 2)
          .select(a, b, c,
            windowExpr(Rank(c :: Nil),
              windowSpec(Nil, c.desc :: Nil, windowFrame)).as("rn"))
          .where(cond)

      comparePlans(
        Optimize.execute(originalQuery1.analyze),
        WithoutOptimize.execute(correctAnswer1.analyze))
    }
  }

  test("SPARK-46941: Can't Insert window group limit node for top-k computation if contains " +
    "SizeBasedWindowFunction") {
    val originalQuery =
      testRelation
        .select(a, b, c,
          windowExpr(Rank(c :: Nil),
            windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rank"),
          windowExpr(PercentRank(c :: Nil),
            windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("percent_rank"))
        .where(Symbol("rank") < 2)

    comparePlans(
      Optimize.execute(originalQuery.analyze),
      WithoutOptimize.execute(originalQuery.analyze))
  }
}
