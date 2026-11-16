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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, CurrentRow, DenseRank, Literal, NthValue, NTile, PercentRank, Rand, Rank, RowFrame, RowNumber, SpecifiedWindowFrame, UnboundedFollowing, UnboundedPreceding}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Complete, Count}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation, LogicalPlan, Window, WindowGroupLimit, WindowHint}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.util.CollationFactory
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, MetadataBuilder, StringType}
import org.apache.spark.unsafe.types.UTF8String

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

  private object WithCollapseWindow extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Insert WindowGroupLimit with CollapseWindow", FixedPoint(10),
        CollapseWindow,
        CollapseProject,
        RemoveNoopOperators,
        PushDownPredicates,
        InferWindowGroupLimit,
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

  test("Eliminate the Window/Filter once WindowGroupLimit already truncates to one row") {
    for (cond <- Seq($"rn" === 1, $"rn" <= 1, $"rn" < 2,
        Literal(1) === $"rn", Literal(1) >= $"rn", Literal(2) > $"rn");
      order <- Seq(c.desc, c.asc)) {
      val originalQuery =
        testRelation
          .select(a, b, c,
            windowExpr(RowNumber(),
              windowSpec(a :: Nil, order :: Nil, windowFrame)).as("rn"))
          .where(cond)

      val correctAnswer =
        testRelation
          .windowGroupLimit(a :: Nil, order :: Nil, RowNumber(), 1)
          .select(a, b, c, Literal(1).as("rn"))

      comparePlans(
        Optimize.execute(originalQuery.analyze),
        WithoutOptimize.execute(correctAnswer.analyze))
    }
  }

  test("Eliminate the Window/Filter with a multi-column partition/order spec") {
    val originalQuery =
      testRelation
        .select(a, b, c,
          windowExpr(RowNumber(),
            windowSpec(Seq(a, b), Seq(c.desc, b.asc), windowFrame)).as("rn"))
        .where($"rn" === 1)

    val correctAnswer =
      testRelation
        .windowGroupLimit(Seq(a, b), Seq(c.desc, b.asc), RowNumber(), 1)
        .select(a, b, c, Literal(1).as("rn"))

    comparePlans(
      Optimize.execute(originalQuery.analyze),
      WithoutOptimize.execute(correctAnswer.analyze))
  }

  test("Eliminate the Window/Filter also fires with an empty partitionSpec when the " +
      "top-k sort fallback is disabled") {
    // With the default config, limit == 1 and an empty partitionSpec take the Limit+Sort
    // fallback path instead (see "Insert limit node for top-k computation" above) and never
    // reach this rule's WindowGroupLimit-based elimination. Disabling the fallback here forces
    // the WindowGroupLimit path so the elimination branch is exercised with no PARTITION BY.
    withSQLConf(SQLConf.TOP_K_SORT_FALLBACK_THRESHOLD.key -> "0") {
      val originalQuery =
        testRelation
          .select(a, b, c,
            windowExpr(RowNumber(),
              windowSpec(Nil, c.desc :: Nil, windowFrame)).as("rn"))
          .where($"rn" === 1)

      val correctAnswer =
        testRelation
          .windowGroupLimit(Nil, c.desc :: Nil, RowNumber(), 1)
          .select(a, b, c, Literal(1).as("rn"))

      comparePlans(
        Optimize.execute(originalQuery.analyze),
        WithoutOptimize.execute(correctAnswer.analyze))
    }
  }

  test("Eliminate the Window/Filter keeps rn's exprId for downstream references") {
    val originalQuery =
      testRelation
        .select(a, b, c,
          windowExpr(RowNumber(),
            windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"))
        .where($"rn" === 1)
        .select(a, b, c, $"rn")

    val result = Optimize.execute(originalQuery.analyze)
    assert(result.output.map(_.exprId) == originalQuery.analyze.output.map(_.exprId))
    assert(!result.exists(_.isInstanceOf[Window]) && !result.exists(_.isInstanceOf[Filter]))
  }

  test("Eliminate the Window/Filter preserves rn's qualifier and metadata") {
    val metadata = new MetadataBuilder().putString("tag", "pii").build()
    val rnAlias = Alias(
      windowExpr(RowNumber(), windowSpec(a :: Nil, c.desc :: Nil, windowFrame)), "rn")(
      qualifier = Seq("t"), explicitMetadata = Some(metadata))

    val originalQuery =
      testRelation
        .select(a, b, c, rnAlias)
        .where($"rn" === 1)
        .analyze

    val optimized = Optimize.execute(originalQuery)
    assert(!optimized.exists(_.isInstanceOf[Window]) && !optimized.exists(_.isInstanceOf[Filter]))
    val rnAttr = optimized.output.last
    assert(rnAttr.qualifier == Seq("t"))
    assert(rnAttr.metadata == metadata)
  }

  test("Not eliminated when the Window carries a hint that the Project rewrite can't preserve") {
    case object TestWindowHint extends WindowHint

    val originalQuery =
      testRelation
        .select(a, b, c,
          windowExpr(RowNumber(),
            windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"))
        .where($"rn" === 1)
        .analyze
        .transform { case w: Window => w.copy(hint = Some(TestWindowHint)) }

    val correctAnswer =
      testRelation
        .windowGroupLimit(a :: Nil, c.desc :: Nil, RowNumber(), 1)
        .select(a, b, c,
          windowExpr(RowNumber(),
            windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"))
        .where($"rn" === 1)
        .analyze
        .transform { case w: Window => w.copy(hint = Some(TestWindowHint)) }

    comparePlans(
      Optimize.execute(originalQuery),
      WithoutOptimize.execute(correctAnswer))
  }

  test("Not eliminated when the Window computes more than the top-1 RowNumber") {
    val originalQuery =
      testRelation
        .select(a, b, c,
          windowExpr(RowNumber(),
            windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"),
          windowExpr(Rank(c :: Nil),
            windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rank"))
        .where($"rn" === 1)

    val correctAnswer =
      testRelation
        .windowGroupLimit(a :: Nil, c.desc :: Nil, RowNumber(), 1)
        .select(a, b, c,
          windowExpr(RowNumber(),
            windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"),
          windowExpr(Rank(c :: Nil),
            windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rank"))
        .where($"rn" === 1)

    comparePlans(
      Optimize.execute(originalQuery.analyze),
      WithoutOptimize.execute(correctAnswer.analyze))
  }

  test("Not eliminated for Rank/DenseRank even when the limit is 1 (their limit iterators " +
      "can let a row past the limit through even without ties)") {
    for (function <- rankLikeFunctions.filterNot(_.isInstanceOf[RowNumber])) {
      val originalQuery =
        testRelation
          .select(a, b, c,
            windowExpr(function,
              windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"))
          .where($"rn" === 1)

      val correctAnswer =
        testRelation
          .windowGroupLimit(a :: Nil, c.desc :: Nil, function, 1)
          .select(a, b, c,
            windowExpr(function,
              windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"))
          .where($"rn" === 1)

      comparePlans(
        Optimize.execute(originalQuery.analyze),
        WithoutOptimize.execute(correctAnswer.analyze))
    }
  }

  test("Not eliminated when a partition key is not binary-stable (e.g. a collated STRING)") {
    val k = AttributeReference("k", StringType(CollationFactory.UTF8_LCASE_COLLATION_ID))()
    val v = AttributeReference("v", IntegerType)()
    val collatedRelation = LocalRelation(Seq(k, v), data = Seq(
      InternalRow(UTF8String.fromString("a"), 1),
      InternalRow(UTF8String.fromString("A"), 2)))

    val originalQuery =
      collatedRelation
        .select(k, v,
          windowExpr(RowNumber(),
            windowSpec(k :: Nil, v.desc :: Nil, windowFrame)).as("rn"))
        .where($"rn" === 1)

    val correctAnswer =
      collatedRelation
        .windowGroupLimit(k :: Nil, v.desc :: Nil, RowNumber(), 1)
        .select(k, v,
          windowExpr(RowNumber(),
            windowSpec(k :: Nil, v.desc :: Nil, windowFrame)).as("rn"))
        .where($"rn" === 1)

    comparePlans(
      Optimize.execute(originalQuery.analyze),
      WithoutOptimize.execute(correctAnswer.analyze))
  }

  test("Eliminate the Window/Filter and keep a deterministic extra conjunct as a Filter " +
      "over the new Project") {
    val originalQuery =
      testRelation
        .select(a, b, c,
          windowExpr(RowNumber(),
            windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"))
        .where($"rn" === 1 && b > 0)

    val correctAnswer =
      testRelation
        .windowGroupLimit(a :: Nil, c.desc :: Nil, RowNumber(), 1)
        .select(a, b, c, Literal(1).as("rn"))
        .where(b > 0)

    comparePlans(
      Optimize.execute(originalQuery.analyze),
      WithoutOptimize.execute(correctAnswer.analyze))
  }

  test("Eliminate the Window/Filter and correctly rebind a remaining conjunct that itself " +
      "references rn") {
    // "rn >= 1" isn't a recognized top-1 shape, so it stays as a remaining conjunct that must
    // still bind to rn's (reused) exprId after rn is rewritten to the literal 1.
    val originalQuery =
      testRelation
        .select(a, b, c,
          windowExpr(RowNumber(),
            windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"))
        .where($"rn" === 1 && $"rn" >= 1)

    val correctAnswer =
      testRelation
        .windowGroupLimit(a :: Nil, c.desc :: Nil, RowNumber(), 1)
        .select(a, b, c, Literal(1).as("rn"))
        .where($"rn" >= 1)

    comparePlans(
      Optimize.execute(originalQuery.analyze),
      WithoutOptimize.execute(correctAnswer.analyze))
  }

  test("Not eliminated when the filter has a nondeterministic extra conjunct") {
    val originalQuery =
      testRelation
        .select(a, b, c,
          windowExpr(RowNumber(),
            windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"))
        .where($"rn" === 1 && Rand(Literal(0L)) > 0.5)

    val correctAnswer =
      testRelation
        .windowGroupLimit(a :: Nil, c.desc :: Nil, RowNumber(), 1)
        .select(a, b, c,
          windowExpr(RowNumber(),
            windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"))
        .where($"rn" === 1 && Rand(Literal(0L)) > 0.5)

    comparePlans(
      Optimize.execute(originalQuery.analyze),
      WithoutOptimize.execute(correctAnswer.analyze))
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

  test("SPARK-58757: collapseWindowWithEmptyOrderSpecInChild keeps WindowGroupLimit by default") {
    val cnt = windowExpr(
      AggregateExpression(Count(c), Complete, isDistinct = false, None),
      windowSpec(a :: Nil, Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing))).as("cnt")
    val rn = windowExpr(
      RowNumber(),
      windowSpec(a :: Nil, c.desc :: Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))).as("rn")

    def analyzed: LogicalPlan = testRelation
      .window(Seq(cnt), a :: Nil, Nil)
      .window(Seq(rn), a :: Nil, c.desc :: Nil)
      .where($"rn" <= 2)
      .analyze

    // By default the config is off, so the empty-order child is not collapsed into the ordered
    // parent and the WindowGroupLimit is preserved.
    val defaultPlan = WithCollapseWindow.execute(analyzed)
    assert(defaultPlan.collect { case _: WindowGroupLimit => 1 }.size == 1)

    // With the config on, the empty-order child is collapsed, disabling the WindowGroupLimit.
    withSQLConf(SQLConf.COLLAPSE_WINDOW_WITH_EMPTY_ORDER_SPEC_IN_CHILD.key -> "true") {
      val enabledPlan = WithCollapseWindow.execute(analyzed)
      assert(enabledPlan.collect { case _: WindowGroupLimit => 1 }.isEmpty)
    }
  }
}
