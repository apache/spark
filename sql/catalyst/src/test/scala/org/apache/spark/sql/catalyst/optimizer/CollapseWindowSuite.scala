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
import org.apache.spark.sql.catalyst.expressions.{
  CurrentRow, RangeFrame, RowFrame, RowNumber, SpecifiedWindowFrame,
  UnboundedFollowing, UnboundedPreceding}
import org.apache.spark.sql.catalyst.expressions.aggregate.{
  AggregateExpression, Complete, Count, First, Sum}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf

class CollapseWindowSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("CollapseWindow", FixedPoint(10),
        CollapseWindow,
        CollapseProject) :: Nil
  }

  val testRelation = LocalRelation($"a".double, $"b".double, $"c".string)
  val a = testRelation.output(0)
  val b = testRelation.output(1)
  val c = testRelation.output(2)
  val partitionSpec1 = Seq(c)
  val partitionSpec2 = Seq(c + 1)
  val orderSpec1 = Seq(c.asc)
  val orderSpec2 = Seq(c.desc)

  test("collapse two adjacent windows with the same partition/order") {
    val query = testRelation
      .window(Seq(min(a).as("min_a")), partitionSpec1, orderSpec1)
      .window(Seq(max(a).as("max_a")), partitionSpec1, orderSpec1)
      .window(Seq(sum(b).as("sum_b")), partitionSpec1, orderSpec1)
      .window(Seq(avg(b).as("avg_b")), partitionSpec1, orderSpec1)

    val analyzed = query.analyze
    val optimized = Optimize.execute(analyzed)
    assert(analyzed.output === optimized.output)

    val correctAnswer = testRelation.window(Seq(
      min(a).as("min_a"),
      max(a).as("max_a"),
      sum(b).as("sum_b"),
      avg(b).as("avg_b")), partitionSpec1, orderSpec1)

    comparePlans(optimized, correctAnswer)
  }

  test("Don't collapse adjacent windows with different partitions or orders") {
    val query1 = testRelation
      .window(Seq(min(a).as("min_a")), partitionSpec1, orderSpec1)
      .window(Seq(max(a).as("max_a")), partitionSpec1, orderSpec2)

    val optimized1 = Optimize.execute(query1.analyze)
    val correctAnswer1 = query1.analyze

    comparePlans(optimized1, correctAnswer1)

    val query2 = testRelation
      .window(Seq(min(a).as("min_a")), partitionSpec1, orderSpec1)
      .window(Seq(max(a).as("max_a")), partitionSpec2, orderSpec1)

    val optimized2 = Optimize.execute(query2.analyze)
    val correctAnswer2 = query2.analyze

    comparePlans(optimized2, correctAnswer2)
  }

  test("Don't collapse adjacent windows with dependent columns") {
    val query = testRelation
      .window(Seq(sum(a).as("sum_a")), partitionSpec1, orderSpec1)
      .window(Seq(max($"sum_a").as("max_sum_a")), partitionSpec1, orderSpec1)
      .analyze

    val expected = query.analyze
    val optimized = Optimize.execute(query.analyze)
    comparePlans(optimized, expected)
  }

  test("Skip windows with empty window expressions") {
    val query = testRelation
      .window(Seq(), partitionSpec1, orderSpec1)
      .window(Seq(sum(a).as("sum_a")), partitionSpec1, orderSpec1)

    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = query.analyze

    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-34565: collapse two windows with the same partition/order " +
    "and a Project between them") {

    val query = testRelation
      .window(Seq(min(a).as("_we0")), partitionSpec1, orderSpec1)
      .select($"a", $"b", $"c", $"_we0" as "min_a")
      .window(Seq(max(a).as("_we1")), partitionSpec1, orderSpec1)
      .select($"a", $"b", $"c", $"min_a", $"_we1" as "max_a")
      .window(Seq(sum(b).as("_we2")), partitionSpec1, orderSpec1)
      .select($"a", $"b", $"c", $"min_a", $"max_a", $"_we2" as "sum_b")
      .window(Seq(avg(b).as("_we3")), partitionSpec1, orderSpec1)
      .select($"a", $"b", $"c", $"min_a", $"max_a", $"sum_b", $"_we3" as "avg_b")
      .analyze

    val optimized = Optimize.execute(query)
    assert(query.output === optimized.output)

    val correctAnswer = testRelation
      .window(Seq(
        min(a).as("_we0"),
        max(a).as("_we1"),
        sum(b).as("_we2"),
        avg(b).as("_we3")
      ), partitionSpec1, orderSpec1)
      .select(
        a, b, c,
        $"_we0" as "min_a", $"_we1" as "max_a", $"_we2" as "sum_b", $"_we3" as "avg_b")
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-34565: do not collapse two windows if project between them " +
    "generates an input column") {

    val query = testRelation
      .window(Seq(min(a).as("min_a")), partitionSpec1, orderSpec1)
      .select($"a", $"b", $"c", $"min_a", ($"a" + $"b").as("d"))
      .window(Seq(max($"d").as("max_d")), partitionSpec1, orderSpec1)
      .analyze

    val optimized = Optimize.execute(query)
    assert(query.output === optimized.output)

    comparePlans(optimized, query)
  }

  test("SPARK-42525: collapse two adjacent windows with the same partition/order " +
    "but qualifiers are different ") {

    val query = testRelation
      .window(Seq(min(a).as("_we0")), Seq(c.withQualifier(Seq("0"))), Seq(c.asc))
      .select($"a", $"b", $"c", $"_we0" as "min_a")
      .window(Seq(max(a).as("_we1")), Seq(c.withQualifier(Seq("1"))), Seq(c.asc))
      .select($"a", $"b", $"c", $"min_a", $"_we1" as "max_a")
      .analyze

    val optimized = Optimize.execute(query)

    val correctAnswer = testRelation
      .window(Seq(min(a).as("_we0"), max(a).as("_we1")), Seq(c), Seq(c.asc))
      .select(a, b, c, $"_we0" as "min_a", $"_we1" as "max_a")
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("collapse windows when one has an empty order spec " +
    "(row_number + count over the whole partition)") {
    val rk = windowExpr(
      RowNumber(),
      windowSpec(partitionSpec1, orderSpec1,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))).as("rk")
    val cnt = windowExpr(
      AggregateExpression(Count(c), Complete, isDistinct = false, None),
      windowSpec(partitionSpec1, Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing))).as("cnt")

    val query = testRelation
      .window(Seq(rk), partitionSpec1, orderSpec1)
      .window(Seq(cnt), partitionSpec1, Nil)

    val analyzed = query.analyze
    val optimized = Optimize.execute(analyzed)
    assert(analyzed.output === optimized.output)

    val correctAnswer = testRelation
      .window(Seq(rk, cnt), partitionSpec1, orderSpec1)

    comparePlans(optimized, correctAnswer)
  }

  test("collapse windows when the empty-order window has multiple window expressions") {
    // Every window expression of the empty-order window must be order-insensitive for the merge.
    val rk = windowExpr(
      RowNumber(),
      windowSpec(partitionSpec1, orderSpec1,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))).as("rk")
    val cnt = windowExpr(
      AggregateExpression(Count(c), Complete, isDistinct = false, None),
      windowSpec(partitionSpec1, Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing))).as("cnt")
    val sm = windowExpr(
      AggregateExpression(Sum(b), Complete, isDistinct = false, None),
      windowSpec(partitionSpec1, Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing))).as("sm")

    val query = testRelation
      .window(Seq(rk), partitionSpec1, orderSpec1)
      .window(Seq(cnt, sm), partitionSpec1, Nil)

    val analyzed = query.analyze
    val optimized = Optimize.execute(analyzed)
    assert(analyzed.output === optimized.output)

    val correctAnswer = testRelation
      .window(Seq(rk, cnt, sm), partitionSpec1, orderSpec1)

    comparePlans(optimized, correctAnswer)
  }

  test("collapse windows when the empty-order window has first() over the whole partition") {
    // `first` is non-deterministic when the order is not determined by the query, so evaluating it
    // under the other window's order spec yields a valid result.
    val rk = windowExpr(
      RowNumber(),
      windowSpec(partitionSpec1, orderSpec1,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))).as("rk")
    val fr = windowExpr(
      First(a, ignoreNulls = true).toAggregateExpression(),
      windowSpec(partitionSpec1, Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing))).as("fr")

    val query = testRelation
      .window(Seq(rk), partitionSpec1, orderSpec1)
      .window(Seq(fr), partitionSpec1, Nil)

    val analyzed = query.analyze
    val optimized = Optimize.execute(analyzed)
    assert(analyzed.output === optimized.output)

    val correctAnswer = testRelation
      .window(Seq(rk, fr), partitionSpec1, orderSpec1)

    comparePlans(optimized, correctAnswer)
  }

  test("don't collapse windows when the empty-order window has a bounded frame") {
    // The frame `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` is order-sensitive: which rows
    // fall in the frame depends on the ordering, so the window cannot be evaluated under the other
    // window's order spec.
    val rk = windowExpr(
      RowNumber(),
      windowSpec(partitionSpec1, orderSpec1,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))).as("rk")
    val cnt = windowExpr(
      AggregateExpression(Count(c), Complete, isDistinct = false, None),
      windowSpec(partitionSpec1, Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))).as("cnt")

    val query = testRelation
      .window(Seq(rk), partitionSpec1, orderSpec1)
      .window(Seq(cnt), partitionSpec1, Nil)

    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = query.analyze

    comparePlans(optimized, correctAnswer)
  }

  test("collapse windows when the empty-order window has a RANGE whole-partition frame") {
    // `RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` covers the whole partition just
    // like `ROWS`, so it also collapses.
    val rk = windowExpr(
      RowNumber(),
      windowSpec(partitionSpec1, orderSpec1,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))).as("rk")
    val cnt = windowExpr(
      AggregateExpression(Count(c), Complete, isDistinct = false, None),
      windowSpec(partitionSpec1, Nil,
        SpecifiedWindowFrame(RangeFrame, UnboundedPreceding, UnboundedFollowing))).as("cnt")

    val query = testRelation
      .window(Seq(rk), partitionSpec1, orderSpec1)
      .window(Seq(cnt), partitionSpec1, Nil)

    val analyzed = query.analyze
    val optimized = Optimize.execute(analyzed)
    assert(analyzed.output === optimized.output)

    val correctAnswer = testRelation
      .window(Seq(rk, cnt), partitionSpec1, orderSpec1)

    comparePlans(optimized, correctAnswer)
  }

  test("collapse windows when the empty-order window is the inner window") {
    // The empty-order window can also be the child of the ordered window. In that case its
    // expressions are evaluated under the ordered window's order spec, which is valid because all
    // of them are order-insensitive. This direction can disable InferWindowGroupLimit, so it is
    // gated by `spark.sql.optimizer.collapseWindowWithEmptyOrderSpecInChild`.
    val rk = windowExpr(
      RowNumber(),
      windowSpec(partitionSpec1, orderSpec1,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))).as("rk")
    val cnt = windowExpr(
      AggregateExpression(Count(c), Complete, isDistinct = false, None),
      windowSpec(partitionSpec1, Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing))).as("cnt")

    val query = testRelation
      .window(Seq(cnt), partitionSpec1, Nil)
      .window(Seq(rk), partitionSpec1, orderSpec1)

    val analyzed = query.analyze
    val optimized = withSQLConf(
        SQLConf.COLLAPSE_WINDOW_WITH_EMPTY_ORDER_SPEC_IN_CHILD.key -> "true") {
      Optimize.execute(analyzed)
    }
    assert(analyzed.output === optimized.output)

    val correctAnswer = testRelation
      .window(Seq(cnt, rk), partitionSpec1, orderSpec1)

    comparePlans(optimized, correctAnswer)
  }

  test("don't collapse the inner empty-order window by default") {
    // Merging an empty-order child into an ordered parent can disable InferWindowGroupLimit for
    // top-k queries, so it is off by default.
    val rk = windowExpr(
      RowNumber(),
      windowSpec(partitionSpec1, orderSpec1,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))).as("rk")
    val cnt = windowExpr(
      AggregateExpression(Count(c), Complete, isDistinct = false, None),
      windowSpec(partitionSpec1, Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing))).as("cnt")

    val query = testRelation
      .window(Seq(cnt), partitionSpec1, Nil)
      .window(Seq(rk), partitionSpec1, orderSpec1)

    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = query.analyze

    comparePlans(optimized, correctAnswer)
  }

  test("collapse windows with a Project between them when one has an empty order spec") {
    // The same merge applies when a Project sits between the two windows and only passes through
    // columns that are available below the inner window (SPARK-34565 shape). The empty-order
    // window is the inner window here, so the config must be enabled.
    val rk = windowExpr(
      RowNumber(),
      windowSpec(partitionSpec1, orderSpec1,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))).as("rk")
    val cnt = windowExpr(
      AggregateExpression(Count(c), Complete, isDistinct = false, None),
      windowSpec(partitionSpec1, Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing))).as("cnt")

    val query = testRelation
      .window(Seq(cnt), partitionSpec1, Nil)
      .select($"a", $"b", $"c", $"cnt")
      .window(Seq(rk), partitionSpec1, orderSpec1)
      .select($"a", $"b", $"c", $"cnt", $"rk")

    val analyzed = query.analyze
    val optimized = withSQLConf(
        SQLConf.COLLAPSE_WINDOW_WITH_EMPTY_ORDER_SPEC_IN_CHILD.key -> "true") {
      Optimize.execute(analyzed)
    }
    assert(analyzed.output === optimized.output)

    val correctAnswer = testRelation
      .window(Seq(cnt, rk), partitionSpec1, orderSpec1)
      .select(a, b, c, $"cnt", $"rk")
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("collapse windows with a Project between them, empty order spec as parent") {
    // The empty-order window is the parent here, so this merges by default without the config.
    val rk = windowExpr(
      RowNumber(),
      windowSpec(partitionSpec1, orderSpec1,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))).as("rk")
    val cnt = windowExpr(
      AggregateExpression(Count(c), Complete, isDistinct = false, None),
      windowSpec(partitionSpec1, Nil,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing))).as("cnt")

    val query = testRelation
      .window(Seq(rk), partitionSpec1, orderSpec1)
      .select($"a", $"b", $"c", $"rk")
      .window(Seq(cnt), partitionSpec1, Nil)
      .select($"a", $"b", $"c", $"rk", $"cnt")

    val optimized = Optimize.execute(query.analyze)
    assert(query.analyze.output === optimized.output)

    val correctAnswer = testRelation
      .window(Seq(rk, cnt), partitionSpec1, orderSpec1)
      .select(a, b, c, $"rk", $"cnt")
      .analyze

    comparePlans(optimized, correctAnswer)
  }
}
