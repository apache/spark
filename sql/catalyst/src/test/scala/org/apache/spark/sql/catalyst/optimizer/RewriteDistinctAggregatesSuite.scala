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
import org.apache.spark.sql.catalyst.expressions.{CaseWhen, Expression, If, Literal, Round}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, CollectSet, Count, Sum}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Expand, LocalRelation, LogicalPlan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StringType}

class RewriteDistinctAggregatesSuite extends PlanTest {
  val nullInt = Literal(null, IntegerType)
  val nullString = Literal(null, StringType)
  val testRelation = LocalRelation($"a".string, $"b".string, $"c".string, $"d".string, $"e".int)
  val testRelation2 = LocalRelation($"a".double, $"b".int, $"c".int, $"d".int, $"e".int)

  private def checkRewrite(rewrite: LogicalPlan): Unit = rewrite match {
    case Aggregate(_, _, Aggregate(_, _, _: Expand, _), _) =>
    case _ => fail(s"Plan is not rewritten:\n$rewrite")
  }

  test("single distinct group") {
    val input = testRelation
      .groupBy($"a")(countDistinct($"e"))
      .analyze
    val rewrite = RewriteDistinctAggregates(input)
    comparePlans(input, rewrite)
  }

  test("single distinct group with partial aggregates") {
    val input = testRelation
      .groupBy($"a", $"d")(
        countDistinct($"e", $"c").as("agg1"),
        max($"b").as("agg2"))
      .analyze
    val rewrite = RewriteDistinctAggregates(input)
    comparePlans(input, rewrite)
  }

  test("multiple distinct groups") {
    val input = testRelation
      .groupBy($"a")(countDistinct($"b", $"c"), countDistinct($"d"))
      .analyze
    checkRewrite(RewriteDistinctAggregates(input))
  }

  test("multiple distinct groups with partial aggregates") {
    val input = testRelation
      .groupBy($"a")(countDistinct($"b", $"c"), countDistinct($"d"), sum($"e"))
      .analyze
    checkRewrite(RewriteDistinctAggregates(input))
  }

  test("multiple distinct groups with non-partial aggregates") {
    val input = testRelation
      .groupBy($"a")(
        countDistinct($"b", $"c"),
        countDistinct($"d"),
        CollectSet($"b").toAggregateExpression())
      .analyze
    checkRewrite(RewriteDistinctAggregates(input))
  }

  test("SPARK-40382: eliminate multiple distinct groups due to superficial differences") {
    val input = testRelation2
      .groupBy($"a")(
        countDistinct($"b" + $"c").as("agg1"),
        countDistinct($"c" + $"b").as("agg2"),
        max($"c").as("agg3"))
      .analyze

    val rewrite = RewriteDistinctAggregates(input)
    rewrite match {
      case Aggregate(_, _, _: LocalRelation, _) =>
      case _ => fail(s"Plan is not as expected:\n$rewrite")
    }
  }

  test("SPARK-40382: reduce multiple distinct groups due to superficial differences") {
    val input = testRelation2
      .groupBy($"a")(
        countDistinct($"b" + $"c" + $"d").as("agg1"),
        countDistinct($"d" + $"c" + $"b").as("agg2"),
        countDistinct($"b" + $"c").as("agg3"),
        countDistinct($"c" + $"b").as("agg4"),
        max($"c").as("agg5"))
      .analyze

    val rewrite = RewriteDistinctAggregates(input)
    rewrite match {
      case Aggregate(_, _, Aggregate(_, _, e: Expand, _), _) =>
        assert(e.projections.size == 3)
      case _ => fail(s"Plan is not rewritten:\n$rewrite")
    }
  }

  test("SPARK-49261: Literals in grouping expressions shouldn't result in unresolved aggregation") {
    val relation = testRelation2
      .select(Literal(6).as("gb"), $"a", $"b", $"c", $"d")
    val input = relation
      .groupBy($"a", $"gb")(
        countDistinct($"b").as("agg1"),
        countDistinct($"d").as("agg2"),
        Round(sum($"c").as("sum1"), 6)).analyze
    val rewriteFold = FoldablePropagation(input)
    // without the fix, the below produces an unresolved plan
    val rewrite = RewriteDistinctAggregates(rewriteFold)
    if (!rewrite.resolved) {
      fail(s"Plan is not as expected:\n$rewrite")
    }
  }

  // ---------------------------------------------------------------------------
  // COUNT(DISTINCT IF/CASE) canonicalization (SPARK-56898)
  // ---------------------------------------------------------------------------

  val conditionalTestRelation = LocalRelation(
    Symbol("a").int, Symbol("b").int, Symbol("c").int, Symbol("d").string)

  private def countDistinctIf(cond: Expression, base: Expression): Expression = {
    Count(If(cond, base, Literal(null))).toAggregateExpression(isDistinct = true)
  }

  private def countDistinctCaseWhen(cond: Expression, base: Expression): Expression = {
    val caseWhen = CaseWhen(
      Seq((cond, base)),
      None)
    Count(caseWhen).toAggregateExpression(isDistinct = true)
  }

  private def countDistinctCaseWhenElseNull(cond: Expression, base: Expression): Expression = {
    val caseWhen = CaseWhen(
      Seq((cond, base)),
      Some(Literal(null)))
    Count(caseWhen).toAggregateExpression(isDistinct = true)
  }

  private def collectAggregateExpressions(plan: LogicalPlan): Seq[AggregateExpression] = {
    plan.collect { case a: Aggregate => a.aggregateExpressions }
      .flatten
      .flatMap(_.collect { case ae: AggregateExpression => ae })
  }

  /**
   * Asserts that the optimized plan has exactly one Expand node with one projection,
   * that the projection contains `baseColName` as a plain attribute, that it
   * contains no expression of `removedWrapperType` (the IF/CaseWhen that was stripped),
   * and that the outer aggregate has moved the condition into a FILTER clause.
   */
  private def assertSingleDistinctGroupExpand(
      optimized: LogicalPlan,
      baseColName: String,
      removedWrapperType: Class[_]): Unit = {
    val expand = optimized.collectFirst { case e: Expand => e }.get
    assert(expand.projections.size == 1,
      s"expected 1 distinct group but got ${expand.projections.size}")
    val baseAttr = conditionalTestRelation.output.find(_.name == baseColName).get
    assert(expand.projections.head.exists(_.semanticEquals(baseAttr)),
      s"expected base column $baseColName in Expand projection")
    assert(!expand.projections.head.exists(e => removedWrapperType.isInstance(e)),
      s"${removedWrapperType.getSimpleName} wrapper should have been removed " +
        "from Expand projection")
    assert(collectAggregateExpressions(optimized).exists(_.filter.isDefined),
      "expected at least one AggregateExpression with a FILTER clause")
  }

  test("conditional: disabled when config is false") {
    withSQLConf(SQLConf.REWRITE_COUNT_DISTINCT_CONDITIONAL_ENABLED.key -> "false") {
      val input = conditionalTestRelation
        .groupBy(Symbol("a"))(
          countDistinctIf(Symbol("b") > 1, Symbol("c")).as("cnt1"),
          countDistinctIf(Symbol("b") > 2, Symbol("c")).as("cnt2"))
        .analyze
      val optimized = RewriteDistinctAggregates(input)
      // The general RewriteDistinctAggregates still fires, but conditional
      // canonicalization is disabled so the two conditional counts stay as 2
      // distinct groups instead of collapsing to 1.
      val expands = optimized.collect { case e: Expand => e }
      assert(expands.head.projections.size == 2,
        "expected 2 distinct groups when conditional canonicalization is disabled")
    }
  }

  test("conditional: rewrite COUNT(DISTINCT IF(cond, col, NULL)) to COUNT(DISTINCT col) FILTER") {
    val input = conditionalTestRelation
      .groupBy(Symbol("a"))(
        countDistinctIf(Symbol("b") > 1, Symbol("c")).as("cnt1"),
        countDistinctIf(Symbol("b") > 2, Symbol("c")).as("cnt2"))
      .analyze
    val optimized = RewriteDistinctAggregates(input)
    assertSingleDistinctGroupExpand(optimized, "c", classOf[If])
  }

  test("conditional: rewrite COUNT(DISTINCT CASE WHEN cond THEN col END) to " +
      "COUNT(DISTINCT col) FILTER") {
    val input = conditionalTestRelation
      .groupBy(Symbol("a"))(
        countDistinctCaseWhen(Symbol("b") > 1, Symbol("c")).as("cnt1"),
        countDistinctCaseWhen(Symbol("b") > 2, Symbol("c")).as("cnt2"))
      .analyze
    val optimized = RewriteDistinctAggregates(input)
    assertSingleDistinctGroupExpand(optimized, "c", classOf[CaseWhen])
  }

  test("conditional: rewrite COUNT(DISTINCT CASE WHEN cond THEN col ELSE NULL END)") {
    val input = conditionalTestRelation
      .groupBy(Symbol("a"))(
        countDistinctCaseWhenElseNull(Symbol("b") > 1, Symbol("c")).as("cnt1"),
        countDistinctCaseWhenElseNull(Symbol("b") > 2, Symbol("c")).as("cnt2"))
      .analyze
    val optimized = RewriteDistinctAggregates(input)
    assertSingleDistinctGroupExpand(optimized, "c", classOf[CaseWhen])
  }

  test("conditional: multiple conditional distinct counts collapse to single distinct group") {
    val input = conditionalTestRelation
      .groupBy(Symbol("a"))(
        countDistinctIf(Symbol("b") > 1, Symbol("c")).as("cnt1"),
        countDistinctIf(Symbol("b") > 2, Symbol("c")).as("cnt2"),
        countDistinctIf(Symbol("b") > 3, Symbol("c")).as("cnt3"))
      .analyze
    val optimized = RewriteDistinctAggregates(input)
    // All three counts share the same base column c, collapsed to 1 distinct group.
    assertSingleDistinctGroupExpand(optimized, "c", classOf[If])
  }

  test("conditional: single conditional distinct count is gated out by mayNeedtoRewrite") {
    // A lone COUNT(DISTINCT IF(...)) must NOT be pushed onto the Expand path.
    // The canonicalization runs inside rewrite(), which is only called when
    // mayNeedtoRewrite returns true. A single conditional distinct count has no filter
    // and forms only one distinct group, so mayNeedtoRewrite returns false, rewrite()
    // is never called, and no Expand is produced.
    val input = conditionalTestRelation
      .groupBy(Symbol("a"))(
        countDistinctIf(Symbol("b") > 1, Symbol("c")).as("cnt1"))
      .analyze
    val optimized = RewriteDistinctAggregates(input)
    val expands = optimized.collect { case e: Expand => e }
    assert(expands.isEmpty, "single conditional distinct count should not produce an Expand")
  }

  test("conditional: do not rewrite IF with non-null else branch") {
    val input = conditionalTestRelation
      .groupBy(Symbol("a"))(
        Count(If(Symbol("b") > 1, Symbol("c"), Literal(0, IntegerType)))
          .toAggregateExpression(isDistinct = true)
          .as("cnt1"),
        Count(If(Symbol("b") > 2, Symbol("c"), Literal(0, IntegerType)))
          .toAggregateExpression(isDistinct = true)
          .as("cnt2"))
      .analyze
    val optimized = RewriteDistinctAggregates(input)
    // Plan is rewritten (2 distinct groups) but not canonicalized
    checkRewrite(optimized)
    val expands = optimized.collect { case e: Expand => e }
    assert(expands.head.projections.size == 2,
      "non-null else branch should not be collapsed to 1 distinct group")
  }

  test("conditional: do not rewrite non-distinct COUNT") {
    val input = conditionalTestRelation
      .groupBy(Symbol("a"))(
        Count(If(Symbol("b") > 1, Symbol("c"), Literal(null, IntegerType)))
          .toAggregateExpression(isDistinct = false)
          .as("cnt1"),
        countDistinct(Symbol("c")).as("cnt2"))
      .analyze
    val optimized = RewriteDistinctAggregates(input)
    // Still a single distinct group (cnt2), no canonicalization of the non-distinct agg
    comparePlans(optimized, input)
  }

  test("conditional: do not rewrite when FILTER already exists") {
    val input = conditionalTestRelation
      .groupBy(Symbol("a"))(
        Count(If(Symbol("b") > 1, Symbol("c"), Literal(null, IntegerType)))
          .toAggregateExpression(isDistinct = true, filter = Some(Symbol("d") === "x"))
          .as("cnt1"),
        Count(If(Symbol("b") > 2, Symbol("c"), Literal(null, IntegerType)))
          .toAggregateExpression(isDistinct = true, filter = Some(Symbol("d") === "y"))
          .as("cnt2"))
      .analyze
    val optimized = RewriteDistinctAggregates(input)
    checkRewrite(optimized)
    val expands = optimized.collect { case e: Expand => e }
    // 2 groups because existing FILTERs prevent canonicalization
    assert(expands.head.projections.size == 2)
  }

  test("conditional: do not rewrite multi-branch CASE WHEN") {
    val caseWhen1 = new CaseWhen(
      Seq(
        (Symbol("b") > Literal(1), Symbol("c")),
        (Symbol("b") > Literal(2), Symbol("a"))),
      Some(Literal(null)))
    val caseWhen2 = new CaseWhen(
      Seq(
        (Symbol("b") > Literal(3), Symbol("c")),
        (Symbol("b") > Literal(4), Symbol("a"))),
      Some(Literal(null)))
    val input = conditionalTestRelation
      .groupBy(Symbol("a"))(
        Count(caseWhen1).toAggregateExpression(isDistinct = true).as("cnt1"),
        Count(caseWhen2).toAggregateExpression(isDistinct = true).as("cnt2"))
      .analyze
    val optimized = RewriteDistinctAggregates(input)
    checkRewrite(optimized)
    val expands = optimized.collect { case e: Expand => e }
    // 2 groups - multi-branch CASE was not canonicalized
    assert(expands.head.projections.size == 2)
  }

  test("conditional: do not rewrite SUM(DISTINCT IF(...))") {
    val input = conditionalTestRelation
      .groupBy(Symbol("a"))(
        Sum(If(Symbol("b") > 1, Symbol("c"), Literal(null, IntegerType)))
          .toAggregateExpression(isDistinct = true)
          .as("sum1"),
        Sum(If(Symbol("b") > 2, Symbol("c"), Literal(null, IntegerType)))
          .toAggregateExpression(isDistinct = true)
          .as("sum2"))
      .analyze
    val optimized = RewriteDistinctAggregates(input)
    checkRewrite(optimized)
    val expands = optimized.collect { case e: Expand => e }
    // 2 groups - SUM(DISTINCT IF) is not canonicalized
    assert(expands.head.projections.size == 2)
  }
}
