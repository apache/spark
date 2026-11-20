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

import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{Average, CollectList, CollectSet, DeclarativeAggregate, ListAgg, Percentile, StddevSamp, Sum}
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, CTERelationRef, Deduplicate, DeleteAction, DeleteFromTable, Distinct, DomainJoin, Except, InsertAction, Intersect, LateralJoin, LocalRelation, LogicalPlan, MergeIntoTable, Sample, Tail, Union, UpdateTable}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, DayTimeIntervalType, DoubleType, IntegerType, LongType, MapType, StringType, TestUDT}

// A DeclarativeAggregate whose evaluate expression does not have its data type.
private case class MistypedAggregate(child: Expression)
  extends DeclarativeAggregate with UnaryLike[Expression] {
  override def dataType: DataType = LongType
  override def nullable: Boolean = true
  private lazy val buffer = AttributeReference("buffer", IntegerType)()
  override lazy val aggBufferAttributes: Seq[AttributeReference] = buffer :: Nil
  override lazy val initialValues: Seq[Expression] = Seq(Literal(0))
  override lazy val updateExpressions: Seq[Expression] = Seq(buffer)
  override lazy val mergeExpressions: Seq[Expression] = Seq(buffer.left)
  override lazy val evaluateExpression: Expression = buffer
  override protected def withNewChildInternal(newChild: Expression): MistypedAggregate =
    copy(child = newChild)
}

class PullupCorrelatedPredicatesSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    override protected val excludedOnceBatches = Set("PullupCorrelatedPredicates")

    val batches =
      Batch("PullupCorrelatedPredicates", Once,
        PullupCorrelatedPredicates) :: Nil
  }

  val testRelation = LocalRelation($"a".int, $"b".double)
  val testRelation2 = LocalRelation($"c".int, $"d".double)

  test("PullupCorrelatedPredicates should not produce unresolved plan") {
    val subPlan =
      testRelation2
        .where($"b" < $"d")
        .select($"c")
    val inSubquery =
      testRelation
        .where(InSubquery(Seq($"a"), ListQuery(subPlan)))
        .select($"a").analyze
    assert(inSubquery.resolved)

    val optimized = Optimize.execute(inSubquery)
    assert(optimized.resolved)
  }

  test("PullupCorrelatedPredicates in correlated subquery idempotency check") {
    val subPlan =
      testRelation2
      .where($"b" < $"d")
      .select($"c")
    val inSubquery =
      testRelation
      .where(InSubquery(Seq($"a"), ListQuery(subPlan)))
      .select($"a").analyze
    assert(inSubquery.resolved)

    val optimized = Optimize.execute(inSubquery)
    val doubleOptimized = Optimize.execute(optimized)
    comparePlans(optimized, doubleOptimized)
  }

  test("PullupCorrelatedPredicates exists correlated subquery idempotency check") {
    val subPlan =
      testRelation2
        .where($"b" === $"d" && $"d" === 1)
        .select(Literal(1))
    val existsSubquery =
      testRelation
        .where(Exists(subPlan))
        .select($"a").analyze
    assert(existsSubquery.resolved)

    val optimized = Optimize.execute(existsSubquery)
    val doubleOptimized = Optimize.execute(optimized)
    comparePlans(optimized, doubleOptimized)
  }

  test("PullupCorrelatedPredicates scalar correlated subquery idempotency check") {
    val subPlan =
      testRelation2
        .where($"b" === $"d" && $"d" === 1)
        .select(max($"d"))
    val scalarSubquery =
      testRelation
        .where(ScalarSubquery(subPlan) === 1)
        .select($"a").analyze

    val optimized = Optimize.execute(scalarSubquery)
    val doubleOptimized = Optimize.execute(optimized)
    comparePlans(optimized, doubleOptimized, false)
  }

  test("PullupCorrelatedPredicates lateral join idempotency check") {
    val right =
      testRelation2
        .where($"b" === $"d" && $"d" === 1)
        .select($"c")
    val left = testRelation
    val lateralJoin = LateralJoin(left, LateralSubquery(right), Inner, Some($"a" === $"c")).analyze
    val optimized = Optimize.execute(lateralJoin)
    val doubleOptimized = Optimize.execute(optimized)
    comparePlans(optimized, doubleOptimized)
  }

  // Returns ScalarSubquery.mayHaveCountBug, and whether DecorrelateInnerQuery handled the count
  // bug, which it does with a left outer domain join, for the aggregates `top` over `lower` (in
  // a struct if there are several, since a scalar subquery returns one column), in a Project
  // over `outer` that also has the columns `others`, under the operators that `above` adds.
  private def countBugHandling(
      lower: LogicalPlan,
      top: Seq[Expression] = Seq(count(Literal(1))),
      grouping: Seq[Expression] = Nil,
      outer: LogicalPlan = testRelation,
      others: Seq[NamedExpression] = Nil,
      above: LogicalPlan => LogicalPlan = identity): (Option[Boolean], Boolean) = {
    val result = if (top.size == 1) top.head else CreateStruct(top)
    val subPlan = lower.groupBy(grouping: _*)(result.as("r"))
    countBugHandlingIn(above(outer.select(others :+ ScalarSubquery(subPlan).as("sq"): _*)))
  }

  // countBugHandling of the correlated scalar subquery with the output `r` in `query`.
  private def countBugHandlingIn(query: LogicalPlan): (Option[Boolean], Boolean) = {
    val plan = query.analyze
    val optimized = Optimize.execute(plan)
    comparePlans(Optimize.execute(optimized), optimized, false)
    val subqueries = optimized.flatMap(_.expressions).flatMap(_.collect {
      case s: ScalarSubquery if s.children.nonEmpty && s.plan.output.head.name == "r" => s
    })
    assert(subqueries.size == 1)
    val countBugDomainJoin = subqueries.head.plan.exists {
      case d: DomainJoin => d.joinType == LeftOuter
      case _ => false
    }
    (subqueries.head.mayHaveCountBug, countBugDomainJoin)
  }

  // ScalarSubquery.countBugResult of the subquery of countBugHandling, as a string.
  private def countBugResult(
      lower: LogicalPlan,
      top: Seq[Expression] = Seq(count(Literal(1))),
      grouping: Seq[Expression] = Nil,
      outer: LogicalPlan = testRelation,
      above: LogicalPlan => LogicalPlan = identity): Option[String] = {
    val result = if (top.size == 1) top.head else CreateStruct(top)
    val subPlan = lower.groupBy(grouping: _*)(result.as("r"))
    val optimized = Optimize.execute(above(outer.select(ScalarSubquery(subPlan).as("sq"))).analyze)
    val subqueries = optimized.flatMap(_.expressions).flatMap(_.collect {
      case s: ScalarSubquery if s.children.nonEmpty && s.plan.output.head.name == "r" => s
    })
    assert(subqueries.size == 1)
    subqueries.head.countBugResult.map(_.toString)
  }

  // DomainJoin.keepLimitAbove of each domain join of the scalar subquery `subPlan`.
  private def keepLimitAbove(subPlan: LogicalPlan): Seq[Boolean] = {
    val optimized = Optimize.execute(testRelation.select(ScalarSubquery(subPlan).as("sq")).analyze)
    optimized.expressions.flatMap(_.collect { case s: ScalarSubquery => s })
      .flatMap(_.plan.collect { case d: DomainJoin => d.keepLimitAbove })
  }

  private def correlated(agg: Expression): LogicalPlan =
    testRelation2.where($"a" === $"c").groupBy()(agg.as("s"))

  private def uncorrelated(agg: Expression): LogicalPlan =
    testRelation2.groupBy()(agg.as("s")).where($"s" > $"b")

  // A correlated lower aggregate with a filter above it, between it and the top aggregate, for
  // which the result for an outer row without matching rows cannot be computed before
  // decorrelation: the filter can remove the row of the lower aggregate for an outer row with
  // matching rows too. So the count bug is handled in DecorrelateInnerQuery.
  private def filtered(agg: Expression): LogicalPlan = correlated(agg).where($"s" > 0)

  test("SPARK-60501: handle count bug in decorrelation for a correlated nested ungrouped " +
      "aggregate") {

    // The lower aggregate returns one row on empty input whatever its function is. Without a
    // function with a default result there, the result for an outer row without matching rows
    // is computed before decorrelation, and constructLeftJoins uses it, without a domain join.
    Seq(max($"d"), sum($"d"), coalesce(sum($"d"), Literal(10.0))).foreach { agg =>
      assert(countBugHandling(correlated(agg)) == (Some(true), false), agg)
      assert(countBugResult(correlated(agg)) == Some("1"), agg)
    }
    // Where it cannot be computed, the count bug is handled in DecorrelateInnerQuery with a
    // domain join, as it is for a lower function with a default result.
    Seq(max($"d"), sum($"d"), coalesce(sum($"d"), Literal(10.0))).foreach { agg =>
      assert(countBugHandling(filtered(agg)) == (Some(false), true), agg)
      assert(countBugResult(filtered(agg)).isEmpty, agg)
    }
    assert(countBugHandling(correlated(count($"d"))) == (Some(false), true))
    assert(countBugResult(correlated(count($"d"))).isEmpty)
    // Also when the correlated predicate is not right below the lower aggregate.
    val deeper = testRelation2.where($"a" === $"c").select($"d").groupBy()(max($"d").as("s"))
    assert(countBugHandling(deeper) == (Some(true), false))
    assert(countBugResult(deeper) == Some("1"))
    assert(countBugHandling(deeper.where($"s" > 0)) == (Some(false), true))
    // A grouped lower aggregate returns no row on empty input, and an uncorrelated one keeps its
    // row, so constructLeftJoins handles the count bug of the top aggregate.
    val grouped = testRelation2.where($"a" === $"c").groupBy($"c")(max($"d").as("s"))
    assert(countBugHandling(grouped) == (Some(true), false))
    assert(countBugHandling(uncorrelated(max($"d"))) == (Some(true), false))
    assert(countBugResult(grouped).isEmpty)
    assert(countBugResult(uncorrelated(max($"d"))).isEmpty)
    // A lower aggregate with a function with a default result is handled in decorrelation, as
    // before, also when it is uncorrelated.
    assert(countBugHandling(uncorrelated(count($"d"))) == (Some(false), true))
    // Only the domain joins of a subquery that is handled in decorrelation since SPARK-60501 keep
    // a LIMIT above; the ones that decorrelation gave before do not: over a lower COUNT, and for
    // a non-equality correlation, which gets a domain join also in a single aggregate, also
    // when countBugResult or the legacy conf keeps constructLeftJoins.
    def countOver(lower: LogicalPlan): LogicalPlan = lower.groupBy()(count(Literal(1)).as("r"))
    val nonEquality = testRelation2.where($"a" > $"c").groupBy()(max($"d").as("s"))
    assert(keepLimitAbove(countOver(filtered(max($"d")))).toSet == Set(true))
    assert(keepLimitAbove(countOver(nonEquality.where($"s" > 0))).toSet == Set(true))
    assert(keepLimitAbove(countOver(correlated(max($"d")))).isEmpty)
    assert(keepLimitAbove(countOver(nonEquality)).toSet == Set(false))
    assert(keepLimitAbove(countOver(correlated(count($"d")))).toSet == Set(false))
    assert(keepLimitAbove(countOver(uncorrelated(count($"d")))).toSet == Set(false))
    assert(keepLimitAbove(nonEquality).toSet == Set(false))
    assert(keepLimitAbove(nonEquality.groupBy()(sum($"s").as("r"))).toSet == Set(false))
    withSQLConf(SQLConf.LEGACY_SCALAR_SUBQUERY_NESTED_AGGREGATE_COUNT_BUG_BEHAVIOR.key -> "true") {
      assert(countBugHandling(correlated(max($"d"))) == (Some(true), false))
      assert(countBugResult(correlated(max($"d"))).isEmpty)
      assert(countBugHandling(filtered(max($"d"))) == (Some(true), false))
      assert(countBugHandling(correlated(count($"d"))) == (Some(false), true))
      assert(countBugHandling(uncorrelated(count($"d"))) == (Some(false), true))
      assert(keepLimitAbove(countOver(nonEquality.where($"s" > 0))).toSet == Set(false))
    }
    // The rest of this test is about subqueries that need the domain joins of
    // DecorrelateInnerQuery, such as `filtered`. Those joins compute the outer input again, so
    // they are not used where it may give other rows then, nor where they fail. The result
    // computed before decorrelation needs no domain join (see the end of this test).
    // A streaming outer query keeps the legacy behavior.
    val streamingOuter = testRelation.copy(isStreaming = true)
    assert(countBugHandling(filtered(max($"d")), outer = streamingOuter) == (Some(true), false))
    assert(countBugHandling(correlated(count($"d")), outer = streamingOuter) ==
      (Some(false), true))
    // So do an outer input that may give other rows when computed again, an operator above it
    // that a later rule may move into it, and outer references that are not orderable; a lower
    // COUNT is still handled in decorrelation there.
    val nonDeterministicOuter = testRelation.where(Rand(0) > 0.5)
    assert(countBugHandling(filtered(max($"d")), outer = nonDeterministicOuter) ==
      (Some(true), false))
    assert(countBugHandling(correlated(count($"d")), outer = nonDeterministicOuter) ==
      (Some(false), true))
    // A CTE that is not inlined may be a non-deterministic one referenced twice.
    val cteOuter = CTERelationRef(0, _resolved = true, testRelation.output, isStreaming = false)
    assert(countBugHandling(filtered(max($"d")), outer = cteOuter) == (Some(true), false))
    assert(countBugHandling(correlated(count($"d")), outer = cteOuter) == (Some(false), true))
    // A non-deterministic filter above the subquery, which a later rule can push below it, but
    // not a non-deterministic expression next to the subquery, which the domain joins do not
    // compute again.
    assert(countBugHandling(filtered(max($"d")), others = Seq(Rand(0).as("r"))) ==
      (Some(false), true))
    assert(countBugHandling(filtered(max($"d")), above = _.where(Rand(0) > 0.5)) ==
      (Some(true), false))
    assert(countBugHandling(correlated(count($"d")), above = _.where(Rand(0) > 0.5)) ==
      (Some(false), true))
    // A LIMIT, a window function or a sample in the outer input, also in a subquery there, but
    // not a LIMIT above the subquery.
    val limitInSubquery = InSubquery(Seq($"a"), ListQuery(testRelation2.select($"c").limit(1)))
    Seq(testRelation.limit(10), testRelation.offset(1), Tail(Literal(1), testRelation),
        Sample(0.0, 0.5, withReplacement = false, 1L, testRelation),
        testRelation.where(limitInSubquery),
        testRelation.join(testRelation2, LeftOuter, Some($"a" === $"c")).limit(10),
        testRelation.window(Seq(windowExpr(RowNumber(),
          windowSpec(Nil, Seq($"a".asc), UnspecifiedFrame)).as("rn")), Nil, Seq($"a".asc))
    ).foreach { outer =>
      assert(countBugHandling(filtered(max($"d")), outer = outer) == (Some(true), false), outer)
      assert(countBugHandling(correlated(count($"d")), outer = outer) == (Some(false), true))
    }
    assert(countBugHandling(filtered(max($"d")), above = _.limit(10)) == (Some(false), true))
    // Only operators and aggregate functions known to give the same rows again: FIRST, a SUM of
    // DOUBLEs, an AVG of INTs (which AVG adds up as DOUBLE), a Deduplicate (which becomes a FIRST
    // later) and a subquery with a LIMIT in a projection do not, a SUM of INTs and the LIMIT 1
    // of an uncorrelated EXISTS do.
    val inLegacy = (Some(true), false)
    val inDecorrelation = (Some(false), true)
    Seq(testRelation.groupBy($"a")($"a", first($"b").as("f")),
        testRelation.groupBy($"a")($"a", sum($"b").as("s")),
        testRelation.groupBy($"a")($"a", avg($"a").as("m")),
        Deduplicate(Seq($"a"), testRelation),
        testRelation.select($"a", ScalarSubquery(testRelation2.select($"c").limit(1)).as("x"))
    ).foreach { outer =>
      assert(countBugHandling(filtered(max($"d")), outer = outer) == inLegacy, outer)
    }
    val groupedOuter = testRelation.groupBy($"a")($"a", sum($"a").as("s"))
    assert(countBugHandling(filtered(max($"d")), outer = groupedOuter) == inDecorrelation)
    // A SUM or AVG of decimals becomes NULL on an overflow that depends on the order, unless in
    // ANSI mode.
    val intervals = LocalRelation($"a".int, AttributeReference("i", DayTimeIntervalType())())
    assert(countBugHandling(filtered(max($"d")),
      outer = intervals.groupBy($"a")($"a", avg($"i").as("m"))) == inDecorrelation)
    // try_sum and try_avg give NULL on an overflow that depends on the order.
    Seq(Sum($"b", NumericEvalContext(EvalMode.TRY)), Sum($"i", NumericEvalContext(EvalMode.TRY)),
        Average($"i", EvalMode.TRY)).foreach { f =>
      val input =
        LocalRelation($"a".int, $"b".long, AttributeReference("i", DayTimeIntervalType())())
      val outer = input.groupBy($"a")($"a", f.toAggregateExpression().as("s"))
      assert(countBugHandling(filtered(max($"d")), outer = outer) == inLegacy, f)
    }
    val decimals = LocalRelation($"a".int, $"x".decimal(10, 2))
    Seq(true, false).foreach { ansi =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi.toString) {
        Seq(sum($"x"), avg($"x")).foreach { agg =>
          val outer = decimals.groupBy($"a")($"a", agg.as("s"))
          assert(countBugHandling(filtered(max($"d")), outer = outer) ==
            (if (ansi) inDecorrelation else inLegacy), (ansi, agg))
        }
        Seq(Sum($"x", NumericEvalContext(EvalMode.TRY)), Average($"x", EvalMode.TRY)).foreach { f =>
          val outer = decimals.groupBy($"a")($"a", f.toAggregateExpression().as("s"))
          assert(countBugHandling(filtered(max($"d")), outer = outer) == inLegacy, (ansi, f))
        }
      }
    }
    val uncorrelatedExists =
      IsNotNull(ScalarSubquery(testRelation2.select(Literal(1).as("col")).limit(1)))
    assert(countBugHandling(filtered(max($"d")), outer = testRelation.where(uncorrelatedExists))
      == inDecorrelation)
    // A LIMIT above does not matter for any outer input: LimitPushDown keeps it above a Project
    // with a scalar subquery with a domain join with keepLimitAbove (see LimitPushdownSuite).
    assert(countBugHandling(filtered(max($"d")), outer = groupedOuter, above = _.limit(21)) ==
      inDecorrelation)
    assert(countBugHandling(filtered(max($"d")), outer = groupedOuter, above = _.limit(1)) ==
      inDecorrelation)
    assert(countBugHandling(filtered(max($"d")), outer = testRelation.groupBy($"a")($"a"),
      above = _.limit(21)) == inDecorrelation)
    val groupOnly = LocalRelation($"z".long).groupBy($"z")($"z")
    assert(countBugHandling(filtered(max($"d")), above = p => Union(p, groupOnly).limit(21)) ==
      inDecorrelation)
    assert(countBugHandling(filtered(max($"d")), above = p => Union(p, groupOnly).limit(1)) ==
      inDecorrelation)
    // A domain join directly in the outer input takes its domain from outside this plan.
    val domainOuter = DomainJoin(Seq(AttributeReference("z", IntegerType)()), testRelation)
    assert(countBugHandling(filtered(max($"d")), outer = domainOuter) == inLegacy)
    val innerOuter = testRelation.join(testRelation2, Inner, Some($"a" === $"c"))
    assert(countBugHandling(filtered(max($"d")), outer = innerOuter, above = _.limit(21)) ==
      inDecorrelation)
    // Above the subquery: a LIMIT, which LimitPushDown keeps above a Project with a scalar
    // subquery with the domain joins that this subquery gets in decorrelation, does not matter,
    // but a filter with a subquery that may give other rows, which PushDownPredicates can push,
    // does, unlike a deterministic filter.
    assert(countBugHandling(filtered(max($"d")), outer = Distinct(testRelation.select($"a")),
      above = _.limit(1)) == inDecorrelation)
    assert(countBugHandling(filtered(max($"d")), others = Seq($"a"),
      above = _.where(limitInSubquery)) == inLegacy)
    assert(countBugHandling(filtered(max($"d")), others = Seq($"a"),
      above = _.where($"a" > 1)) == inDecorrelation)
    // Another subquery of the operator, which a filter above on its alias can inline, but not
    // without such a filter; and likewise of a Project above, also through another alias and the
    // children of a Union, which PushDownPredicates pushes a filter into by position. A subquery
    // that gives the same rows again does not matter.
    val limitedScalar = ScalarSubquery(testRelation2.select($"c").limit(1)).as("x")
    val doubleMaxScalar = ScalarSubquery(testRelation2.groupBy()(max($"d").as("m"))).as("x")
    val repeatableScalar = ScalarSubquery(testRelation2.groupBy()(max($"c").as("m"))).as("x")
    Seq(limitedScalar -> IntegerType, doubleMaxScalar -> DoubleType).foreach { case (other, dt) =>
      assert(countBugHandling(filtered(max($"d")), others = Seq(other)) == inDecorrelation)
      assert(countBugHandling(filtered(max($"d")), others = Seq(other),
        above = _.where($"x" > 0)) == inLegacy)
      assert(countBugHandling(filtered(max($"d")), others = Seq($"a"),
        above = _.select($"a", $"sq", other)) == inDecorrelation)
      assert(countBugHandling(filtered(max($"d")), others = Seq($"a"),
        above = _.select($"a", $"sq", other).where($"x" > 0)) == inLegacy)
      assert(countBugHandling(filtered(max($"d")), others = Seq($"a"),
        above = _.select($"a", $"sq", other).select(($"x" + 1).as("y")).where($"y" > 0)) ==
        inLegacy)
      val first =
        testRelation.select($"a", Literal(1L).as("sq"), Cast($"a", dt).as("x"))
      assert(countBugHandling(filtered(max($"d")), others = Seq($"a"),
        above = p => Union(first, p.select($"a", $"sq", other))) == inDecorrelation)
      assert(countBugHandling(filtered(max($"d")), others = Seq($"a"),
        above = p => Union(first, p.select($"a", $"sq", other)).where($"x" > 0)) == inLegacy)
      // Likewise an EXCEPT ALL or INTERSECT ALL, which become a Union first, but not an EXCEPT,
      // whose filter above stays on its left child.
      assert(countBugHandling(filtered(max($"d")), others = Seq($"a"),
        above = p => Except(first, p.select($"a", $"sq", other), isAll = true).where($"x" > 0)) ==
        inLegacy)
      assert(countBugHandling(filtered(max($"d")), others = Seq($"a"), above = p =>
        Intersect(first, p.select($"a", $"sq", other), isAll = true).where($"x" > 0)) == inLegacy)
      assert(countBugHandling(filtered(max($"d")), others = Seq($"a"), above = p =>
        Except(first, p.select($"a", $"sq", other), isAll = false).where($"x" > 0)) ==
        inDecorrelation)
      // A join condition, which PushPredicateThroughJoin can push, but not a lateral subquery,
      // which stays in the right side of its join.
      assert(countBugHandling(filtered(max($"d")), others = Seq($"a"),
        above = _.select($"a", $"sq", other).join(testRelation2, Inner,
          Some($"x" > 0 && $"a" === $"c"))) == inLegacy)
      assert(countBugHandling(filtered(max($"d")), others = Seq($"a"),
        above = p => LateralJoin(p, LateralSubquery(testRelation2.select(other)), Inner, None)) ==
        inDecorrelation)
    }
    assert(countBugHandling(filtered(max($"d")), others = Seq(repeatableScalar),
      above = _.where($"x" > 0)) == inDecorrelation)
    // A filter above on the alias of the subquery itself pushes a copy of the subquery into the
    // outer input, which counts when its plan may give other rows, as with a MAX of DOUBLEs, even
    // where its result, such as this COUNT, cannot change.
    assert(countBugHandling(filtered(max($"d")), above = _.where($"sq" > 0)) == inLegacy)
    assert(countBugHandling(filtered(max($"c")), above = _.where($"sq" > 0)) == inDecorrelation)
    // In the condition of its own operator, the subquery itself does not count, though its MAX of
    // DOUBLEs may give another value: the domain joins do not compute the operator itself again.
    val ownFilter = ScalarSubquery(filtered(max($"d")).groupBy()(count(Literal(1)).as("r")))
    assert(countBugHandlingIn(testRelation.where(ownFilter === 1L)) == inDecorrelation)
    // ReorderJoin can move a filter with the subquery above the other inputs of the inner joins
    // around it, which then count, but not the inputs of joins that the filter is not over.
    val sqFilter = ScalarSubquery(filtered(max($"c")).groupBy()(count(Literal(1)).as("r"))) === 1L
    val f = LocalRelation($"f".int)
    val crossed = testRelation.join(LocalRelation($"e".int)).where(sqFilter)
    assert(countBugHandlingIn(crossed.join(f.limit(2), Inner, Some($"a" === $"f"))) == inLegacy)
    assert(countBugHandlingIn(crossed.join(f, Inner, Some($"a" === $"f"))) == inDecorrelation)
    assert(countBugHandlingIn(testRelation.where(sqFilter).join(f.limit(2), Inner,
      Some($"a" === $"f"))) == inDecorrelation)
    // Another correlated subquery of the operator, or of a Project above, which CollapseProject
    // can bring into the operator, whose join to the outer input the domain joins compute again,
    // counts when it may return more than one row and give other rows there, e.g. with a
    // non-deterministic filter, but not when it returns one row.
    // With rows, since an empty LocalRelation has at most no rows.
    val twoRows = testRelation2.copy(data = Seq(InternalRow(1, 1.0), InternalRow(2, 2.0)))
    val nonDeterministicRows =
      ScalarSubquery(twoRows.where($"a" === $"c" && Rand(0) > 0.5).select($"d")).as("s1")
    val oneRow = ScalarSubquery(testRelation2.where($"a" === $"c").groupBy()(max($"d").as("m")))
    val deterministicRows = ScalarSubquery(twoRows.where($"a" === $"c").select($"d"))
    assert(countBugHandling(filtered(max($"d")), others = Seq(nonDeterministicRows)) ==
      inLegacy)
    assert(countBugHandling(filtered(max($"d")), others = Seq(oneRow.as("s1"))) ==
      inDecorrelation)
    assert(countBugHandling(filtered(max($"d")), others = Seq(deterministicRows.as("s1"))) ==
      inDecorrelation)
    assert(countBugHandling(filtered(max($"d")), others = Seq($"a"),
      above = _.select($"a", nonDeterministicRows, $"sq")) == inLegacy)
    // Not an uncorrelated one, which is not joined to the input.
    val uncorrelatedRows = ScalarSubquery(twoRows.where(Rand(0) > 0.5).select($"d")).as("s1")
    assert(countBugHandling(filtered(max($"d")), others = Seq(uncorrelatedRows)) ==
      inDecorrelation)
    // A scalar Python UDF over the result of the lower aggregate keeps the previous handling when
    // its conditional evaluation (SPARK-61482) is off, since it would then run on the value over
    // no rows for every outer row.
    val pyUdf = PythonUDF("udf", null, LongType, Seq(max($"d")), PythonEvalType.SQL_BATCHED_UDF,
      udfDeterministic = true)
    Seq(true, false).foreach { enabled =>
      withSQLConf(
          SQLConf.DECORRELATE_INNER_QUERY_COUNT_BUG_EVAL_UDF_CONDITIONALLY_ENABLED.key ->
            enabled.toString) {
        assert(countBugHandling(filtered(pyUdf)) ==
          (if (enabled) inDecorrelation else inLegacy), enabled)
      }
    }
    assert(countBugHandling(filtered(max($"d")), others = Seq($"a"),
      above = _.select($"a", $"sq", repeatableScalar).where($"x" > 0)) == inDecorrelation)
    // MIN, MAX, grouping, DISTINCT, INTERSECT and EXCEPT keep one of the values that compare
    // equal, which one depending on the order: -0.0 or 0.0 for MIN and MAX of floating-point
    // values (the others normalize them), 'a' or 'A' in UTF8_LCASE for all of them.
    val collated = LocalRelation($"a".int, $"b".double,
      AttributeReference("s", StringType("UTF8_LCASE"))())
    Seq(collated.groupBy($"a")($"a", max($"b").as("m")),
        collated.groupBy($"a")($"a", min($"b").as("m")),
        collated.groupBy($"a")($"a", max(CreateStruct(Seq($"b"))).as("m")),
        collated.groupBy($"a")($"a", max($"s").as("m")),
        collated.groupBy($"a")($"a", min($"s").as("m")),
        collated.groupBy($"a", $"s")($"a", $"s"),
        collated.groupBy($"a", $"s")($"a", Upper($"s").as("u")),
        Distinct(collated.select($"a", $"s")),
        Intersect(collated.select($"a", $"s"), collated.select($"a", $"s"), isAll = false),
        Except(collated.select($"a", $"s"), collated.select($"a", $"s"), isAll = false)
    ).foreach { outer =>
      assert(countBugHandling(filtered(max($"d")), outer = outer) == inLegacy, outer)
    }
    val udt = LocalRelation($"a".int, AttributeReference("v", new TestUDT.MyDenseVectorUDT())())
    assert(countBugHandling(filtered(max($"d")),
      outer = udt.groupBy($"a")($"a", max($"v").as("m"))) == inLegacy)
    Seq(collated.groupBy($"a")($"a", max($"a").as("m")),
        // A grouping key that is not in the output.
        collated.groupBy($"a", $"s")($"a"),
        collated.groupBy($"a", $"b")($"a", $"b"),
        Distinct(collated.select($"a", $"b")),
        Intersect(collated.select($"a", $"b"), collated.select($"a", $"b"), isAll = false)
    ).foreach { outer =>
      assert(countBugHandling(filtered(max($"d")), outer = outer) == inDecorrelation, outer)
    }
    val joinOuter = testRelation.join(testRelation2, LeftOuter, Some($"a" === $"c"))
    assert(countBugHandling(filtered(max($"d")), outer = joinOuter, above = _.limit(10)) ==
      (Some(false), true))
    val mapOuter = LocalRelation(AttributeReference("m", MapType(IntegerType, IntegerType))())
    // Use size(m), whose outer reference stays a map also in the full optimizer, where
    // PullOutNestedDataOuterRefExpressions would pull a lookup such as m[0] out.
    def overMap(agg: Expression): LogicalPlan =
      testRelation2.where($"c" === Size($"m")).groupBy()(agg.as("s")).where($"s" > 0)
    assert(countBugHandling(overMap(max($"d")), outer = mapOuter) == (Some(true), false))
    assert(countBugHandling(overMap(count($"d")), outer = mapOuter) == (Some(false), true))
    // Neither applies when the count bug is not handled in decorrelation at all.
    Seq(SQLConf.LEGACY_SCALAR_SUBQUERY_COUNT_BUG_HANDLING.key -> "true",
        SQLConf.DECORRELATE_INNER_QUERY_ENABLED.key -> "false").foreach { kv =>
      withSQLConf(kv) {
        assert(countBugHandling(filtered(max($"d"))) == (Some(true), false), kv)
        assert(countBugHandling(correlated(count($"d"))) == (Some(true), false), kv)
        assert(countBugResult(correlated(max($"d"))).isEmpty, kv)
      }
    }
    // The result computed before decorrelation needs no domain join, so it applies whatever the
    // outer input is and whatever is above the subquery, also where the domain joins are not
    // used or fail.
    Seq(streamingOuter, nonDeterministicOuter, cteOuter, testRelation.limit(10),
        Sample(0.0, 0.5, withReplacement = false, 1L, testRelation), domainOuter,
        collated.groupBy($"a")($"a", max($"b").as("m"))).foreach { outer =>
      assert(countBugHandling(correlated(max($"d")), outer = outer) == inLegacy, outer)
      assert(countBugResult(correlated(max($"d")), outer = outer) == Some("1"), outer)
    }
    assert(countBugResult(correlated(max($"d")), above = _.where(Rand(0) > 0.5)) == Some("1"))
    assert(countBugResult(correlated(max($"d")), above = _.where($"sq" > 0)) == Some("1"))
    val overMapUnfiltered = testRelation2.where($"c" === Size($"m")).groupBy()(max($"d").as("s"))
    assert(countBugResult(overMapUnfiltered, outer = mapOuter) == Some("1"))
    // A Python UDF over the lower aggregate does not matter when the top aggregate does not use
    // its result, but one in the result is not computed before decorrelation.
    Seq(true, false).foreach { enabled =>
      withSQLConf(
          SQLConf.DECORRELATE_INNER_QUERY_COUNT_BUG_EVAL_UDF_CONDITIONALLY_ENABLED.key ->
            enabled.toString) {
        assert(countBugResult(correlated(pyUdf)) == Some("1"), enabled)
        assert(countBugResult(correlated(pyUdf), Seq(sum($"s"))).isEmpty, enabled)
      }
    }
  }

  test("SPARK-60501: compute the result for an outer row without matching rows before " +
      "decorrelation") {
    val inConstructLeftJoins = (Some(true), false)
    val inDecorrelation = (Some(false), true)
    val lowerMax = correlated(max($"d"))
    // The top aggregate over the row of NULLs that the lower MAX returns on empty input, which
    // differs from its result over no rows for some functions, such as COUNT(*): with FILTER and
    // DISTINCT, functions that collect the NULL, and expressions over the functions.
    Seq(count(Literal(1)) -> "1", count($"s") -> "0", sum($"s") -> "null", max($"s") -> "null",
        avg($"s") -> "null", first($"s") -> "null", countDistinct($"s") -> "0",
        count(Literal(1), Some($"s".isNull)) -> "1", count(Literal(1), Some($"s" > 1)) -> "0",
        sum($"s", Some($"s" > 1)) -> "null", collectList($"s") -> "[]",
        CollectList($"s", ignoreNulls = false).toAggregateExpression() -> "[null]",
        CollectSet($"s", ignoreNulls = false).toAggregateExpression() -> "[null]",
        StddevSamp($"s").toAggregateExpression() -> "null",
        coalesce(sum($"s"), Literal(10.0)) -> "10.0", sum($"s") + 1 -> "null",
        Literal(1) -> "1").foreach { case (top, result) =>
      assert(countBugHandling(lowerMax, Seq(top)) == inConstructLeftJoins, top)
      assert(countBugResult(lowerMax, Seq(top)) == Some(result), top)
    }
    assert(countBugResult(lowerMax, Seq(count(Literal(1)), collectList($"s"))) == Some("[1,[]]"))
    // Through Projects, over another function and a literal of the lower aggregate, over three
    // levels and a grouped aggregate between, and for a grouped top aggregate, which returns a row
    // for the group of the row of the lower aggregate.
    assert(countBugResult(lowerMax.select(($"s" + 1).as("t")), Seq(count($"t"))) == Some("0"))
    assert(countBugResult(lowerMax.select(coalesce($"s", Literal(2.0)).as("t")), Seq(sum($"t")))
      == Some("2.0"))
    val lowerMaxAndMin =
      testRelation2.where($"a" === $"c").groupBy()(max($"d").as("s"), min($"d").as("n"))
    assert(countBugResult(lowerMaxAndMin, Seq(sum($"s"), count($"n"))) == Some("[null,0]"))
    val lowerWithLiteral =
      testRelation2.where($"a" === $"c").groupBy()(Literal(10).as("s"), max($"d").as("m"))
    assert(countBugResult(lowerWithLiteral, Seq(sum($"s"))) == Some("10"))
    assert(countBugResult(lowerMax.groupBy()(max($"s").as("s"))) == Some("1"))
    assert(countBugResult(lowerMax.groupBy($"s")($"s")) == Some("1"))
    // A grouped aggregate below returns no row on empty input, so the MAX over it is NULL, not
    // the MAX over a row with its COUNT of 0.
    val groupedBelow = testRelation2.where($"a" === $"c").groupBy($"d")(count(Literal(1)).as("n"))
      .groupBy()(max($"n").as("s"))
    assert(countBugResult(groupedBelow, Seq(count($"s"))) == Some("0"))
    assert(countBugHandling(lowerMax, grouping = Seq($"s")) == inConstructLeftJoins)
    assert(countBugResult(lowerMax, grouping = Seq($"s")) == Some("1"))
    // COLLECT_LIST with RESPECT NULLS over the row of NULLs, with a FILTER that does not hold.
    assert(countBugResult(lowerMax,
      Seq(CollectList($"s", ignoreNulls = false).toAggregateExpression(false, Some($"s" > 1))))
      == Some("[]"))
    // An outer reference between the aggregates, here in a Project, does not change the rows,
    // only values, which are not computed (see below), so it does not matter for a COUNT(*).
    assert(countBugResult(lowerMax.select(($"s" + $"b").as("s"))) == Some("1"))
    // An expression that fails on the row of the lower aggregate does not matter when the top
    // aggregate does not use it.
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      val failing = lowerMax.select((Literal(10.0) / coalesce($"s", Literal(0.0))).as("t"))
      assert(countBugResult(failing) == Some("1"))
      // Otherwise the result is not computed, and the count bug is handled in
      // DecorrelateInnerQuery, which evaluates the expression in the aggregate.
      assert(countBugResult(failing, Seq(sum($"t"))).isEmpty)
      assert(countBugHandling(failing, Seq(sum($"t"))) == inDecorrelation)
    }
    // Neither is it for a filter between the aggregates, a value over an outer reference, a
    // function that is not computed over one row, such as PERCENTILE, a FILTER with a subquery,
    // a Python UDF in the result, and a function whose evaluate expression does not have its
    // data type.
    val subqueryRelation = LocalRelation($"e".double)
    val uncorrelatedFilter = ScalarSubquery(subqueryRelation.groupBy()(max($"e").as("x"))) > 0
    val correlatedFilter = ScalarSubquery(
      subqueryRelation.where($"e" === $"s").groupBy()(max($"e").as("x"))).isNotNull
    val pyUdf = PythonUDF("udf", null, LongType, Seq($"s"), PythonEvalType.SQL_BATCHED_UDF,
      udfDeterministic = true)
    Seq(lowerMax.where($"s".isNull) -> count(Literal(1)),
        lowerMax.select(($"s" + $"b").as("s")) -> sum($"s"),
        lowerMax -> new Percentile($"s", Literal(0.5)).toAggregateExpression(),
        lowerMax -> sum($"s", Some(uncorrelatedFilter)),
        lowerMax -> sum($"s", Some(correlatedFilter)),
        lowerMax -> sum(pyUdf),
        lowerMax -> MistypedAggregate($"s").toAggregateExpression()).foreach { case (lower, top) =>
      assert(countBugResult(lower, Seq(top)).isEmpty, (lower, top))
      assert(countBugHandling(lower, Seq(top)) == inDecorrelation, (lower, top))
    }
    // Where the result is not computed, a top aggregate that returns over the row of NULLs what
    // it returns over no rows stays in constructLeftJoins, with the result over no rows, e.g. next
    // to an uncorrelated subquery, which keeps the result from being a literal. Spark has no
    // property for such functions, so a short list of functions directly over the outputs of
    // lower aggregate functions, also through Projects, is accepted (see
    // sameResultOverEmptyLowerAggregate).
    val uncorrelatedScalar = ScalarSubquery(subqueryRelation.groupBy()(max($"e").as("x")))
    Seq(sum($"s"), max($"s"), min($"s"), avg($"s"), count($"s"), first($"s"), last($"s"),
        collectList($"s"), collectSet($"s"), countDistinct($"s"), sum($"s") + 1).foreach { top =>
      assert(countBugResult(lowerMax, Seq(top, uncorrelatedScalar)).isEmpty, top)
      assert(countBugHandling(lowerMax, Seq(top, uncorrelatedScalar)) == inConstructLeftJoins, top)
    }
    assert(countBugHandling(lowerMax.select($"s".as("t")), Seq(sum($"t"), uncorrelatedScalar)) ==
      inConstructLeftJoins)
    assert(countBugHandling(lowerMax.select($"s", ($"s" + 1).as("t")),
      Seq(sum($"s"), uncorrelatedScalar)) == inConstructLeftJoins)
    assert(countBugHandling(lowerMaxAndMin, Seq(sum($"s"), count($"n"), uncorrelatedScalar)) ==
      inConstructLeftJoins)
    // Other shapes go to DecorrelateInnerQuery: a function that counts the NULL row, one not in
    // the list, one that collects NULLs, a FILTER, an argument other than the output of a lower
    // aggregate function, another function next to it, a computed column, a node other than a
    // Project between the aggregates, and another aggregate below.
    Seq(lowerMax -> count(Literal(1)), lowerMax -> StddevSamp($"s").toAggregateExpression(),
        lowerMax -> CollectList($"s", ignoreNulls = false).toAggregateExpression(),
        lowerMax -> CollectSet($"s", ignoreNulls = false).toAggregateExpression(),
        lowerMax -> sum($"s", Some($"s" > 1)), lowerMax -> sum($"s" + 1),
        lowerMax.select(($"s" + 1).as("s")) -> sum($"s"), lowerMax.where($"s" > 1) -> sum($"s"),
        lowerMax.groupBy()(max($"s").as("s")) -> sum($"s")).foreach { case (lower, top) =>
      assert(countBugHandling(lower, Seq(top, uncorrelatedScalar)) == inDecorrelation,
        (lower, top))
    }
    assert(countBugHandling(lowerMax, Seq(sum($"s"), count(Literal(1)), uncorrelatedScalar)) ==
      inDecorrelation)
    // It is not tried for a lower aggregate with a function with a default result, which is
    // handled in decorrelation as before, even when the top aggregate does not use it or its
    // default result is NULL, for an uncorrelated lower aggregate, and with the legacy behavior.
    val lowerMaxAndCount =
      testRelation2.where($"a" === $"c").groupBy()(max($"d").as("s"), count($"d").as("n"))
    val lowerListAgg = correlated(ListAgg(Cast($"d", StringType)).toAggregateExpression())
    val countBelowProject = correlated(count($"d")).select($"s").groupBy()(max($"s").as("s"))
    Seq(lowerMaxAndCount -> sum($"s"), lowerMaxAndCount -> sum($"n"), lowerListAgg -> count($"s"),
        countBelowProject -> sum($"s")).foreach { case (lower, top) =>
      assert(countBugResult(lower, Seq(top)).isEmpty, (lower, top))
      assert(countBugHandling(lower, Seq(top)) == inDecorrelation, (lower, top))
    }
    assert(countBugResult(uncorrelated(max($"d"))).isEmpty)
    withSQLConf(SQLConf.LEGACY_SCALAR_SUBQUERY_NESTED_AGGREGATE_COUNT_BUG_BEHAVIOR.key -> "true") {
      assert(countBugHandling(lowerMax) == inConstructLeftJoins)
      assert(countBugResult(lowerMax).isEmpty)
      assert(countBugHandling(lowerMaxAndCount, Seq(sum($"s"))) == inDecorrelation)
    }
  }

  test("PullupCorrelatedPredicates should handle deletes") {
    val subPlan = testRelation2.where($"a" === $"c").select($"c")
    val cond = InSubquery(Seq($"a"), ListQuery(subPlan))
    val deletePlan = DeleteFromTable(testRelation, cond).analyze
    assert(deletePlan.resolved)

    val optimized = Optimize.execute(deletePlan)
    assert(optimized.resolved)

    optimized match {
      case DeleteFromTable(_, s: InSubquery) =>
        val outerRefs = SubExprUtils.getOuterReferences(s.query.plan)
        assert(outerRefs.isEmpty, "should be no outer refs")
      case other =>
        fail(s"unexpected logical plan: $other")
    }
  }

  test("PullupCorrelatedPredicates should handle updates") {
    val subPlan = testRelation2.where($"a" === $"c").select($"c")
    val cond = InSubquery(Seq($"a"), ListQuery(subPlan))
    val updatePlan = UpdateTable(testRelation, Seq.empty, Some(cond)).analyze
    assert(updatePlan.resolved)

    val optimized = Optimize.execute(updatePlan)
    assert(optimized.resolved)

    optimized match {
      case UpdateTable(_, _, Some(s: InSubquery)) =>
        val outerRefs = SubExprUtils.getOuterReferences(s.query.plan)
        assert(outerRefs.isEmpty, "should be no outer refs")
      case other =>
        fail(s"unexpected logical plan: $other")
    }
  }

  test("PullupCorrelatedPredicates should handle merge") {
    val testRelation3 = LocalRelation($"e".int, $"f".double)
    val subPlan = testRelation3.where($"a" === $"e").select($"e")
    val cond = InSubquery(Seq($"a"), ListQuery(subPlan))

    val mergePlan = MergeIntoTable(
      testRelation,
      testRelation2,
      cond,
      Seq(DeleteAction(None)),
      Seq(InsertAction(None, Seq(Assignment($"a", $"c"), Assignment($"b", $"d")))),
      Seq(DeleteAction(None)),
      withSchemaEvolution = false)
    val analyzedMergePlan = mergePlan.analyze
    assert(analyzedMergePlan.resolved)

    val optimized = Optimize.execute(analyzedMergePlan)
    assert(optimized.resolved)

    optimized match {
      case MergeIntoTable(_, _, s: InSubquery, _, _, _, _) =>
        val outerRefs = SubExprUtils.getOuterReferences(s.query.plan)
        assert(outerRefs.isEmpty, "should be no outer refs")
      case other =>
        fail(s"unexpected logical plan: $other")
    }
  }
}
