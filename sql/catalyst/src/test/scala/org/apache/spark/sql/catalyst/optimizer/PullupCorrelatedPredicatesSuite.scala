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

import org.apache.spark.sql.catalyst.analysis.{EliminateSubqueryAliases, SimpleAnalyzer}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{CollectList, Count, DeclarativeAggregate}
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, AggregateHint, Assignment, DeleteAction, DeleteFromTable, DomainJoin, InsertAction, LateralJoin, LocalRelation, LogicalPlan, MergeIntoTable, UpdateTable}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, DoubleType, LongType}

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

  // Returns ScalarSubquery.mayHaveCountBug of the scalar subquery `subPlan` after the rule,
  // whether DecorrelateInnerQuery handled the count bug, which it does with a left outer domain
  // join, and the number of Aggregates in the rewritten subquery.
  // With checkAnalysis = false, the plan may have expressions that the analyzer rejects.
  private def countBugHandling(
      subPlan: LogicalPlan,
      checkAnalysis: Boolean = true): (Option[Boolean], Boolean, Int) = {
    val outer = testRelation.select(ScalarSubquery(subPlan).as("sq"))
    val plan = if (checkAnalysis) {
      outer.analyze
    } else {
      EliminateSubqueryAliases(SimpleAnalyzer.execute(outer))
    }
    val optimized = Optimize.execute(plan)
    comparePlans(Optimize.execute(optimized), optimized, false)
    val subqueries = optimized.expressions.flatMap(_.collect {
      case s: ScalarSubquery => s
    })
    assert(subqueries.size == 1)
    val countBugDomainJoin = subqueries.head.plan.exists {
      case d: DomainJoin => d.joinType == LeftOuter
      case _ => false
    }
    val numAggregates = subqueries.head.plan.collect { case a: Aggregate => a }.size
    (subqueries.head.mayHaveCountBug, countBugDomainJoin, numAggregates)
  }

  private def correlated(agg: Expression): LogicalPlan =
    testRelation2.where($"a" === $"c").groupBy()(agg.as("s"))

  private def uncorrelated(agg: Expression): LogicalPlan =
    testRelation2.groupBy()(agg.as("s")).where($"s" > $"b")

  test("SPARK-60501: handle count bug in decorrelation for any correlated nested ungrouped " +
      "aggregate") {
    def overLower(lower: LogicalPlan): (Option[Boolean], Boolean, Int) =
      countBugHandling(lower.groupBy()(new CollectList($"s").toAggregateExpression().as("l")))

    // The lower aggregate returns one row on empty input whatever its function is, so the count
    // bug is handled in DecorrelateInnerQuery with a domain join, not in constructLeftJoins.
    // COLLECT_LIST is an ImperativeAggregate, so the top aggregate is not collapsed.
    Seq(max($"d"), sum($"d"), coalesce(sum($"d"), Literal(10.0)), count($"d")).foreach { agg =>
      assert(overLower(correlated(agg)) == (Some(false), true, 2), agg)
    }
    // Also when the correlated predicate is not right below the lower aggregate.
    val deeper = testRelation2.where($"a" === $"c").select($"d").groupBy()(max($"d").as("s"))
    assert(overLower(deeper) == (Some(false), true, 2))
    // A grouped lower aggregate returns no row on empty input, and an uncorrelated one keeps its
    // row, so constructLeftJoins handles the count bug of the top aggregate.
    val grouped = testRelation2.where($"a" === $"c").groupBy($"c")(max($"d").as("s"))
    assert(overLower(grouped) == (Some(true), false, 2))
    Seq(max($"d"), count($"d")).foreach { agg =>
      assert(overLower(uncorrelated(agg)) == (Some(true), false, 2), agg)
    }
    withSQLConf(SQLConf.LEGACY_SCALAR_SUBQUERY_NESTED_AGGREGATE_COUNT_BUG_BEHAVIOR.key -> "true") {
      assert(overLower(correlated(max($"d"))) == (Some(true), false, 2))
      assert(overLower(correlated(count($"d"))) == (Some(false), true, 2))
      assert(overLower(uncorrelated(count($"d"))) == (Some(false), true, 2))
    }
  }

  test("SPARK-60501: collapse an ungrouped aggregate over one row in a correlated scalar " +
      "subquery") {
    def top(lower: LogicalPlan, agg: Expression): LogicalPlan = lower.groupBy()(agg.as("t"))
    val lower = correlated(max($"d"))

    // The top aggregate over the one row of the lower one becomes a Project, so the count bug of
    // the lower aggregate is handled in constructLeftJoins.
    Seq(count(Literal(1)), count($"s"), sum($"s"), max($"s"), avg($"s"),
      Count($"s").toAggregateExpression(isDistinct = true),
      Count(Literal(1)).toAggregateExpression(isDistinct = false, filter = Some($"s".isNull)),
      sum($"s") + Literal(1.0)).foreach { agg =>
      assert(countBugHandling(top(lower, agg)) == (Some(true), false, 1), agg)
    }
    // Also through Projects, over more than one level, and for a correlated Project on top.
    assert(countBugHandling(
      top(lower.select(($"s" + 1).as("s")), count($"s"))) == (Some(true), false, 1))
    assert(countBugHandling(
      top(top(lower, sum($"s")).select($"t".as("s")), count($"s"))) == (Some(true), false, 1))
    assert(countBugHandling(top(lower, sum($"s")).select(($"t" + $"b").as("u"))) ==
      (Some(false), true, 1))
    // Projects without outer references on top of a single aggregate do not need a domain join.
    assert(countBugHandling(lower.select(($"s" + 1).as("u"))) == (Some(true), false, 1))
    // A top aggregate with no aggregate function, only a literal, also collapses.
    assert(countBugHandling(top(lower, Literal(1))) == (Some(true), false, 1))
    // A top aggregate that carries a hint is kept.
    val hinted =
      top(lower, count($"s")).asInstanceOf[Aggregate].copy(hint = Some(TestAggregateHint))
    assert(countBugHandling(hinted) == (Some(false), true, 2))

    // Not collapsed: an ImperativeAggregate, a non-deterministic input, and nodes that may not
    // return exactly one row between the aggregates.
    val collectList = new CollectList($"s").toAggregateExpression()
    assert(countBugHandling(top(lower, collectList)) == (Some(false), true, 2))
    assert(countBugHandling(top(lower, sum($"s" * Rand(Literal(0L)))), checkAnalysis = false) ==
      (Some(false), true, 2))
    val nonDeterministicFilter = Count(Literal(1))
      .toAggregateExpression(isDistinct = false, filter = Some($"s" > Rand(Literal(0L))))
    assert(countBugHandling(top(lower, nonDeterministicFilter), checkAnalysis = false) ==
      (Some(false), true, 2))
    assert(countBugHandling(top(lower, count(Literal(1)) + Size(collectList))) ==
      (Some(false), true, 2))
    assert(countBugHandling(top(lower.where($"s" > 0), count($"s"))) == (Some(false), true, 2))
    val grouped = testRelation2.where($"a" === $"c").groupBy($"c")(max($"d").as("s"))
    assert(countBugHandling(top(grouped, count($"s"))) == (Some(true), false, 2))
    assert(countBugHandling(top(lower.union(correlated(min($"d"))), count($"s"))) ==
      (Some(false), true, 3))
    // An uncorrelated ungrouped lower aggregate also returns one row. The correlated Filter on
    // top is pulled up, and there is no count bug to handle.
    assert(countBugHandling(top(testRelation2.groupBy()(max($"d").as("s")), count($"s"))
      .where($"t" > $"b")) == (Some(false), false, 1))

    // A result that does not keep the data type or the nullability of the aggregate function.
    def custom(dataType: DataType, nullable: Boolean): Expression =
      OneRowTestAggregate($"s", dataType, nullable).toAggregateExpression()
    assert(countBugHandling(top(lower, custom(DoubleType, nullable = true))) ==
      (Some(true), false, 1))
    assert(countBugHandling(top(lower, custom(LongType, nullable = true))) ==
      (Some(false), true, 2))
    assert(countBugHandling(top(lower, custom(DoubleType, nullable = false))) ==
      (Some(false), true, 2))

    // The confs that keep the plans of earlier versions disable it.
    Seq(
      SQLConf.LEGACY_SCALAR_SUBQUERY_NESTED_AGGREGATE_COUNT_BUG_BEHAVIOR.key -> "true",
      SQLConf.LEGACY_SCALAR_SUBQUERY_COUNT_BUG_HANDLING.key -> "true",
      SQLConf.DECORRELATE_INNER_QUERY_ENABLED.key -> "false").foreach { kv =>
      withSQLConf(kv) {
        val (_, _, numAggregates) = countBugHandling(top(lower, count($"s")))
        assert(numAggregates == 2, kv)
        assert(countBugHandling(lower.select(($"s" + 1).as("u")))._2 ==
          (kv._1 == SQLConf.LEGACY_SCALAR_SUBQUERY_NESTED_AGGREGATE_COUNT_BUG_BEHAVIOR.key), kv)
      }
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

case object TestAggregateHint extends AggregateHint

/**
 * Returns its last input, or null without input, as `resultType` with `resultNullable`, which
 * do not need to match the input.
 */
private[optimizer] case class OneRowTestAggregate(
    child: Expression,
    resultType: DataType,
    resultNullable: Boolean)
  extends DeclarativeAggregate with UnaryLike[Expression] {
  private lazy val last = AttributeReference("last", child.dataType)()
  override lazy val aggBufferAttributes: Seq[AttributeReference] = last :: Nil
  override lazy val initialValues: Seq[Expression] = Seq(Literal.create(null, child.dataType))
  override lazy val updateExpressions: Seq[Expression] = Seq(child)
  override lazy val mergeExpressions: Seq[Expression] = Seq(last.right)
  override lazy val evaluateExpression: Expression = last
  override def dataType: DataType = resultType
  override def nullable: Boolean = resultNullable
  override protected def withNewChildInternal(newChild: Expression): OneRowTestAggregate =
    copy(child = newChild)
}
