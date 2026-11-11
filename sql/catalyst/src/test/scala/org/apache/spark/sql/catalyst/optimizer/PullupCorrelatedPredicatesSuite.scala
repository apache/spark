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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{CollectList, CollectSet, ListAgg, StddevSamp}
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, CTERelationRef, DeleteAction, DeleteFromTable, DomainJoin, InsertAction, LateralJoin, LocalRelation, LogicalPlan, MergeIntoTable, UpdateTable}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, MapType, StringType}

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
  // over `outer` that also has the columns `others`.
  private def countBugHandling(
      lower: LogicalPlan,
      top: Seq[Expression] = Seq(count(Literal(1))),
      grouping: Seq[Expression] = Nil,
      outer: LogicalPlan = testRelation,
      others: Seq[NamedExpression] = Nil): (Option[Boolean], Boolean) = {
    val result = if (top.size == 1) top.head else CreateStruct(top)
    val subPlan = lower.groupBy(grouping: _*)(result.as("r"))
    val plan = outer.select(others :+ ScalarSubquery(subPlan).as("sq"): _*).analyze
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
    (subqueries.head.mayHaveCountBug, countBugDomainJoin)
  }

  private def correlated(agg: Expression): LogicalPlan =
    testRelation2.where($"a" === $"c").groupBy()(agg.as("s"))

  private def uncorrelated(agg: Expression): LogicalPlan =
    testRelation2.groupBy()(agg.as("s")).where($"s" > $"b")

  test("SPARK-60501: handle count bug in decorrelation for a correlated nested ungrouped " +
      "aggregate") {

    // The lower aggregate returns one row on empty input whatever its function is, so the count
    // bug is handled in DecorrelateInnerQuery with a domain join, not in constructLeftJoins.
    Seq(max($"d"), sum($"d"), coalesce(sum($"d"), Literal(10.0)), count($"d")).foreach { agg =>
      assert(countBugHandling(correlated(agg)) == (Some(false), true), agg)
    }
    // Also when the correlated predicate is not right below the lower aggregate.
    val deeper = testRelation2.where($"a" === $"c").select($"d").groupBy()(max($"d").as("s"))
    assert(countBugHandling(deeper) == (Some(false), true))
    // A grouped lower aggregate returns no row on empty input, and an uncorrelated one keeps its
    // row, so constructLeftJoins handles the count bug of the top aggregate.
    val grouped = testRelation2.where($"a" === $"c").groupBy($"c")(max($"d").as("s"))
    assert(countBugHandling(grouped) == (Some(true), false))
    assert(countBugHandling(uncorrelated(max($"d"))) == (Some(true), false))
    // A lower aggregate with a function with a default result is handled in decorrelation, as
    // before, also when it is uncorrelated.
    assert(countBugHandling(uncorrelated(count($"d"))) == (Some(false), true))
    withSQLConf(SQLConf.LEGACY_SCALAR_SUBQUERY_NESTED_AGGREGATE_COUNT_BUG_BEHAVIOR.key -> "true") {
      assert(countBugHandling(correlated(max($"d"))) == (Some(true), false))
      assert(countBugHandling(correlated(count($"d"))) == (Some(false), true))
      assert(countBugHandling(uncorrelated(count($"d"))) == (Some(false), true))
    }
    // A streaming outer query keeps the legacy behavior.
    val streamingOuter = testRelation.copy(isStreaming = true)
    assert(countBugHandling(correlated(max($"d")), outer = streamingOuter) == (Some(true), false))
    assert(countBugHandling(correlated(count($"d")), outer = streamingOuter) ==
      (Some(false), true))
    // So do a non-deterministic expression in the input of the operator with the subquery and
    // outer references that are not orderable; a lower COUNT is still handled in decorrelation
    // there.
    val nonDeterministicOuter = testRelation.where(Rand(0) > 0.5)
    assert(countBugHandling(correlated(max($"d")), outer = nonDeterministicOuter) ==
      (Some(true), false))
    assert(countBugHandling(correlated(count($"d")), outer = nonDeterministicOuter) ==
      (Some(false), true))
    // So does a reference to a CTE that is not inlined, which may be a non-deterministic CTE
    // referenced twice, computed again with other rows.
    val cteOuter = CTERelationRef(0, _resolved = true, testRelation.output, isStreaming = false)
    assert(countBugHandling(correlated(max($"d")), outer = cteOuter) == (Some(true), false))
    assert(countBugHandling(correlated(count($"d")), outer = cteOuter) == (Some(false), true))
    // A non-deterministic expression next to the subquery, not in the input of its operator,
    // does not change the outer query that the domain joins compute again.
    assert(countBugHandling(correlated(max($"d")), others = Seq(Rand(0).as("r"))) ==
      (Some(false), true))
    val mapOuter = LocalRelation(AttributeReference("m", MapType(IntegerType, IntegerType))())
    // Use size(m), whose outer reference stays a map also in the full optimizer, where
    // PullOutNestedDataOuterRefExpressions would pull a lookup such as m[0] out.
    def overMap(agg: Expression): LogicalPlan =
      testRelation2.where($"c" === Size($"m")).groupBy()(agg.as("s"))
    assert(countBugHandling(overMap(max($"d")), outer = mapOuter) == (Some(true), false))
    assert(countBugHandling(overMap(count($"d")), outer = mapOuter) == (Some(false), true))
    // Neither applies when the count bug is not handled in decorrelation at all.
    Seq(SQLConf.LEGACY_SCALAR_SUBQUERY_COUNT_BUG_HANDLING.key -> "true",
        SQLConf.DECORRELATE_INNER_QUERY_ENABLED.key -> "false").foreach { kv =>
      withSQLConf(kv) {
        assert(countBugHandling(correlated(max($"d"))) == (Some(true), false), kv)
        assert(countBugHandling(correlated(count($"d"))) == (Some(true), false), kv)
      }
    }
  }

  test("SPARK-60501: handle count bug in constructLeftJoins when the top aggregate returns the " +
      "same result over the NULL row of a lower aggregate as over no rows") {
    val inConstructLeftJoins = (Some(true), false)
    val inDecorrelation = (Some(false), true)
    val lowerMax = correlated(max($"d"))
    // Each of these functions returns over a row of NULLs what it returns over no rows.
    Seq(sum($"s"), max($"s"), min($"s"), avg($"s"), count($"s"), first($"s"), last($"s"),
        collectList($"s"), collectSet($"s"), countDistinct($"s"), sum($"s") + 1).foreach { top =>
      assert(countBugHandling(lowerMax, Seq(top)) == inConstructLeftJoins, top)
    }
    assert(countBugHandling(lowerMax, Seq(sum($"s"), collectList($"s"))) == inConstructLeftJoins)
    // A FILTER could only keep or drop the NULL row, and its condition is not evaluated on it.
    assert(countBugHandling(lowerMax, Seq(sum($"s", Some($"s" > 1)))) == inConstructLeftJoins)
    assert(countBugHandling(lowerMax, Seq(count($"s", Some($"s".isNull)))) ==
      inConstructLeftJoins)
    // An uncorrelated subquery in a FILTER too, but not a correlated one, on which
    // constructLeftJoins fails.
    val subqueryRelation = LocalRelation($"e".double)
    val uncorrelatedFilter = ScalarSubquery(subqueryRelation.groupBy()(max($"e").as("x"))) > 0
    assert(countBugHandling(lowerMax, Seq(sum($"s", Some(uncorrelatedFilter)))) ==
      inConstructLeftJoins)
    val correlatedFilter = ScalarSubquery(
      subqueryRelation.where($"e" === $"s").groupBy()(max($"e").as("x"))).isNotNull
    assert(countBugHandling(lowerMax, Seq(sum($"s", Some(correlatedFilter)))) == inDecorrelation)
    // Also through Projects that pass the output of the lower aggregate function on.
    assert(countBugHandling(lowerMax.select($"s".as("t")), Seq(sum($"t"))) ==
      inConstructLeftJoins)
    assert(countBugHandling(lowerMax.select($"s"), Seq(sum($"s"))) == inConstructLeftJoins)
    // A computed column next to them does not matter unless the top aggregate uses it as an
    // argument; a FILTER over it could only keep or drop the NULL row.
    val passAndComputed = lowerMax.select($"s", ($"s" + 1).as("t"))
    assert(countBugHandling(passAndComputed, Seq(sum($"s"))) == inConstructLeftJoins)
    assert(countBugHandling(passAndComputed, Seq(sum($"s", Some($"t" > 1)))) ==
      inConstructLeftJoins)
    assert(countBugHandling(passAndComputed, Seq(sum($"t"))) == inDecorrelation)
    // A lower aggregate with two functions without a default result, and a top aggregate
    // without functions.
    val lowerMaxAndMin =
      testRelation2.where($"a" === $"c").groupBy()(max($"d").as("s"), min($"d").as("n"))
    assert(countBugHandling(lowerMaxAndMin, Seq(sum($"s"), count($"n"))) ==
      inConstructLeftJoins)
    assert(countBugHandling(lowerMax, Seq(Literal(1))) == inConstructLeftJoins)

    // Otherwise the count bug is handled in DecorrelateInnerQuery: a function that counts the
    // NULL row, a function not in the list, an argument other than the output of a
    // lower aggregate function that returns NULL on empty input, another function next to it,
    // a node other than a Project between the aggregates, or another aggregate below.
    Seq(Seq(count(Literal(1))), Seq(StddevSamp($"s").toAggregateExpression()),
        Seq(CollectList($"s", ignoreNulls = false).toAggregateExpression()),
        Seq(CollectSet($"s", ignoreNulls = false).toAggregateExpression()),
        Seq(count(Literal(1), Some($"s".isNull))), Seq(sum($"s" + 1)),
        Seq(sum($"s"), count(Literal(1)))).foreach { top =>
      assert(countBugHandling(lowerMax, top) == inDecorrelation, top)
    }
    // A lower aggregate with a function with a default result is handled in decorrelation, as
    // before, even when the top aggregate does not use it, or when its default result is NULL.
    val lowerMaxAndCount =
      testRelation2.where($"a" === $"c").groupBy()(max($"d").as("s"), count($"d").as("n"))
    assert(countBugHandling(lowerMaxAndCount, Seq(sum($"s"))) == inDecorrelation)
    assert(countBugHandling(lowerMaxAndCount, Seq(sum($"n"))) == inDecorrelation)
    val lowerListAgg = correlated(ListAgg(Cast($"d", StringType)).toAggregateExpression())
    assert(countBugHandling(lowerListAgg, Seq(count($"s"))) == inDecorrelation)
    assert(countBugHandling(correlated(coalesce(sum($"d"), Literal(10.0))), Seq(sum($"s"))) ==
      inDecorrelation)
    assert(countBugHandling(lowerMax.select(($"s" + 1).as("t")), Seq(sum($"t"))) ==
      inDecorrelation)
    assert(countBugHandling(lowerMax.where($"s" > 1), Seq(sum($"s"))) == inDecorrelation)
    val threeLevels = lowerMax.groupBy()(max($"s").as("s"))
    assert(countBugHandling(threeLevels, Seq(sum($"s"))) == inDecorrelation)
    val countBelowProject = correlated(count($"d")).select($"s").groupBy()(max($"s").as("s"))
    assert(countBugHandling(countBelowProject, Seq(sum($"s"))) == inDecorrelation)
    // A grouped top aggregate returns a row for the group of the NULL row, but none for no rows.
    assert(countBugHandling(lowerMax, Seq(collectList($"s")), Seq($"s")) == inDecorrelation)
    // With the legacy behavior, constructLeftJoins handles the shapes above that do not have a
    // lower function with a default result.
    withSQLConf(SQLConf.LEGACY_SCALAR_SUBQUERY_NESTED_AGGREGATE_COUNT_BUG_BEHAVIOR.key -> "true") {
      assert(countBugHandling(lowerMax, Seq(count(Literal(1)))) == inConstructLeftJoins)
      assert(countBugHandling(lowerMax, Seq(sum($"s"))) == inConstructLeftJoins)
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
