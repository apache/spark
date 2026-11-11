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
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, DeleteAction, DeleteFromTable, DomainJoin, InsertAction, LateralJoin, LocalRelation, LogicalPlan, MergeIntoTable, UpdateTable}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf

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

  test("SPARK-60501: handle count bug in decorrelation for any correlated nested ungrouped " +
      "aggregate") {
    // Returns ScalarSubquery.mayHaveCountBug, and whether DecorrelateInnerQuery handled the count
    // bug, which it does with a left outer domain join, for COUNT(*) over `lower`.
    def countBugHandling(lower: LogicalPlan): (Option[Boolean], Boolean) = {
      val subPlan = lower.groupBy()(count(Literal(1)).as("cnt"))
      val plan = testRelation.select(ScalarSubquery(subPlan).as("sq")).analyze
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
    def correlated(agg: Expression): LogicalPlan =
      testRelation2.where($"a" === $"c").groupBy()(agg.as("s"))
    def uncorrelated(agg: Expression): LogicalPlan =
      testRelation2.groupBy()(agg.as("s")).where($"s" > $"b")

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
    Seq(max($"d"), count($"d")).foreach { agg =>
      assert(countBugHandling(uncorrelated(agg)) == (Some(true), false), agg)
    }
    withSQLConf(SQLConf.LEGACY_SCALAR_SUBQUERY_NESTED_AGGREGATE_COUNT_BUG_BEHAVIOR.key -> "true") {
      assert(countBugHandling(correlated(max($"d"))) == (Some(true), false))
      assert(countBugHandling(correlated(count($"d"))) == (Some(false), true))
      assert(countBugHandling(uncorrelated(count($"d"))) == (Some(false), true))
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
