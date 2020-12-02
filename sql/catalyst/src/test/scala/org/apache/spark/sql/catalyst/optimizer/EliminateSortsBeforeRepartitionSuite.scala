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

import org.apache.spark.sql.catalyst.analysis.{Analyzer, EmptyFunctionRegistry}
import org.apache.spark.sql.catalyst.catalog.{InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class EliminateSortsBeforeRepartitionSuite extends PlanTest {

  val catalog = new SessionCatalog(new InMemoryCatalog, EmptyFunctionRegistry)
  val analyzer = new Analyzer(catalog)

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int)
  val anotherTestRelation = LocalRelation('d.int, 'e.int)

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Default", FixedPoint(10),
        FoldablePropagation,
        LimitPushDown) ::
      Batch("Eliminate Sorts", Once,
        EliminateSorts) ::
      Batch("Collapse Project", Once,
        CollapseProject) :: Nil
  }

  def repartition(plan: LogicalPlan): LogicalPlan = plan.repartition(10)

  test("sortBy") {
    val plan = testRelation.select('a, 'b).sortBy('a.asc, 'b.desc)
    val optimizedPlan = testRelation.select('a, 'b)
    checkRepartitionCases(plan, optimizedPlan)
  }

  test("sortBy with projection") {
    val plan = testRelation.sortBy('a.asc, 'b.asc).select('a + 1 as "a", 'b + 2 as "b")
    val optimizedPlan = testRelation.select('a + 1 as "a", 'b + 2 as "b")
    checkRepartitionCases(plan, optimizedPlan)
  }

  test("sortBy with projection and filter") {
    val plan = testRelation.sortBy('a.asc, 'b.asc).select('a, 'b).where('a === 10)
    val optimizedPlan = testRelation.select('a, 'b).where('a === 10)
    checkRepartitionCases(plan, optimizedPlan)
  }

  test("sortBy with limit") {
    val plan = testRelation.sortBy('a.asc, 'b.asc).limit(10)
    val optimizedPlan = testRelation.sortBy('a.asc, 'b.asc).limit(10)
    checkRepartitionCases(plan, optimizedPlan)
  }

  test("sortBy with non-deterministic projection") {
    val plan = testRelation.sortBy('a.asc, 'b.asc).select(rand(1), 'a, 'b)
    val optimizedPlan = testRelation.sortBy('a.asc, 'b.asc).select(rand(1), 'a, 'b)
    checkRepartitionCases(plan, optimizedPlan)
  }

  test("orderBy") {
    val plan = testRelation.select('a, 'b).orderBy('a.asc, 'b.asc)
    val optimizedPlan = testRelation.select('a, 'b)
    checkRepartitionCases(plan, optimizedPlan)
  }

  test("orderBy with projection") {
    val plan = testRelation.orderBy('a.asc, 'b.asc).select('a + 1 as "a", 'b + 2 as "b")
    val optimizedPlan = testRelation.select('a + 1 as "a", 'b + 2 as "b")
    checkRepartitionCases(plan, optimizedPlan)
  }

  test("orderBy with projection and filter") {
    val plan = testRelation.orderBy('a.asc, 'b.asc).select('a, 'b).where('a === 10)
    val optimizedPlan = testRelation.select('a, 'b).where('a === 10)
    checkRepartitionCases(plan, optimizedPlan)
  }

  test("orderBy with limit") {
    val plan = testRelation.orderBy('a.asc, 'b.asc).limit(10)
    val optimizedPlan = testRelation.orderBy('a.asc, 'b.asc).limit(10)
    checkRepartitionCases(plan, optimizedPlan)
  }

  test("orderBy with non-deterministic projection") {
    val plan = testRelation.orderBy('a.asc, 'b.asc).select(rand(1), 'a, 'b)
    val optimizedPlan = testRelation.orderBy('a.asc, 'b.asc).select(rand(1), 'a, 'b)
    checkRepartitionCases(plan, optimizedPlan)
  }

  test("additional coalesce and sortBy") {
    val plan = testRelation.sortBy('a.asc, 'b.asc).coalesce(1)
    val optimizedPlan = testRelation.coalesce(1)
    checkRepartitionCases(plan, optimizedPlan)
  }

  test("additional projection, repartition and sortBy") {
    val plan = testRelation.sortBy('a.asc, 'b.asc).repartition(100).select('a + 1 as "a")
    val optimizedPlan = testRelation.repartition(100).select('a + 1 as "a")
    checkRepartitionCases(plan, optimizedPlan)
  }

  test("additional filter, distribute and sortBy") {
    val plan = testRelation.sortBy('a.asc, 'b.asc).distribute('a)(2).where('a === 10)
    val optimizedPlan = testRelation.distribute('a)(2).where('a === 10)
    checkRepartitionCases(plan, optimizedPlan)
  }

  test("join") {
    val plan = testRelation.sortBy('a.asc, 'b.asc).distribute('a)(2).where('a === 10)
    val optimizedPlan = testRelation.distribute('a)(2).where('a === 10)
    val anotherPlan = anotherTestRelation.select('d)
    val joinPlan = plan.join(anotherPlan)
    val optimizedJoinPlan = optimize(joinPlan)
    val correctJoinPlan = analyze(optimizedPlan.join(anotherPlan))
    comparePlans(optimizedJoinPlan, correctJoinPlan)
  }

  test("aggregate") {
    val plan = testRelation.sortBy('a.asc, 'b.asc).distribute('a)(2).where('a === 10)
    val optimizedPlan = testRelation.distribute('a)(2).where('a === 10)
    val aggPlan = plan.groupBy('a)(sum('b))
    val optimizedAggPlan = optimize(aggPlan)
    val correctAggPlan = analyze(optimizedPlan.groupBy('a)(sum('b)))
    comparePlans(optimizedAggPlan, correctAggPlan)
  }

  protected def checkRepartitionCases(plan: LogicalPlan, optimizedPlan: LogicalPlan): Unit = {
    // cannot remove sortBy before repartition without sortBy/orderBy
    val planWithRepartition = repartition(plan)
    val optimizedPlanWithRepartition = optimize(planWithRepartition)
    val correctPlanWithRepartition = analyze(planWithRepartition)
    comparePlans(optimizedPlanWithRepartition, correctPlanWithRepartition)

    // can remove sortBy before repartition with sortBy
    val planWithRepartitionAndSortBy = planWithRepartition.sortBy('a.asc)
    val optimizedPlanWithRepartitionAndSortBy = optimize(planWithRepartitionAndSortBy)
    val correctPlanWithRepartitionAndSortBy = analyze(repartition(optimizedPlan).sortBy('a.asc))
    comparePlans(optimizedPlanWithRepartitionAndSortBy, correctPlanWithRepartitionAndSortBy)

    // can remove sortBy before repartition with orderBy
    val planWithRepartitionAndOrderBy = planWithRepartition.orderBy('a.asc)
    val optimizedPlanWithRepartitionAndOrderBy = optimize(planWithRepartitionAndOrderBy)
    val correctPlanWithRepartitionAndOrderBy = analyze(repartition(optimizedPlan).orderBy('a.asc))
    comparePlans(optimizedPlanWithRepartitionAndOrderBy, correctPlanWithRepartitionAndOrderBy)
  }

  private def analyze(plan: LogicalPlan): LogicalPlan = {
    analyzer.execute(plan)
  }

  private def optimize(plan: LogicalPlan): LogicalPlan = {
    Optimize.execute(analyzer.execute(plan))
  }
}

class EliminateSortsBeforeRepartitionByExprsSuite extends EliminateSortsBeforeRepartitionSuite {
  override def repartition(plan: LogicalPlan): LogicalPlan = plan.distribute('a)(10)

  test("sortBy before repartition with non-deterministic expressions") {
    val plan = testRelation.sortBy('a.asc, 'b.asc).limit(10)
    val planWithRepartition = plan.distribute(rand(1).asc, 'a.asc)(20)
    checkRepartitionCases(plan = planWithRepartition, optimizedPlan = planWithRepartition)
  }

  test("orderBy before repartition with non-deterministic expressions") {
    val plan = testRelation.orderBy('a.asc, 'b.asc).limit(10)
    val planWithRepartition = plan.distribute(rand(1).asc, 'a.asc)(20)
    checkRepartitionCases(plan = planWithRepartition, optimizedPlan = planWithRepartition)
  }
}

class EliminateSortsBeforeCoalesceSuite extends EliminateSortsBeforeRepartitionSuite {
  override def repartition(plan: LogicalPlan): LogicalPlan = plan.coalesce(1)
}
