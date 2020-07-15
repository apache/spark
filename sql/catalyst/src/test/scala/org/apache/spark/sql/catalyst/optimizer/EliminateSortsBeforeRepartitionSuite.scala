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

  val catalog = new SessionCatalog(new InMemoryCatalog, EmptyFunctionRegistry, conf)
  val analyzer = new Analyzer(catalog, conf)
  val testRelation = LocalRelation('a.int, 'b.int, 'c.int)

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
  def isOptimized: Boolean = true

  test("sortBy") {
    val plan = testRelation.select('a, 'b).sortBy('a.asc, 'b.desc)
    val planWithRepartition = repartition(plan)
    val optimizedPlan = Optimize.execute(analyzer.execute(planWithRepartition))
    val correctPlan = if (isOptimized) {
      repartition(testRelation.select('a, 'b))
    } else {
      planWithRepartition
    }
    comparePlans(optimizedPlan, analyzer.execute(correctPlan))
  }

  test("sortBy with projection") {
    val plan = testRelation.select('a, 'b)
      .sortBy('a.asc, 'b.asc)
      .select('a + 1 as "a", 'b + 2 as "b")
    val planWithRepartition = repartition(plan)
    val optimizedPlan = Optimize.execute(analyzer.execute(planWithRepartition))
    val correctPlan = if (isOptimized) {
      repartition(testRelation.select('a + 1 as "a", 'b + 2 as "b"))
    } else {
      planWithRepartition
    }
    comparePlans(optimizedPlan, analyzer.execute(correctPlan))
  }

  test("sortBy with projection and filter") {
    val plan = testRelation.sortBy('a.asc, 'b.asc)
      .select('a, 'b)
      .where('a === 10)
    val planWithRepartition = repartition(plan)
    val optimizedPlan = Optimize.execute(analyzer.execute(planWithRepartition))
    val correctPlan = if (isOptimized) {
      repartition(testRelation.select('a, 'b).where('a === 10))
    } else {
      planWithRepartition
    }
    comparePlans(optimizedPlan, analyzer.execute(correctPlan))
  }

  test("sortBy with limit") {
    val plan = testRelation.sortBy('a.asc, 'b.asc).limit(10)
    val planWithRepartition = repartition(plan)
    val optimizedPlan = Optimize.execute(analyzer.execute(planWithRepartition))
    comparePlans(optimizedPlan, analyzer.execute(planWithRepartition))
  }

  test("sortBy with non-deterministic projection") {
    val plan = testRelation.sortBy('a.asc, 'b.asc).select(rand(1), 'a, 'b)
    val planWithRepartition = repartition(plan)
    val optimizedPlan = Optimize.execute(analyzer.execute(planWithRepartition))
    comparePlans(optimizedPlan, analyzer.execute(planWithRepartition))
  }

  test("orderBy") {
    val plan = testRelation.select('a, 'b).orderBy('a.asc, 'b.asc)
    val planWithRepartition = repartition(plan)
    val optimizedPlan = Optimize.execute(analyzer.execute(planWithRepartition))
    val correctPlan = if (isOptimized) {
      repartition(testRelation.select('a, 'b))
    } else {
      planWithRepartition
    }
    comparePlans(optimizedPlan, analyzer.execute(correctPlan))
  }

  test("orderBy with projection") {
    val plan = testRelation.select('a, 'b)
      .orderBy('a.asc, 'b.asc)
      .select('a + 1 as "a", 'b + 2 as "b")
    val planWithRepartition = repartition(plan)
    val optimizedPlan = Optimize.execute(analyzer.execute(planWithRepartition))
    val correctPlan = if (isOptimized) {
      repartition(testRelation.select('a + 1 as "a", 'b + 2 as "b"))
    } else {
      planWithRepartition
    }
    comparePlans(optimizedPlan, analyzer.execute(correctPlan))
  }

  test("orderBy with projection and filter") {
    val plan = testRelation.orderBy('a.asc, 'b.asc)
      .select('a, 'b)
      .where('a === 10)
    val planWithRepartition = repartition(plan)
    val optimizedPlan = Optimize.execute(analyzer.execute(planWithRepartition))
    val correctPlan = if (isOptimized) {
      repartition(testRelation.select('a, 'b).where('a === 10))
    } else {
      planWithRepartition
    }
    comparePlans(optimizedPlan, analyzer.execute(correctPlan))
  }

  test("orderBy with limit") {
    val plan = testRelation.orderBy('a.asc, 'b.asc).limit(10)
    val planWithRepartition = repartition(plan)
    val optimizedPlan = Optimize.execute(analyzer.execute(planWithRepartition))
    comparePlans(optimizedPlan, analyzer.execute(planWithRepartition))
  }

  test("orderBy with non-deterministic projection") {
    val plan = testRelation.orderBy('a.asc, 'b.asc).select(rand(1), 'a, 'b)
    val planWithRepartition = repartition(plan)
    val optimizedPlan = Optimize.execute(analyzer.execute(planWithRepartition))
    comparePlans(optimizedPlan, analyzer.execute(planWithRepartition))
  }
}

class EliminateSortsBeforeRepartitionByExprsSuite extends EliminateSortsBeforeRepartitionSuite {
  override def repartition(plan: LogicalPlan): LogicalPlan = plan.distribute('a, 'b)(10)
  override def isOptimized: Boolean = true

  test("sortBy before repartition with non-deterministic expressions") {
    val plan = testRelation.sortBy('a.asc, 'b.asc).limit(10)
    val planWithRepartition = plan.distribute(rand(1).asc, 'a.asc)(20)
    val optimizedPlan = Optimize.execute(analyzer.execute(planWithRepartition))
    comparePlans(optimizedPlan, analyzer.execute(planWithRepartition))
  }

  test("orderBy before repartition with non-deterministic expressions") {
    val plan = testRelation.orderBy('a.asc, 'b.asc).limit(10)
    val planWithRepartition = plan.distribute(rand(1).asc, 'a.asc)(20)
    val optimizedPlan = Optimize.execute(analyzer.execute(planWithRepartition))
    comparePlans(optimizedPlan, analyzer.execute(planWithRepartition))
  }
}

class EliminateSortsBeforeCoalesceSuite extends EliminateSortsBeforeRepartitionSuite {
  override def repartition(plan: LogicalPlan): LogicalPlan = plan.coalesce(1)
  override def isOptimized: Boolean = false
}
