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
import org.apache.spark.sql.catalyst.plans.{LeftOuter, PlanTest, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class RemoveSortInSubquerySuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Limit PushDown", FixedPoint(10), LimitPushDown) ::
        Batch("Remove Redundant Sorts", Once, RemoveSortInSubquery) :: Nil
  }

  object PushDownOptimizer extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Limit PushDown", FixedPoint(10), LimitPushDown) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int)
  val testRelationB = LocalRelation('d.int)

  test("remove orderBy in groupBy subquery with count aggs") {
    val projectPlan = testRelation.select('a, 'b)
    val unnecessaryOrderByPlan = projectPlan.orderBy('a.asc, 'b.desc)
    val groupByPlan = unnecessaryOrderByPlan.groupBy('a)(count(1))
    val optimized = Optimize.execute(groupByPlan.analyze)
    val correctAnswer = projectPlan.groupBy('a)(count(1)).analyze
    comparePlans(Optimize.execute(optimized), correctAnswer)
  }

  test("remove orderBy in groupBy subquery with sum aggs") {
    val projectPlan = testRelation.select('a, 'b)
    val unnecessaryOrderByPlan = projectPlan.orderBy('a.asc, 'b.desc)
    val groupByPlan = unnecessaryOrderByPlan.groupBy('a)(sum('a))
    val optimized = Optimize.execute(groupByPlan.analyze)
    val correctAnswer = projectPlan.groupBy('a)(sum('a)).analyze
    comparePlans(Optimize.execute(optimized), correctAnswer)
  }

  test("remove orderBy in groupBy subquery with first aggs") {
    val projectPlan = testRelation.select('a, 'b)
    val orderByPlan = projectPlan.orderBy('a.asc, 'b.desc)
    val groupByPlan = orderByPlan.groupBy('a)(first('a))
    val optimized = Optimize.execute(groupByPlan.analyze)
    val correctAnswer = groupByPlan.analyze
    comparePlans(Optimize.execute(optimized), correctAnswer)
  }

  test("remove orderBy in groupBy subquery with first and count aggs") {
    val projectPlan = testRelation.select('a, 'b)
    val orderByPlan = projectPlan.orderBy('a.asc, 'b.desc)
    val groupByPlan = orderByPlan.groupBy('a)(first('a), count(1))
    val optimized = Optimize.execute(groupByPlan.analyze)
    val correctAnswer = groupByPlan.analyze
    comparePlans(Optimize.execute(optimized), correctAnswer)
  }

  test("should not remove orderBy with limit in groupBy subquery") {
    val projectPlan = testRelation.select('a, 'b)
    val orderByPlan = projectPlan.orderBy('a.asc, 'b.desc).limit(10)
    val groupByPlan = orderByPlan.groupBy('a)(count(1))
    val optimized = Optimize.execute(groupByPlan.analyze)
    val correctAnswer = groupByPlan.analyze
    comparePlans(Optimize.execute(optimized), correctAnswer)
  }

  test("remove orderBy in join subquery") {
    val projectPlan = testRelation.select('a, 'b)
    val unnecessaryOrderByPlan = projectPlan.orderBy('a.asc, 'b.desc)
    val projectPlanB = testRelationB.select('d)
    val joinPlan = unnecessaryOrderByPlan.join(projectPlanB).select('a, 'd)
    val optimized = Optimize.execute(joinPlan.analyze)
    val correctAnswer = projectPlan.join(projectPlanB).select('a, 'd).analyze
    comparePlans(Optimize.execute(optimized), correctAnswer)
  }

  test("should not remove orderBy with limit in join subquery") {
    val projectPlan = testRelation.select('a, 'b)
    val orderByPlan = projectPlan.orderBy('a.asc, 'b.desc).limit(10)
    val projectPlanB = testRelationB.select('d)
    val joinPlan = orderByPlan.join(projectPlanB).select('a, 'd)
    val optimized = Optimize.execute(joinPlan.analyze)
    val correctAnswer = joinPlan.analyze
    comparePlans(Optimize.execute(optimized), correctAnswer)
  }

  test("should not remove orderBy in left join subquery if there is an outer limit") {
    val projectPlan = testRelation.select('a, 'b)
    val orderByPlan = projectPlan.orderBy('a.asc, 'b.desc)
    val projectPlanB = testRelationB.select('d)
    val joinPlan = orderByPlan
      .join(projectPlanB, LeftOuter)
      .limit(10)
    val optimized = Optimize.execute(joinPlan.analyze)
    val correctAnswer = PushDownOptimizer.execute(joinPlan.analyze)
    comparePlans(Optimize.execute(optimized), correctAnswer)
  }

  test("remove orderBy in right join subquery event if there is an outer limit") {
    val projectPlan = testRelation.select('a, 'b)
    val orderByPlan = projectPlan.orderBy('a.asc, 'b.desc)
    val projectPlanB = testRelationB.select('d)
    val joinPlan = orderByPlan
      .join(projectPlanB, RightOuter)
      .limit(10)
    val optimized = Optimize.execute(joinPlan.analyze)
    val correctAnswer = projectPlan
      .join(projectPlanB, RightOuter)
      .limit(10)
    comparePlans(Optimize.execute(optimized), correctAnswer)
  }
}
