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
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class OptimizeOneRowPlanSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Replace Operators", Once, ReplaceDistinctWithAggregate) ::
      Batch("Eliminate Sorts", Once, EliminateSorts) ::
      Batch("Optimize One Row Plan", FixedPoint(10), OptimizeOneRowPlan) :: Nil
  }

  private val t1 = LocalRelation.fromExternalRows(Seq($"a".int), data = Seq(Row(1)))
  private val t2 = LocalRelation.fromExternalRows(Seq($"a".int), data = Seq(Row(1), Row(2)))

  test("SPARK-35906: Remove order by if the maximum number of rows less than or equal to 1") {
    comparePlans(
      Optimize.execute(t2.groupBy()(count(1).as("cnt")).orderBy($"cnt".asc)).analyze,
      t2.groupBy()(count(1).as("cnt")).analyze)

    comparePlans(
      Optimize.execute(t2.limit(Literal(1)).orderBy($"a".asc).orderBy($"a".asc)).analyze,
      t2.limit(Literal(1)).analyze)
  }

  test("Remove sort") {
    // remove local sort
    val plan1 = LocalLimit(0, t1).union(LocalLimit(0, t2)).sortBy($"a".desc).analyze
    val expected = LocalLimit(0, t1).union(LocalLimit(0, t2)).analyze
    comparePlans(Optimize.execute(plan1), expected)

    // do not remove
    val plan2 = t2.orderBy($"a".desc).analyze
    comparePlans(Optimize.execute(plan2), plan2)

    val plan3 = t2.sortBy($"a".desc).analyze
    comparePlans(Optimize.execute(plan3), plan3)
  }

  test("Convert group only aggregate to project") {
    val plan1 = t1.groupBy($"a")($"a").analyze
    comparePlans(Optimize.execute(plan1), t1.select($"a").analyze)

    val plan2 = t1.groupBy($"a" + 1)($"a" + 1).analyze
    comparePlans(Optimize.execute(plan2), t1.select($"a" + 1).analyze)

    // do not remove
    val plan3 = t2.groupBy($"a")($"a").analyze
    comparePlans(Optimize.execute(plan3), plan3)

    val plan4 = t1.groupBy($"a")(sum($"a")).analyze
    comparePlans(Optimize.execute(plan4), plan4)

    val plan5 = t1.groupBy()(sum($"a")).analyze
    comparePlans(Optimize.execute(plan5), plan5)
  }

  test("Remove distinct in aggregate expression") {
    val plan1 = t1.groupBy($"a")(sumDistinct($"a").as("s")).analyze
    val expected1 = t1.groupBy($"a")(sum($"a").as("s")).analyze
    comparePlans(Optimize.execute(plan1), expected1)

    val plan2 = t1.groupBy()(sumDistinct($"a").as("s")).analyze
    val expected2 = t1.groupBy()(sum($"a").as("s")).analyze
    comparePlans(Optimize.execute(plan2), expected2)

    // do not remove
    val plan3 = t2.groupBy($"a")(sumDistinct($"a").as("s")).analyze
    comparePlans(Optimize.execute(plan3), plan3)
  }

  test("Remove in complex case") {
    val plan1 = t1.groupBy($"a")($"a").orderBy($"a".asc).analyze
    val expected1 = t1.select($"a").analyze
    comparePlans(Optimize.execute(plan1), expected1)

    val plan2 = t1.groupBy($"a")(sumDistinct($"a").as("s")).orderBy($"s".asc).analyze
    val expected2 = t1.groupBy($"a")(sum($"a").as("s")).analyze
    comparePlans(Optimize.execute(plan2), expected2)
  }
}
