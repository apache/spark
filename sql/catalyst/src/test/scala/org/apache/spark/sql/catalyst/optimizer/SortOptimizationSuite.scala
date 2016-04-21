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
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class SortOptimizationSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Operator Optimizations", FixedPoint(100),
        ColumnPruning,
        CollapseProject) :: Nil
  }

  private val testRelation = LocalRelation('a.int, 'b.int)

  test("sort on projection") {
    val query =
      testRelation
        .select(('a * 2).as("eval"), 'b)
        .orderBy(('a * 2).asc, ('a * 2 + 3).asc).analyze
    val optimized = Optimize.execute(query)

    val correctAnswer =
      testRelation
        .select(('a * 2).as("eval"), 'b)
        .orderBy('eval.asc, ('eval + 3).asc).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("sort on aggregation") {
    val query =
      testRelation
        .groupBy('a * 2)(sum('b).as("sum"))
        .orderBy(('a * 2).asc, ('a * 2 + 3).asc).analyze

    val optimized = Optimize.execute(query)

    val correctAnswer =
      testRelation
        .groupBy('a * 2)(('a * 2).as("aggOrder"), sum('b).as("sum"))
        .orderBy('aggOrder.asc, ('aggOrder + 3).asc)
        .select('sum)
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("sort on aggregation: order by aggregate expression") {
    val query =
      testRelation
        .groupBy('a)('a)
        .orderBy(sum('b).asc, (sum('b) + 1).asc).analyze

    val optimized = Optimize.execute(query)

    val correctAnswer =
      testRelation
        .groupBy('a)('a, sum('b).as("aggOrder"))
        .orderBy('aggOrder.asc, ('aggOrder + 1).asc)
        .select('a).analyze

    comparePlans(optimized, correctAnswer)
  }
}
