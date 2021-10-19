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
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._

class PullOutSortingExpressionsSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("PullOutComplexExpressions", Once,
      PullOutComplexExpressions,
      CollapseProject) :: Nil
  }

  private val testRelation = LocalRelation('a.int, 'b.int, 'c.int)

  test("Pull out sorting expressions") {
    val originalQuery = testRelation.orderBy(('a - 'b).asc)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .select('a, 'b, 'c, ('a - 'b).as("_sortingexpression"))
        .orderBy($"_sortingexpression".asc)
        .select('a, 'b, 'c)
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Pull out sorting expressions: multiple sorting expressions") {
    val originalQuery = testRelation.orderBy(('a - 'b).asc, 'a.asc, rand(1).asc)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .select('a, 'b, 'c, rand(1).as("_nondeterministic"))
        .select('a, 'b, 'c, $"_nondeterministic", ('a - 'b).as("_sortingexpression"))
        .orderBy($"_sortingexpression".asc, 'a.asc, $"_nondeterministic".asc)
        .select('a, 'b, 'c)
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Shouldn't pull out sorting expressions if it is not global sorting") {
    val originalQuery = testRelation.sortBy(('a - 'b).asc)
    comparePlans(Optimize.execute(originalQuery.analyze), originalQuery.analyze)
  }

  test("Shouldn't pull out sorting expressions if there is no complex expressions") {
    val originalQuery = testRelation.orderBy('a.asc, 'b.asc)
    comparePlans(Optimize.execute(originalQuery.analyze), originalQuery.analyze)
  }
}

