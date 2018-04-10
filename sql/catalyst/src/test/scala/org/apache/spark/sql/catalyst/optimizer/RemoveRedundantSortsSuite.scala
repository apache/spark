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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.{CASE_SENSITIVE, ORDER_BY_ORDINAL}

class RemoveRedundantSortsSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Remove Redundant Sorts", Once,
        RemoveRedundantSorts) ::
      Batch("Collapse Project", Once,
        CollapseProject) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int)

  test("remove redundant order by") {
    val orderedPlan = testRelation.select('a, 'b).orderBy('a.asc, 'b.desc_nullsFirst)
    val unnecessaryReordered = orderedPlan.select('a).orderBy('a.asc, 'b.desc_nullsFirst)
    val optimized = Optimize.execute(unnecessaryReordered.analyze)
    val correctAnswer = orderedPlan.select('a).analyze
    comparePlans(Optimize.execute(optimized), correctAnswer)
  }

  test("do not remove sort if the order is different") {
    val orderedPlan = testRelation.select('a, 'b).orderBy('a.asc, 'b.desc_nullsFirst)
    val reorderedDifferently = orderedPlan.select('a).orderBy('a.asc, 'b.desc)
    val optimized = Optimize.execute(reorderedDifferently.analyze)
    val correctAnswer = reorderedDifferently.analyze
    comparePlans(optimized, correctAnswer)
  }

  test("filters don't affect order") {
    val orderedPlan = testRelation.select('a, 'b).orderBy('a.asc, 'b.desc)
    val filteredAndReordered = orderedPlan.where('a > Literal(10)).orderBy('a.asc, 'b.desc)
    val optimized = Optimize.execute(filteredAndReordered.analyze)
    val correctAnswer = orderedPlan.where('a > Literal(10)).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("limits don't affect order") {
    val orderedPlan = testRelation.select('a, 'b).orderBy('a.asc, 'b.desc)
    val filteredAndReordered = orderedPlan.limit(Literal(10)).orderBy('a.asc, 'b.desc)
    val optimized = Optimize.execute(filteredAndReordered.analyze)
    val correctAnswer = orderedPlan.limit(Literal(10)).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("range is already sorted") {
    val inputPlan = Range(1L, 1000L, 1, 10)
    val orderedPlan = inputPlan.orderBy('id.asc)
    val optimized = Optimize.execute(orderedPlan.analyze)
    val correctAnswer = inputPlan.analyze
    comparePlans(optimized, correctAnswer)

    val reversedPlan = inputPlan.orderBy('id.desc)
    val reversedOptimized = Optimize.execute(reversedPlan.analyze)
    val reversedCorrectAnswer = reversedPlan.analyze
    comparePlans(reversedOptimized, reversedCorrectAnswer)

    val negativeStepInputPlan = Range(10L, 1L, -1, 10)
    val negativeStepOrderedPlan = negativeStepInputPlan.orderBy('id.desc)
    val negativeStepOptimized = Optimize.execute(negativeStepOrderedPlan.analyze)
    val negativeStepCorrectAnswer = negativeStepInputPlan.analyze
    comparePlans(negativeStepOptimized, negativeStepCorrectAnswer)
  }

  test("sort should not be removed when there is a node which doesn't guarantee any order") {
    val orderedPlan = testRelation.select('a, 'b).orderBy('a.asc)
    val groupedAndResorted = orderedPlan.groupBy('a)(sum('a)).orderBy('a.asc)
    val optimized = Optimize.execute(groupedAndResorted.analyze)
    val correctAnswer = groupedAndResorted.analyze
    comparePlans(optimized, correctAnswer)
  }
}
