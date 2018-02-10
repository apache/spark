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
  override val conf = new SQLConf().copy(CASE_SENSITIVE -> true, ORDER_BY_ORDINAL -> false)
  val catalog = new SessionCatalog(new InMemoryCatalog, EmptyFunctionRegistry, conf)
  val analyzer = new Analyzer(catalog, conf)

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
    val optimized = Optimize.execute(analyzer.execute(unnecessaryReordered))
    val correctAnswer = analyzer.execute(orderedPlan.select('a))
    comparePlans(Optimize.execute(optimized), correctAnswer)
  }

  test("do not remove sort if the order is different") {
    val orderedPlan = testRelation.select('a, 'b).orderBy('a.asc, 'b.desc_nullsFirst)
    val reorderedDifferently = orderedPlan.select('a).orderBy('a.asc, 'b.desc)
    val optimized = Optimize.execute(analyzer.execute(reorderedDifferently))
    val correctAnswer = analyzer.execute(reorderedDifferently)
    comparePlans(optimized, correctAnswer)
  }

  test("filters don't affect order") {
    val orderedPlan = testRelation.select('a, 'b).orderBy('a.asc, 'b.desc)
    val filteredAndReordered = orderedPlan.where('a > Literal(10)).orderBy('a.asc, 'b.desc)
    val optimized = Optimize.execute(analyzer.execute(filteredAndReordered))
    val correctAnswer = analyzer.execute(orderedPlan.where('a > Literal(10)))
    comparePlans(optimized, correctAnswer)
  }

  test("limits don't affect order") {
    val orderedPlan = testRelation.select('a, 'b).orderBy('a.asc, 'b.desc)
    val filteredAndReordered = orderedPlan.limit(Literal(10)).orderBy('a.asc, 'b.desc)
    val optimized = Optimize.execute(analyzer.execute(filteredAndReordered))
    val correctAnswer = analyzer.execute(orderedPlan.limit(Literal(10)))
    comparePlans(optimized, correctAnswer)
  }

  test("range is already sorted") {
    val inputPlan = Range(1L, 1000L, 1, 10)
    val orderedPlan = inputPlan.orderBy('id.desc)
    val optimized = Optimize.execute(analyzer.execute(orderedPlan))
    val correctAnswer = analyzer.execute(inputPlan)
    comparePlans(optimized, correctAnswer)
  }

  test("sort should not be removed when there is a node which doesn't guarantee any order") {
    val orderedPlan = testRelation.select('a, 'b).orderBy('a.asc)
    val groupedAndResorted = orderedPlan.groupBy('a)(sum('a)).orderBy('a.asc)
    val optimized = Optimize.execute(analyzer.execute(groupedAndResorted))
    val correctAnswer = analyzer.execute(groupedAndResorted)
    comparePlans(optimized, correctAnswer)
  }
}
