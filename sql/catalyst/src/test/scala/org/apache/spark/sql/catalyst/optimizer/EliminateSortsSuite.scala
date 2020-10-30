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

class EliminateSortsSuite extends PlanTest {
  override val conf = new SQLConf().copy(CASE_SENSITIVE -> true, ORDER_BY_ORDINAL -> false)
  val catalog = new SessionCatalog(new InMemoryCatalog, EmptyFunctionRegistry, conf)
  val analyzer = new Analyzer(catalog, conf)

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Eliminate Sorts", FixedPoint(10),
        FoldablePropagation,
        EliminateSorts) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int)

  test("Empty order by clause") {
    val x = testRelation

    val query = x.orderBy()
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = x.analyze

    comparePlans(optimized, correctAnswer)
  }

  test("All the SortOrder are no-op") {
    val x = testRelation

    val query = x.orderBy(SortOrder(3, Ascending), SortOrder(-1, Ascending))
    val optimized = Optimize.execute(analyzer.execute(query))
    val correctAnswer = analyzer.execute(x)

    comparePlans(optimized, correctAnswer)
  }

  test("Partial order-by clauses contain no-op SortOrder") {
    val x = testRelation

    val query = x.orderBy(SortOrder(3, Ascending), 'a.asc)
    val optimized = Optimize.execute(analyzer.execute(query))
    val correctAnswer = analyzer.execute(x.orderBy('a.asc))

    comparePlans(optimized, correctAnswer)
  }

  test("Remove no-op alias") {
    val x = testRelation

    val query = x.select('a.as('x), Year(CurrentDate()).as('y), 'b)
      .orderBy('x.asc, 'y.asc, 'b.desc)
    val optimized = Optimize.execute(analyzer.execute(query))
    val correctAnswer = analyzer.execute(
      x.select('a.as('x), Year(CurrentDate()).as('y), 'b).orderBy('x.asc, 'b.desc))

    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-32318: should not remove orderBy in distribute statement") {
    val projectPlan = testRelation.select('a, 'b)
    val orderByPlan = projectPlan.orderBy('b.desc)
    val distributedPlan = orderByPlan.distribute('a)(1)
    val optimized = Optimize.execute(distributedPlan.analyze)
    val correctAnswer = distributedPlan.analyze
    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-33183: remove consecutive no-op sorts") {
    val plan = testRelation.orderBy().orderBy().orderBy()
    val optimized = Optimize.execute(plan.analyze)
    val correctAnswer = testRelation.analyze
    comparePlans(optimized, correctAnswer)
  }
}
