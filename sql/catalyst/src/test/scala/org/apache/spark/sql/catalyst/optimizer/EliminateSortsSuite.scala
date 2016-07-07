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

import org.apache.spark.sql.catalyst.SimpleCatalystConf
import org.apache.spark.sql.catalyst.analysis.{Analyzer, EmptyFunctionRegistry}
import org.apache.spark.sql.catalyst.catalog.{InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._

class EliminateSortsSuite extends PlanTest {
  val conf = new SimpleCatalystConf(caseSensitiveAnalysis = true, orderByOrdinal = false)
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
}
