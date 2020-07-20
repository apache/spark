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
import org.apache.spark.sql.catalyst.analysis.{EmptyFunctionRegistry, FakeV2SessionCatalog}
import org.apache.spark.sql.catalyst.catalog.{InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{IsNotNull, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.catalyst.plans.{Inner, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.internal.SQLConf

class InferFiltersPredicatePushdownSuite extends PlanTest {

  object OptimizeOriginal extends Optimizer(
    new CatalogManager(
      new SQLConf(),
      FakeV2SessionCatalog,
      new SessionCatalog(new InMemoryCatalog, EmptyFunctionRegistry, new SQLConf()))) {
    override def defaultBatches: Seq[Batch] = super.defaultBatches
  }

  object Optimize extends RuleExecutor[LogicalPlan] {
    val b = Batch("InferAndPushDownFilters", FixedPoint(100),
      PushPredicateThroughJoin,
      ColumnPruning
    )
    val batches =
      b ::
      Batch("infer filter from constraints", FixedPoint(100),
        PushDownPredicates,
        InferFiltersFromConstraints) ::
        Nil
  }
  val  testRelation = LocalRelation('a.int, 'b.int, 'c.int)

  test("SPARK-30876: optimize constraints in 3-way join") {
    val x = testRelation.subquery('x)
    val y = testRelation.subquery('y)
    val z = testRelation.subquery('z)
    val originalQuery = x.join(y).join(z)
      .where(("x.a".attr === "y.b".attr) && ("y.b".attr === "z.c".attr) && ("z.c".attr === 1))
      .groupBy()(Count(Literal("*"))).analyze
    val optimized = Optimize.execute(originalQuery)
    val correctAnswer = x.where('a === 1 && IsNotNull('a)).select('a)
      .join(y.where('b === 1 && IsNotNull('b))
        .select('b), Inner, Some("x.a".attr === "y.b".attr))
      .select('b)
      .join(z.where('c === 1 && IsNotNull('c))
        .select('c), Inner, Some('b === "z.c".attr))
      .select()
      .groupBy()(Count(Literal("*"))).analyze
    comparePlans(optimized, correctAnswer)
  }

}
