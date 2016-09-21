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
import org.apache.spark.sql.catalyst.expressions.{If, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.{CollectSet, Count}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Expand, LocalRelation, LogicalPlan}
import org.apache.spark.sql.types.{IntegerType, StringType}

class RewriteDistinctAggregatesSuite extends PlanTest {
  val conf = SimpleCatalystConf(caseSensitiveAnalysis = false, groupByOrdinal = false)
  val catalog = new SessionCatalog(new InMemoryCatalog, EmptyFunctionRegistry, conf)
  val analyzer = new Analyzer(catalog, conf)

  val nullInt = Literal(null, IntegerType)
  val nullString = Literal(null, StringType)
  val testRelation = LocalRelation('a.string, 'b.string, 'c.string, 'd.string, 'e.int)

  private def checkRewrite(rewrite: LogicalPlan): Unit = rewrite match {
    case Aggregate(_, _, Aggregate(_, _, _: Expand)) =>
    case _ => fail(s"Plan is not rewritten:\n$rewrite")
  }

  test("single distinct group") {
    val input = testRelation
      .groupBy('a)(countDistinct('e))
      .analyze
    val rewrite = RewriteDistinctAggregates(input)
    comparePlans(input, rewrite)
  }

  test("single distinct group with non-partial aggregates") {
    val input = testRelation
      .groupBy('a, 'd)(
        countDistinct('e, 'c).as('agg1),
        CollectSet('b).toAggregateExpression().as('agg2))
      .analyze
    checkRewrite(RewriteDistinctAggregates(input))
  }

  test("multiple distinct groups") {
    val input = testRelation
      .groupBy('a)(countDistinct('b, 'c), countDistinct('d), sum('e))
      .analyze
    checkRewrite(RewriteDistinctAggregates(input))
  }

  test("multiple distinct groups without non-distinct aggregates") {
    val input = testRelation
      .groupBy('a)(countDistinct('b, 'c), countDistinct('d))
      .analyze
    checkRewrite(RewriteDistinctAggregates(input))
  }
}
