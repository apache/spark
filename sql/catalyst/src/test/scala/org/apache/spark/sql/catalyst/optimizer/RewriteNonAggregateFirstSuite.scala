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
import org.apache.spark.sql.catalyst.expressions.aggregate.{First, Max}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._

class RewriteNonAggregateFirstSuite extends PlanTest {
  val testRelation: LocalRelation = LocalRelation($"a".string, $"b".string)

  private def checkRewrite(rewrite: LogicalPlan): Unit = rewrite match {
    case Aggregate(_, _, GlobalLimit(_, _)) =>
    case _ => fail(s"Plan is not rewritten:\n$rewrite")
  }

  test("single FIRST aggregate and no group by") {
    val input = testRelation.select(
      First($"a", ignoreNulls = false).toAggregateExpression()).analyze
    val rewrite = RewriteNonAggregateFirst(input.analyze)
    checkRewrite(rewrite)
  }

  test("multiple FIRST aggregates and no group by") {
    val input = testRelation.select(
      First($"a", ignoreNulls = false).toAggregateExpression(),
      First($"b", ignoreNulls = false).toAggregateExpression()).analyze
    val rewrite = RewriteNonAggregateFirst(input.analyze)
    checkRewrite(rewrite)
  }

  test("ignoreNulls set to true blocks rewrite") {
    val input = testRelation.select(
      First($"a", ignoreNulls = false).toAggregateExpression(),
      First($"b", ignoreNulls = true).toAggregateExpression()).analyze
    val rewrite = RewriteNonAggregateFirst(input.analyze)
    comparePlans(input, rewrite)
  }

  test("FIRST aggregate with group by") {
    val input = testRelation
      .groupBy($"a")(First($"a", ignoreNulls = false).toAggregateExpression())
      .analyze
    val rewrite = RewriteNonAggregateFirst(input)
    comparePlans(input, rewrite)
  }

  test("mixed aggregates with group by") {
    val input = testRelation
      .groupBy('a)(
        First($"a", ignoreNulls = false).toAggregateExpression().as('agg1),
        Max($"b").toAggregateExpression().as('agg2))
      .analyze
    val rewrite = RewriteNonAggregateFirst(input)
    comparePlans(input, rewrite)
  }

  test("mixed aggregates without group by") {
    val input = testRelation
      .select(
        First($"a", ignoreNulls = false).toAggregateExpression().as('agg1),
        Max($"b").toAggregateExpression().as('agg2))
      .analyze
    val rewrite = RewriteNonAggregateFirst(input)
    comparePlans(input, rewrite)
  }
}
