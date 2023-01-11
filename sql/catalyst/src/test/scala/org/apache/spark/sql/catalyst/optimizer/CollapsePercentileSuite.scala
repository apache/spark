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
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.aggregate.Percentile
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.{ArrayType, DoubleType}

class CollapsePercentileSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("CollapsePercentile", FixedPoint(10), CollapsePercentiles, CollapseProject) :: Nil
  }

  val testRelation = LocalRelation($"a".double, $"b".double, $"c".string)
  val a = testRelation.output(0)
  val b = testRelation.output(1)

  test("collapse percentiles if possible") {
    val query =
      testRelation.select(
        max(a).as("max_a"),
        Percentile(b, 0.3d, 1L).toAggregateExpression().as("p1"),
        (Percentile(b, 0.4d, 1L).toAggregateExpression() +
          Percentile(b, 0.5d, 1L).toAggregateExpression()).as("p2"),
        Percentile(b, 0.6d, 1L).toAggregateExpression().as("p3"))
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer =
      testRelation
        .select(
          max(a).as("max_a"),
          (Percentile(b, 0.4d, 1L).toAggregateExpression() +
            Percentile(b, 0.5d, 1L).toAggregateExpression()).as("p2"),
          Percentile(b, Literal.create(Seq(0.3d, 0.6d), ArrayType(DoubleType, false)), 1L)
            .toAggregateExpression()
            .as("_combined_percentile_0"))
        .select(
          $"max_a",
          $"_combined_percentile_0".getItem(0).as("p1"),
          $"p2",
          $"_combined_percentile_0".getItem(1).as("p3"))
        .analyze
    comparePlans(optimized, correctAnswer)
  }
}
