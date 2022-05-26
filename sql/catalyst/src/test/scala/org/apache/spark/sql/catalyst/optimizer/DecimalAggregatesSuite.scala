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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.DecimalType

class DecimalAggregatesSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("Decimal Optimizations", FixedPoint(100),
      DecimalAggregates) :: Nil
  }

  val testRelation = LocalRelation($"a".decimal(2, 1), $"b".decimal(12, 1))

  test("Decimal Sum Aggregation: Optimized") {
    val originalQuery = testRelation.select(sum($"a"))
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .select(MakeDecimal(sum(UnscaledValue($"a")), 12, 1).as("sum(a)")).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Decimal Sum Aggregation: Not Optimized") {
    val originalQuery = testRelation.select(sum($"b"))
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = originalQuery.analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Decimal Average Aggregation: Optimized") {
    val originalQuery = testRelation.select(avg($"a"))
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .select((avg(UnscaledValue($"a")) / 10.0).cast(DecimalType(6, 5)).as("avg(a)")).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Decimal Average Aggregation: Not Optimized") {
    val originalQuery = testRelation.select(avg($"b"))
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = originalQuery.analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Decimal Sum Aggregation over Window: Optimized") {
    val spec = windowSpec(Seq($"a"), Nil, UnspecifiedFrame)
    val originalQuery = testRelation.select(windowExpr(sum($"a"), spec).as(Symbol("sum_a")))
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .select($"a")
      .window(
        Seq(MakeDecimal(windowExpr(sum(UnscaledValue($"a")), spec), 12, 1).as(Symbol("sum_a"))),
        Seq($"a"),
        Nil)
      .select($"a", $"sum_a", $"sum_a")
      .select($"sum_a")
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Decimal Sum Aggregation over Window: Not Optimized") {
    val spec = windowSpec($"b" :: Nil, Nil, UnspecifiedFrame)
    val originalQuery = testRelation.select(windowExpr(sum($"b"), spec))
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = originalQuery.analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Decimal Average Aggregation over Window: Optimized") {
    val spec = windowSpec(Seq($"a"), Nil, UnspecifiedFrame)
    val originalQuery = testRelation.select(windowExpr(avg($"a"), spec).as(Symbol("avg_a")))
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .select($"a")
      .window(
        Seq((windowExpr(avg(UnscaledValue($"a")), spec) / 10.0).cast(DecimalType(6, 5))
          .as(Symbol("avg_a"))),
        Seq($"a"),
        Nil)
      .select($"a", $"avg_a", $"avg_a")
      .select($"avg_a")
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Decimal Average Aggregation over Window: Not Optimized") {
    val spec = windowSpec($"b" :: Nil, Nil, UnspecifiedFrame)
    val originalQuery = testRelation.select(windowExpr(avg($"b"), spec))
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = originalQuery.analyze

    comparePlans(optimized, correctAnswer)
  }
}
