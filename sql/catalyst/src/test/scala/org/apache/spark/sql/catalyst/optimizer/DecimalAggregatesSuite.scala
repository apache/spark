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

  val testRelation = LocalRelation(Symbol("a").decimal(2, 1), Symbol("b").decimal(12, 1))

  test("Decimal Sum Aggregation: Optimized") {
    val originalQuery = testRelation.select(sum(Symbol("a")))
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .select(MakeDecimal(sum(UnscaledValue(Symbol("a"))), 12, 1).as("sum(a)")).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Decimal Sum Aggregation: Not Optimized") {
    val originalQuery = testRelation.select(sum(Symbol("b")))
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = originalQuery.analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Decimal Average Aggregation: Optimized") {
    val originalQuery = testRelation.select(avg(Symbol("a")))
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .select((avg(UnscaledValue(Symbol("a"))) / 10.0).cast(DecimalType(6, 5)).as("avg(a)")).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Decimal Average Aggregation: Not Optimized") {
    val originalQuery = testRelation.select(avg(Symbol("b")))
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = originalQuery.analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Decimal Sum Aggregation over Window: Optimized") {
    val spec = windowSpec(Seq(Symbol("a")), Nil, UnspecifiedFrame)
    val originalQuery = testRelation.select(windowExpr(sum(Symbol("a")), spec).as(Symbol("sum_a")))
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .select(Symbol("a"))
      .window(
        Seq(MakeDecimal(
          windowExpr(sum(UnscaledValue(Symbol("a"))), spec), 12, 1).as(Symbol("sum_a"))),
        Seq(Symbol("a")),
        Nil)
      .select(Symbol("a"), Symbol("sum_a"), Symbol("sum_a"))
      .select(Symbol("sum_a"))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Decimal Sum Aggregation over Window: Not Optimized") {
    val spec = windowSpec(Symbol("b") :: Nil, Nil, UnspecifiedFrame)
    val originalQuery = testRelation.select(windowExpr(sum(Symbol("b")), spec))
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = originalQuery.analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Decimal Average Aggregation over Window: Optimized") {
    val spec = windowSpec(Seq(Symbol("a")), Nil, UnspecifiedFrame)
    val originalQuery = testRelation.select(windowExpr(avg(Symbol("a")), spec).as(Symbol("avg_a")))
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .select(Symbol("a"))
      .window(
        Seq((windowExpr(avg(UnscaledValue(Symbol("a"))), spec) / 10.0)
          .cast(DecimalType(6, 5)).as(Symbol("avg_a"))),
        Seq(Symbol("a")),
        Nil)
      .select(Symbol("a"), Symbol("avg_a"), Symbol("avg_a"))
      .select(Symbol("avg_a"))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Decimal Average Aggregation over Window: Not Optimized") {
    val spec = windowSpec(Symbol("b") :: Nil, Nil, UnspecifiedFrame)
    val originalQuery = testRelation.select(windowExpr(avg(Symbol("b")), spec))
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = originalQuery.analyze

    comparePlans(optimized, correctAnswer)
  }
}
