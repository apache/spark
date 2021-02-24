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

  val testRelation = LocalRelation("a".attr.decimal(2, 1), "b".attr.decimal(12, 1))

  test("Decimal Sum Aggregation: Optimized") {
    val originalQuery = testRelation.select(sum("a".attr))
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .select(MakeDecimal(sum(UnscaledValue("a".attr)), 12, 1).as("sum(a)")).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Decimal Sum Aggregation: Not Optimized") {
    val originalQuery = testRelation.select(sum("b".attr))
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = originalQuery.analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Decimal Average Aggregation: Optimized") {
    val originalQuery = testRelation.select(avg("a".attr))
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .select((avg(UnscaledValue("a".attr)) / 10.0).cast(DecimalType(6, 5)).as("avg(a)")).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Decimal Average Aggregation: Not Optimized") {
    val originalQuery = testRelation.select(avg("b".attr))
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = originalQuery.analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Decimal Sum Aggregation over Window: Optimized") {
    val spec = windowSpec(Seq("a".attr), Nil, UnspecifiedFrame)
    val originalQuery = testRelation.select(windowExpr(sum("a".attr), spec).as("sum_a"))
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .select("a".attr)
      .window(
        Seq(MakeDecimal(windowExpr(sum(UnscaledValue("a".attr)), spec), 12, 1).as("sum_a")),
        Seq("a".attr),
        Nil)
      .select("a".attr, "sum_a".attr, "sum_a".attr)
      .select("sum_a".attr)
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Decimal Sum Aggregation over Window: Not Optimized") {
    val spec = windowSpec("b".attr :: Nil, Nil, UnspecifiedFrame)
    val originalQuery = testRelation.select(windowExpr(sum("b".attr), spec))
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = originalQuery.analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Decimal Average Aggregation over Window: Optimized") {
    val spec = windowSpec(Seq("a".attr), Nil, UnspecifiedFrame)
    val originalQuery = testRelation.select(windowExpr(avg("a".attr), spec).as("avg_a"))
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .select("a".attr)
      .window(
        Seq((windowExpr(
          avg(UnscaledValue("a".attr)), spec) / 10.0).cast(DecimalType(6, 5)).as("avg_a")),
        Seq("a".attr),
        Nil)
      .select("a".attr, "avg_a".attr, "avg_a".attr)
      .select("avg_a".attr)
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Decimal Average Aggregation over Window: Not Optimized") {
    val spec = windowSpec("b".attr :: Nil, Nil, UnspecifiedFrame)
    val originalQuery = testRelation.select(windowExpr(avg("b".attr), spec))
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = originalQuery.analyze

    comparePlans(optimized, correctAnswer)
  }
}
