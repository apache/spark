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
import org.apache.spark.sql.catalyst.expressions.aggregate.First
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class OptimizeWindowFunctionsSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("OptimizeWindowFunctions", FixedPoint(10),
        OptimizeWindowFunctions) :: Nil
  }

  val testRelation = LocalRelation($"a".double, $"b".double, $"c".string)
  val a = testRelation.output(0)
  val b = testRelation.output(1)
  val c = testRelation.output(2)

  test("replace first by nth_value if frame is UNBOUNDED PRECEDING AND CURRENT ROW") {
    val inputPlan = testRelation.select(
      WindowExpression(
        First(a, false).toAggregateExpression(),
        WindowSpecDefinition(b :: Nil, c.asc :: Nil,
          SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))))
    val correctAnswer = testRelation.select(
      WindowExpression(
        NthValue(a, Literal(1), false),
        WindowSpecDefinition(b :: Nil, c.asc :: Nil,
          SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))))

    val optimized = Optimize.execute(inputPlan)
    assert(optimized == correctAnswer)
  }

  test("replace first by nth_value if frame is UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING") {
    val inputPlan = testRelation.select(
      WindowExpression(
        First(a, false).toAggregateExpression(),
        WindowSpecDefinition(b :: Nil, c.asc :: Nil,
          SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing))))
    val correctAnswer = testRelation.select(
      WindowExpression(
        NthValue(a, Literal(1), false),
        WindowSpecDefinition(b :: Nil, c.asc :: Nil,
          SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing))))

    val optimized = Optimize.execute(inputPlan)
    assert(optimized == correctAnswer)
  }

  test("can't replace first by nth_value if frame is not suitable") {
    val inputPlan = testRelation.select(
      WindowExpression(
        First(a, false).toAggregateExpression(),
        WindowSpecDefinition(b :: Nil, c.asc :: Nil,
          SpecifiedWindowFrame(RowFrame, Literal(1), CurrentRow))))

    val optimized = Optimize.execute(inputPlan)
    assert(optimized == inputPlan)
  }

  test("can't replace first by nth_value if the window frame type is range") {
    val inputPlan = testRelation.select(
      WindowExpression(
        First(a, false).toAggregateExpression(),
        WindowSpecDefinition(b :: Nil, c.asc :: Nil,
          SpecifiedWindowFrame(RangeFrame, UnboundedPreceding, CurrentRow))))

    val optimized = Optimize.execute(inputPlan)
    assert(optimized == inputPlan)
  }

  test("can't replace first by nth_value if the window frame isn't ordered") {
    val inputPlan = testRelation.select(
      WindowExpression(
        First(a, false).toAggregateExpression(),
        WindowSpecDefinition(b :: Nil, Nil,
          SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))))

    val optimized = Optimize.execute(inputPlan)
    assert(optimized == inputPlan)
  }
}
