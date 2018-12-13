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

import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.Literal.{FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._

class BinaryComparisonSimplificationSuite extends PlanTest with PredicateHelper {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("AnalysisNodes", Once,
        EliminateSubqueryAliases) ::
      Batch("Constant Folding", FixedPoint(50),
        NullPropagation,
        ConstantFolding,
        BooleanSimplification,
        SimplifyBinaryComparison,
        PruneFilters) :: Nil
  }

  val nullableRelation = LocalRelation('a.int.withNullability(true))
  val nonNullableRelation = LocalRelation('a.int.withNullability(false))

  test("Preserve nullable exprs in general") {
    for (e <- Seq('a === 'a, 'a <= 'a, 'a >= 'a, 'a < 'a, 'a > 'a)) {
      val plan = nullableRelation.where(e).analyze
      val actual = Optimize.execute(plan)
      val correctAnswer = plan
      comparePlans(actual, correctAnswer)
    }
  }

  test("Preserve non-deterministic exprs") {
    val plan = nonNullableRelation
      .where(Rand(0) === Rand(0) && Rand(1) <=> Rand(1)).analyze
    val actual = Optimize.execute(plan)
    val correctAnswer = plan
    comparePlans(actual, correctAnswer)
  }

  test("Nullable Simplification Primitive: <=>") {
    val plan = nullableRelation.select('a <=> 'a).analyze
    val actual = Optimize.execute(plan)
    val correctAnswer = nullableRelation.select(Alias(TrueLiteral, "(a <=> a)")()).analyze
    comparePlans(actual, correctAnswer)
  }

  test("Non-Nullable Simplification Primitive") {
    val plan = nonNullableRelation
      .select('a === 'a, 'a <=> 'a, 'a <= 'a, 'a >= 'a, 'a < 'a, 'a > 'a).analyze
    val actual = Optimize.execute(plan)
    val correctAnswer = nonNullableRelation
      .select(
        Alias(TrueLiteral, "(a = a)")(),
        Alias(TrueLiteral, "(a <=> a)")(),
        Alias(TrueLiteral, "(a <= a)")(),
        Alias(TrueLiteral, "(a >= a)")(),
        Alias(FalseLiteral, "(a < a)")(),
        Alias(FalseLiteral, "(a > a)")())
      .analyze
    comparePlans(actual, correctAnswer)
  }

  test("Expression Normalization") {
    val plan = nonNullableRelation.where(
      'a * Literal(100) + Pi() === Pi() + Literal(100) * 'a &&
      DateAdd(CurrentDate(), 'a + Literal(2)) <= DateAdd(CurrentDate(), Literal(2) + 'a))
      .analyze
    val actual = Optimize.execute(plan)
    val correctAnswer = nonNullableRelation.analyze
    comparePlans(actual, correctAnswer)
  }
}
