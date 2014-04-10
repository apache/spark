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

import org.apache.spark.sql.catalyst.analysis.EliminateAnalysisOperators
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.types.{DoubleType, IntegerType}

// For implicit conversions
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.dsl.expressions._

class ConstantFoldingSuite extends OptimizerTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("AnalysisNodes", Once,
        EliminateAnalysisOperators) ::
      Batch("ConstantFolding", Once,
        ConstantFolding,
        BooleanSimplification) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int)

  test("eliminate subqueries") {
    val originalQuery =
      testRelation
        .subquery('y)
        .select('a)

    val optimized = Optimize(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .select('a.attr)
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  /**
   * Unit tests for constant folding in expressions.
   */
  test("Constant folding test: expressions only have literals") {
    val originalQuery =
      testRelation
        .select(
          Literal(2) + Literal(3) + Literal(4) as Symbol("2+3+4"),
          Literal(2) * Literal(3) + Literal(4) as Symbol("2*3+4"),
          Literal(2) * (Literal(3) + Literal(4)) as Symbol("2*(3+4)"))
        .where(
          Literal(1) === Literal(1) &&
          Literal(2) > Literal(3) ||
          Literal(3) > Literal(2) )
        .groupBy(
          Literal(2) * Literal(3) - Literal(6) / (Literal(4) - Literal(2))
        )(Literal(9) / Literal(3) as Symbol("9/3"))

    val optimized = Optimize(originalQuery.analyze)

    val correctAnswer =
      testRelation
        .select(
          Literal(9) as Symbol("2+3+4"),
          Literal(10) as Symbol("2*3+4"),
          Literal(14) as Symbol("2*(3+4)"))
        .where(Literal(true))
        .groupBy(Literal(3))(Literal(3) as Symbol("9/3"))
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Constant folding test: expressions have attribute references and literals in " +
    "arithmetic operations") {
    val originalQuery =
      testRelation
        .select(
          Literal(2) + Literal(3) + 'a as Symbol("c1"),
          'a + Literal(2) + Literal(3) as Symbol("c2"),
          Literal(2) * 'a + Literal(4) as Symbol("c3"),
          'a * (Literal(3) + Literal(4)) as Symbol("c4"))

    val optimized = Optimize(originalQuery.analyze)

    val correctAnswer =
      testRelation
        .select(
          Literal(5) + 'a as Symbol("c1"),
          'a + Literal(2) + Literal(3) as Symbol("c2"),
          Literal(2) * 'a + Literal(4) as Symbol("c3"),
          'a * Literal(7) as Symbol("c4"))
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Constant folding test: expressions have attribute references and literals in " +
    "predicates") {
    val originalQuery =
      testRelation
        .where(
          (('a > 1 && Literal(1) === Literal(1)) ||
           ('a < 10 && Literal(1) === Literal(2)) ||
           (Literal(1) === Literal(1) && 'b > 1) ||
           (Literal(1) === Literal(2) && 'b < 10)) &&
           (('a > 1 || Literal(1) === Literal(1)) &&
            ('a < 10 || Literal(1) === Literal(2)) &&
            (Literal(1) === Literal(1) || 'b > 1) &&
            (Literal(1) === Literal(2) || 'b < 10)))

    val optimized = Optimize(originalQuery.analyze)

    val correctAnswer =
      testRelation
        .where(('a > 1 || 'b > 1) && ('a < 10 && 'b < 10))
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Constant folding test: expressions have foldable functions") {
    val originalQuery =
      testRelation
        .select(
          Cast(Literal("2"), IntegerType) + Literal(3) + 'a as Symbol("c1"),
          Coalesce(Seq(Cast(Literal("abc"), IntegerType), Literal(3))) as Symbol("c2"))

    val optimized = Optimize(originalQuery.analyze)

    val correctAnswer =
      testRelation
        .select(
          Literal(5) + 'a as Symbol("c1"),
          Literal(3) as Symbol("c2"))
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Constant folding test: expressions have nonfoldable functions") {
    val originalQuery =
      testRelation
        .select(
          Rand + Literal(1) as Symbol("c1"),
          Sum('a) as Symbol("c2"))

    val optimized = Optimize(originalQuery.analyze)

    val correctAnswer =
      testRelation
        .select(
          Rand + Literal(1.0) as Symbol("c1"),
          Sum('a) as Symbol("c2"))
        .analyze

    comparePlans(optimized, correctAnswer)
  }
}
