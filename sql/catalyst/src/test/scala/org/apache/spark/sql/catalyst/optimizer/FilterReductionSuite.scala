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
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

/**
 * Unit tests for constant propagation in expressions.
 */
class FilterReductionSuite extends PlanTest {

  trait OptimizeBase extends RuleExecutor[LogicalPlan] {
    protected def batches =
      Batch("AnalysisNodes", Once,
        EliminateSubqueryAliases) ::
        Batch("FilterReduction", FixedPoint(10),
          ConstantPropagation,
          FilterReduction,
          ConstantFolding,
          BooleanSimplification,
          SimplifyBinaryComparison,
          PruneFilters) :: Nil
  }

  object Optimize extends OptimizeBase

  object OptimizeWithoutFilterReduction extends OptimizeBase {
    override protected def batches =
      super.batches.map(b => Batch(b.name, b.strategy, b.rules.filterNot(_ == FilterReduction): _*))
  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int.notNull, 'd.int.notNull, 'x.boolean)

  val data = {
    val intElementsWithNull = Seq(null, 1, 2, 3, 4)
    val intElementsWithoutNull = Seq(null, 1, 2, 3, 4)
    val booleanElementsWithNull = Seq(null, true, false)
    for {
      a <- intElementsWithNull
      b <- intElementsWithNull
      c <- intElementsWithoutNull
      d <- intElementsWithoutNull
      x <- booleanElementsWithNull
    } yield (a, b, c, d, x)
  }

  val testRelationWithData = LocalRelation.fromExternalRows(testRelation.output, data.map(Row(_)))

  private def testFilterReduction(
      input: Expression,
      expectEmptyRelation: Boolean,
      expectedConstraints: Seq[Expression] = Seq.empty) = {
    val originalQuery = testRelationWithData.where(input).analyze
    val optimized = Optimize.execute(originalQuery)
    val correctAnswer = if (expectEmptyRelation) {
      testRelation
    } else if (expectedConstraints.isEmpty) {
      testRelationWithData
    } else {
      testRelationWithData.where(expectedConstraints.reduce(And)).analyze
    }
    comparePlans(optimized, correctAnswer)
  }

  private def testSameAsWithoutFilterReduction(input: Expression) = {
    val originalQuery = testRelationWithData.where(input).analyze
    val optimized = Optimize.execute(originalQuery)
    val correctAnswer = OptimizeWithoutFilterReduction.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }

  test("Filter reduction with nullable attributes") {
    testFilterReduction('a < 2 && Literal(2) < 'a, true)
    testFilterReduction('a < 2 && Literal(2) <= 'a, true)
    testFilterReduction('a < 2 && Literal(2) === 'a, true)
    testFilterReduction('a < 2 && Literal(2) <=> 'a, true)
    testFilterReduction('a < 2 && Literal(2) >= 'a, false, Seq('a < 2))
    testFilterReduction('a < 2 && Literal(2) > 'a, false, Seq('a < 2))
    testFilterReduction('a <= 2 && Literal(2) < 'a, true)
    testFilterReduction('a <= 2 && Literal(2) <= 'a, false, Seq('a === 2))
    testFilterReduction('a <= 2 && Literal(2) === 'a, false, Seq('a === 2))
    testFilterReduction('a <= 2 && Literal(2) <=> 'a, false, Seq('a <=> 2))
    testFilterReduction('a <= 2 && Literal(2) >= 'a, false, Seq('a <= 2))
    testFilterReduction('a <= 2 && Literal(2) > 'a, false, Seq('a < 2))
    testFilterReduction('a === 2 && Literal(2) < 'a, true)
    testFilterReduction('a === 2 && Literal(2) <= 'a, false, Seq('a === 2))
    testFilterReduction('a === 2 && Literal(2) === 'a, false, Seq('a === 2))
    testFilterReduction('a === 2 && Literal(2) <=> 'a, false, Seq('a === 2))
    testFilterReduction('a === 2 && Literal(2) >= 'a, false, Seq('a === 2))
    testFilterReduction('a === 2 && Literal(2) > 'a, true)
    testFilterReduction('a <=> 2 && Literal(2) < 'a, true)
    testFilterReduction('a <=> 2 && Literal(2) <= 'a, false, Seq('a <=> 2))
    testFilterReduction('a <=> 2 && Literal(2) === 'a, false, Seq('a <=> 2))
    testFilterReduction('a <=> 2 && Literal(2) <=> 'a, false, Seq('a <=> 2))
    testFilterReduction('a <=> 2 && Literal(2) >= 'a, false, Seq('a <=> 2))
    testFilterReduction('a <=> 2 && Literal(2) > 'a, true)
    testFilterReduction('a >= 2 && Literal(2) < 'a, false, Seq(Literal(2) < 'a))
    testFilterReduction('a >= 2 && Literal(2) <= 'a, false, Seq(Literal(2) <= 'a))
    testFilterReduction('a >= 2 && Literal(2) === 'a, false, Seq('a === 2))
    testFilterReduction('a >= 2 && Literal(2) <=> 'a, false, Seq('a <=> 2))
    testFilterReduction('a >= 2 && Literal(2) >= 'a, false, Seq('a === 2))
    testFilterReduction('a >= 2 && Literal(2) > 'a, true)
    testFilterReduction('a > 2 && Literal(2) < 'a, false, Seq(Literal(2) < 'a))
    testFilterReduction('a > 2 && Literal(2) <= 'a, false, Seq(Literal(2) < 'a))
    testFilterReduction('a > 2 && Literal(2) === 'a, true)
    testFilterReduction('a > 2 && Literal(2) <=> 'a, true)
    testFilterReduction('a > 2 && Literal(2) >= 'a, true)
    testFilterReduction('a > 2 && Literal(2) > 'a, true)

    testFilterReduction(('x || 'a < 2) && Literal(2) < 'a, false, Seq('x, Literal(2) < 'a))
    testFilterReduction(('x || 'a < 2) && Literal(2) <= 'a, false, Seq('x, Literal(2) <= 'a))
    testFilterReduction(('x || 'a < 2) && Literal(2) === 'a, false, Seq('x, Literal(2) === 'a))
    testFilterReduction(('x || 'a < 2) && Literal(2) <=> 'a, false, Seq('x, Literal(2) <=> 'a))
    testFilterReduction(('x || 'a < 2) && Literal(2) >= 'a, false, Seq('x || 'a < 2, 'a <= 2))
    testFilterReduction(('x || 'a < 2) && Literal(2) > 'a, false, Seq('a < 2))
    testFilterReduction(('x || 'a <= 2) && Literal(2) < 'a, false, Seq('x, Literal(2) < 'a))
    testFilterReduction(('x || 'a <= 2) && Literal(2) <= 'a, false,
      Seq('x || 'a === 2, Literal(2) <= 'a))
    testFilterReduction(('x || 'a <= 2) && Literal(2) === 'a, false, Seq(Literal(2) === 'a))
    testFilterReduction(('x || 'a <= 2) && Literal(2) <=> 'a, false, Seq(Literal(2) <=> 'a))
    testFilterReduction(('x || 'a <= 2) && Literal(2) >= 'a, false, Seq('a <= 2))
    testFilterReduction(('x || 'a <= 2) && Literal(2) > 'a, false, Seq('a < 2))
    testFilterReduction(('x || 'a === 2) && Literal(2) < 'a, false, Seq('x, Literal(2) < 'a))
    testFilterReduction(('x || 'a === 2) && Literal(2) <= 'a, false,
      Seq('x || 'a === 2, Literal(2) <= 'a))
    testFilterReduction(('x || 'a === 2) && Literal(2) === 'a, false, Seq(Literal(2) === 'a))
    testFilterReduction(('x || 'a === 2) && Literal(2) <=> 'a, false, Seq(Literal(2) <=> 'a))
    testFilterReduction(('x || 'a === 2) && Literal(2) >= 'a, false, Seq('x || 'a === 2, 'a <= 2))
    testFilterReduction(('x || 'a === 2) && Literal(2) > 'a, false, Seq('x, 'a < 2))
    testFilterReduction(('x || 'a <=> 2) && Literal(2) < 'a, false, Seq('x, Literal(2) < 'a))
    testFilterReduction(('x || 'a <=> 2) && Literal(2) <= 'a, false,
      Seq('x || 'a === 2, Literal(2) <= 'a))
    testFilterReduction(('x || 'a <=> 2) && Literal(2) === 'a, false, Seq(Literal(2) === 'a))
    testFilterReduction(('x || 'a <=> 2) && Literal(2) <=> 'a, false, Seq(Literal(2) <=> 'a))
    testFilterReduction(('x || 'a <=> 2) && Literal(2) >= 'a, false, Seq('x || 'a === 2, 'a <= 2))
    testFilterReduction(('x || 'a <=> 2) && Literal(2) > 'a, false, Seq('x, 'a < 2))
    testFilterReduction(('x || 'a >= 2) && Literal(2) < 'a, false, Seq(Literal(2) < 'a))
    testFilterReduction(('x || 'a >= 2) && Literal(2) <= 'a, false, Seq(Literal(2) <= 'a))
    testFilterReduction(('x || 'a >= 2) && Literal(2) === 'a, false, Seq(Literal(2) === 'a))
    testFilterReduction(('x || 'a >= 2) && Literal(2) <=> 'a, false, Seq(Literal(2) <=> 'a))
    testFilterReduction(('x || 'a >= 2) && Literal(2) >= 'a, false,
      Seq('x || 'a === 2, 'a <= Literal(2)))
    testFilterReduction(('x || 'a >= 2) && Literal(2) > 'a, false, Seq('x, 'a < Literal(2)))
    testFilterReduction(('x || 'a > 2) && Literal(2) < 'a, false, Seq(Literal(2) < 'a))
    testFilterReduction(('x || 'a > 2) && Literal(2) <= 'a, false,
      Seq('x || Literal(2) < 'a, Literal(2) <= 'a))
    testFilterReduction(('x || 'a > 2) && Literal(2) === 'a, false, Seq('x, Literal(2) === 'a))
    testFilterReduction(('x || 'a > 2) && Literal(2) <=> 'a, false, Seq('x, Literal(2) <=> 'a))
    testFilterReduction(('x || 'a > 2) && Literal(2) >= 'a, false, Seq('x, 'a <= Literal(2)))
    testFilterReduction(('x || 'a > 2) && Literal(2) > 'a, false, Seq('x, 'a < Literal(2)))

    testFilterReduction('a < 2 && Literal(3) < 'a, true)
    testFilterReduction('a < 2 && Literal(3) <= 'a, true)
    testFilterReduction('a < 2 && Literal(3) === 'a, true)
    testFilterReduction('a < 2 && Literal(3) <=> 'a, true)
    testFilterReduction('a < 2 && Literal(3) >= 'a, false, Seq('a < 2))
    testFilterReduction('a < 2 && Literal(3) > 'a, false, Seq('a < 2))
    testFilterReduction('a <= 2 && Literal(3) < 'a, true)
    testFilterReduction('a <= 2 && Literal(3) <= 'a, true)
    testFilterReduction('a <= 2 && Literal(3) === 'a, true)
    testFilterReduction('a <= 2 && Literal(3) <=> 'a, true)
    testFilterReduction('a <= 2 && Literal(3) >= 'a, false, Seq('a <= 2))
    testFilterReduction('a <= 2 && Literal(3) > 'a, false, Seq('a <= 2))
    testFilterReduction('a === 2 && Literal(3) < 'a, true)
    testFilterReduction('a === 2 && Literal(3) <= 'a, true)
    testFilterReduction('a === 2 && Literal(3) === 'a, true)
    testFilterReduction('a === 2 && Literal(3) <=> 'a, true)
    testFilterReduction('a === 2 && Literal(3) >= 'a, false, Seq('a === 2))
    testFilterReduction('a === 2 && Literal(3) > 'a, false, Seq('a === 2))
    testFilterReduction('a <=> 2 && Literal(3) < 'a, true)
    testFilterReduction('a <=> 2 && Literal(3) <= 'a, true)
    testFilterReduction('a <=> 2 && Literal(3) === 'a, true)
    testFilterReduction('a <=> 2 && Literal(3) <=> 'a, true)
    testFilterReduction('a <=> 2 && Literal(3) >= 'a, false, Seq('a <=> 2))
    testFilterReduction('a <=> 2 && Literal(3) > 'a, false, Seq('a <=> 2))
    testFilterReduction('a >= 2 && Literal(3) < 'a, false, Seq(Literal(3) < 'a))
    testFilterReduction('a >= 2 && Literal(3) <= 'a, false, Seq(Literal(3) <= 'a))
    testFilterReduction('a >= 2 && Literal(3) === 'a, false, Seq(Literal(3) === 'a))
    testFilterReduction('a >= 2 && Literal(3) <=> 'a, false, Seq(Literal(3) <=> 'a))
    testFilterReduction('a >= 2 && Literal(3) >= 'a, false, Seq(Literal(2) <= 'a, 'a <= 3))
    testFilterReduction('a >= 2 && Literal(3) > 'a, false, Seq(Literal(2) <= 'a, 'a < 3))
    testFilterReduction('a > 2 && Literal(3) < 'a, false, Seq(Literal(3) < 'a))
    testFilterReduction('a > 2 && Literal(3) <= 'a, false, Seq(Literal(3) <= 'a))
    testFilterReduction('a > 2 && Literal(3) === 'a, false, Seq(Literal(3) === 'a))
    testFilterReduction('a > 2 && Literal(3) <=> 'a, false, Seq(Literal(3) <=> 'a))
    testFilterReduction('a > 2 && Literal(3) >= 'a, false, Seq(Literal(2) < 'a, 'a <= 3))
    testFilterReduction('a > 2 && Literal(3) > 'a, false, Seq(Literal(2) < 'a, 'a < 3))

    testFilterReduction(('x || 'a < 2) && Literal(3) < 'a, false, Seq('x, Literal(3) < 'a))
    testFilterReduction(('x || 'a < 2) && Literal(3) <= 'a, false, Seq('x, Literal(3) <= 'a))
    testFilterReduction(('x || 'a < 2) && Literal(3) === 'a, false, Seq('x, Literal(3) === 'a))
    testFilterReduction(('x || 'a < 2) && Literal(3) <=> 'a, false, Seq('x, Literal(3) <=> 'a))
    testFilterReduction(('x || 'a < 2) && Literal(3) >= 'a, false, Seq('x || 'a < 2, 'a <= 3))
    testFilterReduction(('x || 'a < 2) && Literal(3) > 'a, false, Seq('x || 'a < 2, 'a < 3))
    testFilterReduction(('x || 'a <= 2) && Literal(3) < 'a, false, Seq('x, Literal(3) < 'a))
    testFilterReduction(('x || 'a <= 2) && Literal(3) <= 'a, false, Seq('x, Literal(3) <= 'a))
    testFilterReduction(('x || 'a <= 2) && Literal(3) === 'a, false, Seq('x, Literal(3) === 'a))
    testFilterReduction(('x || 'a <= 2) && Literal(3) <=> 'a, false, Seq('x, Literal(3) <=> 'a))
    testFilterReduction(('x || 'a <= 2) && Literal(3) >= 'a, false, Seq('x || 'a <= 2, 'a <= 3))
    testFilterReduction(('x || 'a <= 2) && Literal(3) > 'a, false, Seq('x || 'a <= 2, 'a < 3))
    testFilterReduction(('x || 'a === 2) && Literal(3) < 'a, false, Seq('x, Literal(3) < 'a))
    testFilterReduction(('x || 'a === 2) && Literal(3) <= 'a, false, Seq('x, Literal(3) <= 'a))
    testFilterReduction(('x || 'a === 2) && Literal(3) === 'a, false, Seq('x, Literal(3) === 'a))
    testFilterReduction(('x || 'a === 2) && Literal(3) <=> 'a, false, Seq('x, Literal(3) <=> 'a))
    testFilterReduction(('x || 'a === 2) && Literal(3) >= 'a, false, Seq('x || 'a === 2, 'a <= 3))
    testFilterReduction(('x || 'a === 2) && Literal(3) > 'a, false, Seq('x || 'a === 2, 'a < 3))
    testFilterReduction(('x || 'a <=> 2) && Literal(3) < 'a, false, Seq('x, Literal(3) < 'a))
    testFilterReduction(('x || 'a <=> 2) && Literal(3) <= 'a, false, Seq('x, Literal(3) <= 'a))
    testFilterReduction(('x || 'a <=> 2) && Literal(3) === 'a, false, Seq('x, Literal(3) === 'a))
    testFilterReduction(('x || 'a <=> 2) && Literal(3) <=> 'a, false, Seq('x, Literal(3) <=> 'a))
    testFilterReduction(('x || 'a <=> 2) && Literal(3) >= 'a, false, Seq('x || 'a === 2, 'a <= 3))
    testFilterReduction(('x || 'a <=> 2) && Literal(3) > 'a, false, Seq('x || 'a === 2, 'a < 3))
    testFilterReduction(('x || 'a >= 2) && Literal(3) < 'a, false, Seq(Literal(3) < 'a))
    testFilterReduction(('x || 'a >= 2) && Literal(3) <= 'a, false, Seq(Literal(3) <= 'a))
    testFilterReduction(('x || 'a >= 2) && Literal(3) === 'a, false, Seq(Literal(3) === 'a))
    testFilterReduction(('x || 'a >= 2) && Literal(3) <=> 'a, false, Seq(Literal(3) <=> 'a))
    testFilterReduction(('x || 'a >= 2) && Literal(3) >= 'a, false,
      Seq('x || Literal(2) <= 'a, 'a <= 3))
    testFilterReduction(('x || 'a >= 2) && Literal(3) > 'a, false,
      Seq('x || Literal(2) <= 'a, 'a < 3))
    testFilterReduction(('x || 'a > 2) && Literal(3) < 'a, false, Seq(Literal(3) < 'a))
    testFilterReduction(('x || 'a > 2) && Literal(3) <= 'a, false, Seq(Literal(3) <= 'a))
    testFilterReduction(('x || 'a > 2) && Literal(3) === 'a, false, Seq(Literal(3) === 'a))
    testFilterReduction(('x || 'a > 2) && Literal(3) <=> 'a, false, Seq(Literal(3) <=> 'a))
    testFilterReduction(('x || 'a > 2) && Literal(3) >= 'a, false,
      Seq('x || Literal(2) < 'a, 'a <= 3))
    testFilterReduction(('x || 'a > 2) && Literal(3) > 'a, false,
      Seq('x || Literal(2) < 'a, 'a < 3))

    testFilterReduction('a < 'b && 'b < 'a, true)
    testFilterReduction('a < 'b && 'b <= 'a, true)
    testFilterReduction('a < 'b && 'b === 'a, true)
    testFilterReduction('a < 'b && 'b <=> 'a, true)
    testFilterReduction('a < 'b && 'b >= 'a, false, Seq('a < 'b))
    testFilterReduction('a < 'b && 'b > 'a, false, Seq('a < 'b))
    testFilterReduction('a <= 'b && 'b < 'a, true)
    testFilterReduction('a <= 'b && 'b <= 'a, false, Seq('a === 'b))
    testFilterReduction('a <= 'b && 'b === 'a, false, Seq('a === 'b))
    testFilterReduction('a <= 'b && 'b <=> 'a, false, Seq('a === 'b))
    testFilterReduction('a <= 'b && 'b >= 'a, false, Seq('a <= 'b))
    testFilterReduction('a <= 'b && 'b > 'a, false, Seq('a < 'b))
    testFilterReduction('a === 'b && 'b < 'a, true)
    testFilterReduction('a === 'b && 'b <= 'a, false, Seq('a === 'b))
    testFilterReduction('a === 'b && 'b === 'a, false, Seq('a === 'b))
    testFilterReduction('a === 'b && 'b <=> 'a, false, Seq('a === 'b))
    testFilterReduction('a === 'b && 'b >= 'a, false, Seq('a === 'b))
    testFilterReduction('a === 'b && 'b > 'a, true)
    testFilterReduction('a <=> 'b && 'b < 'a, true)
    testFilterReduction('a <=> 'b && 'b <= 'a, false, Seq('a === 'b))
    testFilterReduction('a <=> 'b && 'b === 'a, false, Seq('a === 'b))
    testFilterReduction('a <=> 'b && 'b <=> 'a, false, Seq('a <=> 'b))
    testFilterReduction('a <=> 'b && 'b >= 'a, false, Seq('a === 'b))
    testFilterReduction('a <=> 'b && 'b > 'a, true)
    testFilterReduction('a >= 'b && 'b < 'a, false, Seq('b < 'a))
    testFilterReduction('a >= 'b && 'b <= 'a, false, Seq('b <= 'a))
    testFilterReduction('a >= 'b && 'b === 'a, false, Seq('a === 'b))
    testFilterReduction('a >= 'b && 'b <=> 'a, false, Seq('a === 'b))
    testFilterReduction('a >= 'b && 'b >= 'a, false, Seq('a === 'b))
    testFilterReduction('a >= 'b && 'b > 'a, true)
    testFilterReduction('a > 'b && 'b < 'a, false, Seq('b < 'a))
    testFilterReduction('a > 'b && 'b <= 'a, false, Seq('b < 'a))
    testFilterReduction('a > 'b && 'b === 'a, true)
    testFilterReduction('a > 'b && 'b <=> 'a, true)
    testFilterReduction('a > 'b && 'b >= 'a, true)
    testFilterReduction('a > 'b && 'b > 'a, true)

    testFilterReduction('a < abs('b) && abs('b) < 'a, true)
    testFilterReduction('a < abs('b) && abs('b) <= 'a, true)
    testFilterReduction('a < abs('b) && abs('b) === 'a, true)
    testFilterReduction('a < abs('b) && abs('b) <=> 'a, true)
    testFilterReduction('a < abs('b) && abs('b) >= 'a, false, Seq('a < abs('b)))
    testFilterReduction('a < abs('b) && abs('b) > 'a, false, Seq('a < abs('b)))
    testFilterReduction('a <= abs('b) && abs('b) < 'a, true)
    testFilterReduction('a <= abs('b) && abs('b) <= 'a, false, Seq('a === abs('b)))
    testFilterReduction('a <= abs('b) && abs('b) === 'a, false, Seq('a === abs('b)))
    testFilterReduction('a <= abs('b) && abs('b) <=> 'a, false, Seq('a === abs('b)))
    testFilterReduction('a <= abs('b) && abs('b) >= 'a, false, Seq('a <= abs('b)))
    testFilterReduction('a <= abs('b) && abs('b) > 'a, false, Seq('a < abs('b)))
    testFilterReduction('a === abs('b) && abs('b) < 'a, true)
    testFilterReduction('a === abs('b) && abs('b) <= 'a, false, Seq('a === abs('b)))
    testFilterReduction('a === abs('b) && abs('b) === 'a, false, Seq('a === abs('b)))
    testFilterReduction('a === abs('b) && abs('b) <=> 'a, false, Seq('a === abs('b)))
    testFilterReduction('a === abs('b) && abs('b) >= 'a, false, Seq('a === abs('b)))
    testFilterReduction('a === abs('b) && abs('b) > 'a, true)
    testFilterReduction('a <=> abs('b) && abs('b) < 'a, true)
    testFilterReduction('a <=> abs('b) && abs('b) <= 'a, false, Seq('a === abs('b)))
    testFilterReduction('a <=> abs('b) && abs('b) === 'a, false, Seq('a === abs('b)))
    testFilterReduction('a <=> abs('b) && abs('b) <=> 'a, false, Seq('a <=> abs('b)))
    testFilterReduction('a <=> abs('b) && abs('b) >= 'a, false, Seq('a === abs('b)))
    testFilterReduction('a <=> abs('b) && abs('b) > 'a, true)
    testFilterReduction('a >= abs('b) && abs('b) < 'a, false, Seq(abs('b) < 'a))
    testFilterReduction('a >= abs('b) && abs('b) <= 'a, false, Seq(abs('b) <= 'a))
    testFilterReduction('a >= abs('b) && abs('b) === 'a, false, Seq('a === abs('b)))
    testFilterReduction('a >= abs('b) && abs('b) <=> 'a, false, Seq('a === abs('b)))
    testFilterReduction('a >= abs('b) && abs('b) >= 'a, false, Seq('a === abs('b)))
    testFilterReduction('a >= abs('b) && abs('b) > 'a, true)
    testFilterReduction('a > abs('b) && abs('b) < 'a, false, Seq(abs('b) < 'a))
    testFilterReduction('a > abs('b) && abs('b) <= 'a, false, Seq(abs('b) < 'a))
    testFilterReduction('a > abs('b) && abs('b) === 'a, true)
    testFilterReduction('a > abs('b) && abs('b) <=> 'a, true)
    testFilterReduction('a > abs('b) && abs('b) >= 'a, true)
    testFilterReduction('a > abs('b) && abs('b) > 'a, true)

    testFilterReduction(('x || 'a < abs('b)) && abs('b) < 'a, false, Seq('x, abs('b) < 'a))
    testFilterReduction(('x || 'a < abs('b)) && abs('b) <= 'a, false, Seq('x, abs('b) <= 'a))
    testFilterReduction(('x || 'a < abs('b)) && abs('b) === 'a, false, Seq('x, abs('b) === 'a))
    testFilterReduction(('x || 'a < abs('b)) && abs('b) <=> 'a, false, Seq('x, abs('b) <=> 'a))
    testFilterReduction(('x || 'a < abs('b)) && abs('b) >= 'a, false,
      Seq('x || 'a < abs('b), 'a <= abs('b)))
    testFilterReduction(('x || 'a < abs('b)) && abs('b) > 'a, false, Seq('a < abs('b)))
    testFilterReduction(('x || 'a <= abs('b)) && abs('b) < 'a, false, Seq('x, abs('b) < 'a))
    testFilterReduction(('x || 'a <= abs('b)) && abs('b) <= 'a, false,
      Seq('x || 'a === abs('b), abs('b) <= 'a))
    testFilterReduction(('x || 'a <= abs('b)) && abs('b) === 'a, false, Seq(abs('b) === 'a))
    testFilterReduction(('x || 'a <= abs('b)) && abs('b) <=> 'a, false,
      Seq('x || 'a === abs('b), abs('b) <=> 'a))
    testFilterReduction(('x || 'a <= abs('b)) && abs('b) >= 'a, false, Seq('a <= abs('b)))
    testFilterReduction(('x || 'a <= abs('b)) && abs('b) > 'a, false, Seq('a < abs('b)))
    testFilterReduction(('x || 'a === abs('b)) && abs('b) < 'a, false, Seq('x, abs('b) < 'a))
    testFilterReduction(('x || 'a === abs('b)) && abs('b) <= 'a, false,
      Seq('x || 'a === abs('b), abs('b) <= 'a))
    testFilterReduction(('x || 'a === abs('b)) && abs('b) === 'a, false, Seq(abs('b) === 'a))
    testFilterReduction(('x || 'a === abs('b)) && abs('b) <=> 'a, false,
      Seq('x || 'a === abs('b), abs('b) <=> 'a))
    testFilterReduction(('x || 'a === abs('b)) && abs('b) >= 'a, false,
      Seq('x || 'a === abs('b), 'a <= abs('b)))
    testFilterReduction(('x || 'a === abs('b)) && abs('b) > 'a, false, Seq('x, 'a < abs('b)))
    testFilterReduction(('x || 'a <=> abs('b)) && abs('b) < 'a, false, Seq('x, abs('b) < 'a))
    testFilterReduction(('x || 'a <=> abs('b)) && abs('b) <= 'a, false,
      Seq('x || 'a === abs('b), abs('b) <= 'a))
    testFilterReduction(('x || 'a <=> abs('b)) && abs('b) === 'a, false, Seq(abs('b) === 'a))
    testFilterReduction(('x || 'a <=> abs('b)) && abs('b) <=> 'a, false, Seq(abs('b) <=> 'a))
    testFilterReduction(('x || 'a <=> abs('b)) && abs('b) >= 'a, false,
      Seq('x || 'a === abs('b), 'a <= abs('b)))
    testFilterReduction(('x || 'a <=> abs('b)) && abs('b) > 'a, false, Seq('x, 'a < abs('b)))
    testFilterReduction(('x || 'a >= abs('b)) && abs('b) < 'a, false, Seq(abs('b) < 'a))
    testFilterReduction(('x || 'a >= abs('b)) && abs('b) <= 'a, false, Seq(abs('b) <= 'a))
    testFilterReduction(('x || 'a >= abs('b)) && abs('b) === 'a, false, Seq(abs('b) === 'a))
    testFilterReduction(('x || 'a >= abs('b)) && abs('b) <=> 'a, false,
      Seq('x || 'a === abs('b), abs('b) <=> 'a))
    testFilterReduction(('x || 'a >= abs('b)) && abs('b) >= 'a, false,
      Seq('x || 'a === abs('b), 'a <= abs('b)))
    testFilterReduction(('x || 'a >= abs('b)) && abs('b) > 'a, false, Seq('x, 'a < abs('b)))
    testFilterReduction(('x || 'a > abs('b)) && abs('b) < 'a, false, Seq(abs('b) < 'a))
    testFilterReduction(('x || 'a > abs('b)) && abs('b) <= 'a, false,
      Seq('x || abs('b) < 'a, abs('b) <= 'a))
    testFilterReduction(('x || 'a > abs('b)) && abs('b) === 'a, false, Seq('x, abs('b) === 'a))
    testFilterReduction(('x || 'a > abs('b)) && abs('b) <=> 'a, false, Seq('x, abs('b) <=> 'a))
    testFilterReduction(('x || 'a > abs('b)) && abs('b) >= 'a, false, Seq('x, 'a <= abs('b)))
    testFilterReduction(('x || 'a > abs('b)) && abs('b) > 'a, false, Seq('x, 'a < abs('b)))
  }

  // These cases test scenarios when there is NullIntolerant node (ex. Not) between Filter and the
  // subtree of And nodes and the expression to be reduced is nullable.
  // For example in these cases the following reduction does not hold when X and Y is null and so
  // FilterReduction should do nothing:
  // Not(X < Y && Y < X)            =>  Not(X < Y && false)
  // Not(X < Y && Not(X < Y))       =>  Not(X < Y && Not(true))
  // Not(X <=> Y && Y < X)          =>  Not(X <=> Y && false)
  // Not(X < Y && Y <=> X)          =>  Not(X < Y && false)
  test("Filter reduction with nullable attributes and NullIntolerant nodes") {
    testSameAsWithoutFilterReduction(Not(('x || 'a < abs('b)) && abs('b) < 'a))
    testSameAsWithoutFilterReduction(Not(('x || 'a < abs('b)) && abs('b) <= 'a))
    testSameAsWithoutFilterReduction(Not(('x || 'a < abs('b)) && abs('b) === 'a))
    testSameAsWithoutFilterReduction(Not(('x || 'a < abs('b)) && abs('b) <=> 'a))
    testSameAsWithoutFilterReduction(Not(('x || 'a < abs('b)) && abs('b) >= 'a))
    testSameAsWithoutFilterReduction(Not(('x || 'a < abs('b)) && abs('b) > 'a))
    testSameAsWithoutFilterReduction(Not(('x || 'a <= abs('b)) && abs('b) < 'a))
    testSameAsWithoutFilterReduction(Not(('x || 'a <= abs('b)) && abs('b) <= 'a))
    testSameAsWithoutFilterReduction(Not(('x || 'a <= abs('b)) && abs('b) === 'a))
    testSameAsWithoutFilterReduction(Not(('x || 'a <= abs('b)) && abs('b) <=> 'a))
    testSameAsWithoutFilterReduction(Not(('x || 'a <= abs('b)) && abs('b) >= 'a))
    testSameAsWithoutFilterReduction(Not(('x || 'a <= abs('b)) && abs('b) > 'a))
    testSameAsWithoutFilterReduction(Not(('x || 'a === abs('b)) && abs('b) < 'a))
    testSameAsWithoutFilterReduction(Not(('x || 'a === abs('b)) && abs('b) <= 'a))
    testSameAsWithoutFilterReduction(Not(('x || 'a === abs('b)) && abs('b) === 'a))
    testSameAsWithoutFilterReduction(Not(('x || 'a === abs('b)) && abs('b) <=> 'a))
    testSameAsWithoutFilterReduction(Not(('x || 'a === abs('b)) && abs('b) >= 'a))
    testSameAsWithoutFilterReduction(Not(('x || 'a === abs('b)) && abs('b) > 'a))
    testSameAsWithoutFilterReduction(Not(('x || 'a <=> abs('b)) && abs('b) < 'a))
    testSameAsWithoutFilterReduction(Not(('x || 'a <=> abs('b)) && abs('b) <= 'a))
    testSameAsWithoutFilterReduction(Not(('x || 'a <=> abs('b)) && abs('b) === 'a))
    // the only exception:
    // testSameAsWithoutFilterReduction(Not(('x || 'a <=> abs('b)) && abs('b) <=> 'a))
    testSameAsWithoutFilterReduction(Not(('x || 'a <=> abs('b)) && abs('b) >= 'a))
    testSameAsWithoutFilterReduction(Not(('x || 'a <=> abs('b)) && abs('b) > 'a))
    testSameAsWithoutFilterReduction(Not(('x || 'a >= abs('b)) && abs('b) < 'a))
    testSameAsWithoutFilterReduction(Not(('x || 'a >= abs('b)) && abs('b) <= 'a))
    testSameAsWithoutFilterReduction(Not(('x || 'a >= abs('b)) && abs('b) === 'a))
    testSameAsWithoutFilterReduction(Not(('x || 'a >= abs('b)) && abs('b) <=> 'a))
    testSameAsWithoutFilterReduction(Not(('x || 'a >= abs('b)) && abs('b) >= 'a))
    testSameAsWithoutFilterReduction(Not(('x || 'a >= abs('b)) && abs('b) > 'a))
    testSameAsWithoutFilterReduction(Not(('x || 'a > abs('b)) && abs('b) < 'a))
    testSameAsWithoutFilterReduction(Not(('x || 'a > abs('b)) && abs('b) <= 'a))
    testSameAsWithoutFilterReduction(Not(('x || 'a > abs('b)) && abs('b) === 'a))
    testSameAsWithoutFilterReduction(Not(('x || 'a > abs('b)) && abs('b) <=> 'a))
    testSameAsWithoutFilterReduction(Not(('x || 'a > abs('b)) && abs('b) >= 'a))
    testSameAsWithoutFilterReduction(Not(('x || 'a > abs('b)) && abs('b) > 'a))

    testSameAsWithoutFilterReduction(Not(('x || Not('a < abs('b))) && abs('b) < 'a))
    testSameAsWithoutFilterReduction(Not(('x || Not('a < abs('b))) && abs('b) <= 'a))
    testSameAsWithoutFilterReduction(Not(('x || Not('a < abs('b))) && abs('b) === 'a))
    testSameAsWithoutFilterReduction(Not(('x || Not('a < abs('b))) && abs('b) <=> 'a))
    testSameAsWithoutFilterReduction(Not(('x || Not('a < abs('b))) && abs('b) >= 'a))
    testSameAsWithoutFilterReduction(Not(('x || Not('a < abs('b))) && abs('b) > 'a))
    testSameAsWithoutFilterReduction(Not(('x || Not('a <= abs('b))) && abs('b) < 'a))
    testSameAsWithoutFilterReduction(Not(('x || Not('a <= abs('b))) && abs('b) <= 'a))
    testSameAsWithoutFilterReduction(Not(('x || Not('a <= abs('b))) && abs('b) === 'a))
    testSameAsWithoutFilterReduction(Not(('x || Not('a <= abs('b))) && abs('b) <=> 'a))
    testSameAsWithoutFilterReduction(Not(('x || Not('a <= abs('b))) && abs('b) >= 'a))
    testSameAsWithoutFilterReduction(Not(('x || Not('a <= abs('b))) && abs('b) > 'a))
    testSameAsWithoutFilterReduction(Not(('x || Not('a === abs('b))) && abs('b) < 'a))
    testSameAsWithoutFilterReduction(Not(('x || Not('a === abs('b))) && abs('b) <= 'a))
    testSameAsWithoutFilterReduction(Not(('x || Not('a === abs('b))) && abs('b) === 'a))
    testSameAsWithoutFilterReduction(Not(('x || Not('a === abs('b))) && abs('b) <=> 'a))
    testSameAsWithoutFilterReduction(Not(('x || Not('a === abs('b))) && abs('b) >= 'a))
    testSameAsWithoutFilterReduction(Not(('x || Not('a === abs('b))) && abs('b) > 'a))
    testSameAsWithoutFilterReduction(Not(('x || Not('a <=> abs('b))) && abs('b) < 'a))
    testSameAsWithoutFilterReduction(Not(('x || Not('a <=> abs('b))) && abs('b) <= 'a))
    testSameAsWithoutFilterReduction(Not(('x || Not('a <=> abs('b))) && abs('b) === 'a))
    // the only exception:
    // testSameAsWithoutFilterReduction(Not(('x || Not('a <=> abs('b))) && abs('b) <=> 'a))
    testSameAsWithoutFilterReduction(Not(('x || Not('a <=> abs('b))) && abs('b) >= 'a))
    testSameAsWithoutFilterReduction(Not(('x || Not('a <=> abs('b))) && abs('b) > 'a))
    testSameAsWithoutFilterReduction(Not(('x || Not('a >= abs('b))) && abs('b) < 'a))
    testSameAsWithoutFilterReduction(Not(('x || Not('a >= abs('b))) && abs('b) <= 'a))
    testSameAsWithoutFilterReduction(Not(('x || Not('a >= abs('b))) && abs('b) === 'a))
    testSameAsWithoutFilterReduction(Not(('x || Not('a >= abs('b))) && abs('b) <=> 'a))
    testSameAsWithoutFilterReduction(Not(('x || Not('a >= abs('b))) && abs('b) >= 'a))
    testSameAsWithoutFilterReduction(Not(('x || Not('a >= abs('b))) && abs('b) > 'a))
    testSameAsWithoutFilterReduction(Not(('x || Not('a > abs('b))) && abs('b) < 'a))
    testSameAsWithoutFilterReduction(Not(('x || Not('a > abs('b))) && abs('b) <= 'a))
    testSameAsWithoutFilterReduction(Not(('x || Not('a > abs('b))) && abs('b) === 'a))
    testSameAsWithoutFilterReduction(Not(('x || Not('a > abs('b))) && abs('b) <=> 'a))
    testSameAsWithoutFilterReduction(Not(('x || Not('a > abs('b))) && abs('b) >= 'a))
    testSameAsWithoutFilterReduction(Not(('x || Not('a > abs('b))) && abs('b) > 'a))
  }
}
