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

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
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

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int, 'x.boolean)

  val data = {
    val intElements = Seq(null, 1, 2, 3)
    val booleanElements = Seq(null, true, false)
    for {
      a <- intElements
      b <- intElements
      c <- intElements
      x <- booleanElements
    } yield (a, b, c, x)
  }

  val testRelationWithData = LocalRelation.fromExternalRows(testRelation.output, data.map(Row(_)))

  private def testPropagation(
      input: Expression,
      expectEmptyRelation: Boolean,
      expectedConstraints: Seq[Expression] = Seq.empty) = {
    val originalQuery = testRelationWithData.where(input).analyze
    val optimized = Optimize.execute(originalQuery)
    val correctAnswer = if (expectEmptyRelation) {
      testRelation
    } else {
      testRelationWithData.where(expectedConstraints.reduce(And)).analyze
    }
    comparePlans(optimized, correctAnswer)
  }

  test("Filter reduction") {
    testPropagation('a < 2 && Literal(2) < 'a, true)
    testPropagation('a < 2 && Literal(2) <= 'a, true)
    testPropagation('a < 2 && Literal(2) === 'a, true)
    testPropagation('a < 2 && Literal(2) <=> 'a, true)
    testPropagation('a < 2 && Literal(2) >= 'a, false, Seq('a < 2))
    testPropagation('a < 2 && Literal(2) > 'a, false, Seq('a < 2))
    testPropagation('a <= 2 && Literal(2) < 'a, true)
    testPropagation('a <= 2 && Literal(2) <= 'a, false, Seq('a === 2))
    testPropagation('a <= 2 && Literal(2) === 'a, false, Seq('a === 2))
    testPropagation('a <= 2 && Literal(2) <=> 'a, false, Seq('a <=> 2))
    testPropagation('a <= 2 && Literal(2) >= 'a, false, Seq('a <= 2))
    testPropagation('a <= 2 && Literal(2) > 'a, false, Seq('a < 2))
    testPropagation('a === 2 && Literal(2) < 'a, true)
    testPropagation('a === 2 && Literal(2) <= 'a, false, Seq('a === 2))
    testPropagation('a === 2 && Literal(2) === 'a, false, Seq('a === 2))
    testPropagation('a === 2 && Literal(2) <=> 'a, false, Seq('a === 2))
    testPropagation('a === 2 && Literal(2) >= 'a, false, Seq('a === 2))
    testPropagation('a === 2 && Literal(2) > 'a, true)
    testPropagation('a <=> 2 && Literal(2) < 'a, true)
    testPropagation('a <=> 2 && Literal(2) <= 'a, false, Seq('a <=> 2))
    testPropagation('a <=> 2 && Literal(2) === 'a, false, Seq('a <=> 2))
    testPropagation('a <=> 2 && Literal(2) <=> 'a, false, Seq('a <=> 2))
    testPropagation('a <=> 2 && Literal(2) >= 'a, false, Seq('a <=> 2))
    testPropagation('a <=> 2 && Literal(2) > 'a, true)
    testPropagation('a >= 2 && Literal(2) < 'a, false, Seq(Literal(2) < 'a))
    testPropagation('a >= 2 && Literal(2) <= 'a, false, Seq(Literal(2) <= 'a))
    testPropagation('a >= 2 && Literal(2) === 'a, false, Seq('a === 2))
    testPropagation('a >= 2 && Literal(2) <=> 'a, false, Seq('a <=> 2))
    testPropagation('a >= 2 && Literal(2) >= 'a, false, Seq('a === 2))
    testPropagation('a >= 2 && Literal(2) > 'a, true)
    testPropagation('a > 2 && Literal(2) < 'a, false, Seq(Literal(2) < 'a))
    testPropagation('a > 2 && Literal(2) <= 'a, false, Seq(Literal(2) < 'a))
    testPropagation('a > 2 && Literal(2) === 'a, true)
    testPropagation('a > 2 && Literal(2) <=> 'a, true)
    testPropagation('a > 2 && Literal(2) >= 'a, true)
    testPropagation('a > 2 && Literal(2) > 'a, true)

    testPropagation(('x || 'a < 2) && Literal(2) < 'a, false, Seq('x, Literal(2) < 'a))
    testPropagation(('x || 'a < 2) && Literal(2) <= 'a, false, Seq('x, Literal(2) <= 'a))
    testPropagation(('x || 'a < 2) && Literal(2) === 'a, false, Seq('x, Literal(2) === 'a))
    testPropagation(('x || 'a < 2) && Literal(2) <=> 'a, false, Seq('x, Literal(2) <=> 'a))
    testPropagation(('x || 'a < 2) && Literal(2) >= 'a, false, Seq('x || 'a < 2, 'a <= 2))
    testPropagation(('x || 'a < 2) && Literal(2) > 'a, false, Seq('a < 2))
    testPropagation(('x || 'a <= 2) && Literal(2) < 'a, false, Seq('x, Literal(2) < 'a))
    testPropagation(('x || 'a <= 2) && Literal(2) <= 'a, false,
      Seq('x || 'a === 2, Literal(2) <= 'a))
    testPropagation(('x || 'a <= 2) && Literal(2) === 'a, false, Seq(Literal(2) === 'a))
    testPropagation(('x || 'a <= 2) && Literal(2) <=> 'a, false, Seq(Literal(2) <=> 'a))
    testPropagation(('x || 'a <= 2) && Literal(2) >= 'a, false, Seq('a <= 2))
    testPropagation(('x || 'a <= 2) && Literal(2) > 'a, false, Seq('a < 2))
    testPropagation(('x || 'a === 2) && Literal(2) < 'a, false, Seq('x, Literal(2) < 'a))
    testPropagation(('x || 'a === 2) && Literal(2) <= 'a, false,
      Seq('x || 'a === 2, Literal(2) <= 'a))
    testPropagation(('x || 'a === 2) && Literal(2) === 'a, false, Seq(Literal(2) === 'a))
    testPropagation(('x || 'a === 2) && Literal(2) <=> 'a, false, Seq(Literal(2) <=> 'a))
    testPropagation(('x || 'a === 2) && Literal(2) >= 'a, false, Seq('x || 'a === 2, 'a <= 2))
    testPropagation(('x || 'a === 2) && Literal(2) > 'a, false, Seq('x, 'a < 2))
    testPropagation(('x || 'a <=> 2) && Literal(2) < 'a, false, Seq('x, Literal(2) < 'a))
    testPropagation(('x || 'a <=> 2) && Literal(2) <= 'a, false,
      Seq('x || 'a === 2, Literal(2) <= 'a))
    testPropagation(('x || 'a <=> 2) && Literal(2) === 'a, false, Seq(Literal(2) === 'a))
    testPropagation(('x || 'a <=> 2) && Literal(2) <=> 'a, false, Seq(Literal(2) <=> 'a))
    testPropagation(('x || 'a <=> 2) && Literal(2) >= 'a, false, Seq('x || 'a === 2, 'a <= 2))
    testPropagation(('x || 'a <=> 2) && Literal(2) > 'a, false, Seq('x, 'a < 2))
    testPropagation(('x || 'a >= 2) && Literal(2) < 'a, false, Seq(Literal(2) < 'a))
    testPropagation(('x || 'a >= 2) && Literal(2) <= 'a, false, Seq(Literal(2) <= 'a))
    testPropagation(('x || 'a >= 2) && Literal(2) === 'a, false, Seq(Literal(2) === 'a))
    testPropagation(('x || 'a >= 2) && Literal(2) <=> 'a, false, Seq(Literal(2) <=> 'a))
    testPropagation(('x || 'a >= 2) && Literal(2) >= 'a, false,
      Seq('x || 'a === 2, 'a <= Literal(2)))
    testPropagation(('x || 'a >= 2) && Literal(2) > 'a, false, Seq('x, 'a < Literal(2)))
    testPropagation(('x || 'a > 2) && Literal(2) < 'a, false, Seq(Literal(2) < 'a))
    testPropagation(('x || 'a > 2) && Literal(2) <= 'a, false,
      Seq('x || Literal(2) < 'a, Literal(2) <= 'a))
    testPropagation(('x || 'a > 2) && Literal(2) === 'a, false, Seq('x, Literal(2) === 'a))
    testPropagation(('x || 'a > 2) && Literal(2) <=> 'a, false, Seq('x, Literal(2) <=> 'a))
    testPropagation(('x || 'a > 2) && Literal(2) >= 'a, false, Seq('x, 'a <= Literal(2)))
    testPropagation(('x || 'a > 2) && Literal(2) > 'a, false, Seq('x, 'a < Literal(2)))

    testPropagation('a < 2 && Literal(3) < 'a, true)
    testPropagation('a < 2 && Literal(3) <= 'a, true)
    testPropagation('a < 2 && Literal(3) === 'a, true)
    testPropagation('a < 2 && Literal(3) <=> 'a, true)
    testPropagation('a < 2 && Literal(3) >= 'a, false, Seq('a < 2))
    testPropagation('a < 2 && Literal(3) > 'a, false, Seq('a < 2))
    testPropagation('a <= 2 && Literal(3) < 'a, true)
    testPropagation('a <= 2 && Literal(3) <= 'a, true)
    testPropagation('a <= 2 && Literal(3) === 'a, true)
    testPropagation('a <= 2 && Literal(3) <=> 'a, true)
    testPropagation('a <= 2 && Literal(3) >= 'a, false, Seq('a <= 2))
    testPropagation('a <= 2 && Literal(3) > 'a, false, Seq('a <= 2))
    testPropagation('a === 2 && Literal(3) < 'a, true)
    testPropagation('a === 2 && Literal(3) <= 'a, true)
    testPropagation('a === 2 && Literal(3) === 'a, true)
    testPropagation('a === 2 && Literal(3) <=> 'a, true)
    testPropagation('a === 2 && Literal(3) >= 'a, false, Seq('a === 2))
    testPropagation('a === 2 && Literal(3) > 'a, false, Seq('a === 2))
    testPropagation('a <=> 2 && Literal(3) < 'a, true)
    testPropagation('a <=> 2 && Literal(3) <= 'a, true)
    testPropagation('a <=> 2 && Literal(3) === 'a, true)
    testPropagation('a <=> 2 && Literal(3) <=> 'a, true)
    testPropagation('a <=> 2 && Literal(3) >= 'a, false, Seq('a <=> 2))
    testPropagation('a <=> 2 && Literal(3) > 'a, false, Seq('a <=> 2))
    testPropagation('a >= 2 && Literal(3) < 'a, false, Seq(Literal(3) < 'a))
    testPropagation('a >= 2 && Literal(3) <= 'a, false, Seq(Literal(3) <= 'a))
    testPropagation('a >= 2 && Literal(3) === 'a, false, Seq(Literal(3) === 'a))
    testPropagation('a >= 2 && Literal(3) <=> 'a, false, Seq(Literal(3) <=> 'a))
    testPropagation('a >= 2 && Literal(3) >= 'a, false, Seq(Literal(2) <= 'a, 'a <= 3))
    testPropagation('a >= 2 && Literal(3) > 'a, false, Seq(Literal(2) <= 'a, 'a < 3))
    testPropagation('a > 2 && Literal(3) < 'a, false, Seq(Literal(3) < 'a))
    testPropagation('a > 2 && Literal(3) <= 'a, false, Seq(Literal(3) <= 'a))
    testPropagation('a > 2 && Literal(3) === 'a, false, Seq(Literal(3) === 'a))
    testPropagation('a > 2 && Literal(3) <=> 'a, false, Seq(Literal(3) <=> 'a))
    testPropagation('a > 2 && Literal(3) >= 'a, false, Seq(Literal(2) < 'a, 'a <= 3))
    testPropagation('a > 2 && Literal(3) > 'a, false, Seq(Literal(2) < 'a, 'a < 3))

    testPropagation(('x || 'a < 2) && Literal(3) < 'a, false, Seq('x, Literal(3) < 'a))
    testPropagation(('x || 'a < 2) && Literal(3) <= 'a, false, Seq('x, Literal(3) <= 'a))
    testPropagation(('x || 'a < 2) && Literal(3) === 'a, false, Seq('x, Literal(3) === 'a))
    testPropagation(('x || 'a < 2) && Literal(3) <=> 'a, false, Seq('x, Literal(3) <=> 'a))
    testPropagation(('x || 'a < 2) && Literal(3) >= 'a, false, Seq('x || 'a < 2, 'a <= 3))
    testPropagation(('x || 'a < 2) && Literal(3) > 'a, false, Seq('x || 'a < 2, 'a < 3))
    testPropagation(('x || 'a <= 2) && Literal(3) < 'a, false, Seq('x, Literal(3) < 'a))
    testPropagation(('x || 'a <= 2) && Literal(3) <= 'a, false, Seq('x, Literal(3) <= 'a))
    testPropagation(('x || 'a <= 2) && Literal(3) === 'a, false, Seq('x, Literal(3) === 'a))
    testPropagation(('x || 'a <= 2) && Literal(3) <=> 'a, false, Seq('x, Literal(3) <=> 'a))
    testPropagation(('x || 'a <= 2) && Literal(3) >= 'a, false, Seq('x || 'a <= 2, 'a <= 3))
    testPropagation(('x || 'a <= 2) && Literal(3) > 'a, false, Seq('x || 'a <= 2, 'a < 3))
    testPropagation(('x || 'a === 2) && Literal(3) < 'a, false, Seq('x, Literal(3) < 'a))
    testPropagation(('x || 'a === 2) && Literal(3) <= 'a, false, Seq('x, Literal(3) <= 'a))
    testPropagation(('x || 'a === 2) && Literal(3) === 'a, false, Seq('x, Literal(3) === 'a))
    testPropagation(('x || 'a === 2) && Literal(3) <=> 'a, false, Seq('x, Literal(3) <=> 'a))
    testPropagation(('x || 'a === 2) && Literal(3) >= 'a, false, Seq('x || 'a === 2, 'a <= 3))
    testPropagation(('x || 'a === 2) && Literal(3) > 'a, false, Seq('x || 'a === 2, 'a < 3))
    testPropagation(('x || 'a <=> 2) && Literal(3) < 'a, false, Seq('x, Literal(3) < 'a))
    testPropagation(('x || 'a <=> 2) && Literal(3) <= 'a, false, Seq('x, Literal(3) <= 'a))
    testPropagation(('x || 'a <=> 2) && Literal(3) === 'a, false, Seq('x, Literal(3) === 'a))
    testPropagation(('x || 'a <=> 2) && Literal(3) <=> 'a, false, Seq('x, Literal(3) <=> 'a))
    testPropagation(('x || 'a <=> 2) && Literal(3) >= 'a, false, Seq('x || 'a === 2, 'a <= 3))
    testPropagation(('x || 'a <=> 2) && Literal(3) > 'a, false, Seq('x || 'a === 2, 'a < 3))
    testPropagation(('x || 'a >= 2) && Literal(3) < 'a, false, Seq(Literal(3) < 'a))
    testPropagation(('x || 'a >= 2) && Literal(3) <= 'a, false, Seq(Literal(3) <= 'a))
    testPropagation(('x || 'a >= 2) && Literal(3) === 'a, false, Seq(Literal(3) === 'a))
    testPropagation(('x || 'a >= 2) && Literal(3) <=> 'a, false, Seq(Literal(3) <=> 'a))
    testPropagation(('x || 'a >= 2) && Literal(3) >= 'a, false,
      Seq('x || Literal(2) <= 'a, 'a <= 3))
    testPropagation(('x || 'a >= 2) && Literal(3) > 'a, false, Seq('x || Literal(2) <= 'a, 'a < 3))
    testPropagation(('x || 'a > 2) && Literal(3) < 'a, false, Seq(Literal(3) < 'a))
    testPropagation(('x || 'a > 2) && Literal(3) <= 'a, false, Seq(Literal(3) <= 'a))
    testPropagation(('x || 'a > 2) && Literal(3) === 'a, false, Seq(Literal(3) === 'a))
    testPropagation(('x || 'a > 2) && Literal(3) <=> 'a, false, Seq(Literal(3) <=> 'a))
    testPropagation(('x || 'a > 2) && Literal(3) >= 'a, false, Seq('x || Literal(2) < 'a, 'a <= 3))
    testPropagation(('x || 'a > 2) && Literal(3) > 'a, false, Seq('x || Literal(2) < 'a, 'a < 3))

    testPropagation('a < 'b && 'b < 'a, true)
    testPropagation('a < 'b && 'b <= 'a, true)
    testPropagation('a < 'b && 'b === 'a, true)
    testPropagation('a < 'b && 'b <=> 'a, true)
    testPropagation('a < 'b && 'b >= 'a, false, Seq('a < 'b))
    testPropagation('a < 'b && 'b > 'a, false, Seq('a < 'b))
    testPropagation('a <= 'b && 'b < 'a, true)
    testPropagation('a <= 'b && 'b <= 'a, false, Seq('a === 'b))
    testPropagation('a <= 'b && 'b === 'a, false, Seq('a === 'b))
    testPropagation('a <= 'b && 'b <=> 'a, false, Seq('a === 'b))
    testPropagation('a <= 'b && 'b >= 'a, false, Seq('a <= 'b))
    testPropagation('a <= 'b && 'b > 'a, false, Seq('a < 'b))
    testPropagation('a === 'b && 'b < 'a, true)
    testPropagation('a === 'b && 'b <= 'a, false, Seq('a === 'b))
    testPropagation('a === 'b && 'b === 'a, false, Seq('a === 'b))
    testPropagation('a === 'b && 'b <=> 'a, false, Seq('a === 'b))
    testPropagation('a === 'b && 'b >= 'a, false, Seq('a === 'b))
    testPropagation('a === 'b && 'b > 'a, true)
    testPropagation('a <=> 'b && 'b < 'a, true)
    testPropagation('a <=> 'b && 'b <= 'a, false, Seq('a === 'b))
    testPropagation('a <=> 'b && 'b === 'a, false, Seq('a === 'b))
    testPropagation('a <=> 'b && 'b <=> 'a, false, Seq('a <=> 'b))
    testPropagation('a <=> 'b && 'b >= 'a, false, Seq('a === 'b))
    testPropagation('a <=> 'b && 'b > 'a, true)
    testPropagation('a >= 'b && 'b < 'a, false, Seq('b < 'a))
    testPropagation('a >= 'b && 'b <= 'a, false, Seq('b <= 'a))
    testPropagation('a >= 'b && 'b === 'a, false, Seq('a === 'b))
    testPropagation('a >= 'b && 'b <=> 'a, false, Seq('a === 'b))
    testPropagation('a >= 'b && 'b >= 'a, false, Seq('a === 'b))
    testPropagation('a >= 'b && 'b > 'a, true)
    testPropagation('a > 'b && 'b < 'a, false, Seq('b < 'a))
    testPropagation('a > 'b && 'b <= 'a, false, Seq('b < 'a))
    testPropagation('a > 'b && 'b === 'a, true)
    testPropagation('a > 'b && 'b <=> 'a, true)
    testPropagation('a > 'b && 'b >= 'a, true)
    testPropagation('a > 'b && 'b > 'a, true)

    testPropagation('a < abs('b) && abs('b) < 'a, true)
    testPropagation('a < abs('b) && abs('b) <= 'a, true)
    testPropagation('a < abs('b) && abs('b) === 'a, true)
    testPropagation('a < abs('b) && abs('b) <=> 'a, true)
    testPropagation('a < abs('b) && abs('b) >= 'a, false, Seq('a < abs('b)))
    testPropagation('a < abs('b) && abs('b) > 'a, false, Seq('a < abs('b)))
    testPropagation('a <= abs('b) && abs('b) < 'a, true)
    testPropagation('a <= abs('b) && abs('b) <= 'a, false, Seq('a === abs('b)))
    testPropagation('a <= abs('b) && abs('b) === 'a, false, Seq('a === abs('b)))
    testPropagation('a <= abs('b) && abs('b) <=> 'a, false, Seq('a === abs('b)))
    testPropagation('a <= abs('b) && abs('b) >= 'a, false, Seq('a <= abs('b)))
    testPropagation('a <= abs('b) && abs('b) > 'a, false, Seq('a < abs('b)))
    testPropagation('a === abs('b) && abs('b) < 'a, true)
    testPropagation('a === abs('b) && abs('b) <= 'a, false, Seq('a === abs('b)))
    testPropagation('a === abs('b) && abs('b) === 'a, false, Seq('a === abs('b)))
    testPropagation('a === abs('b) && abs('b) <=> 'a, false, Seq('a === abs('b)))
    testPropagation('a === abs('b) && abs('b) >= 'a, false, Seq('a === abs('b)))
    testPropagation('a === abs('b) && abs('b) > 'a, true)
    testPropagation('a <=> abs('b) && abs('b) < 'a, true)
    testPropagation('a <=> abs('b) && abs('b) <= 'a, false, Seq('a === abs('b)))
    testPropagation('a <=> abs('b) && abs('b) === 'a, false, Seq('a === abs('b)))
    testPropagation('a <=> abs('b) && abs('b) <=> 'a, false, Seq('a <=> abs('b)))
    testPropagation('a <=> abs('b) && abs('b) >= 'a, false, Seq('a === abs('b)))
    testPropagation('a <=> abs('b) && abs('b) > 'a, true)
    testPropagation('a >= abs('b) && abs('b) < 'a, false, Seq(abs('b) < 'a))
    testPropagation('a >= abs('b) && abs('b) <= 'a, false, Seq(abs('b) <= 'a))
    testPropagation('a >= abs('b) && abs('b) === 'a, false, Seq('a === abs('b)))
    testPropagation('a >= abs('b) && abs('b) <=> 'a, false, Seq('a === abs('b)))
    testPropagation('a >= abs('b) && abs('b) >= 'a, false, Seq('a === abs('b)))
    testPropagation('a >= abs('b) && abs('b) > 'a, true)
    testPropagation('a > abs('b) && abs('b) < 'a, false, Seq(abs('b) < 'a))
    testPropagation('a > abs('b) && abs('b) <= 'a, false, Seq(abs('b) < 'a))
    testPropagation('a > abs('b) && abs('b) === 'a, true)
    testPropagation('a > abs('b) && abs('b) <=> 'a, true)
    testPropagation('a > abs('b) && abs('b) >= 'a, true)
    testPropagation('a > abs('b) && abs('b) > 'a, true)

    testPropagation(('x || 'a < abs('b)) && abs('b) < 'a, false, Seq('x, abs('b) < 'a))
    testPropagation(('x || 'a < abs('b)) && abs('b) <= 'a, false, Seq('x, abs('b) <= 'a))
    testPropagation(('x || 'a < abs('b)) && abs('b) === 'a, false, Seq('x, abs('b) === 'a))
    testPropagation(('x || 'a < abs('b)) && abs('b) <=> 'a, false, Seq('x, abs('b) <=> 'a))
    testPropagation(('x || 'a < abs('b)) && abs('b) >= 'a, false,
      Seq('x || 'a < abs('b), 'a <= abs('b)))
    testPropagation(('x || 'a < abs('b)) && abs('b) > 'a, false, Seq('a < abs('b)))
    testPropagation(('x || 'a <= abs('b)) && abs('b) < 'a, false, Seq('x, abs('b) < 'a))
    testPropagation(('x || 'a <= abs('b)) && abs('b) <= 'a, false,
      Seq('x || 'a === abs('b), abs('b) <= 'a))
    testPropagation(('x || 'a <= abs('b)) && abs('b) === 'a, false, Seq(abs('b) === 'a))
    testPropagation(('x || 'a <= abs('b)) && abs('b) <=> 'a, false,
      Seq('x || 'a === abs('b), abs('b) <=> 'a))
    testPropagation(('x || 'a <= abs('b)) && abs('b) >= 'a, false, Seq('a <= abs('b)))
    testPropagation(('x || 'a <= abs('b)) && abs('b) > 'a, false, Seq('a < abs('b)))
    testPropagation(('x || 'a === abs('b)) && abs('b) < 'a, false, Seq('x, abs('b) < 'a))
    testPropagation(('x || 'a === abs('b)) && abs('b) <= 'a, false,
      Seq('x || 'a === abs('b), abs('b) <= 'a))
    testPropagation(('x || 'a === abs('b)) && abs('b) === 'a, false, Seq(abs('b) === 'a))
    testPropagation(('x || 'a === abs('b)) && abs('b) <=> 'a, false,
      Seq('x || 'a === abs('b), abs('b) <=> 'a))
    testPropagation(('x || 'a === abs('b)) && abs('b) >= 'a, false,
      Seq('x || 'a === abs('b), 'a <= abs('b)))
    testPropagation(('x || 'a === abs('b)) && abs('b) > 'a, false, Seq('x, 'a < abs('b)))
    testPropagation(('x || 'a <=> abs('b)) && abs('b) < 'a, false, Seq('x, abs('b) < 'a))
    testPropagation(('x || 'a <=> abs('b)) && abs('b) <= 'a, false,
      Seq('x || 'a === abs('b), abs('b) <= 'a))
    testPropagation(('x || 'a <=> abs('b)) && abs('b) === 'a, false, Seq(abs('b) === 'a))
    testPropagation(('x || 'a <=> abs('b)) && abs('b) <=> 'a, false, Seq(abs('b) <=> 'a))
    testPropagation(('x || 'a <=> abs('b)) && abs('b) >= 'a, false,
      Seq('x || 'a === abs('b), 'a <= abs('b)))
    testPropagation(('x || 'a <=> abs('b)) && abs('b) > 'a, false, Seq('x, 'a < abs('b)))
    testPropagation(('x || 'a >= abs('b)) && abs('b) < 'a, false, Seq(abs('b) < 'a))
    testPropagation(('x || 'a >= abs('b)) && abs('b) <= 'a, false, Seq(abs('b) <= 'a))
    testPropagation(('x || 'a >= abs('b)) && abs('b) === 'a, false, Seq(abs('b) === 'a))
    testPropagation(('x || 'a >= abs('b)) && abs('b) <=> 'a, false,
      Seq('x || 'a === abs('b), abs('b) <=> 'a))
    testPropagation(('x || 'a >= abs('b)) && abs('b) >= 'a, false,
      Seq('x || 'a === abs('b), 'a <= abs('b)))
    testPropagation(('x || 'a >= abs('b)) && abs('b) > 'a, false, Seq('x, 'a < abs('b)))
    testPropagation(('x || 'a > abs('b)) && abs('b) < 'a, false, Seq(abs('b) < 'a))
    testPropagation(('x || 'a > abs('b)) && abs('b) <= 'a, false,
      Seq('x || abs('b) < 'a, abs('b) <= 'a))
    testPropagation(('x || 'a > abs('b)) && abs('b) === 'a, false, Seq('x, abs('b) === 'a))
    testPropagation(('x || 'a > abs('b)) && abs('b) <=> 'a, false, Seq('x, abs('b) <=> 'a))
    testPropagation(('x || 'a > abs('b)) && abs('b) >= 'a, false, Seq('x, 'a <= abs('b)))
    testPropagation(('x || 'a > abs('b)) && abs('b) > 'a, false, Seq('x, 'a < abs('b)))
  }
}
