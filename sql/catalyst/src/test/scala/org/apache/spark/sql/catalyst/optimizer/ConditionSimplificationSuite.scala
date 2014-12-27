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
import org.apache.spark.sql.catalyst.expressions.{Literal, Expression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.dsl.expressions._

class ConditionSimplificationSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("AnalysisNodes", Once,
        EliminateAnalysisOperators) ::
        Batch("Constant Folding", FixedPoint(50),
          NullPropagation,
          ConstantFolding,
          ConditionSimplification,
          BooleanSimplification,
          SimplifyFilters) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int, 'd.string)

  def checkCondition(originCondition: Expression, optimizedCondition: Expression): Unit = {
    val originQuery = testRelation.where(originCondition).analyze
    val optimized = Optimize(originQuery)
    val expected = testRelation.where(optimizedCondition).analyze
    comparePlans(optimized, expected)
  }

  def checkCondition(originCondition: Expression): Unit = {
    val originQuery = testRelation.where(originCondition).analyze
    val optimized = Optimize(originQuery)
    val expected = testRelation
    comparePlans(optimized, expected)
  }

  test("literals in front of attribute") {
    checkCondition(Literal(1) < 'a || Literal(2) < 'a, Literal(1) < 'a)
    checkCondition(Literal(1) < 'a && Literal(2) < 'a, Literal(2) < 'a)
  }

  test("And/Or with the same conditions") {
    checkCondition('a < 1 || 'a < 1, 'a < 1)
    checkCondition('a < 1 || 'a < 1 || 'a < 1 || 'a < 1, 'a < 1)
    checkCondition('a > 2 && 'a > 2, 'a > 2)
    checkCondition('a > 2 && 'a > 2 && 'a > 2 && 'a > 2, 'a > 2)
    checkCondition(('a < 1 && 'a < 2) || ('a < 1 && 'a < 2), 'a < 1)
  }

  test("one And/Or with literal conditions") {
    checkCondition('a === 1 && 'a < 1)
    checkCondition('a === 1 || 'a < 1, 'a === 1 || 'a < 1)

    checkCondition('a === 1 && 'a === 2)
    checkCondition('a === 1 || 'a === 2, 'a === 1 || 'a === 2)

    checkCondition('a <= 1 && 'a > 1)
    checkCondition('a <= 1 || 'a > 1)

    checkCondition('a < 1 && 'a >= 1)
    checkCondition('a < 1 || 'a >= 1)

    checkCondition('a > 3 && 'a > 2, 'a > 3)
    checkCondition('a > 3 || 'a > 2, 'a > 2)

    checkCondition('a < 2 || 'a === 3 , 'a < 2 || 'a === 3)
    checkCondition('a === 3 || 'a > 5, 'a === 3 || 'a > 5)
    checkCondition('a < 2 || 'a > 5, 'a < 2 || 'a > 5)

    checkCondition('a >= 1 && 'a <= 1, 'a === 1)

  }

  test("different data type comparison") {
    checkCondition('a > "abc")
    checkCondition('a > "a" && 'a < "b")

    checkCondition('a > "a" || 'a < "b")

    checkCondition('a > "9" || 'a < "0", 'a > 9.0 || 'a < 0.0)
    checkCondition('d > 9 && 'd < 1, 'd > 9.0 && 'd < 1.0 )

    checkCondition('a > "9" || 'a < "0", 'a > 9.0 || 'a < 0.0)
  }

  test("two same And/Or with literal conditions") {
    checkCondition('a < 1 || 'b > 2 || 'a >= 1)
    checkCondition('a < 1 && 'b > 2 && 'a >= 1)

    checkCondition('a < 2 || 'b > 3 || 'b > 2,  'b > 2 || 'a < 2)
    checkCondition('a < 2 && 'b > 3 && 'b > 2, 'b > 3 && 'a < 2)

    checkCondition('a < 2 || ('b > 3 || 'b > 2), 'a < 2 || 'b > 2)
    checkCondition('a < 2 && ('b > 3 && 'b > 2), 'a < 2 && 'b > 3)

    checkCondition('a < 2 || 'a === 3 || 'a > 5, 'a < 2 || 'a === 3 || 'a > 5)
  }

  test("two different And/Or with literal conditions") {
    checkCondition(('a < 2 || 'a > 3) && 'a > 4, 'a > 4)
    checkCondition(('a < 2 || 'b > 3) && 'a < 2, 'a < 2)
    checkCondition(('a < 2 && 'b > 3) || 'a < 2, 'a < 2)
  }

  test("more than two And/Or: one child is literal condition") {
    checkCondition(('a < 2 || 'a > 3 || 'b > 5) && 'a < 2, 'a < 2)
    checkCondition(('a < 2 && 'a > 3 && 'b > 5) && 'a < 2)
    checkCondition(('a < 2 && 'a > 3 && 'b > 5) || 'a < 2,  'a < 2)
  }

  test("more than two And/Or") {
    checkCondition(('a < 2 || 'b > 3) && ('a < 2 || 'c > 5), ('b > 3 && 'c > 5) || 'a < 2)

    var input: Expression = ('a === 'b || 'b > 3) && ('a === 'b || 'a > 3) && ('a === 'b || 'a < 5)
    var expected: Expression = ('a > 3 && 'a < 5 && 'b > 3) || 'a === 'b
    checkCondition(input, expected)

    input = ('a === 'b || 'b > 3) && ('a === 'b || 'a > 3) && ('a === 'b || 'a > 1)
    expected = ('a > 3 && 'b > 3) || 'a === 'b
    checkCondition(input, expected)

    input = ('a === 'b && 'b > 3 && 'c > 2) ||
      ('a === 'b && 'c < 1 && 'a === 5) ||
      ('a === 'b && 'b < 5 && 'a > 1)

    expected =
      (((('b > 3) && ('c > 2)) ||
        (('c < 1) && ('a === 5))) ||
        (('b < 5) && ('a > 1))) && ('a === 'b)
    checkCondition(input, expected)

    input = ('a < 2 || 'b > 5 || 'a < 2 || 'b > 1) && ('a < 2 || 'b > 1)
    expected = 'a < 2 || 'b > 1
    checkCondition(input, expected)

    input = ('a === 'b || 'b > 5) && ('a === 'b || 'c > 3) && ('a === 'b || 'b > 1)
    expected = ('b > 5 && 'c > 3) ||  ('a === 'b)
    checkCondition(input, expected)
  }
}
