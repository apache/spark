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

class BooleanSimplificationSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("AnalysisNodes", Once,
        EliminateAnalysisOperators) ::
      Batch("Constant Folding", FixedPoint(50),
        NullPropagation,
        ConstantFolding,
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

  test("a && a => a") {
    checkCondition(Literal(1) < 'a && Literal(1) < 'a, Literal(1) < 'a)
    checkCondition(Literal(1) < 'a && Literal(1) < 'a && Literal(1) < 'a, Literal(1) < 'a)
  }

  test("a || a => a") {
    checkCondition(Literal(1) < 'a || Literal(1) < 'a, Literal(1) < 'a)
    checkCondition(Literal(1) < 'a || Literal(1) < 'a || Literal(1) < 'a, Literal(1) < 'a)
  }

  test("(a && b && c && ...) || (a && b && d && ...) || (a && b && e && ...) ...") {
    checkCondition('b > 3 || 'c > 5, 'b > 3 || 'c > 5)

    checkCondition(('a < 2 && 'a > 3 && 'b > 5) || 'a < 2,  'a < 2)

    checkCondition('a < 2 || ('a < 2 && 'a > 3 && 'b > 5),  'a < 2)

    val input = ('a === 'b && 'b > 3 && 'c > 2) ||
      ('a === 'b && 'c < 1 && 'a === 5) ||
      ('a === 'b && 'b < 5 && 'a > 1)

    val expected =
      (((('b > 3) && ('c > 2)) ||
        (('c < 1) && ('a === 5))) ||
        (('b < 5) && ('a > 1))) && ('a === 'b)
    checkCondition(input, expected)

  }

  test("(a || b || c || ...) && (a || b || d || ...) && (a || b || e || ...) ...") {
    checkCondition('b > 3 && 'c > 5, 'b > 3 && 'c > 5)

    checkCondition(('a < 2 || 'a > 3 || 'b > 5) && 'a < 2, 'a < 2)

    checkCondition('a < 2 && ('a < 2 || 'a > 3 || 'b > 5) , 'a < 2)

    checkCondition(('a < 2 || 'b > 3) && ('a < 2 || 'c > 5), ('b > 3 && 'c > 5) || 'a < 2)

    var input: Expression = ('a === 'b || 'b > 3) && ('a === 'b || 'a > 3) && ('a === 'b || 'a < 5)
    var expected: Expression = ('b > 3 && 'a > 3 && 'a < 5) || 'a === 'b
    checkCondition(input, expected)
  }
}
