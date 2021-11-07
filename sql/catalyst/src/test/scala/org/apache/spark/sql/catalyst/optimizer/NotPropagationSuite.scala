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
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.types.BooleanType

class NotPropagationSuite extends PlanTest with ExpressionEvalHelper {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("AnalysisNodes", Once, EliminateSubqueryAliases) ::
      Batch("Not Propagation", FixedPoint(50),
        NullPropagation,
        NullDownPropagation,
        ConstantFolding,
        SimplifyConditionals,
        BooleanSimplification,
        NotPropagation,
        PruneFilters) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int, 'd.string,
    'e.boolean, 'f.boolean, 'g.boolean, 'h.boolean)

  val testRelationWithData = LocalRelation.fromExternalRows(
    testRelation.output, Seq(Row(1, 2, 3, "abc"))
  )

  private def checkCondition(input: Expression, expected: LogicalPlan): Unit = {
    val plan = testRelationWithData.where(input).analyze
    val actual = Optimize.execute(plan)
    comparePlans(actual, expected)
  }

  private def checkCondition(input: Expression, expected: Expression): Unit = {
    val plan = testRelation.where(input).analyze
    val actual = Optimize.execute(plan)
    val correctAnswer = testRelation.where(expected).analyze
    comparePlans(actual, correctAnswer)
  }

  test("Using (Not(a) === b) == (a === Not(b)), (Not(a) <=> b) == (a <=> Not(b)) rules") {
    checkCondition(Not('e) === Literal(true), 'e === Literal(false))
    checkCondition(Not('e) === Literal(false), 'e === Literal(true))
    checkCondition(Not('e) === Literal(null, BooleanType), testRelation)
    checkCondition(Literal(true) === Not('e), Literal(false) === 'e)
    checkCondition(Literal(false) === Not('e), Literal(true) === 'e)
    checkCondition(Literal(null, BooleanType) === Not('e), testRelation)
    checkCondition(Not('e) <=> Literal(true), 'e <=> Literal(false))
    checkCondition(Not('e) <=> Literal(false), 'e <=> Literal(true))
    checkCondition(Not('e) <=> Literal(null, BooleanType), IsNull('e))
    checkCondition(Literal(true) <=> Not('e), Literal(false) <=> 'e)
    checkCondition(Literal(false) <=> Not('e), Literal(true) <=> 'e)
    checkCondition(Literal(null, BooleanType) <=> Not('e), IsNull('e))

    checkCondition(Not('e) === Not('f), 'e === 'f)
    checkCondition(Not('e) <=> Not('f), 'e <=> 'f)

    checkCondition(IsNull('e) === Not('f), IsNotNull('e) === 'f)
    checkCondition(Not('e) === IsNull('f), 'e === IsNotNull('f))
    checkCondition(IsNull('e) <=> Not('f), IsNotNull('e) <=> 'f)
    checkCondition(Not('e) <=> IsNull('f), 'e <=> IsNotNull('f))

    checkCondition(IsNotNull('e) === Not('f), IsNull('e) === 'f)
    checkCondition(Not('e) === IsNotNull('f), 'e === IsNull('f))
    checkCondition(IsNotNull('e) <=> Not('f), IsNull('e) <=> 'f)
    checkCondition(Not('e) <=> IsNotNull('f), 'e <=> IsNull('f))

    checkCondition(Not('e) === Not(And('f, 'g)), 'e === And('f, 'g))
    checkCondition(Not(And('e, 'f)) === Not('g), And('e, 'f) === 'g)
    checkCondition(Not('e) <=> Not(And('f, 'g)), 'e <=> And('f, 'g))
    checkCondition(Not(And('e, 'f)) <=> Not('g), And('e, 'f) <=> 'g)

    checkCondition(Not('e) === Not(Or('f, 'g)), 'e === Or('f, 'g))
    checkCondition(Not(Or('e, 'f)) === Not('g), Or('e, 'f) === 'g)
    checkCondition(Not('e) <=> Not(Or('f, 'g)), 'e <=> Or('f, 'g))
    checkCondition(Not(Or('e, 'f)) <=> Not('g), Or('e, 'f) <=> 'g)

    checkCondition(('a > 'b) === Not('f), ('a <= 'b) === 'f)
    checkCondition(Not('e) === ('a > 'b), 'e === ('a <= 'b))
    checkCondition(('a > 'b) <=> Not('f), ('a <= 'b) <=> 'f)
    checkCondition(Not('e) <=> ('a > 'b), 'e <=> ('a <= 'b))

    checkCondition(('a >= 'b) === Not('f), ('a < 'b) === 'f)
    checkCondition(Not('e) === ('a >= 'b), 'e === ('a < 'b))
    checkCondition(('a >= 'b) <=> Not('f), ('a < 'b) <=> 'f)
    checkCondition(Not('e) <=> ('a >= 'b), 'e <=> ('a < 'b))

    checkCondition(('a < 'b) === Not('f), ('a >= 'b) === 'f)
    checkCondition(Not('e) === ('a < 'b), 'e === ('a >= 'b))
    checkCondition(('a < 'b) <=> Not('f), ('a >= 'b) <=> 'f)
    checkCondition(Not('e) <=> ('a < 'b), 'e <=> ('a >= 'b))

    checkCondition(('a <= 'b) === Not('f), ('a > 'b) === 'f)
    checkCondition(Not('e) === ('a <= 'b), 'e === ('a > 'b))
    checkCondition(('a <= 'b) <=> Not('f), ('a > 'b) <=> 'f)
    checkCondition(Not('e) <=> ('a <= 'b), 'e <=> ('a > 'b))
  }

  test("Using (a =!= b) == (a === Not(b)), Not(a <=> b) == (a <=> Not(b)) rules") {
    checkCondition('e =!= Literal(true), 'e === Literal(false))
    checkCondition('e =!= Literal(false), 'e === Literal(true))
    checkCondition('e =!= Literal(null, BooleanType), testRelation)
    checkCondition(Literal(true) =!= 'e, Literal(false) === 'e)
    checkCondition(Literal(false) =!= 'e, Literal(true) === 'e)
    checkCondition(Literal(null, BooleanType) =!= 'e, testRelation)
    checkCondition(Not(('a <=> 'b) <=> Literal(true)), ('a <=> 'b) <=> Literal(false))
    checkCondition(Not(('a <=> 'b) <=> Literal(false)), ('a <=> 'b) <=> Literal(true))
    checkCondition(Not(('a <=> 'b) <=> Literal(null, BooleanType)), testRelationWithData)
    checkCondition(Not(Literal(true) <=> ('a <=> 'b)), Literal(false) <=> ('a <=> 'b))
    checkCondition(Not(Literal(false) <=> ('a <=> 'b)), Literal(true) <=> ('a <=> 'b))
    checkCondition(Not(Literal(null, BooleanType) <=> IsNull('e)), testRelationWithData)

    checkCondition('e =!= Not('f), 'e === 'f)
    checkCondition(Not('e) =!= 'f, 'e === 'f)
    checkCondition(Not(('a <=> 'b) <=> Not(('b <=> 'c))), ('a <=> 'b) <=> ('b <=> 'c))
    checkCondition(Not(Not(('a <=> 'b)) <=> ('b <=> 'c)), ('a <=> 'b) <=> ('b <=> 'c))

    checkCondition('e =!= IsNull('f), 'e === IsNotNull('f))
    checkCondition(IsNull('e) =!= 'f, IsNotNull('e) === 'f)
    checkCondition(Not(('a <=> 'b) <=> IsNull('f)), ('a <=> 'b) <=> IsNotNull('f))
    checkCondition(Not(IsNull('e) <=> ('b <=> 'c)), IsNotNull('e) <=> ('b <=> 'c))

    checkCondition('e =!= IsNotNull('f), 'e === IsNull('f))
    checkCondition(IsNotNull('e) =!= 'f, IsNull('e) === 'f)
    checkCondition(Not(('a <=> 'b) <=> IsNotNull('f)), ('a <=> 'b) <=> IsNull('f))
    checkCondition(Not(IsNotNull('e) <=> ('b <=> 'c)), IsNull('e) <=> ('b <=> 'c))

    checkCondition('e =!= Not(And('f, 'g)), 'e === And('f, 'g))
    checkCondition(Not(And('e, 'f)) =!= 'g, And('e, 'f) === 'g)
    checkCondition('e =!= Not(Or('f, 'g)), 'e === Or('f, 'g))
    checkCondition(Not(Or('e, 'f)) =!= 'g, Or('e, 'f) === 'g)

    checkCondition(('a > 'b) =!= 'f, ('a <= 'b) === 'f)
    checkCondition('e =!= ('a > 'b), 'e === ('a <= 'b))
    checkCondition(('a >= 'b) =!= 'f, ('a < 'b) === 'f)
    checkCondition('e =!= ('a >= 'b), 'e === ('a < 'b))
    checkCondition(('a < 'b) =!= 'f, ('a >= 'b) === 'f)
    checkCondition('e =!= ('a < 'b), 'e === ('a >= 'b))
    checkCondition(('a <= 'b) =!= 'f, ('a > 'b) === 'f)
    checkCondition('e =!= ('a <= 'b), 'e === ('a > 'b))

    checkCondition('e =!= ('f === ('g === Not('h))), 'e === ('f === ('g === 'h)))

  }

  test("Properly avoid non optimize-able cases") {
    checkCondition(Not(('a > 'b) <=> 'f), Not(('a > 'b) <=> 'f))
    checkCondition(Not('e <=> ('a > 'b)), Not('e <=> ('a > 'b)))
    checkCondition(('a === 'b) =!= ('a === 'c), ('a === 'b) =!= ('a === 'c))
    checkCondition(('a === 'b) =!= ('c in(1, 2, 3)), ('a === 'b) =!= ('c in(1, 2, 3)))
  }
}
