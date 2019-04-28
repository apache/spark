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

import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf

/**
 * Unit tests for constant propagation in expressions.
 */
class ConstantPropagationSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("AnalysisNodes", Once,
        EliminateSubqueryAliases) ::
        Batch("ConstantPropagation", FixedPoint(10),
          ConstantPropagation,
          ConstantFolding,
          BooleanSimplification) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int, 'd.int.notNull, 'e.int.notNull)

  private val columnA = 'a
  private val columnB = 'b
  private val columnC = 'c
  private val columnD = 'd
  private val columnE = 'e

  test("basic test") {
    val query = testRelation
      .select(columnA)
      .where(columnA === Add(columnB, Literal(1)) && columnB === Literal(10))

    val correctAnswer =
      testRelation
        .select(columnA)
        .where(columnA === Literal(11) && columnB === Literal(10)).analyze

    comparePlans(Optimize.execute(query.analyze), correctAnswer)
  }

  test("with combination of AND and OR predicates") {
    val query = testRelation
      .select(columnA)
      .where(
        columnA === Add(columnB, Literal(1)) &&
          columnB === Literal(10) &&
          (columnA === Add(columnC, Literal(3)) || columnB === columnC))
      .analyze

    val correctAnswer =
      testRelation
        .select(columnA)
        .where(
          columnA === Literal(11) &&
            columnB === Literal(10) &&
            (Literal(11) === Add(columnC, Literal(3)) || Literal(10) === columnC))
        .analyze

    comparePlans(Optimize.execute(query), correctAnswer)
  }

  test("equality predicates outside a `NOT` can be propagated within a `NOT`") {
    val query = testRelation
      .select(columnA)
      .where(Not(columnA === Add(columnB, Literal(1))) && columnB === Literal(10))
      .analyze

    val correctAnswer =
      testRelation
        .select(columnA)
        .where(Not(columnA === Literal(11)) && columnB === Literal(10))
        .analyze

    comparePlans(Optimize.execute(query), correctAnswer)
  }

  test("equality predicates inside a `NOT` should not be picked for propagation") {
    val query = testRelation
      .select(columnA)
      .where(Not(columnB === Literal(10)) && columnA === Add(columnB, Literal(1)))
      .analyze

    comparePlans(Optimize.execute(query), query)
  }

  test("equality predicates outside a `OR` can be propagated within a `OR`") {
    val query = testRelation
      .select(columnA)
      .where(
        columnA === Literal(2) &&
          (columnA === Add(columnB, Literal(3)) || columnB === Literal(9)))
      .analyze

    val correctAnswer = testRelation
      .select(columnA)
      .where(
        columnA === Literal(2) &&
          (Literal(2) === Add(columnB, Literal(3)) || columnB === Literal(9)))
      .analyze

    comparePlans(Optimize.execute(query), correctAnswer)
  }

  test("equality predicates inside a `OR` should not be picked for propagation") {
    val query = testRelation
      .select(columnA)
      .where(
        columnA === Add(columnB, Literal(2)) &&
          (columnA === Add(columnB, Literal(3)) || columnB === Literal(9)))
      .analyze

    comparePlans(Optimize.execute(query), query)
  }

  test("equality operator not immediate child of root `AND` should not be used for propagation") {
    val query = testRelation
      .select(columnA)
      .where(
        columnA === Literal(0) &&
          ((columnB === columnA) === (columnB === Literal(0))))
      .analyze

    val correctAnswer = testRelation
      .select(columnA)
      .where(
        columnA === Literal(0) &&
          ((columnB === Literal(0)) === (columnB === Literal(0))))
      .analyze

    comparePlans(Optimize.execute(query), correctAnswer)
  }

  test("conflicting equality predicates") {
    val query = testRelation
      .where(
        columnA === Literal(1) && columnA === Literal(2) && columnB === Add(columnA, Literal(3)))
      .analyze

    val correctAnswer = testRelation.where(Literal.FalseLiteral)

    comparePlans(Optimize.execute(query.analyze), correctAnswer)
  }

  test("SPARK-30447: take nullability into account") {
    val query = testRelation
      .select(columnA)
      .where(!(columnA === Literal(1) && Add(columnA, 1) === Literal(1)))
      .analyze
    val correctAnswer = testRelation
      .select(columnA)
      .where(columnA =!= Literal(1) || Add(columnA, 1) =!= Literal(1))
      .analyze
    comparePlans(Optimize.execute(query), correctAnswer)

    val query2 = testRelation
      .select(columnD)
      .where(!(columnD === Literal(1) && Add(columnD, 1) === Literal(1)))
      .analyze
    val correctAnswer2 = testRelation
      .select(columnD)
      .where(true)
      .analyze
    comparePlans(Optimize.execute(query2), correctAnswer2)
  }

  test("Constant propagation in conflicting equalities") {
    val query = testRelation
      .select(columnA)
      .where(columnA === Literal(1) && columnA === Literal(2))
      .analyze
    val correctAnswer = testRelation
      .select(columnA)
      .where(Literal.FalseLiteral)
      .analyze
    comparePlans(Optimize.execute(query), correctAnswer)
  }

  test("Enhanced constant propagation") {
    def testSelect(expression: Expression, expected: Expression): Unit = {
      val plan = testRelation.select(expression.as("x")).analyze
      val expectedPlan = testRelation.select(expected.as("x")).analyze
      comparePlans(Optimize.execute(plan), expectedPlan)
    }

    def testFilter(expression: Expression, expected: Expression): Unit = {
      val plan = testRelation.select(columnA).where(expression).analyze
      val expectedPlan = testRelation.select(columnA).where(expected).analyze
      comparePlans(Optimize.execute(plan), expectedPlan)
    }

    val nullable =
      abs(columnA) === Literal(1) && columnB === Literal(1) && abs(columnA) <= columnB
    val reducedNullable = abs(columnA) === Literal(1) && columnB === Literal(1)

    val nonNullable =
      abs(columnD) === Literal(1) && columnE === Literal(1) && abs(columnD) <= columnE
    val reducedNonNullable = abs(columnD) === Literal(1) && columnE === Literal(1)

    val expression = nullable || nonNullable
    val partlyReduced = nullable || reducedNonNullable
    val reduced = reducedNullable || reducedNonNullable

    val simplifiedNegatedNullable =
      abs(columnA) =!= Literal(1) || columnB =!= Literal(1) || abs(columnA) > columnB
    val reducedSimplifiedNegatedNullable = abs(columnA) =!= Literal(1) || columnB =!= Literal(1)

    val reducedSimplifiedNegatedNonNullable = abs(columnD) =!= Literal(1) || columnE =!= Literal(1)

    val partlyReducedSimplifiedNegated =
      simplifiedNegatedNullable && reducedSimplifiedNegatedNonNullable
    val reducedSimplifiedNegated =
      reducedSimplifiedNegatedNullable && reducedSimplifiedNegatedNonNullable

    testSelect(expression, partlyReduced)
    testSelect(If(expression, expression, expression),
      If(reduced, partlyReduced, partlyReduced))
    testSelect(CaseWhen(Seq((expression, expression)), expression),
      CaseWhen(Seq((reduced, partlyReduced)), partlyReduced))
    testSelect(ArrayFilter(CreateArray(Seq(expression)), LambdaFunction(expression, Nil)),
      ArrayFilter(CreateArray(Seq(partlyReduced)), LambdaFunction(reduced, Nil)))
    Seq(true, false).foreach { tvl =>
      withSQLConf(SQLConf.LEGACY_ARRAY_EXISTS_FOLLOWS_THREE_VALUED_LOGIC.key -> s"$tvl") {
        testSelect(ArrayExists(CreateArray(Seq(expression)), LambdaFunction(expression, Nil)),
          ArrayExists(CreateArray(Seq(partlyReduced)),
            LambdaFunction(if (tvl) partlyReduced else reduced, Nil)))
      }
    }
    testSelect(MapFilter(CreateMap(Seq(expression, expression)), LambdaFunction(expression, Nil)),
      MapFilter(CreateMap(Seq(partlyReduced, partlyReduced)), LambdaFunction(reduced, Nil)))
    testSelect(Not(If(expression, Not(expression), Not(expression))),
      Not(If(reduced, partlyReducedSimplifiedNegated, partlyReducedSimplifiedNegated)))

    testFilter(expression, reduced)
    testFilter(If(expression, expression, expression),
      If(reduced, reduced, reduced))
    testFilter(CaseWhen(Seq((expression, expression)), expression),
      CaseWhen(Seq((reduced, reduced)), reduced))
    testFilter(
      GetArrayItem(ArrayFilter(CreateArray(Seq(expression)), LambdaFunction(expression, Nil)), 1),
      GetArrayItem(ArrayFilter(CreateArray(Seq(reduced)), LambdaFunction(reduced, Nil)), 1))
    Seq(true, false).foreach { tvl =>
      withSQLConf(SQLConf.LEGACY_ARRAY_EXISTS_FOLLOWS_THREE_VALUED_LOGIC.key -> s"$tvl") {
        testFilter(ArrayExists(CreateArray(Seq(expression)), LambdaFunction(expression, Nil)),
          ArrayExists(CreateArray(Seq(reduced)),
            LambdaFunction(if (tvl) partlyReduced else reduced, Nil)))
      }
    }
    testFilter(
      GetMapValue(MapFilter(CreateMap(Seq(expression, expression)),
        LambdaFunction(expression, Nil)), true),
      GetMapValue(MapFilter(CreateMap(Seq(reduced, reduced)), LambdaFunction(reduced, Nil)), true))
    testFilter(Not(If(expression, Not(expression), Not(expression))),
      Not(If(reduced, reducedSimplifiedNegated, reducedSimplifiedNegated)))
  }
}
