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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{And, CaseWhen, Coalesce, Expression, If, IsNotNull, Literal, Not, Or, Rand}
import org.apache.spark.sql.catalyst.expressions.Literal.{FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.plans.{Inner, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{DeleteFromTable, LocalRelation, LogicalPlan, UpdateTable}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.{BooleanType, IntegerType}

class SimplifyConditionalsInPredicateSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("SimplifyConditionalsInPredicate", FixedPoint(10),
        NullPropagation,
        ConstantFolding,
        BooleanSimplification,
        SimplifyConditionals,
        SimplifyConditionalsInPredicate) :: Nil
  }

  private val testRelation =
    LocalRelation($"i".int, $"b".boolean, $"a".array(IntegerType), Symbol("m")
      .map(IntegerType, IntegerType))
  private val anotherTestRelation = LocalRelation($"d".int)

  test("IF(cond, trueVal, false) => AND(cond, trueVal)") {
    val originalCond = If(
      UnresolvedAttribute("i") > Literal(10),
      UnresolvedAttribute("b"),
      FalseLiteral)
    val expectedCond = And(
      UnresolvedAttribute("i") > Literal(10),
      UnresolvedAttribute("b"))
    testFilter(originalCond, expectedCond = expectedCond)
    testJoin(originalCond, expectedCond = expectedCond)
    testDelete(originalCond, expectedCond = expectedCond)
    testUpdate(originalCond, expectedCond = expectedCond)
    testProjection(originalCond, expectedExpr = originalCond)
  }

  test("IF(cond, trueVal, true) => OR(NOT(cond), trueVal)") {
    val originalCond = If(
      UnresolvedAttribute("i") > Literal(10),
      UnresolvedAttribute("b"),
      TrueLiteral)
    val expectedCond = Or(
      Not(Coalesce(Seq(UnresolvedAttribute("i") > Literal(10), FalseLiteral))),
      UnresolvedAttribute("b"))
    testFilter(originalCond, expectedCond = expectedCond)
    testJoin(originalCond, expectedCond = expectedCond)
    testDelete(originalCond, expectedCond = expectedCond)
    testUpdate(originalCond, expectedCond = expectedCond)
    testProjection(originalCond, expectedExpr = originalCond)
  }

  test("IF(cond, false, falseVal) => AND(NOT(cond), elseVal)") {
    val originalCond = If(
      UnresolvedAttribute("i") > Literal(10),
      FalseLiteral,
      UnresolvedAttribute("b"))
    val expectedCond = And(
      Not(Coalesce(Seq(UnresolvedAttribute("i") > Literal(10), FalseLiteral))),
      UnresolvedAttribute("b"))
    testFilter(originalCond, expectedCond = expectedCond)
    testJoin(originalCond, expectedCond = expectedCond)
    testDelete(originalCond, expectedCond = expectedCond)
    testUpdate(originalCond, expectedCond = expectedCond)
    testProjection(originalCond, expectedExpr = originalCond)
  }

  test("IF(cond, true, falseVal) => OR(cond, elseVal)") {
    val originalCond = If(
      UnresolvedAttribute("i") > Literal(10),
      TrueLiteral,
      UnresolvedAttribute("b"))
    val expectedCond = Or(
      UnresolvedAttribute("i") > Literal(10),
      UnresolvedAttribute("b"))
    testFilter(originalCond, expectedCond = expectedCond)
    testJoin(originalCond, expectedCond = expectedCond)
    testDelete(originalCond, expectedCond = expectedCond)
    testUpdate(originalCond, expectedCond = expectedCond)
    testProjection(originalCond, expectedExpr = originalCond)
  }

  test("CASE WHEN cond THEN trueVal ELSE false END => AND(cond, trueVal)") {
    Seq(Some(FalseLiteral), None, Some(Literal(null, BooleanType))).foreach { elseExp =>
      val originalCond = CaseWhen(
        Seq((UnresolvedAttribute("i") > Literal(10), UnresolvedAttribute("b"))),
        elseExp)
      val expectedCond = And(
        UnresolvedAttribute("i") > Literal(10),
        UnresolvedAttribute("b"))
      testFilter(originalCond, expectedCond = expectedCond)
      testJoin(originalCond, expectedCond = expectedCond)
      testDelete(originalCond, expectedCond = expectedCond)
      testUpdate(originalCond, expectedCond = expectedCond)
      testProjection(originalCond,
        expectedExpr = CaseWhen(
          Seq((UnresolvedAttribute("i") > Literal(10), UnresolvedAttribute("b"))),
          elseExp.filterNot(_.semanticEquals(Literal(null, BooleanType)))))
    }
  }

  test("CASE WHEN cond THEN trueVal ELSE true END => OR(NOT(cond), trueVal)") {
    val originalCond = CaseWhen(
      Seq((UnresolvedAttribute("i") > Literal(10), UnresolvedAttribute("b"))),
      TrueLiteral)
    val expectedCond = Or(
      Not(Coalesce(Seq(UnresolvedAttribute("i") > Literal(10), FalseLiteral))),
      UnresolvedAttribute("b"))
    testFilter(originalCond, expectedCond = expectedCond)
    testJoin(originalCond, expectedCond = expectedCond)
    testDelete(originalCond, expectedCond = expectedCond)
    testUpdate(originalCond, expectedCond = expectedCond)
    testProjection(originalCond, expectedExpr = originalCond)
  }

  test("CASE WHEN cond THEN false ELSE elseVal END => AND(NOT(cond), elseVal)") {
    val originalCond = CaseWhen(
      Seq((UnresolvedAttribute("i") > Literal(10), FalseLiteral)),
      UnresolvedAttribute("b"))
    val expectedCond = And(
      Not(Coalesce(Seq(UnresolvedAttribute("i") > Literal(10), FalseLiteral))),
      UnresolvedAttribute("b"))
    testFilter(originalCond, expectedCond = expectedCond)
    testJoin(originalCond, expectedCond = expectedCond)
    testDelete(originalCond, expectedCond = expectedCond)
    testUpdate(originalCond, expectedCond = expectedCond)
    testProjection(originalCond, expectedExpr = originalCond)
  }

  test("CASE WHEN cond THEN false END => false") {
    val originalCond = CaseWhen(
      Seq((UnresolvedAttribute("i") > Literal(10), FalseLiteral)))
    testFilter(originalCond, expectedCond = FalseLiteral)
    testJoin(originalCond, expectedCond = FalseLiteral)
    testDelete(originalCond, expectedCond = FalseLiteral)
    testUpdate(originalCond, expectedCond = FalseLiteral)
    testProjection(originalCond, expectedExpr = originalCond)
  }

  test("CASE WHEN non-deterministic-cond THEN false END") {
    val originalCond =
      CaseWhen(Seq((UnresolvedAttribute("i") > Rand(0), FalseLiteral)))
    val expectedCond = And(UnresolvedAttribute("i") > Rand(0), FalseLiteral)
    // nondeterministic expressions are only allowed in Project, Filter, Aggregate or Window,
    testFilter(originalCond, expectedCond = FalseLiteral)
    testProjection(originalCond, expectedExpr = originalCond)
  }

  test("CASE WHEN cond THEN true ELSE elseVal END  => OR(cond, elseVal)") {
    val originalCond = CaseWhen(
      Seq((UnresolvedAttribute("i") > Literal(10), TrueLiteral)),
      UnresolvedAttribute("b"))
    val expectedCond = Or(
      UnresolvedAttribute("i") > Literal(10),
      UnresolvedAttribute("b"))
    testFilter(originalCond, expectedCond = expectedCond)
    testJoin(originalCond, expectedCond = expectedCond)
    testDelete(originalCond, expectedCond = expectedCond)
    testUpdate(originalCond, expectedCond = expectedCond)
    testProjection(originalCond, expectedExpr = originalCond)
  }

  test("CASE WHEN cond THEN true END => cond") {
    val originalCond = CaseWhen(
      Seq((UnresolvedAttribute("i") > Literal(10), TrueLiteral)))
    val expectedCond = UnresolvedAttribute("i") > Literal(10)
    testFilter(originalCond, expectedCond = expectedCond)
    testJoin(originalCond, expectedCond = expectedCond)
    testDelete(originalCond, expectedCond = expectedCond)
    testUpdate(originalCond, expectedCond = expectedCond)
    testProjection(originalCond, expectedExpr = originalCond)
  }

  test("Simplify conditional in conditions of CaseWhen inside another CaseWhen") {
    val nestedCaseWhen = CaseWhen(
      Seq((UnresolvedAttribute("i") > Literal(10)) -> UnresolvedAttribute("b")),
      FalseLiteral)
    val originalCond = CaseWhen(Seq(IsNotNull(nestedCaseWhen) -> FalseLiteral))
    val expectedCond = FalseLiteral

    testFilter(originalCond, expectedCond = expectedCond)
    testJoin(originalCond, expectedCond = expectedCond)
    testDelete(originalCond, expectedCond = expectedCond)
    testUpdate(originalCond, expectedCond = expectedCond)
    testProjection(originalCond, expectedExpr = originalCond)
  }

  test("Not expected type - SimplifyConditionalsInPredicate") {
    val e = intercept[AnalysisException] {
      testFilter(originalCond = Literal(null, IntegerType), expectedCond = FalseLiteral)
    }.getMessage
    assert(e.contains("'CAST(NULL AS INT)' of type int is not a boolean"))
  }

  private def testFilter(originalCond: Expression, expectedCond: Expression): Unit = {
    test((rel, exp) => rel.where(exp), originalCond, expectedCond)
  }

  private def testJoin(originalCond: Expression, expectedCond: Expression): Unit = {
    test((rel, exp) => rel.join(anotherTestRelation, Inner, Some(exp)), originalCond, expectedCond)
  }

  private def testProjection(originalExpr: Expression, expectedExpr: Expression): Unit = {
    test((rel, exp) => rel.select(exp), originalExpr.as("out"), expectedExpr.as("out"))
  }

  private def testDelete(originalCond: Expression, expectedCond: Expression): Unit = {
    test((rel, expr) => DeleteFromTable(rel, expr), originalCond, expectedCond)
  }

  private def testUpdate(originalCond: Expression, expectedCond: Expression): Unit = {
    test((rel, expr) => UpdateTable(rel, Seq.empty, Some(expr)), originalCond, expectedCond)
  }

  private def test(
      func: (LogicalPlan, Expression) => LogicalPlan,
      originalExpr: Expression,
      expectedExpr: Expression): Unit = {

    val originalPlan = func(testRelation, originalExpr).analyze
    val optimizedPlan = Optimize.execute(originalPlan)
    val expectedPlan = func(testRelation, expectedExpr).analyze
    comparePlans(optimizedPlan, expectedPlan)
  }
}
