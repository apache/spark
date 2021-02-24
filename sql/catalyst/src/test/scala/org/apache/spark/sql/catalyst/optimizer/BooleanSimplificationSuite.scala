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
import org.apache.spark.sql.catalyst.catalog.{InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.types.BooleanType

class BooleanSimplificationSuite extends PlanTest with ExpressionEvalHelper with PredicateHelper {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("AnalysisNodes", Once,
        EliminateSubqueryAliases) ::
      Batch("Constant Folding", FixedPoint(50),
        NullPropagation,
        ConstantFolding,
        SimplifyConditionals,
        BooleanSimplification,
        PruneFilters) :: Nil
  }

  val testRelation = LocalRelation("a".attr.int, "b".attr.int, "c".attr.int, "d".attr.string,
    "e".attr.boolean, "f".attr.boolean, "g".attr.boolean, "h".attr.boolean)

  val testRelationWithData = LocalRelation.fromExternalRows(
    testRelation.output, Seq(Row(1, 2, 3, "abc"))
  )

  val testNotNullableRelation =
    LocalRelation("a".attr.int.notNull, "b".attr.int.notNull, "c".attr.int.notNull,
    "d".attr.string.notNull, "e".attr.boolean.notNull, "f".attr.boolean.notNull,
      "g".attr.boolean.notNull, "h".attr.boolean.notNull)

  val testNotNullableRelationWithData = LocalRelation.fromExternalRows(
    testNotNullableRelation.output, Seq(Row(1, 2, 3, "abc"))
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

  private def checkConditionInNotNullableRelation(
      input: Expression, expected: Expression): Unit = {
    val plan = testNotNullableRelationWithData.where(input).analyze
    val actual = Optimize.execute(plan)
    val correctAnswer = testNotNullableRelationWithData.where(expected).analyze
    comparePlans(actual, correctAnswer)
  }

  private def checkConditionInNotNullableRelation(
      input: Expression, expected: LogicalPlan): Unit = {
    val plan = testNotNullableRelationWithData.where(input).analyze
    val actual = Optimize.execute(plan)
    comparePlans(actual, expected)
  }

  test("a && a => a") {
    checkCondition(Literal(1) < "a".attr && Literal(1) < "a".attr, Literal(1) < "a".attr)
    checkCondition(Literal(1) < "a".attr && Literal(1) < "a".attr &&
      Literal(1) < "a".attr, Literal(1) < "a".attr)
  }

  test("a || a => a") {
    checkCondition(Literal(1) < "a".attr || Literal(1) < "a".attr,
      Literal(1) < "a".attr)
    checkCondition(Literal(1) < "a".attr || Literal(1) < "a".attr ||
      Literal(1) < "a".attr, Literal(1) < "a".attr)
  }

  test("(a && b && c && ...) || (a && b && d && ...) || (a && b && e && ...) ...") {
    checkCondition("b".attr > 3 || "c".attr > 5, "b".attr > 3 || "c".attr > 5)

    checkCondition(("a".attr < 2 && "a".attr > 3 && "b".attr > 5) || "a".attr < 2, "a".attr < 2)

    checkCondition("a".attr < 2 || ("a".attr < 2 && "a".attr > 3 && "b".attr > 5), "a".attr < 2)

    val input = ("a".attr === "b".attr && "b".attr > 3 && "c".attr > 2) ||
      ("a".attr === "b".attr && "c".attr < 1 && "a".attr === 5) ||
      ("a".attr === "b".attr && "b".attr < 5 && "a".attr > 1)

    val expected = "a".attr === "b".attr && (
      ("b".attr > 3 && "c".attr > 2) || ("c".attr < 1 && "a".attr === 5) ||
        ("b".attr < 5 && "a".attr > 1))

    checkCondition(input, expected)
  }

  test("(a || b || c || ...) && (a || b || d || ...) && (a || b || e || ...) ...") {
    checkCondition("b".attr > 3 && "c".attr > 5, "b".attr > 3 && "c".attr > 5)

    checkCondition(("a".attr < 2 || "a".attr > 3 || "b".attr > 5) && "a".attr < 2, "a".attr < 2)

    checkCondition("a".attr < 2 && ("a".attr < 2 || "a".attr > 3 || "b".attr > 5), "a".attr < 2)

    checkCondition(("a".attr < 2 || "b".attr > 3) && ("a".attr < 2 || "c".attr > 5), "a".attr < 2
      || ("b".attr > 3 && "c".attr > 5))

    checkCondition(
      ("a".attr === "b".attr || "b".attr > 3) && ("a".attr === "b".attr || "a".attr > 3) &&
        ("a".attr === "b".attr || "a".attr < 5),
      "a".attr === "b".attr || "b".attr > 3 && "a".attr > 3 && "a".attr < 5)
  }

  test("e && (!e || f) - not nullable") {
    checkConditionInNotNullableRelation("e".attr && (!"e".attr || "f".attr ), "e".attr && "f".attr)

    checkConditionInNotNullableRelation("e".attr && ("f".attr || !"e".attr ), "e".attr && "f".attr)

    checkConditionInNotNullableRelation((!"e".attr || "f".attr ) && "e".attr, "f".attr && "e".attr)

    checkConditionInNotNullableRelation(("f".attr || !"e".attr ) && "e".attr, "f".attr && "e".attr)
  }

  test("e && (!e || f) - nullable") {
    Seq ("e".attr && (!"e".attr || "f".attr ),
        "e".attr && ("f".attr || !"e".attr ),
        (!"e".attr || "f".attr ) && "e".attr,
        ("f".attr || !"e".attr ) && "e".attr,
        "e".attr || (!"e".attr && "f".attr),
        "e".attr || ("f".attr && !"e".attr),
        ("e".attr && "f".attr) || !"e".attr,
        ("f".attr && "e".attr) || !"e".attr).foreach { expr =>
      checkCondition(expr, expr)
    }
  }

  test("a < 1 && (!(a < 1) || f) - not nullable") {
    checkConditionInNotNullableRelation("a".attr < 1 && (!("a".attr < 1) || "f".attr),
      ("a".attr < 1) && "f".attr)
    checkConditionInNotNullableRelation("a".attr < 1 && ("f".attr || !("a".attr < 1)),
      ("a".attr < 1) && "f".attr)

    checkConditionInNotNullableRelation("a".attr <= 1 && (!("a".attr <= 1) || "f".attr),
      ("a".attr <= 1) && "f".attr)
    checkConditionInNotNullableRelation("a".attr <= 1 && ("f".attr || !("a".attr <= 1)),
      ("a".attr <= 1) && "f".attr)

    checkConditionInNotNullableRelation("a".attr > 1 && (!("a".attr > 1) || "f".attr),
      ("a".attr > 1) && "f".attr)
    checkConditionInNotNullableRelation("a".attr > 1 && ("f".attr || !("a".attr > 1)),
      ("a".attr > 1) && "f".attr)

    checkConditionInNotNullableRelation("a".attr >= 1 && (!("a".attr >= 1) || "f".attr),
      ("a".attr >= 1) && "f".attr)
    checkConditionInNotNullableRelation("a".attr >= 1 && ("f".attr || !("a".attr >= 1)),
      ("a".attr >= 1) && "f".attr)
  }

  test("a < 1 && ((a >= 1) || f) - not nullable") {
    checkConditionInNotNullableRelation("a".attr < 1 && ("a".attr >= 1 || "f".attr ),
      ("a".attr < 1) && "f".attr)
    checkConditionInNotNullableRelation("a".attr < 1 && ("f".attr || "a".attr >= 1),
      ("a".attr < 1) && "f".attr)

    checkConditionInNotNullableRelation("a".attr <= 1 && ("a".attr > 1 || "f".attr ),
      ("a".attr <= 1) && "f".attr)
    checkConditionInNotNullableRelation("a".attr <= 1 && ("f".attr || "a".attr > 1),
      ("a".attr <= 1) && "f".attr)

    checkConditionInNotNullableRelation("a".attr > 1 && (("a".attr <= 1) || "f".attr),
      ("a".attr > 1) && "f".attr)
    checkConditionInNotNullableRelation("a".attr > 1 && ("f".attr || ("a".attr <= 1)),
      ("a".attr > 1) && "f".attr)

    checkConditionInNotNullableRelation("a".attr >= 1 && (("a".attr < 1) || "f".attr),
      ("a".attr >= 1) && "f".attr)
    checkConditionInNotNullableRelation("a".attr >= 1 && ("f".attr || ("a".attr < 1)),
      ("a".attr >= 1) && "f".attr)
  }

  test("DeMorgan's law") {
    checkCondition(!("e".attr && "f".attr), !"e".attr || !"f".attr)

    checkCondition(!("e".attr || "f".attr), !"e".attr && !"f".attr)

    checkCondition(!(("e".attr && "f".attr) || ("g".attr && "h".attr)), (!"e".attr || !"f".attr) &&
      (!"g".attr || !"h".attr))

    checkCondition(!(("e".attr || "f".attr) && ("g".attr || "h".attr)), (!"e".attr && !"f".attr) ||
      (!"g".attr && !"h".attr))
  }

  private val analyzer = new Analyzer(
    new SessionCatalog(new InMemoryCatalog, EmptyFunctionRegistry))

  test("(a && b) || (a && c) => a && (b || c) when case insensitive") {
    val plan = analyzer.execute(
      testRelation.where(("a".attr > 2 && "b".attr > 3) || ("A".attr > 2 && "b".attr < 5)))
    val actual = Optimize.execute(plan)
    val expected = analyzer.execute(
      testRelation.where("a".attr > 2 && ("b".attr > 3 || "b".attr < 5)))
    comparePlans(actual, expected)
  }

  test("(a || b) && (a || c) => a || (b && c) when case insensitive") {
    val plan = analyzer.execute(
      testRelation.where(("a".attr > 2 || "b".attr > 3) && ("A".attr > 2 || "b".attr < 5)))
    val actual = Optimize.execute(plan)
    val expected = analyzer.execute(
      testRelation.where("a".attr > 2 || ("b".attr > 3 && "b".attr < 5)))
    comparePlans(actual, expected)
  }

  test("Complementation Laws") {
    checkConditionInNotNullableRelation("e".attr && !"e".attr, testNotNullableRelation)
    checkConditionInNotNullableRelation(!"e".attr && "e".attr, testNotNullableRelation)

    checkConditionInNotNullableRelation("e".attr || !"e".attr, testNotNullableRelationWithData)
    checkConditionInNotNullableRelation(!"e".attr || "e".attr, testNotNullableRelationWithData)
  }

  test("Complementation Laws - null handling") {
    checkCondition("e".attr && !"e".attr,
      testRelationWithData.where(And(Literal(null, BooleanType), "e".attr.isNull)).analyze)
    checkCondition(!"e".attr && "e".attr,
      testRelationWithData.where(And(Literal(null, BooleanType), "e".attr.isNull)).analyze)

    checkCondition("e".attr || !"e".attr,
      testRelationWithData.where(Or("e".attr.isNotNull, Literal(null, BooleanType))).analyze)
    checkCondition(!"e".attr || "e".attr,
      testRelationWithData.where(Or("e".attr.isNotNull, Literal(null, BooleanType))).analyze)
  }

  test("Complementation Laws - negative case") {
    checkCondition("e".attr && !"f".attr,
      testRelationWithData.where("e".attr && !"f".attr).analyze)
    checkCondition(!"f".attr && "e".attr,
      testRelationWithData.where(!"f".attr && "e".attr).analyze)

    checkCondition("e".attr || !"f".attr,
      testRelationWithData.where("e".attr || !"f".attr).analyze)
    checkCondition(!"f".attr || "e".attr,
      testRelationWithData.where(!"f".attr || "e".attr).analyze)
  }

  test("simplify NOT(IsNull(x)) and NOT(IsNotNull(x))") {
    checkCondition(Not(IsNotNull("b".attr)), IsNull("b".attr))
    checkCondition(Not(IsNull("b".attr)), IsNotNull("b".attr))
  }

  protected def assertEquivalent(e1: Expression, e2: Expression): Unit = {
    val correctAnswer = Project(Alias(e2, "out")() :: Nil, OneRowRelation()).analyze
    val actual = Optimize.execute(Project(Alias(e1, "out")() :: Nil, OneRowRelation()).analyze)
    comparePlans(actual, correctAnswer)
  }

  test("filter reduction - positive cases") {
    val fields = Seq(
      "col1NotNULL".attr.boolean.notNull,
      "col2NotNULL".attr.boolean.notNull
    )
    val Seq(col1NotNULL, col2NotNULL) = fields.zipWithIndex.map { case (f, i) => f.at(i) }

    val exprs = Seq(
      // actual expressions of the transformations: original -> transformed
      (col1NotNULL && (!col1NotNULL || col2NotNULL)) -> (col1NotNULL && col2NotNULL),
      (col1NotNULL && (col2NotNULL || !col1NotNULL)) -> (col1NotNULL && col2NotNULL),
      ((!col1NotNULL || col2NotNULL) && col1NotNULL) -> (col2NotNULL && col1NotNULL),
      ((col2NotNULL || !col1NotNULL) && col1NotNULL) -> (col2NotNULL && col1NotNULL),

      (col1NotNULL || (!col1NotNULL && col2NotNULL)) -> (col1NotNULL || col2NotNULL),
      (col1NotNULL || (col2NotNULL && !col1NotNULL)) -> (col1NotNULL || col2NotNULL),
      ((!col1NotNULL && col2NotNULL) || col1NotNULL) -> (col2NotNULL || col1NotNULL),
      ((col2NotNULL && !col1NotNULL) || col1NotNULL) -> (col2NotNULL || col1NotNULL)
    )

    // check plans
    for ((originalExpr, expectedExpr) <- exprs) {
      assertEquivalent(originalExpr, expectedExpr)
    }

    // check evaluation
    val binaryBooleanValues = Seq(true, false)
    for (col1NotNULLVal <- binaryBooleanValues;
        col2NotNULLVal <- binaryBooleanValues;
        (originalExpr, expectedExpr) <- exprs) {
      val inputRow = create_row(col1NotNULLVal, col2NotNULLVal)
      val optimizedVal = evaluateWithoutCodegen(expectedExpr, inputRow)
      checkEvaluation(originalExpr, optimizedVal, inputRow)
    }
  }
}
