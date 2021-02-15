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

  val testRelation = LocalRelation(
    Symbol("a").int, Symbol("b").int, Symbol("c").int, Symbol("d").string,
    Symbol("e").boolean, Symbol("f").boolean, Symbol("g").boolean, Symbol("h").boolean)

  val testRelationWithData = LocalRelation.fromExternalRows(
    testRelation.output, Seq(Row(1, 2, 3, "abc"))
  )

  val testNotNullableRelation = LocalRelation(
    Symbol("a").int.notNull, Symbol("b").int.notNull, Symbol("c").int.notNull,
    Symbol("d").string.notNull, Symbol("e").boolean.notNull, Symbol("f").boolean.notNull,
    Symbol("g").boolean.notNull, Symbol("h").boolean.notNull)

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
    checkCondition(Literal(1) < Symbol("a") && Literal(1) < Symbol("a"), Literal(1) < Symbol("a"))
    checkCondition(Literal(1) < Symbol("a") && Literal(1) < Symbol("a") &&
      Literal(1) < Symbol("a"), Literal(1) < Symbol("a"))
  }

  test("a || a => a") {
    checkCondition(Literal(1) < Symbol("a") || Literal(1) < Symbol("a"), Literal(1) < Symbol("a"))
    checkCondition(Literal(1) < Symbol("a") || Literal(1) < Symbol("a") ||
      Literal(1) < Symbol("a"), Literal(1) < Symbol("a"))
  }

  test("(a && b && c && ...) || (a && b && d && ...) || (a && b && e && ...) ...") {
    checkCondition(Symbol("b") > 3 || Symbol("c") > 5, Symbol("b") > 3 || Symbol("c") > 5)

    checkCondition((Symbol("a") < 2 && Symbol("a") > 3 &&
      Symbol("b") > 5) || Symbol("a") < 2, Symbol("a") < 2)

    checkCondition(Symbol("a") < 2 || (Symbol("a") < 2 &&
      Symbol("a") > 3 && Symbol("b") > 5), Symbol("a") < 2)

    val input = (Symbol("a") === Symbol("b") && Symbol("b") > 3 && Symbol("c") > 2) ||
      (Symbol("a") === Symbol("b") && Symbol("c") < 1 && Symbol("a") === 5) ||
      (Symbol("a") === Symbol("b") && Symbol("b") < 5 && Symbol("a") > 1)

    val expected = Symbol("a") === Symbol("b") &&
      ((Symbol("b") > 3 && Symbol("c") > 2) || (Symbol("c") < 1 &&
        Symbol("a") === 5) || (Symbol("b") < 5 && Symbol("a") > 1))

    checkCondition(input, expected)
  }

  test("(a || b || c || ...) && (a || b || d || ...) && (a || b || e || ...) ...") {
    checkCondition(Symbol("b") > 3 && Symbol("c") > 5, Symbol("b") > 3 && Symbol("c") > 5)

    checkCondition((Symbol("a") < 2 || Symbol("a") > 3 ||
      Symbol("b") > 5) && Symbol("a") < 2, Symbol("a") < 2)

    checkCondition(Symbol("a") < 2 &&
      (Symbol("a") < 2 || Symbol("a") > 3 || Symbol("b") > 5), Symbol("a") < 2)

    checkCondition((Symbol("a") < 2 || Symbol("b") > 3) &&
      (Symbol("a") < 2 || Symbol("c") > 5), Symbol("a") < 2 || (Symbol("b") > 3 && Symbol("c") > 5))

    checkCondition(
      (Symbol("a") === Symbol("b") || Symbol("b") > 3) &&
        (Symbol("a") === Symbol("b") || Symbol("a") > 3) &&
        (Symbol("a") === Symbol("b") || Symbol("a") < 5),
      Symbol("a") === Symbol("b") || Symbol("b") > 3 && Symbol("a") > 3 && Symbol("a") < 5)
  }

  test("e && (!e || f) - not nullable") {
    checkConditionInNotNullableRelation(Symbol("e") &&
      (!Symbol("e") || Symbol("f") ), Symbol("e") && Symbol("f"))

    checkConditionInNotNullableRelation(Symbol("e") &&
      (Symbol("f") || !Symbol("e") ), Symbol("e") && Symbol("f"))

    checkConditionInNotNullableRelation((!Symbol("e") || Symbol("f") ) &&
      Symbol("e"), Symbol("f") && Symbol("e"))

    checkConditionInNotNullableRelation((Symbol("f") || !Symbol("e") ) &&
      Symbol("e"), Symbol("f") && Symbol("e"))
  }

  test("e && (!e || f) - nullable") {
    Seq (Symbol("e") && (!Symbol("e") || Symbol("f") ),
        Symbol("e") && (Symbol("f") || !Symbol("e") ),
        (!Symbol("e") || Symbol("f") ) && Symbol("e"),
        (Symbol("f") || !Symbol("e") ) && Symbol("e"),
        Symbol("e") || (!Symbol("e") && Symbol("f")),
        Symbol("e") || (Symbol("f") && !Symbol("e")),
        (Symbol("e") && Symbol("f")) || !Symbol("e"),
        (Symbol("f") && Symbol("e")) || !Symbol("e")).foreach { expr =>
      checkCondition(expr, expr)
    }
  }

  test("a < 1 && (!(a < 1) || f) - not nullable") {
    checkConditionInNotNullableRelation(Symbol("a") < 1 &&
      (!(Symbol("a") < 1) || Symbol("f")), (Symbol("a") < 1) && Symbol("f"))
    checkConditionInNotNullableRelation(Symbol("a") < 1 &&
      (Symbol("f") || !(Symbol("a") < 1)), (Symbol("a") < 1) && Symbol("f"))

    checkConditionInNotNullableRelation(Symbol("a") <= 1 &&
      (!(Symbol("a") <= 1) || Symbol("f")), (Symbol("a") <= 1) && Symbol("f"))
    checkConditionInNotNullableRelation(Symbol("a") <= 1 &&
      (Symbol("f") || !(Symbol("a") <= 1)), (Symbol("a") <= 1) && Symbol("f"))

    checkConditionInNotNullableRelation(Symbol("a") > 1 &&
      (!(Symbol("a") > 1) || Symbol("f")), (Symbol("a") > 1) && Symbol("f"))
    checkConditionInNotNullableRelation(Symbol("a") > 1 &&
      (Symbol("f") || !(Symbol("a") > 1)), (Symbol("a") > 1) && Symbol("f"))

    checkConditionInNotNullableRelation(Symbol("a") >= 1 &&
      (!(Symbol("a") >= 1) || Symbol("f")), (Symbol("a") >= 1) && Symbol("f"))
    checkConditionInNotNullableRelation(Symbol("a") >= 1 &&
      (Symbol("f") || !(Symbol("a") >= 1)), (Symbol("a") >= 1) && Symbol("f"))
  }

  test("a < 1 && ((a >= 1) || f) - not nullable") {
    checkConditionInNotNullableRelation(Symbol("a") < 1 &&
      (Symbol("a") >= 1 || Symbol("f") ), (Symbol("a") < 1) && Symbol("f"))
    checkConditionInNotNullableRelation(Symbol("a") < 1 &&
      (Symbol("f") || Symbol("a") >= 1), (Symbol("a") < 1) && Symbol("f"))

    checkConditionInNotNullableRelation(Symbol("a") <= 1 &&
      (Symbol("a") > 1 || Symbol("f") ), (Symbol("a") <= 1) && Symbol("f"))
    checkConditionInNotNullableRelation(Symbol("a") <= 1 &&
      (Symbol("f") || Symbol("a") > 1), (Symbol("a") <= 1) && Symbol("f"))

    checkConditionInNotNullableRelation(Symbol("a") > 1 &&
      ((Symbol("a") <= 1) || Symbol("f")), (Symbol("a") > 1) && Symbol("f"))
    checkConditionInNotNullableRelation(Symbol("a") > 1 &&
      (Symbol("f") || (Symbol("a") <= 1)), (Symbol("a") > 1) && Symbol("f"))

    checkConditionInNotNullableRelation(Symbol("a") >= 1 &&
      ((Symbol("a") < 1) || Symbol("f")), (Symbol("a") >= 1) && Symbol("f"))
    checkConditionInNotNullableRelation(Symbol("a") >= 1 &&
      (Symbol("f") || (Symbol("a") < 1)), (Symbol("a") >= 1) && Symbol("f"))
  }

  test("DeMorgan's law") {
    checkCondition(!(Symbol("e") && Symbol("f")), !Symbol("e") || !Symbol("f"))

    checkCondition(!(Symbol("e") || Symbol("f")), !Symbol("e") && !Symbol("f"))

    checkCondition(!((Symbol("e") && Symbol("f")) || (Symbol("g") &&
      Symbol("h"))), (!Symbol("e") || !Symbol("f")) && (!Symbol("g") || !Symbol("h")))

    checkCondition(!((Symbol("e") || Symbol("f")) &&
      (Symbol("g") || Symbol("h"))), (!Symbol("e") &&
      !Symbol("f")) || (!Symbol("g") && !Symbol("h")))
  }

  private val analyzer = new Analyzer(
    new SessionCatalog(new InMemoryCatalog, EmptyFunctionRegistry))

  test("(a && b) || (a && c) => a && (b || c) when case insensitive") {
    val plan = analyzer.execute(
      testRelation.where((Symbol("a") > 2 &&
        Symbol("b") > 3) || (Symbol("a") > 2 && Symbol("b") < 5)))
    val actual = Optimize.execute(plan)
    val expected = analyzer.execute(
      testRelation.where(Symbol("a") > 2 &&
        (Symbol("b") > 3 || Symbol("b") < 5)))
    comparePlans(actual, expected)
  }

  test("(a || b) && (a || c) => a || (b && c) when case insensitive") {
    val plan = analyzer.execute(
      testRelation.where((Symbol("a") > 2 || Symbol("b") > 3) &&
        (Symbol("a") > 2 || Symbol("b") < 5)))
    val actual = Optimize.execute(plan)
    val expected = analyzer.execute(
      testRelation.where(Symbol("a") > 2 || (Symbol("b") > 3 && Symbol("b") < 5)))
    comparePlans(actual, expected)
  }

  test("Complementation Laws") {
    checkConditionInNotNullableRelation(Symbol("e") && !Symbol("e"), testNotNullableRelation)
    checkConditionInNotNullableRelation(!Symbol("e") && Symbol("e"), testNotNullableRelation)

    checkConditionInNotNullableRelation(
      Symbol("e") || !Symbol("e"), testNotNullableRelationWithData)
    checkConditionInNotNullableRelation(
      !Symbol("e") || Symbol("e"), testNotNullableRelationWithData)
  }

  test("Complementation Laws - null handling") {
    checkCondition(Symbol("e") && !Symbol("e"),
      testRelationWithData.where(And(Literal(null, BooleanType), Symbol("e").isNull)).analyze)
    checkCondition(!Symbol("e") && Symbol("e"),
      testRelationWithData.where(And(Literal(null, BooleanType), Symbol("e").isNull)).analyze)

    checkCondition(Symbol("e") || !Symbol("e"),
      testRelationWithData.where(Or(Symbol("e").isNotNull, Literal(null, BooleanType))).analyze)
    checkCondition(!Symbol("e") || Symbol("e"),
      testRelationWithData.where(Or(Symbol("e").isNotNull, Literal(null, BooleanType))).analyze)
  }

  test("Complementation Laws - negative case") {
    checkCondition(Symbol("e") && !Symbol("f"),
      testRelationWithData.where(Symbol("e") && !Symbol("f")).analyze)
    checkCondition(!Symbol("f") && Symbol("e"),
      testRelationWithData.where(!Symbol("f") && Symbol("e")).analyze)

    checkCondition(Symbol("e") || !Symbol("f"),
      testRelationWithData.where(Symbol("e") || !Symbol("f")).analyze)
    checkCondition(!Symbol("f") || Symbol("e"),
      testRelationWithData.where(!Symbol("f") || Symbol("e")).analyze)
  }

  test("simplify NOT(IsNull(x)) and NOT(IsNotNull(x))") {
    checkCondition(Not(IsNotNull(Symbol("b"))), IsNull(Symbol("b")))
    checkCondition(Not(IsNull(Symbol("b"))), IsNotNull(Symbol("b")))
  }

  protected def assertEquivalent(e1: Expression, e2: Expression): Unit = {
    val correctAnswer = Project(Alias(e2, "out")() :: Nil, OneRowRelation()).analyze
    val actual = Optimize.execute(Project(Alias(e1, "out")() :: Nil, OneRowRelation()).analyze)
    comparePlans(actual, correctAnswer)
  }

  test("filter reduction - positive cases") {
    val fields = Seq(
      Symbol("col1NotNULL").boolean.notNull,
      Symbol("col2NotNULL").boolean.notNull
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
