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

  val testRelation = LocalRelation($"a".int, $"b".int, $"c".int, $"d".string,
    $"e".boolean, $"f".boolean, $"g".boolean, $"h".boolean)

  val testRelationWithData = LocalRelation.fromExternalRows(
    testRelation.output, Seq(Row(1, 2, 3, "abc"))
  )

  val testNotNullableRelation = LocalRelation($"a".int.notNull, $"b".int.notNull, $"c".int.notNull,
    $"d".string.notNull, $"e".boolean.notNull, $"f".boolean.notNull, $"g".boolean.notNull,
    $"h".boolean.notNull)

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
    checkCondition(Literal(1) < $"a" && Literal(1) < $"a", Literal(1) < $"a")
    checkCondition(Literal(1) < $"a" && Literal(1) < $"a" && Literal(1) < $"a", Literal(1) < $"a")
  }

  test("a || a => a") {
    checkCondition(Literal(1) < $"a" || Literal(1) < $"a", Literal(1) < $"a")
    checkCondition(Literal(1) < $"a" || Literal(1) < $"a" || Literal(1) < $"a", Literal(1) < $"a")
  }

  test("(a && b && c && ...) || (a && b && d && ...) || (a && b && e && ...) ...") {
    checkCondition($"b" > 3 || $"c" > 5, $"b" > 3 || $"c" > 5)

    checkCondition(($"a" < 2 && $"a" > 3 && $"b" > 5) || $"a" < 2, $"a" < 2)

    checkCondition($"a" < 2 || ($"a" < 2 && $"a" > 3 && $"b" > 5), $"a" < 2)

    val input = ($"a" === $"b" && $"b" > 3 && $"c" > 2) ||
      ($"a" === $"b" && $"c" < 1 && $"a" === 5) ||
      ($"a" === $"b" && $"b" < 5 && $"a" > 1)

    val expected = $"a" === $"b" && (
      ($"b" > 3 && $"c" > 2) || ($"c" < 1 && $"a" === 5) || ($"b" < 5 && $"a" > 1))

    checkCondition(input, expected)
  }

  test("(a || b || c || ...) && (a || b || d || ...) && (a || b || e || ...) ...") {
    checkCondition($"b" > 3 && $"c" > 5, $"b" > 3 && $"c" > 5)

    checkCondition(($"a" < 2 || $"a" > 3 || $"b" > 5) && $"a" < 2, $"a" < 2)

    checkCondition($"a" < 2 && ($"a" < 2 || $"a" > 3 || $"b" > 5), $"a" < 2)

    checkCondition(($"a" < 2 || $"b" > 3) && ($"a" < 2 || $"c" > 5), $"a" < 2
      || ($"b" > 3 && $"c" > 5))

    checkCondition(
      ($"a" === $"b" || $"b" > 3) && ($"a" === $"b" || $"a" > 3) && ($"a" === $"b" || $"a" < 5),
      $"a" === $"b" || $"b" > 3 && $"a" > 3 && $"a" < 5)
  }

  test("SPARK-34222: simplify conjunctive predicates (a && b) && a && (a && c) => a && b && c") {
    checkCondition(($"a" > 1 && $"b" > 2) && $"a" > 1 && ($"a" > 1 && $"c" > 3),
      $"a" > 1 && ($"b" > 2 && $"c" > 3))

    checkCondition(($"a" > 1 && $"b" > 2) && ($"a" > 4 && $"b" > 5) && ($"a" > 1 && $"c" > 3),
      ($"a" > 1 && $"b" > 2) && ($"c" > 3 && $"a" > 4) && $"b" > 5)

    checkCondition(
      $"a" > 1 && $"b" > 3 && ($"a" > 1 && $"b" > 3 && ($"a" > 1 && $"b" > 3 && $"c" > 1)),
      $"a" > 1 && $"b" > 3 && $"c" > 1)

    checkCondition(
      ($"a" > 1 || $"b" > 3) && (($"a" > 1 || $"b" > 3) && $"d" > 0L
        && (($"a" > 1 || $"b" > 3) && $"c" > 1)),
      ($"a" > 1 || $"b" > 3) && $"d" > 0L && $"c" > 1)

    checkCondition(
      $"a" > 1 && $"b" > 2 && $"a" > 1 && $"c" > 3,
      $"a" > 1 && $"b" > 2 && $"c" > 3)

    checkCondition(
      ($"a" > 1 && $"b" > 3 && $"a" > 1) || ($"a" > 1 && $"b" > 3 && $"a" > 1 && $"c" > 1),
      $"a" > 1 && $"b" > 3)
  }

  test("SPARK-34222: simplify disjunctive predicates (a || b) || a || (a || c) => a || b || c") {
    checkCondition(($"a" > 1 || $"b" > 2) || $"a" > 1 || ($"a" > 1 || $"c" > 3),
      $"a" > 1 || $"b" > 2 || $"c" > 3)

    checkCondition(($"a" > 1 || $"b" > 2) || ($"a" > 4 || $"b" > 5) ||($"a" > 1 || $"c" > 3),
      ($"a" > 1 || $"b" > 2) || ($"a" > 4 || $"b" > 5) || $"c" > 3)

    checkCondition(
      $"a" > 1 || $"b" > 3 || ($"a" > 1 || $"b" > 3 || ($"a" > 1 || $"b" > 3 || $"c" > 1)),
      $"a" > 1 || $"b" > 3 || $"c" > 1)

    checkCondition(
      ($"a" > 1 && $"b" > 3) || (($"a" > 1 && $"b" > 3) || (($"a" > 1 && $"b" > 3) || $"c" > 1)),
      ($"a" > 1 && $"b" > 3) || $"c" > 1)

    checkCondition(
      $"a" > 1 || $"b" > 2 || $"a" > 1 || $"c" > 3,
      $"a" > 1 || $"b" > 2 || $"c" > 3)

    checkCondition(
      ($"a" > 1 || $"b" > 3 || $"a" > 1) && ($"a" > 1 || $"b" > 3 || $"a" > 1 || $"c" > 1 ),
      $"a" > 1 || $"b" > 3)
  }

  test("e && (!e || f) - not nullable") {
    checkConditionInNotNullableRelation($"e" && (!$"e" || $"f" ), $"e" && $"f")

    checkConditionInNotNullableRelation($"e" && ($"f" || !$"e" ), $"e" && $"f")

    checkConditionInNotNullableRelation((!$"e" || $"f" ) && $"e", $"f" && $"e")

    checkConditionInNotNullableRelation(($"f" || !$"e" ) && $"e", $"f" && $"e")
  }

  test("e && (!e || f) - nullable") {
    Seq ($"e" && (!$"e" || $"f" ),
        $"e" && ($"f" || !$"e" ),
        (!$"e" || $"f" ) && $"e",
        ($"f" || !$"e" ) && $"e",
        $"e" || (!$"e" && $"f"),
        $"e" || ($"f" && !$"e"),
        ($"e" && $"f") || !$"e",
        ($"f" && $"e") || !$"e").foreach { expr =>
      checkCondition(expr, expr)
    }
  }

  test("a < 1 && (!(a < 1) || f) - not nullable") {
    checkConditionInNotNullableRelation($"a" < 1 && (!($"a" < 1) || $"f"), ($"a" < 1) && $"f")
    checkConditionInNotNullableRelation($"a" < 1 && ($"f" || !($"a" < 1)), ($"a" < 1) && $"f")

    checkConditionInNotNullableRelation($"a" <= 1 && (!($"a" <= 1) || $"f"), ($"a" <= 1) && $"f")
    checkConditionInNotNullableRelation($"a" <= 1 && ($"f" || !($"a" <= 1)), ($"a" <= 1) && $"f")

    checkConditionInNotNullableRelation($"a" > 1 && (!($"a" > 1) || $"f"), ($"a" > 1) && $"f")
    checkConditionInNotNullableRelation($"a" > 1 && ($"f" || !($"a" > 1)), ($"a" > 1) && $"f")

    checkConditionInNotNullableRelation($"a" >= 1 && (!($"a" >= 1) || $"f"), ($"a" >= 1) && $"f")
    checkConditionInNotNullableRelation($"a" >= 1 && ($"f" || !($"a" >= 1)), ($"a" >= 1) && $"f")
  }

  test("a < 1 && ((a >= 1) || f) - not nullable") {
    checkConditionInNotNullableRelation($"a" < 1 && ($"a" >= 1 || $"f" ), ($"a" < 1) && $"f")
    checkConditionInNotNullableRelation($"a" < 1 && ($"f" || $"a" >= 1), ($"a" < 1) && $"f")

    checkConditionInNotNullableRelation($"a" <= 1 && ($"a" > 1 || $"f" ), ($"a" <= 1) && $"f")
    checkConditionInNotNullableRelation($"a" <= 1 && ($"f" || $"a" > 1), ($"a" <= 1) && $"f")

    checkConditionInNotNullableRelation($"a" > 1 && (($"a" <= 1) || $"f"), ($"a" > 1) && $"f")
    checkConditionInNotNullableRelation($"a" > 1 && ($"f" || ($"a" <= 1)), ($"a" > 1) && $"f")

    checkConditionInNotNullableRelation($"a" >= 1 && (($"a" < 1) || $"f"), ($"a" >= 1) && $"f")
    checkConditionInNotNullableRelation($"a" >= 1 && ($"f" || ($"a" < 1)), ($"a" >= 1) && $"f")
  }

  test("DeMorgan's law") {
    checkCondition(!($"e" && $"f"), !$"e" || !$"f")

    checkCondition(!($"e" || $"f"), !$"e" && !$"f")

    checkCondition(!(($"e" && $"f") || ($"g" && $"h")), (!$"e" || !$"f") && (!$"g" || !$"h"))

    checkCondition(!(($"e" || $"f") && ($"g" || $"h")), (!$"e" && !$"f") || (!$"g" && !$"h"))
  }

  private val analyzer = new Analyzer(
    new SessionCatalog(new InMemoryCatalog, EmptyFunctionRegistry))

  test("(a && b) || (a && c) => a && (b || c) when case insensitive") {
    val plan = analyzer.execute(
      testRelation.where(($"a" > 2 && $"b" > 3) || ($"A" > 2 && $"b" < 5)))
    val actual = Optimize.execute(plan)
    val expected = analyzer.execute(
      testRelation.where($"a" > 2 && ($"b" > 3 || $"b" < 5)))
    comparePlans(actual, expected)
  }

  test("(a || b) && (a || c) => a || (b && c) when case insensitive") {
    val plan = analyzer.execute(
      testRelation.where(($"a" > 2 || $"b" > 3) && ($"A" > 2 || $"b" < 5)))
    val actual = Optimize.execute(plan)
    val expected = analyzer.execute(
      testRelation.where($"a" > 2 || ($"b" > 3 && $"b" < 5)))
    comparePlans(actual, expected)
  }

  test("Complementation Laws") {
    checkConditionInNotNullableRelation($"e" && !$"e", testNotNullableRelation)
    checkConditionInNotNullableRelation(!$"e" && $"e", testNotNullableRelation)

    checkConditionInNotNullableRelation($"e" || !$"e", testNotNullableRelationWithData)
    checkConditionInNotNullableRelation(!$"e" || $"e", testNotNullableRelationWithData)
  }

  test("Complementation Laws - null handling") {
    checkCondition($"e" && !$"e",
      testRelationWithData.where(And(Literal(null, BooleanType), $"e".isNull)).analyze)
    checkCondition(!$"e" && $"e",
      testRelationWithData.where(And(Literal(null, BooleanType), $"e".isNull)).analyze)

    checkCondition($"e" || !$"e",
      testRelationWithData.where(Or($"e".isNotNull, Literal(null, BooleanType))).analyze)
    checkCondition(!$"e" || $"e",
      testRelationWithData.where(Or($"e".isNotNull, Literal(null, BooleanType))).analyze)
  }

  test("Complementation Laws - negative case") {
    checkCondition($"e" && !$"f", testRelationWithData.where($"e" && !$"f").analyze)
    checkCondition(!$"f" && $"e", testRelationWithData.where(!$"f" && $"e").analyze)

    checkCondition($"e" || !$"f", testRelationWithData.where($"e" || !$"f").analyze)
    checkCondition(!$"f" || $"e", testRelationWithData.where(!$"f" || $"e").analyze)
  }

  test("simplify NOT(IsNull(x)) and NOT(IsNotNull(x))") {
    checkCondition(Not(IsNotNull($"b")), IsNull($"b"))
    checkCondition(Not(IsNull($"b")), IsNotNull($"b"))
  }

  protected def assertEquivalent(e1: Expression, e2: Expression): Unit = {
    val correctAnswer = Project(Alias(e2, "out")() :: Nil, OneRowRelation()).analyze
    val actual = Optimize.execute(Project(Alias(e1, "out")() :: Nil, OneRowRelation()).analyze)
    comparePlans(actual, correctAnswer)
  }

  test("filter reduction - positive cases") {
    val fields = Seq(
      $"col1NotNULL".boolean.notNull,
      $"col2NotNULL".boolean.notNull
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
