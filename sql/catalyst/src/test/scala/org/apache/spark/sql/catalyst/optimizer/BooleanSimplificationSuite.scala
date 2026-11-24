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
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.BooleanType

class BooleanSimplificationSuite extends PlanTest with ExpressionEvalHelper {

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
    testRelation.output, Seq(Row(1, 2, 3, "abc", true, true, true, true))
  )

  val testNotNullableRelation = LocalRelation($"a".int.notNull, $"b".int.notNull, $"c".int.notNull,
    $"d".string.notNull, $"e".boolean.notNull, $"f".boolean.notNull, $"g".boolean.notNull,
    $"h".boolean.notNull)

  val testNotNullableRelationWithData = LocalRelation.fromExternalRows(
    testNotNullableRelation.output, Seq(Row(1, 2, 3, "abc", true, true, true, true))
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

  test("SPARK-61938: do not move a throwable common factor ahead of the other operands") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      val r = IsNull(RaiseError($"d"))
      // `r || (a < 5 && c < 50)` and `r && (a > 5 || c > 50)` would evaluate `r` also on the rows
      // where the operand written before it already decides the predicate. `e || r` is written as
      // `r || e` on the other side, where `r` is evaluated before `e`.
      Seq(
        ($"a" < 5 || r) && ($"c" < 50 || r),
        ($"a" > 5 && r) || ($"c" > 50 && r),
        ($"a" > 5 && ($"e" || r)) || ((r || $"e") && $"c" > 50)).foreach { input =>
        checkCondition(input, input)
      }
      // A throwable common factor that only the extracted ones come before on one side is
      // extracted.
      checkCondition((r || $"a" < 5) && ($"c" < 50 || r), r || ($"a" < 5 && $"c" < 50))
      checkCondition(($"a" < 5 || r) && (r || $"c" < 50), r || ($"a" < 5 && $"c" < 50))
      checkCondition(
        ($"b" > 1 && r && $"a" > 5) || ($"b" > 1 && r && $"c" > 50),
        $"b" > 1 && r && ($"a" > 5 || $"c" > 50))
      // A common factor that is not throwable is extracted alone, also a division, which can
      // fail in ANSI mode.
      val f = $"a" / $"b" > 1.0
      checkCondition(
        ($"a" > 5 && f && r) || ($"c" > 50 && f && r),
        f && (($"a" > 5 && r) || ($"c" > 50 && r)))
    }
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

  test("SPARK-61938: e && (!e || f) - not nullable, f can fail") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      // A division can fail in ANSI mode, although it is not throwable. `f || e` and `f && e`
      // would evaluate it also on the rows where `!e` already decides the operand that contains
      // it, so `e` is evaluated first.
      val f = $"a" / $"b" > 1.0
      checkConditionInNotNullableRelation((!$"e" && f) || $"e", $"e" || f)
      // `comparePlans` sorts the conjuncts of a filter, so the condition is compared as it is.
      val actual = Optimize.execute(testNotNullableRelationWithData.where((!$"e" || f) && $"e")
        .analyze)
      val expected = testNotNullableRelationWithData.where($"e" && f).analyze
      assert(actual.asInstanceOf[Filter].condition == expected.asInstanceOf[Filter].condition)
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

  test("SPARK-54881: simplify Not(Expr) in single pass") {
    def executeRuleOnce(exprToTest: Expression, optimizedExprExpected: Expression): Unit = {
      val planAfterRuleApp = BooleanSimplification.apply(testRelation.where(exprToTest).analyze)
      val expectedOptPlan = testRelation.where(optimizedExprExpected).analyze
      comparePlans(expectedOptPlan, planAfterRuleApp)
    }
    // check simplify Not(A <= B OR A >= B) to (a > b AND a < b) in single pass
    executeRuleOnce(
      Not(($"a" <= $"b") || ($"a" >= $"b")),
      $"a" > $"b" && $"a" < $"b"
    )

    // check simplify Not((expr1 OR expr2) OR (expr3 AND expr4)) in single pass
    executeRuleOnce(
      Not(($"a" <= $"b" || $"c" > $"a" + 4) || ($"a" >= $"b" && $"c" < $"a")),
      And(
        And($"a" > $"b", $"c" <= $"a" + 4),
        Or($"a" < $"b", $"c" >= $"a")
      )
    )

    // check simplify Not((expr1 OR expr2) AND (expr3 OR expr4)) in single pass
    executeRuleOnce(
      Not(($"a" <= $"b" || $"c" > $"a" + 4) && ($"a" >= $"b" || $"c" < $"a")),
      Or(
        And($"a" > $"b", $"c" <= $"a" + 4),
        And($"a" < $"b", $"c" >= $"a")
      )
    )
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
      // `ExprUtils.canEvaluateUnconditionally` does not accept a BoundReference, so the guard
      // stays ahead of `col2NotNULL` where it was written ahead of it.
      (col1NotNULL && (!col1NotNULL || col2NotNULL)) -> (col1NotNULL && col2NotNULL),
      (col1NotNULL && (col2NotNULL || !col1NotNULL)) -> (col1NotNULL && col2NotNULL),
      ((!col1NotNULL || col2NotNULL) && col1NotNULL) -> (col1NotNULL && col2NotNULL),
      ((col2NotNULL || !col1NotNULL) && col1NotNULL) -> (col2NotNULL && col1NotNULL),

      (col1NotNULL || (!col1NotNULL && col2NotNULL)) -> (col1NotNULL || col2NotNULL),
      (col1NotNULL || (col2NotNULL && !col1NotNULL)) -> (col1NotNULL || col2NotNULL),
      ((!col1NotNULL && col2NotNULL) || col1NotNULL) -> (col1NotNULL || col2NotNULL),
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
