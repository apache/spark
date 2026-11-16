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
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, BooleanType, DoubleType, IntegerType, LongType, StringType}

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

  val testRelation = LocalRelation($"a".int, $"b".int, $"c".int, $"d".int.notNull)

  private val columnA = $"a"
  private val columnB = $"b"
  private val columnC = $"c"
  private val columnD = $"d"

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
      .select(columnA)
      .where(
        columnA === Literal(1) && columnA === Literal(2) && columnB === Add(columnA, Literal(3)))

    val correctAnswer = testRelation
      .select(columnA, columnB)
      .where(Literal.FalseLiteral)
      .select(columnA).analyze

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

  test("SPARK-42500: ConstantPropagation supports more cases") {
    comparePlans(
      Optimize.execute(testRelation.where(columnA === 1 && columnB > columnA + 2).analyze),
      testRelation.where(columnA === 1 && columnB > 3).analyze)

    comparePlans(
      Optimize.execute(testRelation.where(columnA === 1 && columnA === 2).analyze),
      testRelation.where(Literal.FalseLiteral).analyze)

    comparePlans(
      Optimize.execute(testRelation.where(columnA === 1 && columnA === columnA + 2).analyze),
      testRelation.where(Literal.FalseLiteral).analyze)

    comparePlans(
      Optimize.execute(
        testRelation.where((columnA === 1 || columnB === 2) && columnB === 1).analyze),
      testRelation.where(columnA === 1 && columnB === 1).analyze)

    comparePlans(
      Optimize.execute(testRelation.where(columnA === 1 && columnA === 1).analyze),
      testRelation.where(columnA === 1).analyze)

    comparePlans(
      Optimize.execute(testRelation.where(Not(columnA === 1 && columnA === columnA + 2)).analyze),
      testRelation.where(Not(columnA === 1) || Not(columnA === columnA + 2)).analyze)
  }

  test("SPARK-60983: Do not propagate a constant into an expression that can fail") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      def divide(e: Expression): Expression = Divide(Literal(10.0), e.cast(DoubleType))

      // The conjunct can also run on rows where `b` is not 0. With `b` replaced, it would divide by
      // zero on such a row with `c` = 0, where `c - b` is not 0.
      val notConstant =
        testRelation.where(divide(columnC - columnB) > Literal(0.0) && columnB === 0).analyze
      comparePlans(Optimize.execute(notConstant), notConstant)

      // It can become constant, but keeps the attribute if it fails: no row may reach it.
      val failsToFold = testRelation.where(columnB === 0 && columnA > divide(columnB)).analyze
      comparePlans(Optimize.execute(failsToFold), failsToFold)

      comparePlans(
        Optimize.execute(testRelation.where(columnB === 2 && columnA > divide(columnB)).analyze),
        testRelation.where(columnB === 2 && columnA > Literal(5.0)).analyze)

      // An expression that fails on its own does not stop the other side of the comparison.
      comparePlans(
        Optimize.execute(testRelation.where(columnB === 2 && divide(columnC) > columnB).analyze),
        testRelation.where(columnB === 2 && divide(columnC) > Literal(2.0)).analyze)

      // The guard `b <> 0` becomes TRUE, so the equality goes ahead of the conjunct that keeps `b`.
      val guarded = Optimize.execute(testRelation
        .where(columnB =!= 0 && divide(columnC - columnB) > Literal(0.0) && columnB === 5).analyze)
      comparePlans(guarded,
        testRelation.where(columnB === 5 && divide(columnC - columnB) > Literal(0.0)).analyze)
      val Filter(guardedCondition, _) = guarded
      assert(splitConjunctivePredicates(guardedCondition).head.sql == "(b = 5)")

      // The condition of an IF decides whether the branch that can fail runs.
      val conditional = testRelation
        .where(If(columnC > columnB, Literal(true), divide(columnC) > Literal(0.0)) &&
          columnB === 0).analyze
      comparePlans(Optimize.execute(conditional), conditional)
    }
  }

  test("SPARK-61033: Do not propagate a constant into an operand that guards an expression " +
      "that can fail") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      def divide(e: Expression): Expression = Divide(Literal(10.0), e.cast(DoubleType))
      def first(plan: LogicalPlan): String = plan match {
        case Filter(condition, _) => splitConjunctivePredicates(condition).head.sql
      }

      // `a < 3` and `a > 3` decide whether the division runs. With `a` replaced, it would run on a
      // row with `a` = 1 and `c` = 0. The conjunct keeps `a`, so the equality goes first.
      val nested = testRelation.where(
        (columnA < 3 || (columnA > 3 && divide(columnC) > Literal(1.0))) && columnA === 5).analyze
      val optimizedNested = Optimize.execute(nested)
      comparePlans(optimizedNested, nested)
      assert(first(optimizedNested) == "(a = 5)")

      // With `k` replaced, the map lookup can be NULL on a row with another `k`.
      val maps = LocalRelation($"k".string, Symbol("m").map(StringType, DoubleType), $"x".int)
      val inComparison = maps.where(Coalesce(Seq(GetMapValue($"m", $"k"), divide($"x"))) >
        Literal(0.0) && $"k" === "a").analyze
      comparePlans(Optimize.execute(inComparison), inComparison)

      // A conjunct guards the ones after it as well. It is rewritten, and the equality goes first.
      val conjunct = Optimize.execute(testRelation
        .where(columnA > 0 && divide(columnC) > Literal(0.0) && columnA === 1).analyze)
      comparePlans(conjunct,
        testRelation.where(columnA === 1 && divide(columnC) > Literal(0.0)).analyze)
      assert(first(conjunct) == "(a = 1)")
    }
  }

  test("SPARK-60882: Propagate a constant into an expression that can not fail") {
    // An addition with ANSI mode off can not fail, so `b` is replaced in it, although `c` is not.
    val legacy = NumericEvalContext(EvalMode.LEGACY)
    comparePlans(
      Optimize.execute(
        testRelation.where(columnA > Add(columnC, columnB, legacy) && columnB === 2).analyze),
      testRelation.where(columnA > Add(columnC, Literal(2), legacy) && columnB === 2).analyze)

    // In ANSI mode it can overflow, so it keeps `b`, and the equality goes first.
    val ansi = NumericEvalContext(EvalMode.ANSI)
    comparePlans(
      Optimize.execute(
        testRelation.where(columnA > Add(columnC, columnB, ansi) && columnB === 2).analyze),
      testRelation.where(columnB === 2 && columnA > Add(columnC, columnB, ansi)).analyze)

    // Also under a `COLLATE`.
    val strings = LocalRelation($"s".string, $"n".int)
    comparePlans(
      Optimize.execute(strings.where($"s" === "ab" &&
        Length(Collate(Substring($"s", Literal(1), $"n"), ResolvedCollation("UNICODE_CI"))) > 1)
        .analyze),
      strings.where($"s" === "ab" &&
        Length(Collate(Substring(Literal("ab"), Literal(1), $"n"), ResolvedCollation("UNICODE_CI")))
          > 1).analyze)

    // Not into a cast of a DOUBLE to a string: a row with `x` = -0.0 passes `x = 0.0`, but
    // `CAST(x * y AS STRING)` keeps the sign.
    val doubles = LocalRelation($"x".double, $"y".double)
    val signed = doubles.where($"x" === 0.0 &&
      Cast(Multiply($"x", $"y", ansi), StringType) === "-0.0").analyze
    comparePlans(Optimize.execute(signed), signed)

    // Nor a string of a trim collation into `length`: a row with `s` = 'a ' passes `s = 'a'`.
    val trimmed = LocalRelation(AttributeReference("s", StringType("UTF8_BINARY_RTRIM"))())
    val lengthOfTrimmed = trimmed.where($"s" === "a" && Length($"s") === 2).analyze
    comparePlans(Optimize.execute(lengthOfTrimmed), lengthOfTrimmed)
  }

  // SPARK-61437: the columns of the projection below that can fail.
  private val strings = LocalRelation($"s".string, $"id".long, $"x".long)
  private def castS: Expression = $"s".cast(IntegerType)
  private def projected: LogicalPlan = strings.select($"s", $"id", $"x", castS.as("i"))
  private def divide(e: Expression): Expression = Divide(Literal(10.0), e.cast(DoubleType))

  private def conjuncts(plan: LogicalPlan): Seq[String] = plan match {
    case Filter(condition, _) => splitConjunctivePredicates(condition).map(_.sql)
    case other => fail(s"Expected a Filter: $other")
  }

  /** Checks the optimized plan also in the order of the conjuncts, which `comparePlans` sorts. */
  private def checkOrdered(child: LogicalPlan, condition: Expression, expected: Expression)
    : Unit = {
    val optimized = Optimize.execute(child.where(condition).analyze)
    val expectedPlan = child.where(expected).analyze
    comparePlans(optimized, expectedPlan)
    assert(conjuncts(optimized) == conjuncts(expectedPlan))
  }

  test("SPARK-61437: Do not move an equality over a column of the projection below that can " +
      "fail") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      // `i = 1` can fail, as the cast does, so it is not moved ahead of `id > 5`. Its constant is
      // propagated in place, so only `i = 1` reads `i`.
      val written = $"id" > 5L && $"i".cast(LongType) * $"id" > 0L && $"i" === 1
      val expected = $"id" > 5L && Literal(1L) * $"id" > 0L && $"i" === 1
      Seq(
        projected,
        projected.select($"s", $"id", $"i"),
        projected.where($"id" > 1L).select($"s", $"id", $"i"),
        projected.sample(0.0, 0.5, withReplacement = false, 1L).select($"s", $"id", $"i"),
        projected.select($"s", $"id", $"i".as("j")).select($"s", $"id", $"j".as("i"))
      ).foreach(child => checkOrdered(child, written, expected))
    }
  }

  test("SPARK-61437: Propagate an equality over a column that can fail in place") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      // Into every conjunct, also one that can fail, as before SPARK-60983, so that a conjunct
      // that it rewrites no longer reads `i`.
      val twoCasts = projected.select($"s", $"id", $"x", $"i", castS.as("j"))
      val threeCasts = twoCasts.select($"s", $"id", $"x", $"i", $"j", castS.as("k"))
      Seq(
        ($"id" > 5L && $"i" + 1 > 1 && $"i" === 1) -> ($"id" > 5L && $"i" === 1),
        (($"id" < 0L || $"i" + 1 > 3) && $"i" === 1) -> ($"id" < 0L && $"i" === 1),
        (divide($"i") > 1.0 && $"i" === 1) -> ($"i" === 1),
        ($"id" > 5L && ($"i" <=> 1) && $"i" > 0) -> ($"id" > 5L && ($"i" <=> 1)),
        ($"id" > 5L && (Literal(1) <=> $"i") && $"i" > 0) -> ($"id" > 5L && (Literal(1) <=> $"i")),
        (divide($"x") > 0.0 && $"i" > 0 && $"i" === 1) -> (divide($"x") > 0.0 && $"i" === 1),
        ($"i" + 1 > 1 && $"i" === 1 && divide($"x") > 0.0) -> ($"i" === 1 && divide($"x") > 0.0),
        ($"id" > 5L && $"i" > 0 && divide($"x") > 0.0 && $"i" === 1) ->
          ($"id" > 5L && divide($"x") > 0.0 && $"i" === 1),
        ($"id" > 5L && $"i" > 0 && divide($"x") > 0.0 && Literal(1) === $"i") ->
          ($"id" > 5L && divide($"x") > 0.0 && Literal(1) === $"i"),
        ($"id" > 5L && $"i" > 0 && divide($"x") > 0.0 && ($"i" <=> 1)) ->
          ($"id" > 5L && divide($"x") > 0.0 && ($"i" <=> 1)),
        // The division reads `i` with the constant, so it does not fail on a row where `id` is
        // 8 and `i` is 8, but does on one where `id` is 1 and `i` is 2, as before SPARK-60983.
        (divide($"id" - $"i".cast(LongType)) > 0.0 && $"i" === 1) ->
          (divide($"id" - 1L) > 0.0 && $"i" === 1),
        // `id > 0` is rewritten by `id = 7`, which goes first.
        ($"id" > 0L && divide($"x") > 0.0 && $"i" === 1 && $"i" + 1 > 1 && $"id" === 7L) ->
          ($"id" === 7L && divide($"x") > 0.0 && $"i" === 1),
        // But `id = 7` rewrites nothing here, so it stays, as on 4.3.0.
        ($"i" + 1 > 1 && divide($"x") > 0.0 && $"i" === 1 && $"id" === 7L) ->
          (divide($"x") > 0.0 && $"i" === 1 && $"id" === 7L)
      ).foreach { case (condition, expected) => checkOrdered(projected, condition, expected) }
      Seq(
        ($"j" > 0 && $"i" > 0 && $"j" === 2 && divide($"x") > 0.0 && $"i" === 1) ->
          ($"j" === 2 && divide($"x") > 0.0 && $"i" === 1),
        ($"i" > 0 && $"k" > 5 && $"i" === 1 && $"j" + 1 > 1 && $"j" === 2) ->
          ($"k" > 5 && $"i" === 1 && $"j" === 2),
        // `i + 1` does not overflow on a row where `i` is the largest integer.
        ($"i" + 1 > 0 && $"k" > 5 && $"i" === 1) -> ($"k" > 5 && $"i" === 1),
        // `j < 5` no longer stops a row before `i = 2`, as before SPARK-60983.
        ($"j" < 5 && $"i" > 0 && $"i" === 2 && $"j" === 1) -> ($"i" === 2 && $"j" === 1)
      ).foreach { case (condition, expected) => checkOrdered(threeCasts, condition, expected) }

      // But not where the condition then fails to fold, which would fail the query with no row,
      // unlike in a conditional branch, which is not folded where it fails. Then the equalities
      // are handled as since SPARK-60983, and go first.
      val failsToFold = $"i" === 0 && $"x".cast(DoubleType) > divide($"i")
      checkOrdered(projected, failsToFold, failsToFold)
      checkOrdered(projected,
        $"id" > 5L && $"i" + 1 > 0 && $"x".cast(DoubleType) > divide($"i") && $"i" === 0,
        $"i" === 0 && $"id" > 5L && $"x".cast(DoubleType) > divide($"i"))
      // Also where the comparison folds, but the conjunct does not.
      checkOrdered(projected,
        $"i" === 0 && ElementAt(CreateArray(Seq(Literal(true))), ($"i" > 0).cast(IntegerType)),
        $"i" === 0 && ElementAt(Literal.create(Seq(true), ArrayType(BooleanType, false)),
          ($"i" > 0).cast(IntegerType)))
      checkOrdered(projected,
        $"i" === 0 && If($"x" > 0L, divide($"i"), Literal(0.0)) > 0.0,
        $"i" === 0 && If($"x" > 0L, divide(Literal(0)), Literal(0.0)) > 0.0)
      checkOrdered(projected,
        CaseWhen(Seq(($"id" < 0L) -> ($"x".cast(DoubleType) > divide($"i"))), $"s".isNotNull) &&
          $"i" === 0,
        CaseWhen(Seq(($"id" < 0L) -> ($"x".cast(DoubleType) > divide(Literal(0)))),
          $"s".isNotNull) && $"i" === 0)
      // Trying to fold it leaves no mark on a part that fails in a conditional branch, which
      // would keep `ConstantFolding` from failing on the same part elsewhere.
      val byZero = Divide(Literal(10.0), Literal(0.0))
      val sharesByZero = projected.where($"x".cast(DoubleType) > byZero && $"i" === 0 &&
        If($"x" > 0L, byZero, Literal(0.0)) > $"i".cast(DoubleType)).analyze
      intercept[ArithmeticException](Optimize.execute(sharesByZero))
      intercept[ArithmeticException](Optimize.execute(projected
        .where($"i" === 0 && If($"x" > 0L, byZero, Literal(0.0)) > $"i".cast(DoubleType))
        .select($"id", byZero.as("z")).analyze))
      // Nor does it remove a mark that is there already.
      byZero.setTagValue(ConstantFolding.FAILED_TO_EVALUATE, ())
      Optimize.execute(sharesByZero)

      // Not a string of a trim collation into `length`, as on 4.3.0: a row with `u` = 'A '
      // passes `u = 'A'`.
      val trimmed = LocalRelation(AttributeReference("s", StringType("UTF8_BINARY_RTRIM"))())
        .select(Upper($"s").as("u"))
      val lengthOfTrimmed = trimmed.where($"u" === "A" && Length($"u") === 2).analyze
      comparePlans(Optimize.execute(lengthOfTrimmed), lengthOfTrimmed)
    }
  }

  test("SPARK-61437: A conjunct over a column that can fail can fail") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      // `id = 7` folds the guard `id > 0`, so it goes ahead of `i > 0`, which can fail.
      checkOrdered(projected, $"id" > 0L && $"i" > 0 && $"id" === 7L, $"id" === 7L && $"i" > 0)
      checkOrdered(projected, $"id" > 0L && $"i" === 1 && $"id" === 7L,
        $"id" === 7L && $"i" === 1)
      // `id < 3` decides whether `i > 0` runs. `i > 0` counts as one that can not fail there, as
      // on master, so `id < 3` is rewritten, and the equality goes first, ahead of `i > 0`.
      checkOrdered(projected, ($"id" < 3L || $"i" > 0) && $"id" === 5L, $"id" === 5L && $"i" > 0)
      // So `id > 0` is rewritten too, and becomes TRUE, also inside another operator, so the
      // operator folds away and `i` is not read.
      Seq(
        ($"id" > 0L || $"i" > 0) && $"id" === 7L,
        ($"id" > 0L || $"x" > 0L || $"i" > 0) && $"id" === 7L
      ).foreach(condition => checkOrdered(projected, condition, $"id" === 7L))
      // `NullPropagation`, not in this batch, then drops the operands after the 7, and the
      // equality goes first until then.
      checkOrdered(projected, (Coalesce(Seq($"id", $"x")) > 0L || $"i" > 0) && $"id" === 7L,
        $"id" === 7L && (Coalesce(Seq(Literal(7L), $"x")) > 0L || $"i" > 0))
      checkOrdered(projected, Coalesce(Seq($"id", $"i".cast(LongType))) > 0L && $"id" === 7L,
        $"id" === 7L && Coalesce(Seq(Literal(7L), $"i".cast(LongType))) > 0L)
      checkOrdered(projected, (($"id" < 0L && $"i" > 0) || $"x" > 0L) && $"id" === 7L,
        $"id" === 7L && $"x" > 0L)

      // Also one that reads the propagated attribute, which a collation keeps.
      val lcase = LocalRelation($"s".string, AttributeReference("c", StringType("UTF8_LCASE"))())
        .select($"c", castS.as("i"))
      checkOrdered(lcase, ($"i" > 0 || Length($"c") > 0) && $"c" === "a",
        $"c" === "a" && ($"i" > 0 || Length($"c") > 0))

      // Also under a `NOT`, with `s` and `id` not nullable.
      val notNull = LocalRelation($"s".string.notNull, $"id".long.notNull)
        .select($"s", $"id", castS.as("i"))
      checkOrdered(notNull, Not($"id" > 0L && $"i" > 0 && $"id" === 7L),
        Not($"id" === 7L) || $"i" <= 0)
      // The cast is marked nullable (`Cast.forceNullable`), so `i = 1` is not propagated under
      // it, as on master.
      checkOrdered(notNull, Not($"i" + 1 > 3 && $"i" === 1), $"i" + 1 <= 3 || Not($"i" === 1))
      // `upper` is not, so the condition inside becomes FALSE, as on master.
      val upperNotNull = notNull.select($"s", Upper($"s").as("u"))
      comparePlans(
        Optimize.execute(upperNotNull.where(Not(Length($"u") > 3 && $"u" === "X")).analyze),
        upperNotNull.where(true).analyze)
      // And one that becomes NULL stays NULL, which the `NOT` keeps NULL.
      checkOrdered(upperNotNull,
        Not(If($"u" === "X", Literal(null, StringType), $"u") === "Y" && $"u" === "X"),
        Literal(null, BooleanType) || Not($"u" === "X"))
    }
  }

  test("SPARK-61437: A condition that becomes FALSE with the constants is FALSE") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      val twoCasts = projected.select($"s", $"id", $"x", $"i", castS.as("j"))
      val threeCasts = twoCasts.select($"s", $"id", $"x", $"i", $"j", castS.as("k"))
      // Wherever the conjunct that becomes FALSE is.
      Seq(
        $"i" + 1 > 3 && divide($"x") > 0.0 && $"i" === 1,
        $"i" === 1 && divide($"x") > 0.0 && $"i" === 2,
        $"i" > 0 && divide($"x") > 0.0 && $"i" + 1 > 3 && $"i" === 1,
        $"i" > 0 && $"j" > 5 && $"i" > 10 && $"i" === 1,
        $"i" > 0 && divide($"x") > 0.0 && $"i" === 1 && $"i" + 1 > 3
      ).foreach { condition =>
        comparePlans(Optimize.execute(twoCasts.where(condition).analyze),
          twoCasts.where(false).analyze)
      }
      comparePlans(Optimize.execute(threeCasts.where(
        $"i" > 0 && $"k" > 5 && $"j" < 0 && $"i" === 1 && $"j" === 2).analyze),
        threeCasts.where(false).analyze)
      Seq(
        $"id" > 5L && $"i" + 1 > 3 && $"i" === 1,
        $"i" === 1 && $"i" + 1 > 3,
        $"i" + 1 > 3 && Literal(1) === $"i",
        $"i" === 1 && $"id" > 5L && $"i" === 2,
        $"i".cast(LongType) + $"id" > 3L && $"i" === 1 && $"id" === 1L
      ).foreach { condition =>
        comparePlans(Optimize.execute(projected.where(condition).analyze),
          projected.where(false).analyze)
      }
      comparePlans(
        Optimize.execute(
          projected.where(($"id" < 0L || ($"i" + 1 > 3 && $"i" === 1)) && $"id" > 5L).analyze),
        projected.where($"id" < 0L && $"id" > 5L).analyze)
    }
  }

  test("SPARK-61437: Columns that can not fail") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      // A non-deterministic alias is computed on every row the projection gets, so it raises no
      // new error here, and an equality over it is propagated and moved.
      val random = strings.select($"id", $"x", Rand(1L).as("r"))
      checkOrdered(random, $"r" > 0.0 && $"r" === 0.5, $"r" === 0.5)
      checkOrdered(random, $"r" > 0.0 && divide($"x") > 0.0 && $"r" === 0.5,
        $"r" === 0.5 && divide($"x") > 0.0)
      // So is one over a column that reads it with an expression that can not fail.
      val randomPlusOne = random.select($"id", ($"r" + 1.0).as("r1"))
      checkOrdered(randomPlusOne, $"r1" > 0.0 && $"r1" === 1.5, $"r1" === 1.5)
      checkOrdered(randomPlusOne, $"r1" > 0.0 && divide($"id") > 0.0 && $"r1" === 1.5,
        $"r1" === 1.5 && divide($"id") > 0.0)
      // And one that stands for a cast that can not fail.
      val widened = strings.select($"s", $"id", $"x", $"id".cast(DoubleType).as("k"))
      checkOrdered(widened, $"id" > 5L && $"k" > 0.0 && $"k" === 3.0, $"id" > 5L && $"k" === 3.0)
      checkOrdered(widened, $"k" > 0.0 && divide($"x") > 0.0 && $"k" === 3.0,
        $"k" === 3.0 && divide($"x") > 0.0)
    }

    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      // The cast can not fail with ANSI mode off, so `i = 1` goes ahead of `length(u) > 0`, which
      // counts as one that can fail, as `upper` is not in the allowlist of
      // `ExprUtils.canEvaluateUnconditionally`.
      val withUpper = projected.select($"s", $"id", $"i", Upper($"s").as("u"))
      checkOrdered(withUpper, $"id" > 5L && $"i" > 0 && Length($"u") > 0 && $"i" === 1,
        $"i" === 1 && $"id" > 5L && Length($"u") > 0)

      // So an alias of `upper` or `lower` counts as one that can fail in every mode, and an
      // equality over it is not moved, but propagated in place.
      val cased = strings.select($"s", $"id", Upper($"s").as("u"), Lower($"s").as("l"))
      checkOrdered(cased, Length($"u") > 0 && Length($"l") > 0 && $"u" === "X",
        Length($"l") > 0 && $"u" === "X")
      checkOrdered(cased, Length($"u") > 0 && divide($"id") > 0.0 && $"u" === "X",
        divide($"id") > 0.0 && $"u" === "X")
      // But an operand that decides whether one over it runs is rewritten, as on master.
      checkOrdered(cased, ($"id" < 3L || $"u" > "A") && $"id" === 7L, $"id" === 7L && $"u" > "A")

      // Each projection reads the column below it twice. The check does not expand the column,
      // whose expression would have 2^40 leaves.
      val chain = (1 to 40).foldLeft(strings.select($"id".as("c"))) { (child, _) =>
        child.select(($"c" + $"c").as("c"))
      }
      checkOrdered(chain, $"c" > 0L && $"c" === 2L, $"c" === 2L)
    }
  }
}
