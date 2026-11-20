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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, LeftOuter, LeftSingle, PlanTest, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical.{CTERelationDef, CTERelationRef, Expand, Filter, LocalRelation, LogicalPlan, WithCTE}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, BooleanType, DataType, DoubleType, FloatType, IntegerType, LongType, StringType, StructType}

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
  private val stringIds = LocalRelation($"s".string, $"id".long, $"x".long)
  private def castS: Expression = $"s".cast(IntegerType)
  private def projected: LogicalPlan = stringIds.select($"s", $"id", $"x", castS.as("i"))
  private def twoCasts: LogicalPlan = projected.select($"s", $"id", $"x", $"i", castS.as("j"))
  private def threeCasts: LogicalPlan =
    twoCasts.select($"s", $"id", $"x", $"i", $"j", castS.as("k"))
  private def tenDividedBy(e: Expression): Expression = Divide(Literal(10.0), e.cast(DoubleType))
  /** `e * id`, which can overflow in ANSI mode, so a constant goes into it only in place. */
  private def timesId(e: Expression): Expression = e.cast(LongType) * $"id"

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
      val written = $"id" > 5L && timesId($"i") > 0L && $"i" === 1
      val expected = $"id" > 5L && Literal(1L) * $"id" > 0L && $"i" === 1
      Seq(
        projected,
        projected.select($"s", $"id", $"i"),
        projected.where($"id" > 1L).select($"s", $"id", $"i"),
        projected.sample(0.0, 0.5, withReplacement = false, 1L).select($"s", $"id", $"i"),
        projected.limit(5).select($"s", $"id", $"i"),
        projected.offset(1).select($"s", $"id", $"i"),
        projected.orderBy($"id".asc).select($"s", $"id", $"i"),
        projected.select($"s", $"id", $"i".as("j")).select($"s", $"id", $"j".as("i")),
        // Also from any child of a union, either side of a join, and an aggregation with grouping
        // keys, which computes a result column over its grouping keys only for the rows that reach
        // a condition reading it, under whole-stage codegen.
        stringIds.select($"s", $"id", $"x", Literal(1).as("i")).union(projected),
        projected.union(stringIds.select($"s", $"id", $"x", Literal(1).as("i"))),
        projected.join(LocalRelation($"k".long), Inner, Some($"x" === $"k")),
        LocalRelation($"k".long).join(projected, Inner, Some($"x" === $"k")),
        stringIds.groupBy($"s", $"id")($"s", $"id", castS.as("i")),
        // And from either side of a lateral join, which `RewriteLateralSubquery` turns into a join
        // after this rule runs, and from the plan of a CTE that is not inlined (for a reference to
        // one, see below).
        projected.lateralJoin(LocalRelation($"k".long)),
        LocalRelation($"k".long).lateralJoin(projected),
        WithCTE(projected, Nil)
      ).foreach(child => checkOrdered(child, written, expected))
    }
  }

  test("SPARK-61437: Propagate an equality over a column that can fail in place") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      // Into every conjunct, also one that can fail, as before SPARK-60983, so that a conjunct
      // that it rewrites no longer reads `i`. Without the column of the projection below, the
      // equality would go first, with `i` kept in such a conjunct.
      Seq(
        (($"id" < 0L || timesId($"i") > 3L) && $"i" === 1) ->
          (($"id" < 0L || Literal(1L) * $"id" > 3L) && $"i" === 1),
        (tenDividedBy($"x" - $"i".cast(LongType)) > 1.0 && $"i" === 1) ->
          (tenDividedBy($"x" - 1L) > 1.0 && $"i" === 1),
        ($"id" > 5L && ($"i" <=> 1) && timesId($"i") > 0L) ->
          ($"id" > 5L && ($"i" <=> 1) && Literal(1L) * $"id" > 0L),
        ($"id" > 5L && (Literal(1) <=> $"i") && timesId($"i") > 0L) ->
          ($"id" > 5L && (Literal(1) <=> $"i") && Literal(1L) * $"id" > 0L),
        (tenDividedBy($"x") > 0.0 && timesId($"i") > 0L && $"i" === 1) ->
          (tenDividedBy($"x") > 0.0 && Literal(1L) * $"id" > 0L && $"i" === 1),
        (timesId($"i") > 1L && $"i" === 1 && tenDividedBy($"x") > 0.0) ->
          (Literal(1L) * $"id" > 1L && $"i" === 1 && tenDividedBy($"x") > 0.0),
        // A conjunct that the constant folds away no longer stops a row before the ones after it.
        ($"id" > 5L && $"i" > 0 && tenDividedBy($"x") > 0.0 && $"i" === 1) ->
          ($"id" > 5L && tenDividedBy($"x") > 0.0 && $"i" === 1),
        ($"id" > 5L && $"i" > 0 && tenDividedBy($"x") > 0.0 && Literal(1) === $"i") ->
          ($"id" > 5L && tenDividedBy($"x") > 0.0 && Literal(1) === $"i"),
        ($"id" > 5L && $"i" > 0 && tenDividedBy($"x") > 0.0 && ($"i" <=> 1)) ->
          ($"id" > 5L && tenDividedBy($"x") > 0.0 && ($"i" <=> 1)),
        // The division reads `i` with the constant, so it does not fail on a row where `id` is
        // 8 and `i` is 8, but does on one where `id` is 1 and `i` is 2, as before SPARK-60983.
        (tenDividedBy($"id" - $"i".cast(LongType)) > 0.0 && $"i" === 1) ->
          (tenDividedBy($"id" - 1L) > 0.0 && $"i" === 1),
        // `id > 0` is rewritten by `id = 7`, which goes first.
        ($"id" > 0L && tenDividedBy($"x") > 0.0 && $"i" === 1 && $"i" + 1 > 1 &&
          $"id" === 7L) -> ($"id" === 7L && tenDividedBy($"x") > 0.0 && $"i" === 1),
        // But `id = 7` rewrites nothing here, so it stays, as on 4.3.0.
        ($"i" + 1 > 1 && tenDividedBy($"x") > 0.0 && $"i" === 1 && $"id" === 7L) ->
          (tenDividedBy($"x") > 0.0 && $"i" === 1 && $"id" === 7L)
      ).foreach { case (condition, expected) => checkOrdered(projected, condition, expected) }
      Seq(
        ($"j" > 0 && $"i" > 0 && $"j" === 2 && tenDividedBy($"x") > 0.0 && $"i" === 1) ->
          ($"j" === 2 && tenDividedBy($"x") > 0.0 && $"i" === 1),
        ($"i" > 0 && $"k" > 5 && $"i" === 1 && timesId($"j") > 1L && $"j" === 2) ->
          ($"k" > 5 && $"i" === 1 && Literal(2L) * $"id" > 1L && $"j" === 2),
        // `i + k` no longer reads `i`, so it does not overflow on a row where `i` is the largest
        // integer and `k` is 1.
        ($"i" + $"k" > 0 && $"i" === 1) -> (Literal(1) + $"k" > 0 && $"i" === 1),
        // `j * id < 5` no longer stops a row before `i = 2`, as before SPARK-60983.
        (timesId($"j") < 5L && $"i" > 0 && $"i" === 2 && $"j" === 1) ->
          (Literal(1L) * $"id" < 5L && $"i" === 2 && $"j" === 1)
      ).foreach { case (condition, expected) => checkOrdered(threeCasts, condition, expected) }

      // As before SPARK-60983, an attribute whose type is not binary stable is replaced only in a
      // comparison with an attribute of the same type.
      val collated = LocalRelation(
        AttributeReference("c", StringType("UTF8_LCASE"))(),
        AttributeReference("v", StringType("UTF8_LCASE"))()).select($"v", Upper($"c").as("u"))
      checkOrdered(collated, $"v" === $"u" && $"u" === "a", $"v" === "a" && $"u" === "a")
      checkOrdered(collated, Length($"u") > 1 && $"u" === "a",
        Length($"u") > 1 && $"u" === "a")

      // Not a string of a trim collation into `length`, as on 4.3.0: a row with `u` = 'A '
      // passes `u = 'A'`.
      val trimmed = LocalRelation(AttributeReference("s", StringType("UTF8_BINARY_RTRIM"))())
        .select(Upper($"s").as("u"))
      val lengthOfTrimmed = trimmed.where($"u" === "A" && Length($"u") === 2).analyze
      comparePlans(Optimize.execute(lengthOfTrimmed), lengthOfTrimmed)
    }
  }

  test("SPARK-61437: Do not propagate a zero in place where its sign can matter") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      // A row with `d` = -0.0 passes `d = 0.0`, but `CAST(d * y AS STRING)` keeps the sign, so `d`
      // is kept there, as `replaceConstants` keeps it, although the equality is not moved.
      val doubles = stringIds.select($"x".cast(DoubleType).as("y"), $"s".cast(DoubleType).as("d"))
      val withE = doubles.select($"y", $"d", ($"d" * 2.0).as("e"))
      def signed(e: Expression): Expression = Cast(e, StringType) === "-0.0"
      Seq(
        $"d" === 0.0 && signed($"d" * $"y"),
        $"d" === -0.0 && signed($"d" * $"y"),
        signed($"d" * $"y") && $"d" === 0.0,
        // `pow(-0.0, y)` and `pow(0.0, y)` differ for a negative `y`.
        Pow($"d", $"y") < 0.0 && $"d" === 0.0,
        // Also below arithmetic, under an operator that keeps the sign.
        signed(Divide($"d", $"y")) && $"d" === 0.0,
        // And where `replaceConstants` would not fold the operator: in an operand that guards
        // another one, under an `IF` that reads another column or that is a conjunct, or under
        // another operator that reads another column.
        (Cast($"d", StringType) === "-0.0" || tenDividedBy($"y") > 1.0) && $"d" === 0.0,
        If($"y" > 0.0, Cast($"d", StringType), Literal("a")) === "-0.0" && $"d" === 0.0,
        If($"y" > 0.0, signed($"d"), Literal(false)) && $"d" === 0.0,
        Concat(Seq(Cast($"d", StringType), Cast($"y", StringType))) === "-0.01.0" &&
          $"d" === 0.0
      ).foreach(condition => checkOrdered(withE, condition, condition))
      Seq(
        // But where all of the attributes under such an operator get a constant, they are
        // replaced, as `replaceConstants` folds the operator then, although `pow(-0.0, -1.0)` is
        // -Infinity and `pow(0.0, -1.0)` is Infinity.
        (Pow($"d", Literal(-1.0)) < 0.0 && $"d" === 0.0) -> Literal(false),
        (Cast($"d", StringType) === "0.0" && tenDividedBy($"y") > 0.0 && $"d" === 0.0) ->
          (tenDividedBy($"y") > 0.0 && $"d" === 0.0),
        (signed($"d" + $"e") && $"d" === 0.0 && $"e" === 0.0) -> Literal(false),
        // Also where the constant of another one is put in by `replaceConstants`.
        (signed($"d" * $"y") && $"d" === 0.0 && $"y" === 1.0) -> Literal(false),
        (Cast($"d" * $"y", StringType) === "0.0" && $"d" === 0.0 && $"y" === 1.0) ->
          ($"y" === 1.0 && $"d" === 0.0),
        // And so it is under an operator that gives equal results for equal values, also under
        // arithmetic that can fail, as a division by either zero fails.
        ($"d" + $"y" > 1.0 && $"d" === 0.0) -> (Literal(0.0) + $"y" > 1.0 && $"d" === 0.0),
        (Divide(Literal(10.0), $"d" - $"y") > 0.0 && $"d" === 0.0) ->
          (Divide(Literal(10.0), Literal(0.0) - $"y") > 0.0 && $"d" === 0.0),
        // Any other value is equal only to itself.
        (signed($"d" * $"y") && $"d" === 1.0) -> (signed(Literal(1.0) * $"y") && $"d" === 1.0)
      ).foreach { case (condition, expected) => checkOrdered(withE, condition, expected) }
      // It folds where `replaceConstants` reaches on the condition as written, also where the
      // constant of another column that can fail makes the operand after it one that can not
      // fail, so this is FALSE.
      val withD = projected.select($"s", $"id", $"x", $"i", $"s".cast(DoubleType).as("d"))
      checkOrdered(withD, (signed($"d") || tenDividedBy($"i") > 100.0) && $"d" === 0.0 &&
        $"i" === 1, Literal(false))
      // Also a FLOAT.
      val floats = stringIds.select($"x".cast(FloatType).as("y"), $"s".cast(FloatType).as("f"))
      val signedFloat = floats.where($"f" === 0.0f && signed($"f" * $"y")).analyze
      comparePlans(Optimize.execute(signedFloat), signedFloat)
      // And a struct with a DOUBLE in it, whose fields the check does not look at, but which is
      // replaced where it is the only attribute under such an operator.
      val structs = stringIds.select($"x".cast(DoubleType).as("y"),
        CreateNamedStruct(Seq(Literal("a"), $"s".cast(DoubleType))).as("t"))
      val oneStruct = Literal.create(InternalRow(1.0), new StructType().add("a", DoubleType))
      val signedStruct = structs.where($"t" === oneStruct &&
        Cast(CreateNamedStruct(Seq(Literal("a"), $"t", Literal("b"), $"y")), StringType) ===
          "{{-0.0}, 1.0}").analyze
      comparePlans(Optimize.execute(signedStruct), signedStruct)
      checkOrdered(structs, $"t" === oneStruct && Cast($"t", StringType) === "{-0.0}",
        Literal(false))
    }
  }

  test("SPARK-61437: An equality over a column that can fail is handled as before when the " +
      "condition with all of the constants fails to fold") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      // With the constant, the condition would fail to fold, which would fail the query with no
      // row, unlike in a conditional branch, which is not folded where it fails. Then the
      // equalities are handled as since SPARK-60983, and go first.
      val failsToFold = $"i" === 0 && $"x".cast(DoubleType) > tenDividedBy($"i")
      checkOrdered(projected, failsToFold, failsToFold)
      checkOrdered(projected,
        $"id" > 5L && $"i" + 1 > 0 && $"x".cast(DoubleType) > tenDividedBy($"i") && $"i" === 0,
        $"i" === 0 && $"id" > 5L && $"x".cast(DoubleType) > tenDividedBy($"i"))
      // Also where the comparison folds, but the conjunct does not.
      checkOrdered(projected,
        $"i" === 0 && ElementAt(CreateArray(Seq(Literal(true))), ($"i" > 0).cast(IntegerType)),
        $"i" === 0 && ElementAt(Literal.create(Seq(true), ArrayType(BooleanType, false)),
          ($"i" > 0).cast(IntegerType)))
      // Also where it fails only with the constants of the other equalities in, which `COALESCE`
      // no longer guards, or which make the division divide by zero.
      checkOrdered(projected,
        Coalesce(Seq(tenDividedBy($"i"), $"id".cast(DoubleType))) > 0.0 && $"i" === 0 &&
          $"id" === 7L,
        $"i" === 0 && $"id" === 7L &&
          Coalesce(Seq(tenDividedBy($"i"), Literal(7.0))) > 0.0)
      checkOrdered(projected,
        tenDividedBy($"id" - 6L - $"i".cast(LongType)) > 0.0 && $"i" === 1 && $"id" === 7L,
        $"i" === 1 && $"id" === 7L && tenDividedBy($"id" - 6L - $"i".cast(LongType)) > 0.0)
      // Also with a constant that a later iteration folds, e.g. of `id = 7` for a BIGINT `id`.
      checkOrdered(projected,
        Coalesce(Seq(tenDividedBy($"i"), $"id".cast(DoubleType))) > 0.0 && $"i" === 0 &&
          $"id" === Literal(7).cast(LongType),
        $"i" === 0 && $"id" === 7L &&
          Coalesce(Seq(tenDividedBy($"i"), Literal(7.0))) > 0.0)
      // Or with the foldable columns of the projection below in, which `FoldablePropagation`
      // puts in, also where they make the condition of a branch constant.
      val withTotal = projected.select($"s", $"id", $"x", $"i", Literal(100.0).as("total"))
      checkOrdered(withTotal, $"id".cast(DoubleType) > $"total" / $"i".cast(DoubleType) &&
        $"i" === 0, $"i" === 0 && $"id".cast(DoubleType) > $"total" / $"i".cast(DoubleType))
      checkOrdered(withTotal, If($"total" > 0.0, tenDividedBy($"i"), Literal(0.0)) > 0.0 &&
        $"i" === 0, $"i" === 0 && If($"total" > 0.0, tenDividedBy($"i"), Literal(0.0)) > 0.0)
      // Also through other projections, which pass the column on, rename it or compute another one
      // from it, and through other operators that `FoldablePropagation` looks through.
      Seq(
        withTotal.select($"id", $"i", $"total").where($"id" > 0L),
        withTotal.select($"id", $"i", $"total".as("t")).select($"id", $"i", $"t".as("total")),
        withTotal.select($"id", $"i", ($"total" * 2.0).as("t")).select($"id", $"i",
          ($"t" / 2.0).as("total")),
        stringIds.select($"s", $"id", Literal(100.0).as("total")).limit(5)
          .select($"id", castS.as("i"), $"total")
      ).foreach { child =>
        checkOrdered(child, $"id".cast(DoubleType) > $"total" / $"i".cast(DoubleType) &&
          $"i" === 0, $"i" === 0 && $"id".cast(DoubleType) > $"total" / $"i".cast(DoubleType))
      }
      // And from an aggregate, through an expand, and from either side of a join, as
      // `FoldablePropagation` does.
      val totals = LocalRelation($"k".long).select($"k", Literal(100.0).as("total"))
      val withTotalS = stringIds.select($"s", $"id", $"x", Literal(100.0).as("total"))
      Seq(
        stringIds.groupBy($"s", $"id")($"s", $"id", Literal(100.0).as("total")),
        withTotalS.groupBy($"s", $"id", $"total")($"s", $"id", $"total"),
        Expand(Seq(withTotalS.output, withTotalS.output), withTotalS.output, withTotalS),
        stringIds.join(totals, Inner, Some($"x" === $"k")),
        stringIds.join(totals, RightOuter, Some($"x" === $"k")),
        totals.join(stringIds, LeftOuter, Some($"x" === $"k"))
      ).map(_.select($"id", castS.as("i"), $"total")).foreach { child =>
        checkOrdered(child, $"id".cast(DoubleType) > $"total" / $"i".cast(DoubleType) &&
          $"i" === 0, $"i" === 0 && $"id".cast(DoubleType) > $"total" / $"i".cast(DoubleType))
      }
      // But not a column that an outer join can make NULL, which it does not put in, so `i = 0` is
      // propagated in place.
      // Nor from a streaming aggregate or a join of two streams.
      val streamingIds = stringIds.copy(isStreaming = true)
      val streamingTotals = LocalRelation(Seq($"k".long), isStreaming = true)
        .select($"k", Literal(100.0).as("total"))
      Seq(
        stringIds.join(totals, LeftOuter, Some($"x" === $"k")),
        stringIds.join(totals, LeftSingle, Some($"x" === $"k")),
        totals.join(stringIds, RightOuter, Some($"x" === $"k")),
        stringIds.join(totals, FullOuter, Some($"x" === $"k")),
        streamingIds.groupBy($"s", $"id")($"s", $"id", Literal(100.0).as("total")),
        streamingIds.join(streamingTotals, Inner, Some($"x" === $"k"))
      ).map(_.select($"id", castS.as("i"), $"total")).foreach { child =>
        checkOrdered(child, $"id".cast(DoubleType) > $"total" / $"i".cast(DoubleType) &&
          $"i" === 0, $"id".cast(DoubleType) > $"total" / Literal(0.0) && $"i" === 0)
      }
      // Nor where `spark.sql.optimizer.excludedRules` excludes `FoldablePropagation`, which then
      // does not put them in.
      withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> FoldablePropagation.ruleName) {
        checkOrdered(withTotal, $"id".cast(DoubleType) > $"total" / $"i".cast(DoubleType) &&
          $"i" === 0, $"id".cast(DoubleType) > $"total" / Literal(0.0) && $"i" === 0)
      }
      // Then the equalities are also ordered as since SPARK-60983: `i = 0`, after the conjunct
      // that the constants fold to TRUE, does not count as one that can fail, so the written
      // order is kept. `10 / zero` makes the condition fail to fold once `zero` is in.
      val withZero = projected.select($"s", $"id", $"x", $"i", Literal(0.0).as("zero"))
      checkOrdered(withZero,
        $"x".cast(DoubleType) > Literal(10.0) / $"zero" && $"x" > 0L &&
          ($"id" > 5L || tenDividedBy($"i") > 1.0).cast(IntegerType) === 1 && $"i" === 0 &&
          $"id" === 7L,
        $"x".cast(DoubleType) > Literal(10.0) / $"zero" && $"x" > 0L && $"i" === 0 &&
          $"id" === 7L)
      // Where the condition folds, `id = 7` goes first, as `i = 0`, after the conjunct that it
      // folds to TRUE, can fail.
      checkOrdered(projected,
        $"x" > 0L && ($"id" > 5L || tenDividedBy($"i") > 1.0).cast(IntegerType) === 1 &&
          $"i" === 0 && $"id" === 7L,
        $"id" === 7L && $"x" > 0L && $"i" === 0)

      // But a conjunct that fails to fold only as it is rewritten is replaced with what it folds to
      // with all of the constants in, if that can not fail, and the equalities are handled as
      // without such a failure: here `id = 7` goes first, as `i = 0` after it can fail.
      checkOrdered(projected,
        $"x" > 0L && ($"id" > 0L || tenDividedBy($"i") > 1.0) && $"i" === 0 && $"id" === 7L,
        $"id" === 7L && $"x" > 0L && $"i" === 0)
      checkOrdered(projected,
        (($"id" > 10L && tenDividedBy($"i") > 1.0) || $"x" > 5L) && $"i" === 0 && $"id" === 7L,
        $"id" === 7L && $"x" > 5L && $"i" === 0)
      // Also where that can fail, after which `id = 7` goes first, so that it runs only on the rows
      // where `id` is 7, also where no conjunct after it can fail.
      checkOrdered(projected,
        (($"id" > 10L && tenDividedBy($"i") > 1.0) || tenDividedBy($"x") > 5.0) && $"i" === 0 &&
          $"id" === 7L,
        $"id" === 7L && tenDividedBy($"x") > 5.0 && $"i" === 0)
      checkOrdered(projected,
        $"i" === 0 && (($"id" > 10L && tenDividedBy($"i") > 1.0) || tenDividedBy($"x") > 5.0) &&
          $"id" === 7L,
        $"id" === 7L && $"i" === 0 && tenDividedBy($"x") > 5.0)
      // Also with a constant that a later iteration folds, e.g. of `id = 7` for a BIGINT `id`.
      checkOrdered(projected,
        (($"id" > 10L && tenDividedBy($"i") > 1.0) || tenDividedBy($"x") > 5.0) && $"i" === 0 &&
          $"id" === Literal(7).cast(LongType),
        $"id" === 7L && tenDividedBy($"x") > 5.0 && $"i" === 0)
      // But not one over a column that can fail, which stays where it is written, and whose
      // constant goes in place in the next iteration.
      checkOrdered(projected.select($"s", $"id", $"x", $"i", castS.cast(LongType).as("l")),
        (($"id" > 10L && tenDividedBy($"i") > 1.0) || tenDividedBy($"x") > 5.0) && $"i" === 0 &&
          $"id" === 7L && $"l" === Literal(7).cast(LongType),
        $"id" === 7L && tenDividedBy($"x") > 5.0 && $"i" === 0 && $"l" === 7L)
      // A part that failed in a conditional branch is not folded once another rule rewrites it or
      // takes it out of the branch, as in the batch, so this does not fall back and move `i = 0`
      // first. Also where a part can not be copied, but for one that another rule rewrites.
      def failsAtZero(i: Expression): Expression = tenDividedBy(i) > 1.0
      val atZero = Divide(Literal(10.0), Literal(0).cast(DoubleType))
      Seq(
        ($"x": Expression, failsAtZero _, atZero > 1.0, true),
        ($"x", (i: Expression) => Not(failsAtZero(i)), atZero <= 1.0, true),
        ($"x", (i: Expression) => Not(Not(failsAtZero(i))), atZero > 1.0, true),
        (NotCopyable($"x")(), failsAtZero _, atZero > 1.0, true),
        (NotCopyable($"x")(), (i: Expression) => Not(failsAtZero(i)), atZero <= 1.0, false)
      ).foreach { case (x, branch, folded, kept) =>
        def taken(b: Expression): Expression = If(x > 0L || $"id" > 0L, b, Literal(false))
        checkOrdered(projected,
          $"x" < -100L && taken(branch($"i")) && $"i" === 0 && $"id" === 7L,
          if (kept) {
            $"id" === 7L && $"x" < -100L && taken(folded) && $"i" === 0
          } else {
            $"id" === 7L && $"i" === 0 && $"x" < -100L && taken(folded)
          })
      }
      // Nor does it tag a part that a copy shares, e.g. an object.
      ConstantPropagation(projected.where($"x" < -100L &&
        If($"x" > 0L || $"id" > 0L, FailingObject, Literal(false)) && $"i" === 0 &&
        $"id" === 7L).analyze)
      assert(!FailingObject.containsTag(ConstantFolding.FAILED_TO_EVALUATE))
      // A part that the copy of a conjunct that can not be copied leaves untagged is not put into
      // the plan, where `ConstantFolding` would fold it outside of its branch, so the equalities
      // are handled as since SPARK-60983 there.
      val untagged = conjuncts(Optimize.execute(projected.where($"x" < -100L &&
        NotCopyable($"x")() + ($"id" > 0L || tenDividedBy($"i") > 1.0).cast(LongType) >
          If($"id" > 5L || NotCopyable($"x")() > 0L, tenDividedBy($"i").cast(LongType),
            Literal(0L)) && $"i" === 0 && $"id" === 7L).analyze))
      assert(untagged.take(2) == Seq("(i = 0)", "(id = 7L)"), untagged)
      // It folds a conditional branch before `SimplifyConditionals` takes it out, as in the batch.
      checkOrdered(projected, If($"x" > 0L, tenDividedBy($"i") > 1.0, tenDividedBy($"i") > 1.0) &&
        $"i" === 0,
        If($"x" > 0L, tenDividedBy(Literal(0)) > 1.0, tenDividedBy(Literal(0)) > 1.0) && $"i" === 0)
      // And runs `OptimizeRand` before `ConstantFolding`, which makes `rand(1) > 1.0` FALSE, so
      // that the division by zero after it is not folded, as in the batch, unless it is excluded.
      object OptimizeWithRand extends RuleExecutor[LogicalPlan] {
        val batches = Batch("ConstantPropagation", FixedPoint(10),
          ConstantPropagation, OptimizeRand, ConstantFolding, BooleanSimplification) :: Nil
      }
      val withDd = stringIds.select($"s", $"id", $"s".cast(DoubleType).as("dd"))
      val dividedByZero = Divide(Literal(10.0), $"dd" * $"dd" - 1.0) > 1.0
      val guardedByRand = ((Rand(1L) > $"dd" && dividedByZero) || $"id" > 0L) && $"dd" === 1.0
      Seq(
        "" -> ($"id" > 0L && $"dd" === 1.0),
        OptimizeRand.ruleName ->
          ($"dd" === 1.0 && ((Rand(1L) > $"dd" && dividedByZero) || $"id" > 0L))
      ).foreach { case (excluded, expected) =>
        withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> excluded) {
          val optimized = OptimizeWithRand.execute(withDd.where(guardedByRand).analyze)
          comparePlans(optimized, withDd.where(expected).analyze)
        }
      }
      // It also runs `SimplifyExtractValueOps`, which takes the field out of `UpdateFields`, so
      // that the condition becomes FALSE.
      val struct = CreateNamedStruct(Seq(Literal("a"), $"x".cast(DoubleType)))
      val updated = UpdateFields(UpdateFields(struct, Seq(WithField("a", $"i".cast(DoubleType)))),
        Seq(WithField("b", $"x".cast(DoubleType))))
      checkOrdered(projected, GetStructField(updated, 0) > 5.0 && tenDividedBy($"x") > 0.0 &&
        $"i" === 0, Literal(false))
      // Where `ConstantFolding` is excluded, nothing fails to fold, so the constant goes in place.
      object OptimizeWithoutFolding extends RuleExecutor[LogicalPlan] {
        val batches = Batch("ConstantPropagation", FixedPoint(10),
          ConstantPropagation, BooleanSimplification) :: Nil
      }
      withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ConstantFolding.ruleName) {
        comparePlans(
          OptimizeWithoutFolding.execute(projected.where(
            $"i" === 0 && $"x".cast(DoubleType) > tenDividedBy($"i")).analyze),
          projected.where($"i" === 0 && $"x".cast(DoubleType) > tenDividedBy(Literal(0))).analyze)
      }
      checkOrdered(projected,
        tenDividedBy($"x" - $"id") > 0.0 && $"x" > 0L && ($"id" > 0L || tenDividedBy($"i") > 1.0) &&
          $"i" === 0 && $"id" === 7L,
        $"id" === 7L && tenDividedBy($"x" - $"id") > 0.0 && $"x" > 0L && $"i" === 0)
      // Where the cast of such an operand is compared with another column, `replaceConstants`
      // folds the cast whole, so the rewritten conjunct does not fail.
      checkOrdered(projected,
        $"x" > 0L && ($"id" > 5L || tenDividedBy($"i") > 1.0).cast(LongType) <= $"x" &&
          $"i" === 0 && $"id" === 7L,
        $"id" === 7L && $"x" > 0L && Literal(1L) <= $"x" && $"i" === 0)

      // But not where only a conditional branch would fail, which is not folded.
      checkOrdered(projected,
        $"i" === 0 && If($"x" > 0L, tenDividedBy($"i"), Literal(0.0)) > 0.0,
        $"i" === 0 && If($"x" > 0L, tenDividedBy(Literal(0)), Literal(0.0)) > 0.0)
      checkOrdered(projected,
        CaseWhen(Seq(($"id" < 0L) -> ($"x".cast(DoubleType) > tenDividedBy($"i"))),
          $"s".isNotNull) && $"i" === 0,
        CaseWhen(Seq(($"id" < 0L) -> ($"x".cast(DoubleType) > tenDividedBy(Literal(0)))),
          $"s".isNotNull) && $"i" === 0)
      // Trying to fold it leaves no mark on a part that fails in a conditional branch, which
      // would keep `ConstantFolding` from failing on the same part elsewhere.
      val byZero = Divide(Literal(10.0), Literal(0.0))
      intercept[ArithmeticException](Optimize.execute(projected
        .where($"x".cast(DoubleType) > byZero && $"i" === 0 &&
          If($"x" > 0L, byZero, Literal(0.0)) > $"i".cast(DoubleType)).analyze))
      intercept[ArithmeticException](Optimize.execute(projected
        .where($"i" === 0 && If($"x" > 0L, byZero, Literal(0.0)) > $"i".cast(DoubleType))
        .select($"id", byZero.as("z")).analyze))
    }
  }

  test("SPARK-61437: A conjunct over a column that can fail can fail") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      // `id = 7` folds the guard `id > 0`, so it goes ahead of `i > 0`, which can fail.
      checkOrdered(projected, $"id" > 0L && $"i" > 0 && $"id" === 7L, $"id" === 7L && $"i" > 0)
      checkOrdered(projected, $"id" > 0L && $"i" === 1 && $"id" === 7L,
        $"id" === 7L && $"i" === 1)
      // `id < 3` decides whether `i > 0` runs. `i > 0` counts as one that can not fail there, as
      // since SPARK-61033, so `id < 3` is rewritten, and the equality goes first, ahead of `i > 0`.
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
      // it, as for any other nullable column.
      checkOrdered(notNull, Not($"i" + 1 > 3 && $"i" === 1), $"i" + 1 <= 3 || Not($"i" === 1))
      // `upper` is not, so the condition inside becomes FALSE, as for any other column.
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
      // Wherever the conjunct that becomes FALSE is.
      Seq(
        $"i" + 1 > 3 && tenDividedBy($"x") > 0.0 && $"i" === 1,
        $"i" === 1 && tenDividedBy($"x") > 0.0 && $"i" === 2,
        $"i" > 0 && tenDividedBy($"x") > 0.0 && $"i" + 1 > 3 && $"i" === 1,
        $"i" > 0 && $"j" > 5 && $"i" > 10 && $"i" === 1,
        $"i" > 0 && tenDividedBy($"x") > 0.0 && $"i" === 1 && $"i" + 1 > 3
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
        $"i".cast(LongType) + $"id" > 3L && $"i" === 1 && $"id" === 1L,
        // Also where only another rule of the batch makes it FALSE, here `NullPropagation`, after
        // `SimplifyConditionals` makes the `CASE WHEN` NULL, so that the division does not run on
        // a row where `i` is not 1.
        tenDividedBy($"x" - $"i".cast(LongType)) > 0.0 &&
          CaseWhen(Seq(($"id" > 10L) -> $"x")) + $"x" > 1L && $"i" === 1 && $"id" === 7L
      ).foreach { condition =>
        comparePlans(Optimize.execute(projected.where(condition).analyze),
          projected.where(false).analyze)
      }
      comparePlans(
        Optimize.execute(
          projected.where(($"id" < 0L || ($"i" + 1 > 3 && $"i" === 1)) && $"id" > 5L).analyze),
        projected.where($"id" < 0L && $"id" > 5L).analyze)
      // Also where the constant of an equality over another column makes a branch FALSE, which
      // that constant does not go into, as the branch reads another column. Otherwise `id = 7`
      // would go first, and the division would run on a row where `i` is not 1.
      Seq(
        CaseWhen(Seq(($"id" > 10L) -> tenDividedBy($"x")), Literal(0.0)) > 1.0,
        If($"id" > 7L, $"x" > 1L, Literal(false)),
        // Also where it takes `BooleanSimplification`.
        ($"id" < 7L && tenDividedBy($"x") > 1.0) || ($"id" > 7L && tenDividedBy($"x") > 2.0),
        // Also where it is NULL, which a filter takes as FALSE.
        CaseWhen(Seq(($"id" > 10L) -> tenDividedBy($"x"))) > 1.0,
        If($"id" > 7L, $"x" > 1L, Literal(null, BooleanType)),
        // Also where it takes `SimplifyConditionals` and `BooleanSimplification` twice.
        If(If($"id" > 7L, $"x" > 1L, Literal(false)) && $"x" > 0L, tenDividedBy($"x"),
          Literal(0.0)) > 1.0
      ).foreach { conjunct =>
        comparePlans(Optimize.execute(projected.where(
          tenDividedBy($"x" - $"i".cast(LongType)) > 0.0 && conjunct && $"i" === 1 &&
            $"id" === 7L).analyze),
          projected.where(false).analyze)
      }
      // Also with a constant that a later iteration folds, e.g. of `id = 7` for a BIGINT `id`.
      Seq($"id" === Literal(7).cast(LongType), Literal(7).cast(LongType) === $"id").foreach {
        equality =>
          comparePlans(Optimize.execute(projected.where(
            tenDividedBy($"x" - $"i".cast(LongType)) > 0.0 &&
              CaseWhen(Seq(($"id" > 10L) -> tenDividedBy($"x")), Literal(0.0)) > 1.0 &&
              $"i" === 1 && equality).analyze),
            projected.where(false).analyze)
      }
      // But a NULL under a `NOT` is not FALSE. With `s` not nullable, `upper(s)` is not either,
      // so `u = 'A'` is propagated there.
      val notNull = LocalRelation($"s".string.notNull, $"id".long.notNull, $"x".long.notNull)
        .select($"id", $"x", Upper($"s").as("u"))
      checkOrdered(notNull,
        Not(If($"id" > 7L, $"x" > 1L, Literal(null, BooleanType)) && $"u" === "A" &&
          $"id" === 7L),
        Not($"id" === 7L) || Not(If($"id" > 7L, $"x" > 1L, Literal(null, BooleanType))) ||
          Not($"u" === "A"))
      // There, a constant that a later iteration folds goes in if the literal does, also for a
      // nullable expression, as in the next iteration: here `d = 7` makes the `IF` FALSE.
      val withD = LocalRelation($"s".string.notNull, $"d".int.notNull, $"x".long.notNull)
        .select($"d", $"x", Upper($"s").as("u"))
      val nullableSeven = If(Literal(true), Literal(7), Literal(null, IntegerType))
      val underNot = withD.where(Not($"u" === "X" && $"d" === nullableSeven &&
        If($"d" > 10, tenDividedBy($"x"), Literal(0.0)) > 1.0)).analyze
      comparePlans(ConstantPropagation(underNot), withD.where(Not(Literal(false))).analyze)
    }
  }

  test("SPARK-61437: A column that can fail through a reference to a CTE that is not inlined") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      val definition = CTERelationDef(projected.analyze)
      // A reference has attributes of its own, mapped to those of the definition by position.
      val reference = CTERelationRef(definition.id, _resolved = true,
        definition.output.map(_.newInstance()), isStreaming = false)
      val written = $"id" > 5L && timesId($"i") > 0L && $"i" === 1
      val expected = $"id" > 5L && Literal(1L) * $"id" > 0L && $"i" === 1
      val optimized = Optimize.execute(WithCTE(reference.where(written).analyze, Seq(definition)))
      val expectedPlan = WithCTE(reference.where(expected).analyze, Seq(definition))
      comparePlans(optimized, expectedPlan)
      assert(optimized.collectFirst { case f: Filter => conjuncts(f) } ==
        expectedPlan.collectFirst { case f: Filter => conjuncts(f) })
      // Only the columns of the definition that can fail count: `id = 7` goes first.
      val moved = WithCTE(reference.where($"id" > 0L && $"i" > 0 && $"id" === 7L).analyze,
        Seq(definition))
      assert(Optimize.execute(moved).collectFirst { case f: Filter => conjuncts(f) } ==
        Some(conjuncts(reference.where($"id" === 7L && $"i" > 0).analyze)))
      // Without the definition, the reference reads columns that can not fail, as before.
      checkOrdered(reference, $"id" > 0L && $"i" > 0 && $"id" === 7L, $"i" > 0 && $"id" === 7L)
    }
  }

  test("SPARK-61437: Columns that can not fail") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      // A non-deterministic alias is computed on every row the projection gets, so it raises no
      // new error here, and an equality over it is propagated and moved.
      val random = stringIds.select($"id", $"x", Rand(1L).as("r"))
      checkOrdered(random, $"r" > 0.0 && tenDividedBy($"x") > 0.0 && $"r" === 0.5,
        $"r" === 0.5 && tenDividedBy($"x") > 0.0)
      // So is one over a column that reads it with an expression that can not fail.
      val randomPlusOne = random.select($"id", ($"r" + 1.0).as("r1"))
      checkOrdered(randomPlusOne, $"r1" > 0.0 && tenDividedBy($"id") > 0.0 && $"r1" === 1.5,
        $"r1" === 1.5 && tenDividedBy($"id") > 0.0)
      // And one that a projection below another operator computes, e.g. for a shuffle, for every
      // row that operator gets.
      val shuffled = stringIds.select($"id", castS.as("i")).repartition(2).select($"id", $"i")
      checkOrdered(shuffled, $"id" > 0L && $"i" > 0 && $"id" === 7L, $"i" > 0 && $"id" === 7L)
      // And the result of an aggregate function, whose input an aggregation computes for every
      // row it gets, a grouping key, a result column of a global aggregation, which computes it for
      // its one row up front, and a non-deterministic result column.
      Seq(
        stringIds.groupBy($"id")($"id", max(castS).as("i")),
        stringIds.select($"id", castS.as("i")).groupBy($"i")($"i", max($"id").as("id")),
        stringIds.groupBy()(max($"id").as("id"), max($"s").cast(IntegerType).as("i")),
        stringIds.groupBy($"s", $"id")($"s", $"id",
          (castS + Rand(1L).cast(IntegerType)).as("i"))
      ).foreach { child =>
        checkOrdered(child, $"id" > 0L && $"i" > 0 && $"id" === 7L, $"i" > 0 && $"id" === 7L)
      }
      // And one that stands for a cast that can not fail.
      val widened = stringIds.select($"s", $"id", $"x", $"id".cast(DoubleType).as("k"))
      checkOrdered(widened, $"k" > 0.0 && tenDividedBy($"x") > 0.0 && $"k" === 3.0,
        $"k" === 3.0 && tenDividedBy($"x") > 0.0)
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
      val cased = stringIds.select($"s", $"id", Upper($"s").as("u"), Lower($"s").as("l"))
      checkOrdered(cased, Length($"u") > 0 && Length($"l") > 0 && $"u" === "X",
        Length($"l") > 0 && $"u" === "X")
      // `length(l) > 0` can fail, so `id = 7`, which folds `id > 0`, goes ahead of it.
      checkOrdered(cased, $"id" > 0L && Length($"l") > 0 && $"id" === 7L,
        $"id" === 7L && Length($"l") > 0)
      checkOrdered(cased, Length($"u") > 0 && tenDividedBy($"id") > 0.0 && $"u" === "X",
        tenDividedBy($"id") > 0.0 && $"u" === "X")
      // But an operand that decides whether one over it runs is rewritten, as since SPARK-61033.
      checkOrdered(cased, ($"id" < 3L || $"u" > "A") && $"id" === 7L, $"id" === 7L && $"u" > "A")

      // Each projection reads the column below it twice. The check does not expand the column,
      // whose expression would have 2^40 leaves.
      val chain = (1 to 40).foldLeft(stringIds.select($"id".as("c"))) { (child, _) =>
        child.select(($"c" + $"c").as("c"))
      }
      checkOrdered(chain, $"c" > 0L && $"c" === 2L, $"c" === 2L)
    }
  }

  test("SPARK-61704: Do not propagate an EqualTo with a NULL literal under a NOT") {
    val nullInt = Literal(null, IntegerType)
    // `d = NULL` is NULL on every row, so the condition under the `NOT` is NULL or FALSE.
    // Replacing `d` with NULL would make `d + 0 <=> 5` FALSE, and the condition FALSE where it is
    // NULL, or, below an `OR`, `d > b` NULL, and the `AND` NULL where it is FALSE.
    Seq(
      Not(columnD === nullInt && (columnD + 0 <=> 5)),
      Not(nullInt === columnD && (columnD + 0 <=> 5)),
      Not((columnD === nullInt && columnD > columnB) || columnB > 0)
    ).foreach { condition =>
      val withNull = testRelation.where(condition).analyze
      comparePlans(ConstantPropagation(withNull), withNull)
    }
    // An `EqualTo` with another literal on the left is still propagated, as one on the right is in
    // the SPARK-30447 test.
    comparePlans(
      ConstantPropagation(testRelation.where(Not(Literal(1) === columnD && columnD > columnB))
        .analyze),
      testRelation.where(Not(Literal(1) === columnD && Literal(1) > columnB)).analyze)
    // Outside a `NOT`, where NULL counts as FALSE, the condition is never TRUE whatever `a` is
    // replaced with, so it is still propagated, as before.
    Seq(columnA === nullInt, nullInt === columnA).foreach { equality =>
      comparePlans(
        ConstantPropagation(testRelation.where(equality && columnA > 5).analyze),
        testRelation.where(equality && nullInt > 5).analyze)
    }
    // An `EqualNullSafe` with a NULL literal is TRUE exactly where the attribute is NULL, so
    // it is still propagated ...
    Seq(columnA <=> nullInt, nullInt <=> columnA).foreach { nullSafe =>
      comparePlans(
        Optimize.execute(testRelation.where(nullSafe && (columnA + 0 <=> 5)).analyze),
        testRelation.where(Literal(false)).analyze)
    }
    // ... also under a `NOT`, for an attribute that is not nullable ...
    Seq(columnD <=> nullInt, nullInt <=> columnD).foreach { nullSafe =>
      comparePlans(
        ConstantPropagation(testRelation.where(Not(nullSafe && columnD > columnB)).analyze),
        testRelation.where(Not(nullSafe && nullInt > columnB)).analyze)
    }
    // ... but, as before, not over a nullable one.
    Seq(columnA <=> nullInt, nullInt <=> columnA).foreach { nullSafe =>
      val nullable = testRelation.where(Not(nullSafe && columnA > columnB)).analyze
      comparePlans(ConstantPropagation(nullable), nullable)
    }
  }
}

/** An expression that can not be copied, as `TreeNode.clone` fails for it. */
private case class NotCopyable(child: Expression)(val unused: Int = 0)
    extends UnaryExpression with CodegenFallback {
  override def dataType: DataType = child.dataType
  override def eval(input: InternalRow): Any = child.eval(input)
  override protected def withNewChildInternal(newChild: Expression): NotCopyable =
    NotCopyable(newChild)(unused)
}

/** A foldable object that fails to evaluate, which `TreeNode.clone` returns as it is. */
private case object FailingObject extends LeafExpression with CodegenFallback {
  override def dataType: DataType = BooleanType
  override def nullable: Boolean = false
  override def foldable: Boolean = true
  override def eval(input: InternalRow): Any = throw new ArithmeticException("fails")
}
