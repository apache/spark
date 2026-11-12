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
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType}

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
      // `i = 1` can fail, as the cast does, so it is not moved ahead of `id > 5`.
      val written = $"id" > 5L && $"i".cast(LongType) * $"id" > 0L && $"i" === 1
      Seq(
        projected,
        projected.select($"s", $"id", $"i"),
        projected.where($"id" > 1L).select($"s", $"id", $"i"),
        projected.sample(0.0, 0.5, withReplacement = false, 1L).select($"s", $"id", $"i"),
        projected.select($"s", $"id", $"i".as("j")).select($"s", $"id", $"j".as("i"))
      ).foreach(child => checkOrdered(child, written, written))
    }
  }

  test("SPARK-61437: Propagate an equality over a column that can fail where it is written") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      // A conjunct before it that it rewrites can pass a row it would not only where it would fail
      // as well or where `i` does not fail, so `i = 1` raises no new error, and the conjuncts
      // after it only run where it does not fail.
      Seq(
        ($"id" > 5L && $"i" + 1 > 1 && $"i" === 1) -> ($"id" > 5L && $"i" === 1),
        (($"id" < 0L || $"i" + 1 > 3) && $"i" === 1) -> ($"id" < 0L && $"i" === 1),
        (divide($"i") > 1.0 && $"i" === 1) -> ($"i" === 1),
        ($"id" > 5L && ($"i" <=> 1) && $"i" > 0) -> ($"id" > 5L && ($"i" <=> 1)),
        ($"id" > 5L && (Literal(1) <=> $"i") && $"i" > 0) -> ($"id" > 5L && (Literal(1) <=> $"i")),
        // Only the conjuncts from the first rewritten one to it count.
        (divide($"x") > 0.0 && $"i" > 0 && $"i" === 1) -> (divide($"x") > 0.0 && $"i" === 1),
        ($"i" + 1 > 1 && $"i" === 1 && divide($"x") > 0.0) -> ($"i" === 1 && divide($"x") > 0.0),
        // `id > 0` is rewritten by `id = 7`, which goes first. Then `i = 1` is propagated too.
        ($"id" > 0L && divide($"x") > 0.0 && $"i" === 1 && $"i" + 1 > 1 && $"id" === 7L) ->
          ($"id" === 7L && divide($"x") > 0.0 && $"i" === 1)
      ).foreach { case (condition, expected) => checkOrdered(projected, condition, expected) }
    }
  }

  test("SPARK-61437: Do not propagate an equality over a column that can fail past a " +
      "conjunct that can fail") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      // The division would run on the rows that `i > 0` passes when rewritten, i.e. all rows.
      val twoCasts = projected.select($"s", $"id", $"x", $"i", castS.as("j"))
      Seq(
        $"id" > 5L && $"i" > 0 && divide($"x") > 0.0 && $"i" === 1,
        $"id" > 5L && $"i" > 0 && divide($"x") > 0.0 && Literal(1) === $"i",
        $"id" > 5L && $"i" > 0 && divide($"x") > 0.0 && ($"i" <=> 1),
        $"id" > 5L && $"i" > 0 && divide($"x") > 0.0 && (Literal(1) <=> $"i"),
        // Without `i = 1`, `i > 0` can fail, and it comes between `j = 2` and `j > 0`.
        $"j" > 0 && $"i" > 0 && $"j" === 2 && divide($"x") > 0.0 && $"i" === 1
      ).foreach(condition => checkOrdered(twoCasts, condition, condition))

      // Unless a conjunct before it becomes FALSE, or NULL, which is made FALSE, so no row
      // reaches it.
      Seq(
        $"i" + 1 > 3 && divide($"x") > 0.0 && $"i" === 1,
        $"i" === 1 && divide($"x") > 0.0 && $"i" === 2,
        If($"i" === 1, Literal(null, IntegerType), $"i") > 0 && divide($"x") > 0.0 && $"i" === 1
      ).foreach { condition =>
        comparePlans(Optimize.execute(projected.where(condition).analyze),
          projected.where(false).analyze)
      }
      // But not NULL under a `NOT`, where the division still runs after it.
      val upperNotNull = LocalRelation($"s".string.notNull, $"x".long.notNull)
        .select($"x", Upper($"s").as("u"))
      Optimize.execute(upperNotNull.where(Not(
        If($"u" === "X", Literal(null, StringType), $"u") === "Y" && divide($"x") > 0.0 &&
          $"u" === "X")).analyze) match {
        case Filter(negated, _) => assert(negated.exists(_.isInstanceOf[If]))
        case other => fail(s"Expected a Filter: $other")
      }
    }
  }

  test("SPARK-61437: A conjunct over a column that can fail can fail") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      // `id = 7` folds the guard `id > 0`, so it goes ahead of `i > 0`, which can fail.
      checkOrdered(projected, $"id" > 0L && $"i" > 0 && $"id" === 7L, $"id" === 7L && $"i" > 0)
      checkOrdered(projected, $"id" > 0L && $"i" === 1 && $"id" === 7L,
        $"id" === 7L && $"i" === 1)
      // `id < 3` decides whether `i > 0` runs, so it keeps `id`, and the equality goes first.
      checkOrdered(projected, ($"id" < 3L || $"i" > 0) && $"id" === 5L,
        $"id" === 5L && ($"id" < 3L || $"i" > 0))

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
      // But one that becomes NULL is not made FALSE, which the `NOT` would turn into TRUE.
      val nullInside = Optimize.execute(upperNotNull.where(Not(
        If($"u" === "X", Literal(null, StringType), $"u") === "Y" && $"u" === "X")).analyze)
      assert(!nullInside.fastEquals(upperNotNull.where(true).analyze))
    }
  }

  test("SPARK-61437: A condition that becomes FALSE with the constants is FALSE") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
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
      // And one that stands for a cast that can not fail.
      val widened = strings.select($"s", $"id", $"x", $"id".cast(DoubleType).as("k"))
      checkOrdered(widened, $"id" > 5L && $"k" > 0.0 && $"k" === 3.0, $"id" > 5L && $"k" === 3.0)
      checkOrdered(widened, $"k" > 0.0 && divide($"x") > 0.0 && $"k" === 3.0,
        $"k" === 3.0 && divide($"x") > 0.0)
    }

    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      // The cast can not fail with ANSI mode off, so `i = 1` goes ahead of `length(u) > 0`, which
      // can, as `upper` is not in the allowlist of `ExprUtils.canEvaluateUnconditionally`.
      val withUpper = projected.select($"s", $"id", $"i", Upper($"s").as("u"))
      checkOrdered(withUpper, $"id" > 5L && $"i" > 0 && Length($"u") > 0 && $"i" === 1,
        $"i" === 1 && $"id" > 5L && Length($"u") > 0)

      // So an alias of `upper` or `lower` counts as one that can fail in every mode.
      val cased = strings.select($"s", $"id", Upper($"s").as("u"), Lower($"s").as("l"))
      val condition = Length($"u") > 0 && Length($"l") > 0 && $"u" === "X"
      checkOrdered(cased, condition, condition)

      // Each projection reads the column below it twice. The check does not expand the column,
      // whose expression would have 2^40 leaves.
      val chain = (1 to 40).foldLeft(strings.select($"id".as("c"))) { (child, _) =>
        child.select(($"c" + $"c").as("c"))
      }
      checkOrdered(chain, $"c" > 0L && $"c" === 2L, $"c" === 2L)
    }
  }
}
