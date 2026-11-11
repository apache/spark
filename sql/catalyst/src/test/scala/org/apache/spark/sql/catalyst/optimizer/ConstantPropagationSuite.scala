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
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}

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

  /** Checks that `plan` optimizes to `expected` with the conjuncts in the same order. */
  private def checkInOrder(plan: LogicalPlan, expected: LogicalPlan): Unit = {
    val optimized = Optimize.execute(plan)
    comparePlans(optimized, expected)
    def conjuncts(p: LogicalPlan): Seq[String] = p match {
      case Filter(condition, _) => splitConjunctivePredicates(condition).map(_.sql)
      case other => fail(s"Not a Filter: $other")
    }
    assert(conjuncts(optimized) == conjuncts(expected))
  }

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
      checkInOrder(notConstant,
        testRelation.where(columnB === 0 && divide(columnC - columnB) > Literal(0.0)).analyze)

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

  test("SPARK-61354: Do not move an equality ahead of a non-deterministic conjunct") {
    def divide(e: Expression): Expression = Divide(Literal(10.0), e.cast(DoubleType))
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      // A conjunct takes the constants of the equalities before its segment and in it, but not
      // of those after it, also when it refers to all of them.
      val mixed = testRelation.where(columnD === 3 && Rand(10) < Literal(0.5) &&
        columnD + columnA + columnB > columnC && columnA === 1 && Rand(1) < Literal(0.5) &&
        columnB === 2).analyze
      assert(ConstantPropagation(mixed).fastEquals(testRelation.where(columnD === 3 &&
        Rand(10) < Literal(0.5) && Literal(3) + Literal(1) + columnB > columnC && columnA === 1 &&
        Rand(1) < Literal(0.5) && columnB === 2).analyze))
    }
    Seq(true, false).foreach { ansi =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi.toString) {
        // `rand(10)` would see only the rows with `a` = 1 with the equality first, and the rows
        // with `a` <= 0 too with `a > 0` replaced.
        val before = testRelation.where(columnA > 0 && Rand(10) < Literal(0.5) && columnA === 1)
          .analyze
        checkInOrder(before, before)

        // The conjuncts after it take the constants of the equalities before it.
        checkInOrder(
          testRelation.where(columnA === 1 && Rand(10) < Literal(0.5) && columnA > 0).analyze,
          testRelation.where(columnA === 1 && Rand(10) < Literal(0.5)).analyze)
      }
    }

    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      // An equality still goes first among the conjuncts after the non-deterministic one.
      checkInOrder(
        testRelation.where(Rand(10) < Literal(0.5) && columnA > 0 &&
          divide(columnC) > Literal(0.0) && columnA === 1).analyze,
        testRelation.where(Rand(10) < Literal(0.5) && columnA === 1 &&
          divide(columnC) > Literal(0.0)).analyze)
      // And only those: the one before `rand(10)` is not rewritten, as `a = 1` does not go
      // ahead of it.
      checkInOrder(
        testRelation.where(columnA > 0 && Rand(10) < Literal(0.5) && columnB > 0 &&
          divide(columnC) > Literal(0.0) && columnA === 1 && columnB === 2).analyze,
        testRelation.where(columnA > 0 && Rand(10) < Literal(0.5) && columnA === 1 &&
          columnB === 2 && divide(columnC) > Literal(0.0)).analyze)

      // So a conjunct that can fail before it runs on the rows that an equality after it drops,
      // as written.
      val failsBefore = testRelation
        .where(divide(columnC - columnB) > Literal(0.0) && Rand(10) < Literal(0.5) && columnB === 5)
        .analyze
      checkInOrder(failsBefore, failsBefore)
      // The constants of the earlier segments reach every later one.
      checkInOrder(
        testRelation.where(columnA === 1 && Rand(10) < Literal(0.5) && Rand(1) < Literal(0.5) &&
          columnC > columnA).analyze,
        testRelation.where(columnA === 1 && Rand(10) < Literal(0.5) && Rand(1) < Literal(0.5) &&
          columnC > 1).analyze)
      // Also in an `OR` and under a `NOT`.
      val inOr = testRelation
        .where((columnA > 0 && Rand(10) < Literal(0.5) && columnA === 1) || columnB > 0).analyze
      comparePlans(Optimize.execute(inOr), inOr)
      val underNot = testRelation
        .where(Not(columnD > 0 && Rand(10) < Literal(0.5) && columnD === 1)).analyze
      comparePlans(ConstantPropagation(underNot), underNot)

      // An equality in another form, or a copy of the propagated one, is an equality too, so the
      // conjuncts after it take its constant also across a non-deterministic conjunct, and it is
      // kept. In one pass of the rule, as the other rules of the batch drop `a = 1` after `1 = a`.
      val qualifiedA = testRelation.output.head.withQualifier(Seq("t"))
      Seq(Literal(1) === columnA, columnA <=> 1, columnA === 1, qualifiedA === 1).foreach { first =>
        assert(ConstantPropagation(testRelation
          .where(first && columnC > columnA && Rand(10) < Literal(0.5) && columnA === 1).analyze)
          .fastEquals(testRelation
            .where(first && columnC > 1 && Rand(10) < Literal(0.5) && columnA === 1).analyze))
      }
      // In the segment of the propagated one, another form folds away as on master.
      checkInOrder(
        testRelation.where(Literal(1) === columnA && columnC > columnA && columnA === 1).analyze,
        testRelation.where(columnC > 1 && columnA === 1).analyze)

      // The non-deterministic conjunct takes the constants of the equalities before it, but not
      // of those after it, in one pass of the rule.
      val randOverA = Rand(10) < columnA.cast(DoubleType)
      comparePlans(
        ConstantPropagation(testRelation.where(columnA === 1 && randOverA).analyze),
        testRelation.where(columnA === 1 && Rand(10) < Literal(1).cast(DoubleType)).analyze)
      val randBefore = testRelation.where(randOverA && columnA === 1).analyze
      comparePlans(ConstantPropagation(randBefore), randBefore)

      // A conjunct that is FALSE with the constants of all the equalities still makes the
      // condition FALSE, as no row passes it.
      Seq[(Expression, Expression)](
        (columnA === 1 && Rand(10) < Literal(0.5) && columnA === 2, Literal(false)),
        (columnB > 0 && columnA > 5 && Rand(10) < Literal(0.5) && columnA === 1, Literal(false)),
        ((columnA === 1 && Rand(10) < Literal(0.5) && columnA === 2) || columnB > 0, columnB > 0)
      ).foreach { case (condition, expected) =>
        comparePlans(
          Optimize.execute(testRelation.where(condition).analyze),
          testRelation.where(expected).analyze)
      }
      // Also under a `NOT`, where the attributes are not nullable, so the equalities are not NULL.
      Seq(divide(columnC) > Literal(0.0), Rand(10) < Literal(0.5)).foreach { between =>
        comparePlans(
          ConstantPropagation(
            testRelation.where(Not(columnD === 1 && between && columnD === 2)).analyze),
          testRelation.where(Not(Literal(false))).analyze)
      }
      // There, a conjunct that is NULL with the constants does not make the condition FALSE.
      val nullInt = Literal(null, IntegerType)
      comparePlans(
        ConstantPropagation(testRelation.where(Not(columnD === 1 &&
          divide(columnC) > Literal(0.0) && columnD > nullInt)).analyze),
        testRelation.where(Not(columnD === 1 && divide(columnC) > Literal(0.0) &&
          Literal(1) > nullInt)).analyze)
      // An `EqualTo` with a NULL literal is NULL on every row, so its literal is not propagated.
      Seq(
        Not(columnD === nullInt && divide(columnC) > Literal(0.0) && (columnD + 0 <=> 5)),
        Not(nullInt === columnD && divide(columnC) > Literal(0.0) && (columnD + 0 <=> 5)),
        columnA === nullInt && columnA > 5 && divide(columnC) > Literal(0.0)
      ).foreach { condition =>
        val withNull = testRelation.where(condition).analyze
        comparePlans(ConstantPropagation(withNull), withNull)
      }
      // Nor is it an equality in another form of a propagated `EqualNullSafe` with one.
      val nullForm = testRelation.where(columnA === nullInt && Rand(10) < Literal(0.5) &&
        columnC > columnA && Rand(1) < Literal(0.5) && (columnA <=> nullInt)).analyze
      comparePlans(ConstantPropagation(nullForm), nullForm)
      // An `EqualNullSafe` with one is TRUE exactly where the attribute is NULL, so it is.
      comparePlans(
        Optimize.execute(testRelation.where((columnA <=> nullInt) && (columnA + 0 <=> 5)).analyze),
        testRelation.where(Literal(false)).analyze)

      // The shape of the `AND` is kept when nothing moves.
      val Filter(rightNested, _) = ConstantPropagation(
        testRelation.where(columnA > 0 && (columnC > columnA && columnA === 1)).analyze)
      val Filter(expectedRightNested, _) =
        testRelation.where(Literal(1) > 0 && (columnC > 1 && columnA === 1)).analyze
      assert(rightNested.fastEquals(expectedRightNested))
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
    checkInOrder(
      testRelation.where(columnA > Add(columnC, columnB, ansi) && columnB === 2).analyze,
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
}
