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
import org.apache.spark.sql.catalyst.expressions.{Cast, Divide, EvalMode, Expression, If, InSubquery, IsNotNull, IsNull, ListQuery, Literal, NumericEvalContext, RaiseError, Rand}
import org.apache.spark.sql.catalyst.planning.ExtractFiltersAndInnerJoins
import org.apache.spark.sql.catalyst.plans.{Cross, Inner, InnerLike, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.IntegerType

class JoinOptimizationSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Subqueries", Once,
        EliminateSubqueryAliases) ::
      Batch("Filter Pushdown", FixedPoint(100),
        CombineFilters,
        PushPredicateThroughNonJoin,
        BooleanSimplification,
        ReorderJoin,
        PushPredicateThroughJoin,
        ColumnPruning,
        RemoveNoopOperators,
        CollapseProject) :: Nil

  }

  val testRelation = LocalRelation($"a".int, $"b".int, $"c".int)
  val testRelation1 = LocalRelation($"d".int)

  test("extract filters and joins") {
    val x = testRelation.subquery("x")
    val y = testRelation1.subquery("y")
    val z = testRelation.subquery("z")

    def testExtract(plan: LogicalPlan,
        expected: Option[(Seq[LogicalPlan], Seq[Expression])]): Unit = {
      val expectedNoCross = expected map {
        seq_pair => {
          val plans = seq_pair._1
          val noCartesian = plans map { plan => (plan, Inner) }
          (noCartesian, seq_pair._2)
        }
      }
      testExtractCheckCross(plan, expectedNoCross)
    }

    def testExtractCheckCross(plan: LogicalPlan,
        expected: Option[(Seq[(LogicalPlan, InnerLike)], Seq[Expression])]): Unit = {
      assert(
        ExtractFiltersAndInnerJoins.unapply(plan) === expected.map(e => (e._1, e._2)))
    }

    testExtract(x, None)
    testExtract(x.where("x.b".attr === 1), None)
    testExtract(x.join(y), Some((Seq(x, y), Seq())))
    testExtract(x.join(y, condition = Some("x.b".attr === "y.d".attr)),
      Some((Seq(x, y), Seq("x.b".attr === "y.d".attr))))
    testExtract(x.join(y).where("x.b".attr === "y.d".attr),
      Some((Seq(x, y), Seq("x.b".attr === "y.d".attr))))
    testExtract(x.join(y).join(z), Some((Seq(x, y, z), Seq())))
    testExtract(x.join(y).where("x.b".attr === "y.d".attr).join(z),
      Some((Seq(x, y, z), Seq("x.b".attr === "y.d".attr))))
    testExtract(x.join(y).join(x.join(z)), Some((Seq(x, y, x.join(z)), Seq())))
    testExtract(x.join(y).join(x.join(z)).where("x.b".attr === "y.d".attr),
      Some((Seq(x, y, x.join(z)), Seq("x.b".attr === "y.d".attr))))

    testExtractCheckCross(x.join(y, Cross), Some((Seq((x, Cross), (y, Cross)), Seq())))
    testExtractCheckCross(x.join(y, Cross).join(z, Cross),
      Some((Seq((x, Cross), (y, Cross), (z, Cross)), Seq())))
    testExtractCheckCross(x.join(y, Cross, Some("x.b".attr === "y.d".attr)).join(z, Cross),
      Some((Seq((x, Cross), (y, Cross), (z, Cross)), Seq("x.b".attr === "y.d".attr))))
    testExtractCheckCross(x.join(y, Inner, Some("x.b".attr === "y.d".attr)).join(z, Cross),
      Some((Seq((x, Inner), (y, Inner), (z, Cross)), Seq("x.b".attr === "y.d".attr))))
    testExtractCheckCross(x.join(y, Cross, Some("x.b".attr === "y.d".attr)).join(z, Inner),
      Some((Seq((x, Cross), (y, Cross), (z, Inner)), Seq("x.b".attr === "y.d".attr))))
  }

  test("reorder inner joins") {
    val x = testRelation.subquery("x")
    val y = testRelation1.subquery("y")
    val z = testRelation.subquery("z")

    val queryAnswers = Seq(
      (
        x.join(y).join(z).where(("x.b".attr === "z.b".attr) && ("y.d".attr === "z.a".attr)),
        x.join(z, condition = Some("x.b".attr === "z.b".attr))
          .join(y, condition = Some("y.d".attr === "z.a".attr))
          .select(Seq("x.a", "x.b", "x.c", "y.d", "z.a", "z.b", "z.c").map(_.attr): _*)
      ),
      (
        x.join(y, Cross).join(z, Cross)
          .where(("x.b".attr === "z.b".attr) && ("y.d".attr === "z.a".attr)),
        x.join(z, Cross, Some("x.b".attr === "z.b".attr))
          .join(y, Cross, Some("y.d".attr === "z.a".attr))
          .select(Seq("x.a", "x.b", "x.c", "y.d", "z.a", "z.b", "z.c").map(_.attr): _*)
      ),
      (
        x.join(y, Inner).join(z, Cross).where("x.b".attr === "z.a".attr),
        x.join(z, Cross, Some("x.b".attr === "z.a".attr)).join(y, Inner)
          .select(Seq("x.a", "x.b", "x.c", "y.d", "z.a", "z.b", "z.c").map(_.attr): _*)
      )
    )

    queryAnswers foreach { queryAnswerPair =>
      val optimized = Optimize.execute(queryAnswerPair._1.analyze)
      comparePlans(optimized, queryAnswerPair._2.analyze)
    }
  }

  // The conjuncts of the top filter in their written order, which `comparePlans` does not check.
  private def topFilterConjuncts(plan: LogicalPlan): Seq[Expression] =
    normalizeExprIds(plan).collectFirst { case Filter(condition, _) => condition }
      .map(splitConjunctivePredicates(_).map(_.canonicalized)).getOrElse(Nil)

  test("SPARK-61930: reorder inner joins without moving a throwable condition into a join") {
    val x = testRelation.subquery("x")
    val y = testRelation1.subquery("y")
    val z = testRelation.subquery("z")

    // A throwable condition stays above the joins, so that it is not evaluated on the rows that
    // a join removes. The first one could go into the condition of the first join, and the
    // second one into that of the last join.
    def raiseUnless(cond: Expression): Expression =
      If(cond, Literal(true), IsNull(RaiseError(Literal("boom"))))
    val throwables = raiseUnless("x.a".attr > 0) && raiseUnless("y.d".attr === "z.c".attr)
    val query = x.join(y).join(z)
      .where(("x.b".attr === "z.b".attr) && ("y.d".attr === "z.a".attr) && throwables)
    val answer = x.join(z, condition = Some("x.b".attr === "z.b".attr))
      .join(y, condition = Some("y.d".attr === "z.a".attr))
      .where(throwables)
      .select(Seq("x.a", "x.b", "x.c", "y.d", "z.a", "z.b", "z.c").map(_.attr): _*)

    val optimized = Optimize.execute(query.analyze)
    comparePlans(optimized, answer.analyze)
    assert(topFilterConjuncts(optimized) === topFilterConjuncts(answer.analyze))
  }

  test("SPARK-61930: reorder inner joins without moving a condition that can fail ahead of a " +
    "non-deterministic one") {
    // The divisions are built in ANSI mode, where they can fail, and so can `q`, which one of them
    // computes. After `rand(0) > 0.1`, a condition that can fail by itself or reads `q` stays above
    // the joins, while one that can not fail goes into a join. So does a division written before
    // `rand(0) > 0.1`. Only ReorderJoin runs, so that this does not depend on
    // PushPredicateThroughJoin.
    def divide(l: Expression, r: Expression): Expression =
      Divide(l, r, NumericEvalContext(EvalMode.ANSI))
    val x = testRelation.select($"a", $"b", divide($"a", $"b").as("q"))
    val z = LocalRelation($"e".int, $"f".int)
    val query = x.join(testRelation1).join(z).where($"a" === $"d" && $"d" === $"e" &&
      divide($"b", $"d") > 1 && Rand(0) > 0.1 && divide($"b", $"f") > 1 && $"q" > 0 &&
      $"b" > $"f")
    val answer = x.join(testRelation1, condition = Some($"a" === $"d" && divide($"b", $"d") > 1))
      .join(z, condition = Some($"d" === $"e" && $"b" > $"f"))
      .where(Rand(0) > 0.1 && divide($"b", $"f") > 1 && $"q" > 0)

    val reordered = ReorderJoin(query.analyze)
    comparePlans(reordered, answer.analyze)
    assert(topFilterConjuncts(reordered) === topFilterConjuncts(answer.analyze))
  }

  test("SPARK-62069: reorder inner joins with a lower join or filter that has a condition that " +
    "may not move as one of the joined plans") {
    // A lower join or filter removes a row on which its condition is NULL, so no condition above
    // it is evaluated on the row. It is joined as one plan, with its condition. Only ReorderJoin
    // runs.
    val z = LocalRelation($"e".int, $"f".int)
    val w = LocalRelation($"g".int, $"h".int)
    val throwing = If($"a" > 0, $"b" > $"d", IsNull(RaiseError(Literal("boom"))))
    Seq(
      testRelation.join(testRelation1, condition = Some($"a" === $"d" && throwing)),
      testRelation.join(testRelation1, condition = Some($"a" === $"d")).where(Rand(0) > 0.5)
    ).foreach { lower =>
      val query = lower.join(w).join(z).where($"e" === $"c" && $"g" === $"e")
      val answer = lower.join(z, condition = Some($"e" === $"c"))
        .join(w, condition = Some($"g" === $"e"))
        .select($"a", $"b", $"c", $"d", $"g", $"h", $"e", $"f")
      comparePlans(ReorderJoin(query.analyze), answer.analyze)
      // It is an `Inner` plan also under a cross join, so that StarSchemaDetection, which only
      // reorders `Inner` base tables, does not join it after them.
      assert(ExtractFiltersAndInnerJoins.unapply(lower.join(w, Cross).join(z)).map(_._1.map(_._2))
        === Some(Seq(Inner, Cross, Inner)))
    }
  }

  test("SPARK-62069: reorder inner joins with a condition of the top join that may not move in " +
    "a filter above them") {
    // The filter takes the place of the top join. Only ReorderJoin runs.
    val z = LocalRelation($"e".int, $"f".int)
    val throwing = If($"a" > 0, $"b" > $"d", IsNull(RaiseError(Literal("boom"))))
    val query = testRelation.join(testRelation1)
      .join(z, condition = Some($"e" === $"a" && $"f" === $"d" && throwing))
    val answer = testRelation.join(z, condition = Some($"e" === $"a"))
      .join(testRelation1, condition = Some($"f" === $"d"))
      .where(throwing)
      .select($"a", $"b", $"c", $"d", $"e", $"f")
    comparePlans(ReorderJoin(query.analyze), answer.analyze)
  }

  private def ansiDivide(l: Expression, r: Expression): Expression =
    Divide(l, r, NumericEvalContext(EvalMode.ANSI))

  test("SPARK-62226: reorder inner joins with a lower filter whose condition can be NULL") {
    // The lower filter removes a row on which `b > d` is NULL, but in the join condition that the
    // filter is put in, `NULL AND x` evaluates `x`. So a condition above it over two of its plans
    // that can fail in ANSI mode, by itself or by reading `q`, is evaluated only where `b > d` is
    // TRUE, with IS NOT NULL checks of its columns. One that can not fail once its cast of a
    // literal is folded, one that also reads `z`, and the ones above a filter whose condition can
    // not be NULL or of a streaming plan go into a join as before. Only ReorderJoin runs.
    val canNotFail =
      $"c" > $"d" || $"c" === Cast(Literal("1"), IntegerType, evalMode = EvalMode.ANSI)
    Seq(($"b" > $"d", false, true), ($"b".isNotNull, false, false), ($"b" > $"d", true, false))
      .foreach { case (cond, isStreaming, guarded) =>
        val x = testRelation.copy(isStreaming = isStreaming)
          .select($"a", $"b", $"c", ansiDivide($"a", $"b").as("q"))
        val y = testRelation1.copy(isStreaming = isStreaming)
        val z = LocalRelation(Seq($"e".int, $"f".int), Nil, isStreaming)
        val failable = Seq(ansiDivide($"c", $"d") > 1, $"q" > $"d")
        val query = x.join(y, condition = Some($"a" === $"d")).where(cond)
          .join(z, condition = Some((Seq($"e" === $"a", canNotFail) ++ failable).reduce(_ && _)))
          .where(ansiDivide($"c", $"f") > 1)
        val moved = if (guarded) {
          failable.map(If(cond, _, Literal(false))) ++ Seq($"c", $"d", $"q").map(IsNotNull(_))
        } else {
          failable
        }
        val answer = x.join(y,
            condition = Some((Seq($"a" === $"d", cond, canNotFail) ++ moved).reduce(_ && _)))
          .join(z, condition = Some($"e" === $"a" && ansiDivide($"c", $"f") > 1))
        comparePlans(ReorderJoin(query.analyze), answer.analyze)
      }
  }

  test("SPARK-62226: guard a condition above stacked filters whose condition can be NULL") {
    // A condition over two plans is guarded by both filters, the lower one outermost. One over one
    // plan is pushed down to it with the conjuncts of the filters that only read it, so only
    // `c > 0`, which can be NULL, guards it, and nothing guards one over `z`. Only ReorderJoin
    // runs.
    val z = LocalRelation($"e".int, $"f".int)
    val w = LocalRelation($"g".int, $"h".int)
    val (overTwo, overOne, overZ) =
      (ansiDivide($"c", $"d") > 1, ansiDivide($"c", $"b") > 1, ansiDivide($"e", $"f") > 1)
    val lower = $"b" > $"d" && $"c" > 0 && $"a".isNotNull
    val query = testRelation.join(testRelation1, condition = Some($"a" === $"d"))
      .where(lower)
      .join(z, condition = Some($"e" === $"a")).where($"f" > $"b")
      .join(w, condition = Some($"g" === $"a"))
      .where(overTwo && overOne && overZ)
    val answer = testRelation.join(testRelation1, condition = Some($"a" === $"d" && lower &&
        IsNotNull($"c") && IsNotNull($"d") && If($"c" > 0, overOne, Literal(false)) &&
        IsNotNull($"b")))
      .join(z, condition = Some($"e" === $"a" && $"f" > $"b" &&
        If(lower, If($"f" > $"b", overTwo, Literal(false)), Literal(false)) && overZ))
      .join(w, condition = Some($"g" === $"a"))
    comparePlans(ReorderJoin(query.analyze), answer.analyze)
  }

  test("SPARK-62226: do not guard a condition that ReorderJoin keeps out of the join conditions") {
    // The IN subquery stays out of the join conditions, and `rand(0) > 0.5` and the division after
    // it in a filter above all the joins, where they only see the rows that the lower filter
    // keeps. Guarding them would wrap them again whenever the pushdown puts the lower filter, whose
    // IN subquery can not go into a join either, back between the joins, until the batch reaches
    // its iteration limit.
    val z = LocalRelation($"e".int, $"f".int)
    val s = LocalRelation($"y".int)
    val query = testRelation.join(testRelation1, condition = Some($"a" === $"d"))
      .where(InSubquery(Seq($"a" + $"d"), ListQuery(s.select($"y"))))
      .join(z, condition = Some($"e" === $"a"))
      .where(InSubquery(Seq($"b" + $"d"), ListQuery(s.select($"y"))) && Rand(0) > 0.5 &&
        ansiDivide($"c", $"d") > 1)
    val optimized = Optimize.execute(query.analyze)
    assert(!optimized.exists(_.expressions.exists(_.exists(_.isInstanceOf[If]))), optimized)
  }
}
