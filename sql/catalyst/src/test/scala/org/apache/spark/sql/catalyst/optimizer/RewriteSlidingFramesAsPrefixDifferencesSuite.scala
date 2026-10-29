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

import org.scalactic.source
import org.scalatest.Tag

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Window}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, LongType, MetadataBuilder}

class RewriteSlidingFramesAsPrefixDifferencesSuite extends PlanTest {

  // The rule requires a legacy (ANSI-off) session, so every test runs with ANSI off: both
  // plan construction (which captures the session's ANSI setting in `Sum`) and rule execution
  // happen inside the conf block.
  override protected def test(
      testName: String, testTags: Tag*)(testFun: => Any)(implicit pos: source.Position): Unit =
    super.test(testName, testTags: _*)(
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "false")(testFun))(pos)

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Rewrite sliding frames as prefix differences", FixedPoint(10),
        RewriteSlidingFramesAsPrefixDifferences) :: Nil
  }

  // After one pass, `Once` applies the rule again and fails if the plan still changes.
  object OptimizeOnce extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Rewrite sliding frames as prefix differences", Once,
        RewriteSlidingFramesAsPrefixDifferences) :: Nil
  }

  // a, b: non-nullable; n: nullable; d: double; dec: decimal; c: partition key.
  private val testRelation = LocalRelation(
    $"a".int.notNull, $"b".long.notNull, $"n".int, $"d".double, $"dec".decimal(10, 2), $"c".string)
  private val a = testRelation.output(0)
  private val b = testRelation.output(1)
  private val n = testRelation.output(2)
  private val d = testRelation.output(3)
  private val dec = testRelation.output(4)
  private val c = testRelation.output(5)

  private val part = Seq(c)
  private val order = Seq(a.asc)

  /**
   * `ROWS BETWEEN n PRECEDING AND CURRENT ROW` with the bound already constant-folded to
   * `Literal(-n)`. Analysis leaves the bound as [[slidingUnfolded]]; the folded form appears
   * only on an optimizer pass that runs after `ConstantFolding`, which is later in the rule set
   * than the rule under test.
   */
  private def sliding(nPreceding: Int): SpecifiedWindowFrame =
    SpecifiedWindowFrame(RowFrame, Literal(-nPreceding, IntegerType), CurrentRow)

  /** `n PRECEDING` as the analyzer emits it, i.e. the bound the rule sees on its first pass. */
  private def slidingUnfolded(nPreceding: Int): SpecifiedWindowFrame =
    SpecifiedWindowFrame(RowFrame,
      UnaryMinus(Literal(nPreceding, IntegerType)), CurrentRow)

  private def running: SpecifiedWindowFrame =
    SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow)

  private def spec(frame: WindowFrame): WindowSpecDefinition = windowSpec(part, order, frame)

  private def query(exprs: NamedExpression*): LogicalPlan =
    testRelation.window(Seq(exprs: _*), part, order)

  /** `lag(input, offset, default) OVER (PARTITION BY c ORDER BY a)` with an offset frame. */
  private def lagExpr(input: Expression, offset: Int, default: Expression): WindowExpression = {
    val frame = SpecifiedWindowFrame(RowFrame,
      Literal(-offset, IntegerType), Literal(-offset, IntegerType))
    WindowExpression(
      Lag(input, Literal(offset, IntegerType), default, false), windowSpec(part, order, frame))
  }

  private def sumDiff(name: String, offset: Int, prefix: Expression): NamedExpression =
    Alias(
      Subtract(prefix,
        Coalesce(Seq(lagExpr(prefix, offset, Literal(null, LongType)), Literal(0L, LongType))),
        NumericEvalContext(EvalMode.LEGACY)),
      name)(ExprId(0))

  private def prefixAttr(name: String, nullable: Boolean): AttributeReference =
    AttributeReference(name, LongType, nullable)(ExprId(0))

  private def execute(plan: LogicalPlan): LogicalPlan =
    withSQLConf(SQLConf.WINDOW_PREFIX_REWRITE_ENABLED.key -> "true") {
      Optimize.execute(plan)
    }

  private def executeOnce(plan: LogicalPlan): LogicalPlan =
    withSQLConf(SQLConf.WINDOW_PREFIX_REWRITE_ENABLED.key -> "true") {
      OptimizeOnce.execute(plan)
    }

  test("rewrite sliding frames as a prefix sum and lag differences") {
    val s1 = windowExpr(sum(b), spec(sliding(2))).as("s1")
    val s2 = windowExpr(sum(b), spec(sliding(30))).as("s2")
    val input = query(s1, s2)

    // Widths 3 + 31 = 34 >= 16, one shared running sum, two lag frames.
    val prefix = Alias(windowExpr(sum(b), spec(running)), "window_prefix_sum_0")(ExprId(0))
    val prefixSum = prefixAttr("window_prefix_sum_0", nullable = true)
    val expected = Window(
      Seq(sumDiff("s1", 3, prefixSum), sumDiff("s2", 31, prefixSum)), part, order,
      Window(Seq(prefix), part, order, testRelation))

    comparePlans(execute(input), expected)
  }

  test("do not rewrite when the total frame width is below the cost gate") {
    // One width-3 frame and one width-10 frame sum to 13 < 16.
    val s1 = windowExpr(sum(b), spec(sliding(2))).as("s1")
    val s2 = windowExpr(sum(b), spec(sliding(9))).as("s2")
    val input = query(s1, s2)

    comparePlans(execute(input), input)
  }

  test("the cost gate is the boundary of the rewrite: width 15 no, width 16 yes") {
    val below = windowExpr(sum(b), spec(sliding(14))).as("s")
    comparePlans(execute(query(below)), query(below))

    val at = windowExpr(sum(b), spec(sliding(15))).as("s")
    val prefix = Alias(windowExpr(sum(b), spec(running)), "window_prefix_sum_0")(ExprId(0))
    val prefixSum = prefixAttr("window_prefix_sum_0", nullable = true)
    val expected = Window(
      Seq(sumDiff("s", 16, prefixSum)), part, order,
      Window(Seq(prefix), part, order, testRelation))
    comparePlans(execute(query(at)), expected)
  }

  test("rewrite a single wide frame") {
    val s = windowExpr(sum(b), spec(sliding(31))).as("s")
    val input = query(s)

    val prefix = Alias(windowExpr(sum(b), spec(running)), "window_prefix_sum_0")(ExprId(0))
    val prefixSum = prefixAttr("window_prefix_sum_0", nullable = true)
    val expected = Window(
      Seq(sumDiff("s", 32, prefixSum)), part, order,
      Window(Seq(prefix), part, order, testRelation))

    comparePlans(execute(input), expected)
  }

  test("rewrite the unfolded frame bound the analyzer produces") {
    // A real query reaches the rule with `UnaryMinus(Literal(31))` as the lower bound, not with
    // the folded `Literal(-31)`, so both shapes must produce the same width.
    val unfolded = windowExpr(sum(b), spec(slidingUnfolded(31))).as("s")
    val folded = windowExpr(sum(b), spec(sliding(31))).as("s")

    val prefix = Alias(windowExpr(sum(b), spec(running)), "window_prefix_sum_0")(ExprId(0))
    val prefixSum = prefixAttr("window_prefix_sum_0", nullable = true)
    val expected = Window(
      Seq(sumDiff("s", 32, prefixSum)), part, order,
      Window(Seq(prefix), part, order, testRelation))

    comparePlans(execute(query(unfolded)), expected)
    comparePlans(execute(query(folded)), expected)
  }

  test("rewrite with a NULL guard for nullable measures") {
    val s = windowExpr(sum(n), spec(sliding(31))).as("s")
    val input = query(s)

    val prefix = Seq(
      Alias(windowExpr(sum(n), spec(running)), "window_prefix_sum_0")(ExprId(0)),
      Alias(windowExpr(count(n), spec(running)), "window_prefix_count_0")(ExprId(0)))
    val prefixSum = prefixAttr("window_prefix_sum_0", nullable = true)
    val prefixCount = prefixAttr("window_prefix_count_0", nullable = false)
    val countDiff = Subtract(prefixCount,
      lagExpr(prefixCount, 32, Literal(0L, LongType)), NumericEvalContext(EvalMode.LEGACY))
    val guarded = CaseWhen(
      Seq((EqualTo(countDiff, Literal(0L, LongType)), Literal(null, LongType))),
      Some(Subtract(prefixSum,
        Coalesce(Seq(lagExpr(prefixSum, 32, Literal(null, LongType)), Literal(0L, LongType))),
        NumericEvalContext(EvalMode.LEGACY))))
    val expected = Window(
      Seq(Alias(guarded, "s")(ExprId(0))), part, order,
      Window(prefix, part, order, testRelation))

    comparePlans(execute(input), expected)
  }

  test("rewrite several groups in one window operator") {
    val s1 = windowExpr(sum(b), spec(sliding(31))).as("s1")
    val s2 = windowExpr(sum(a), spec(sliding(2))).as("s2")
    val input = query(s1, s2)

    // Two measures: one running sum each, named by group index, and each candidate keeps the
    // lag offset of its own frame width.
    val prefixB = Alias(windowExpr(sum(b), spec(running)), "window_prefix_sum_0")(ExprId(0))
    val prefixA = Alias(windowExpr(sum(a), spec(running)), "window_prefix_sum_1")(ExprId(0))
    val prefixSumB = prefixAttr("window_prefix_sum_0", nullable = true)
    val prefixSumA = prefixAttr("window_prefix_sum_1", nullable = true)
    val expected = Window(
      Seq(sumDiff("s1", 32, prefixSumB), sumDiff("s2", 3, prefixSumA)), part, order,
      Window(Seq(prefixB, prefixA), part, order, testRelation))

    comparePlans(execute(input), expected)
  }

  test("do not rewrite floating-point sums") {
    val s = windowExpr(sum(d), spec(sliding(31))).as("s")
    val input = query(s)

    comparePlans(execute(input), input)
  }

  test("do not rewrite decimal sums") {
    val s = windowExpr(sum(dec), spec(sliding(31))).as("s")
    val input = query(s)

    comparePlans(execute(input), input)
  }

  test("do not rewrite try_sum") {
    val trySum = Sum(b, NumericEvalContext(EvalMode.TRY))
      .toAggregateExpression()
    val s = windowExpr(trySum, spec(sliding(31))).as("s")
    val input = query(s)

    comparePlans(execute(input), input)
  }

  test("do not rewrite ANSI sums") {
    val ansiSum = Sum(b, NumericEvalContext(EvalMode.ANSI)).toAggregateExpression()
    val s = windowExpr(ansiSum, spec(sliding(31))).as("s")
    val input = query(s)

    comparePlans(execute(input), input)
  }

  test("do not rewrite filtered sums") {
    val filtered = sum(b, filter = Some(GreaterThan(a, Literal(0))))
    val s = windowExpr(filtered, spec(sliding(31))).as("s")
    val input = query(s)

    comparePlans(execute(input), input)
  }

  test("do not rewrite distinct sums") {
    val s = windowExpr(sumDistinct(b), spec(sliding(31))).as("s")
    val input = query(s)

    // `checkAnalysis = false`: a DISTINCT window function is rejected by analysis
    // (`DISTINCT_WINDOW_FUNCTION_UNSUPPORTED`), so this input can only exist as a hand-built
    // plan. The rule must still leave it untouched rather than rewrite around the DISTINCT.
    comparePlans(execute(input), input, checkAnalysis = false)
  }

  test("do not rewrite min/max sliding frames") {
    val s = windowExpr(min(b), spec(sliding(31))).as("s")
    val input = query(s)

    comparePlans(execute(input), input)
  }

  test("do not rewrite sliding frames without ORDER BY") {
    val noOrder = windowSpec(Seq(c), Nil, sliding(31))
    val s = windowExpr(sum(b), noOrder).as("s")
    val input = testRelation.window(Seq(s), Seq(c), Nil)

    comparePlans(execute(input), input)
  }

  test("do not rewrite RANGE frames") {
    val rangeFrame = SpecifiedWindowFrame(RangeFrame, Literal(-2, IntegerType), CurrentRow)
    val s = windowExpr(sum(b), windowSpec(part, order, rangeFrame)).as("s")
    val input = query(s)

    comparePlans(execute(input), input)
  }

  test("do not rewrite growing frames") {
    val s = windowExpr(sum(b), spec(running)).as("s")
    val input = query(s)

    comparePlans(execute(input), input)
  }

  test("do not rewrite a frame with a FOLLOWING upper bound") {
    // `2 PRECEDING AND 1 FOLLOWING` is not a frame anchored at the current row, so it is
    // not a prefix difference of the running sum. Its lower bound and width (3) qualify, so
    // only the CURRENT ROW check in the pattern rejects it; without that check the frame
    // would silently turn into a lag difference of the wrong rows. Width 3 is alone under
    // the cost gate, so the plan must stay untouched.
    val frame = SpecifiedWindowFrame(RowFrame, UnaryMinus(Literal(2, IntegerType)),
      Literal(1, IntegerType))
    val s = windowExpr(sum(b), spec(frame)).as("s")
    val input = query(s)

    comparePlans(execute(input), input)
  }

  test("do not rewrite a frame whose upper bound precedes CURRENT ROW") {
    // `2 PRECEDING AND 2 PRECEDING` is an empty frame whose upper bound sits before the
    // current row. It is not a candidate either, and alone under the cost gate.
    val frame = SpecifiedWindowFrame(RowFrame, UnaryMinus(Literal(2, IntegerType)),
      UnaryMinus(Literal(2, IntegerType)))
    val s = windowExpr(sum(b), spec(frame)).as("s")
    val input = query(s)

    comparePlans(execute(input), input)
  }

  test("do not rewrite a frame with an UNBOUNDED FOLLOWING upper bound") {
    // `2 PRECEDING AND UNBOUNDED FOLLOWING` is a growing frame: its width is not even
    // constant, so there is no lag offset that reproduces it.
    val frame = SpecifiedWindowFrame(RowFrame, UnaryMinus(Literal(2, IntegerType)),
      UnboundedFollowing)
    val s = windowExpr(sum(b), spec(frame)).as("s")
    val input = query(s)

    comparePlans(execute(input), input)
  }

  test("keep a FOLLOWING upper bound frame when its CURRENT ROW sibling is rewritten") {
    // Partial-rewrite guard for the upper bound: the qualifying width-32 sibling rewrites,
    // while the `2 PRECEDING AND 1 FOLLOWING` member is not a prefix frame and must keep
    // its original expression. If the pattern ever accepts a non-CURRENT ROW upper bound,
    // this member turns into a lag difference and the expected plan diverges.
    val frame = SpecifiedWindowFrame(RowFrame, UnaryMinus(Literal(2, IntegerType)),
      Literal(1, IntegerType))
    val following = windowExpr(sum(b), spec(frame)).as("sf")
    val sibling = windowExpr(sum(b), spec(sliding(31))).as("s31")
    val input = query(following, sibling)

    val prefix = Alias(windowExpr(sum(b), spec(running)), "window_prefix_sum_0")(ExprId(0))
    val prefixSum = prefixAttr("window_prefix_sum_0", nullable = true)
    val expected = Window(
      Seq(following, sumDiff("s31", 32, prefixSum)), part, order,
      Window(Seq(prefix), part, order, testRelation))

    comparePlans(execute(input), expected)
  }

  test("do not rewrite a 0 PRECEDING bound") {
    // `0 PRECEDING` reaches the rule as `UnaryMinus(Literal(0))`: a width-1 frame, rejected by
    // the branch's `v >= 1` guard. The width-15 sibling stays under the cost gate on its own, so
    // an accepted width-1 frame would push the group over it and rewrite both members.
    val zero = windowExpr(sum(b), spec(slidingUnfolded(0))).as("s0")
    val sibling = windowExpr(sum(b), spec(sliding(14))).as("s14")
    val input = query(zero, sibling)

    comparePlans(execute(input), input)
  }

  test("do not rewrite a bound whose width does not fit Int") {
    // `2147483647 PRECEDING` needs a lag offset of Int.MaxValue + 1. Like the folded boundary
    // case below, the plan must stay untouched rather than carry a wrapped negative offset.
    val tooWide = windowExpr(sum(b), spec(slidingUnfolded(Int.MaxValue))).as("s")

    comparePlans(execute(query(tooWide)), query(tooWide))
  }

  test("do not rewrite the folded bound of an empty frame") {
    // `-49 PRECEDING` is `49 FOLLOWING`, i.e. an empty frame whose sum is always NULL. Constant
    // folding turns its bound into the positive literal `Literal(49)`, which the literal case's
    // `v <= -1` guard rejects. The sibling frame alone clears the cost gate, so the group is
    // rewritten partially and only the sibling changes.
    val emptyFrame = SpecifiedWindowFrame(RowFrame, Literal(49, IntegerType), CurrentRow)
    val empty = windowExpr(sum(b), spec(emptyFrame)).as("empty")
    val sibling = windowExpr(sum(b), spec(sliding(65))).as("s66")
    val input = query(empty, sibling)

    val prefix = Alias(windowExpr(sum(b), spec(running)), "window_prefix_sum_0")(ExprId(0))
    val prefixSum = prefixAttr("window_prefix_sum_0", nullable = true)
    val expected = Window(
      Seq(empty, sumDiff("s66", 66, prefixSum)), part, order,
      Window(Seq(prefix), part, order, testRelation))

    comparePlans(execute(input), expected)
  }

  test("keep window expressions whose spec differs from the window operator's own spec") {
    val otherSpec = windowSpec(Seq(c), Seq(b.asc), sliding(31))
    val input = Window(Seq(Alias(windowExpr(sum(b), otherSpec), "s")()), part, order, testRelation)

    comparePlans(execute(input), input)
  }

  test("keep window expressions whose partition spec differs from the operator's own") {
    // The expression is partitioned by (c, a) but sits in an operator partitioned only by
    // c: rewriting it under the operator's partition would sum rows of different (c, a)
    // groups together, so the plan must stay untouched.
    val otherSpec = windowSpec(Seq(c, a), order, sliding(31))
    val input = Window(Seq(Alias(windowExpr(sum(b), otherSpec), "s")()), part, order, testRelation)

    comparePlans(execute(input), input)
  }

  test("keep non-qualifying members when a group is partially rewritten") {
    val s = windowExpr(sum(b), spec(sliding(31))).as("s")
    val m = windowExpr(min(b), spec(sliding(31))).as("m")
    val input = query(s, m)

    val prefix = Alias(windowExpr(sum(b), spec(running)), "window_prefix_sum_0")(ExprId(0))
    val prefixSum = prefixAttr("window_prefix_sum_0", nullable = true)
    val expected = Window(
      Seq(sumDiff("s", 32, prefixSum),
        Alias(windowExpr(min(b), spec(sliding(31))), "m")(ExprId(0))),
      part, order,
      Window(Seq(prefix), part, order, testRelation))

    comparePlans(execute(input), expected)
  }

  test("keep uncertified sums when a group is partially rewritten") {
    val s = windowExpr(sum(b), spec(sliding(31))).as("s")
    // Passes the sliding-`sum` shape check but is a floating-point measure, so it fails the
    // safety certificate while its sibling in the same window operator is rewritten.
    val sd = windowExpr(sum(d), spec(sliding(31))).as("sd")
    val input = query(s, sd)

    val prefix = Alias(windowExpr(sum(b), spec(running)), "window_prefix_sum_0")(ExprId(0))
    val prefixSum = prefixAttr("window_prefix_sum_0", nullable = true)
    val expected = Window(
      Seq(sumDiff("s", 32, prefixSum), sd), part, order,
      Window(Seq(prefix), part, order, testRelation))

    comparePlans(execute(input), expected)
  }

  test("rewrite width-2 frames: two 1 PRECEDING members and a wide sibling") {
    // The acceptance edge `UnaryMinus(IntegerLiteral(v)) if v >= 1` at v = 1, i.e. a 1
    // PRECEDING frame of width 2, whose lag offset is exactly 2. The group total 2 + 2 + 16
    // clears the cost gate; the expected plan pins the width-2 members as lag differences,
    // so a guard tightened past v = 1 diverges even though the width-16 sibling still rewrites.
    val w2a = windowExpr(sum(b), spec(slidingUnfolded(1))).as("s2a")
    val w2b = windowExpr(sum(b), spec(slidingUnfolded(1))).as("s2b")
    val wide = windowExpr(sum(b), spec(sliding(15))).as("s16")
    val input = query(w2a, w2b, wide)

    val prefix = Alias(windowExpr(sum(b), spec(running)), "window_prefix_sum_0")(ExprId(0))
    val prefixSum = prefixAttr("window_prefix_sum_0", nullable = true)
    val expected = Window(
      Seq(sumDiff("s2a", 2, prefixSum), sumDiff("s2b", 2, prefixSum),
        sumDiff("s16", 16, prefixSum)),
      part, order,
      Window(Seq(prefix), part, order, testRelation))

    comparePlans(execute(input), expected)
  }

  test("rewrite a folded width-2 bound from 1 PRECEDING") {
    // The folded shape `Literal(-1, IntegerType)` exercises the `v <= -1` branch at its
    // edge v = -1: the same 1 PRECEDING frame, width 2, as it appears after constant
    // folding. The expected plan pins the width-2 member as a lag difference, so a guard
    // tightened past v = -1 diverges even though the width-16 sibling still rewrites.
    val folded = windowExpr(sum(b), spec(sliding(1))).as("s2")
    val wide = windowExpr(sum(b), spec(sliding(15))).as("s16")
    val input = query(folded, wide)

    val prefix = Alias(windowExpr(sum(b), spec(running)), "window_prefix_sum_0")(ExprId(0))
    val prefixSum = prefixAttr("window_prefix_sum_0", nullable = true)
    val expected = Window(
      Seq(sumDiff("s2", 2, prefixSum), sumDiff("s16", 16, prefixSum)), part, order,
      Window(Seq(prefix), part, order, testRelation))

    comparePlans(execute(input), expected)
  }

  test("the cost gate counts only the frames it rewrites") {
    // Certified width 8 (`sum(b)`) plus uncertified width 8 (`sum(d)`): only the certified frame
    // would be rewritten, so its own width - 8, below the threshold - is what the gate must
    // compare. Counting the uncertified width too would cross the gate at 16 and buy a whole
    // prefix pass to replace a single 8-row fold.
    val s = windowExpr(sum(b), spec(sliding(7))).as("s")
    val sd = windowExpr(sum(d), spec(sliding(7))).as("sd")
    val input = query(s, sd)

    comparePlans(execute(input), input)
  }

  test("frame width boundary: the last Int-representable width rewrites, one wider does not") {
    // `Int.MaxValue - 1` PRECEDING needs a lag offset of Int.MaxValue, which still fits Int.
    val widest = windowExpr(sum(b), spec(sliding(Int.MaxValue - 1))).as("s")
    val prefix = Alias(windowExpr(sum(b), spec(running)), "window_prefix_sum_0")(ExprId(0))
    val prefixSum = prefixAttr("window_prefix_sum_0", nullable = true)
    val expected = Window(
      Seq(sumDiff("s", Int.MaxValue, prefixSum)), part, order,
      Window(Seq(prefix), part, order, testRelation))
    comparePlans(execute(query(widest)), expected)

    // `Int.MaxValue` PRECEDING would need a lag offset of Int.MaxValue + 1, which does not
    // fit Int, so the frame is left to the original plan.
    val tooWide = windowExpr(sum(b), spec(sliding(Int.MaxValue))).as("s")
    comparePlans(execute(query(tooWide)), query(tooWide))
  }

  test("the rule is idempotent") {
    val s1 = windowExpr(sum(b), spec(sliding(2))).as("s1")
    val s2 = windowExpr(sum(b), spec(sliding(30))).as("s2")
    val input = query(s1, s2)

    // The single pass must already equal the plan `FixedPoint` settles on.
    comparePlans(executeOnce(input), execute(input))
  }

  test("emitted running sums use ROWS frames, never RANGE frames") {
    val s1 = windowExpr(sum(n), spec(sliding(31))).as("s1")
    val s2 = windowExpr(sum(b), spec(sliding(31))).as("s2")
    val input = query(s1, s2)

    val optimized = execute(input)
    // collect is top-down: the first window is the outer one, the second the inner one.
    val windows = optimized.collect { case w: Window => w }
    assert(windows.size === 2)
    // The inner window computes the running prefixes with explicit ROWS frames.
    val inner = windows(1)
    assert(inner.windowExpressions.nonEmpty)
    inner.windowExpressions.foreach {
      case Alias(WindowExpression(_, spec: WindowSpecDefinition), _) =>
        spec.frameSpecification match {
          case f: SpecifiedWindowFrame =>
            assert(f.frameType === RowFrame)
            assert(f.lower == UnboundedPreceding)
            assert(f.upper == CurrentRow)
          case _ => fail("Unspecified frame in the emitted plan")
        }
      case other => fail(s"Unexpected window expression: $other")
    }
    // No RangeFrame anywhere in the emitted plan.
    optimized.foreach {
      case w: Window => w.windowExpressions.foreach { ne =>
        ne.collect { case WindowSpecDefinition(_, _, f: SpecifiedWindowFrame) => f }
          .foreach(f => assert(f.frameType === RowFrame))
      }
      case _ =>
    }
  }

  test("both window operators keep the same partition and order specs") {
    val s = windowExpr(sum(b), spec(sliding(31))).as("s")
    val input = query(s)

    val optimized = execute(input)
    val windows = optimized.collect { case w: Window => w }
    assert(windows.size === 2)
    assert(windows(0).partitionSpec === windows(1).partitionSpec)
    assert(windows(0).orderSpec.map(_.canonicalized) ===
      windows(1).orderSpec.map(_.canonicalized))
    // The output attributes of the candidates are preserved.
    assert(optimized.output.map(_.name).toSet ===
      input.output.map(_.name).toSet ++ Set("window_prefix_sum_0"))
  }

  test("the rewrite keeps each candidate's exprId, qualifier and metadata") {
    // `comparePlans` normalizes aliases to `ExprId(0)` with a default qualifier and metadata, so
    // the attribute identity the rule promises is only observable on the rewritten output.
    val metadata = new MetadataBuilder().putString("origin", "test").build()
    val s = Alias(windowExpr(sum(b), spec(sliding(31))), "s")(
      qualifier = Seq("q"), explicitMetadata = Some(metadata))
    val optimized = execute(query(s)) match {
      case w: Window => w
      case other => fail(s"expected a Window at the root, got: $other")
    }
    val rewritten = optimized.windowExpressions.head

    assert(rewritten.exprId === s.exprId)
    assert(rewritten.name == s.name)
    assert(rewritten.qualifier === s.qualifier)
    assert(rewritten.metadata === s.metadata)
    // The emitted prefix alias is a new column, so it must not reuse the candidate's exprId.
    assert(!optimized.children.head.output.exists(_.exprId == s.exprId))
  }

  test("rewrite threads DESC ordering and composite partition keys into both operators") {
    val multiKey = Seq(c, a)
    val descOrder = Seq(b.desc)
    val s = windowExpr(sum(b), windowSpec(multiKey, descOrder, sliding(31))).as("s")
    val input = testRelation.window(Seq(s), multiKey, descOrder)

    val optimized = execute(input)
    val windows = optimized.collect { case w: Window => w }
    assert(windows.size === 2)

    // The rule must emit both operators with the operator's own non-default spec, not a
    // defaulted one. Compare against the input operator: it predates the rule run, so a
    // spec rebuilt with defaults diverges from it, whereas comparing the two emitted
    // operators against each other would compare the rule's own output to itself.
    val inputWindow = input.collect { case w: Window => w }.head
    windows.foreach { w =>
      assert(w.partitionSpec === inputWindow.partitionSpec)
      assert(w.orderSpec.map(_.canonicalized) ===
        inputWindow.orderSpec.map(_.canonicalized))
    }
  }
}
