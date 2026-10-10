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
import org.apache.spark.sql.types.{IntegerType, LongType}

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

  /** `ROWS BETWEEN n PRECEDING AND CURRENT ROW`, the analyzed (folded) form. */
  private def sliding(nPreceding: Int): SpecifiedWindowFrame =
    SpecifiedWindowFrame(RowFrame, Literal(-nPreceding, IntegerType), CurrentRow)

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

  test("keep window expressions whose spec differs from the window operator's own spec") {
    val otherSpec = windowSpec(Seq(c), Seq(b.asc), sliding(31))
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

  test("the rule is idempotent") {
    val s1 = windowExpr(sum(b), spec(sliding(2))).as("s1")
    val s2 = windowExpr(sum(b), spec(sliding(30))).as("s2")
    val input = query(s1, s2)

    val optimized = execute(input)
    comparePlans(execute(optimized), optimized)
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
}
