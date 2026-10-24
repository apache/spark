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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Complete, Count, Sum}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Window}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.WINDOW
import org.apache.spark.sql.types.{IntegerType, IntegralType, LongType}

/**
 * Rewrites groups of sliding `ROWS BETWEEN n PRECEDING AND CURRENT ROW` window frames of `sum`
 * over integral measures as one running aggregate plus lag differences.
 *
 * `WindowExec` re-aggregates every row currently inside a bounded frame on every output row
 * (`SlidingWindowFunctionFrame.write`), so a group of nested sliding frames over one
 * `(PARTITION BY, ORDER BY)` key costs the sum of the frame widths aggregate-update
 * evaluations per output row. This rule rewrites each qualifying group as
 *
 * {{{
 * C = sum(x) OVER (PARTITION BY k ORDER BY d ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
 * sum(x) OVER (PARTITION BY k ORDER BY d ROWS BETWEEN n PRECEDING AND CURRENT ROW)
 *   = C - coalesce(lag(C, n + 1) OVER (PARTITION BY k ORDER BY d), 0)
 * }}}
 *
 * which makes every frame O(1) while keeping the plan shape the same: both window operators
 * share one sort over the same partition and order spec, so no new exchange or sort is added.
 *
 * Safety is decided per aggregate class by a certificate instead of by inspecting expressions.
 * For integral accumulators whose updates wrap on overflow (the legacy evaluation mode), the
 * original frame fold already computes in Z/2^64Z, and modular reduction commutes with
 * subtraction, so `P_i - P_{i-(n+1)}` is bit-identical to the frame fold even when the
 * running prefix - which can reach magnitudes no bounded frame ever did - overflows.
 * Floating-point buffers are excluded outright (addition is not associative, so the rewrite
 * would differ in the last bits), as are decimals (an overflowed prefix silently becomes NULL
 * under `CheckOverflowInSum`) and any aggregate whose overflow does not wrap - e.g. one
 * analyzed in an ANSI session or a `try_sum`, where the newly reachable prefix magnitudes
 * would error or null out instead. Filtered (`FILTER (WHERE ...)`) and distinct sums are not
 * candidates either.
 *
 * When the measure is nullable, a running `count(m)` guard distinguishes an all-NULL frame
 * (which must return NULL, as the original fold does) from a difference of 0, and the result
 * becomes
 *
 * {{{
 * CASE WHEN k - lag(k, n + 1) = 0 THEN NULL ELSE C - coalesce(lag(C, n + 1), 0) END
 * }}}
 *
 * Non-nullable measures skip the guard. Other window expressions of the same window operator
 * are left untouched, so a group is rewritten partially when only some of its members qualify.
 */
object RewriteSlidingFramesAsPrefixDifferences extends Rule[LogicalPlan] {

  // Minimum total frame width (the sum of the frame widths of all qualifying window
  // expressions in one window operator) for the rewrite to apply. Deliberately not a SQLConf:
  // the only user-facing switch is the enabled flag, and operators should not have to audit
  // per-query frame widths. Section D of WindowPrefixRewriteBenchmark: at total width 8
  // the rewrite is still ~9% slower (518 vs 475 ms); at 16 it is ~13% faster (514 vs
  // 593 ms). The plans cross near width 11, and 16 is ~1.5x that point, so groups that
  // are still cheaper as a sliding sum stay on the naive plan.
  private val MIN_TOTAL_FRAME_WIDTH = 16

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan.transformUpWithPruning(_.containsPattern(WINDOW), ruleId) {
      case w @ Window(_, _, orderSpec, _, _)
          if conf.windowPrefixRewriteEnabled && orderSpec.nonEmpty =>
        rewriteWindow(w)
    }

  /**
   * A candidate for the rewrite: a `sum` over a sliding `ROWS` frame with literal bounds.
   *
   * @param alias the original window expression, to be replaced in place.
   * @param sum the aggregate expression of the candidate.
   * @param width the number of rows in the frame, i.e. (rows preceding) + 1.
   */
  private case class Candidate(alias: Alias, sum: Sum, width: Int)

  private def rewriteWindow(w: Window): Window = {
    val qualified = w.windowExpressions
      .flatMap(slidingSumCandidate(w, _))
      .filter(c => isPrefixDifferenceSafe(c.sum))
    if (qualified.isEmpty || qualified.map(_.width).sum < MIN_TOTAL_FRAME_WIDTH) {
      w
    } else {
      rewriteGroup(w, qualified)
    }
  }

  /**
   * Returns a candidate if the given window expression is a non-distinct, unfiltered `sum`
   * over a sliding `ROWS BETWEEN n PRECEDING AND CURRENT ROW` frame over the window
   * operator's own partition and order spec.
   */
  private def slidingSumCandidate(w: Window, ne: NamedExpression): Option[Candidate] = ne match {
    case a @ Alias(WindowExpression(
        AggregateExpression(s: Sum, Complete, false, None, _),
        WindowSpecDefinition(partSpec, orderSpec,
          SpecifiedWindowFrame(RowFrame, lower, CurrentRow))), _)
        if specsMatch(w, partSpec, orderSpec) =>
      frameWidth(lower).map(Candidate(a, s, _))
    case _ => None
  }

  /**
   * Whether the aggregate can be rewritten as a prefix difference. Default-deny: only `sum`
   * over an integral (byte/short/int/long) measure with a legacy evaluation mode qualifies.
   * Its accumulator is a `LongType` buffer with wrapping updates, so both the original frame
   * fold and the emitted subtraction compute in Z/2^64Z and the rewrite is bit-identical even
   * when the running prefix - a magnitude no bounded frame can reach - overflows. Decimal
   * (silent NULL on prefix overflow) and floating point (non-associative addition) never
   * qualify, and neither does any aggregate with a non-legacy evaluation mode (e.g. one
   * analyzed in an ANSI session or a `try_sum`).
   */
  private def isPrefixDifferenceSafe(s: Sum): Boolean =
    s.child.dataType.isInstanceOf[IntegralType] &&
      s.evalContext.evalMode == EvalMode.LEGACY

  private def specsMatch(w: Window, partSpec: Seq[Expression], orderSpec: Seq[SortOrder]): Boolean =
    partSpec.length == w.partitionSpec.length &&
      partSpec.zip(w.partitionSpec).forall { case (l, r) => l.semanticEquals(r) } &&
      orderSpec.length == w.orderSpec.length &&
      orderSpec.zip(w.orderSpec).forall { case (l, r) => l.semanticEquals(r) }

  // Compute the width in Long and reject widths that do not fit Int: the width becomes a lag
  // offset, and a wrapped value would silently turn the lag into a lookahead.
  private def frameWidth(lower: Expression): Option[Int] = {
    val width = lower match {
      case IntegerLiteral(v) if v <= -1 => Some(1L - v)
      case UnaryMinus(IntegerLiteral(v), _) if v >= 1 => Some(v + 1L)
      case _ => None
    }
    width.filter(_ <= Int.MaxValue).map(_.toInt)
  }

  /**
   * Builds the rewritten plan for one group of qualifying candidates:
   *   - an inner window over the original child, computing one running
   *     `sum(m) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)` per distinct measure
   *     (plus a running `count(m)` per nullable measure for the NULL guard), and
   *   - the original window node with the candidates replaced by lag differences over the
   *     running aggregates, keeping the same partition spec, order spec and output attributes.
   */
  private def rewriteGroup(w: Window, candidates: Seq[Candidate]): Window = {
    val prefixSpec = WindowSpecDefinition(w.partitionSpec, w.orderSpec,
      SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))

    // Group the candidates by measure in first-appearance order, so the emitted prefix aliases
    // are deterministic. The measure expression itself is the key (structural equality
    // including expression IDs), so distinct attributes with equal names are never merged.
    val measureGroups = mutable.LinkedHashMap.empty[Expression, mutable.ArrayBuffer[Candidate]]
    candidates.foreach { candidate =>
      measureGroups.getOrElseUpdate(candidate.sum.child, mutable.ArrayBuffer.empty) += candidate
    }

    // One running sum (and count, for a nullable measure) per measure, shared by all the
    // measure's candidate frames.
    val prefixAliases = mutable.ArrayBuffer.empty[NamedExpression]
    val differences = mutable.Map.empty[ExprId, NamedExpression]
    measureGroups.values.zipWithIndex.foreach { case (group, index) =>
      val sum = group.head.sum
      val sumAlias = Alias(WindowExpression(
        Sum(sum.child, sum.evalContext).toAggregateExpression(), prefixSpec),
        s"window_prefix_sum_$index")()
      prefixAliases += sumAlias
      val sumAttr = sumAlias.toAttribute
      // A nullable measure needs a running count to tell an all-NULL frame (which must
      // return NULL, as the original fold does) apart from a difference of 0. The count's
      // buffer is a long bounded by the partition row count, so it cannot overflow.
      val countAttr = if (sum.child.nullable) {
        val countAlias = Alias(WindowExpression(
          Count(sum.child).toAggregateExpression(), prefixSpec),
          s"window_prefix_count_$index")()
        prefixAliases += countAlias
        Some(countAlias.toAttribute)
      } else {
        None
      }
      group.foreach(c => differences(c.alias.exprId) = prefixDifference(c, w, sumAttr, countAttr))
    }

    w.copy(
      windowExpressions = w.windowExpressions.map { ne =>
        differences.getOrElse(ne.exprId, ne)
      },
      child = Window(prefixAliases.toSeq, w.partitionSpec, w.orderSpec, w.child))
  }

  /**
   * The lag-difference expression replacing one candidate window expression. The lag returns
   * NULL when the baseline row does not exist, or when the earlier prefix sum is NULL (an
   * all-NULL earlier prefix); in both cases the frame is the clamped whole prefix, so the
   * baseline coalesces to 0 exactly as the original frame folds it.
   */
  private def prefixDifference(
      candidate: Candidate,
      w: Window,
      sumAttr: Attribute,
      countAttr: Option[Attribute]): NamedExpression = {
    val width = candidate.width
    val lagSpec = WindowSpecDefinition(w.partitionSpec, w.orderSpec,
      SpecifiedWindowFrame(RowFrame, Literal(-width, IntegerType), Literal(-width, IntegerType)))

    val lagSum = WindowExpression(
      Lag(sumAttr, Literal(width, IntegerType), Literal(null, LongType), false), lagSpec)
    // Subtraction with a legacy evaluation mode keeps the wraparound semantics of the
    // original frame fold regardless of the session's ANSI setting.
    val difference = Subtract(sumAttr,
      Coalesce(Seq(lagSum, Literal(0L, LongType))), NumericEvalContext(EvalMode.LEGACY))

    val result = countAttr match {
      case None =>
        difference
      case Some(count) =>
        // The count lag's default is 0, so the baseline never needs coalescing: count values
        // are never NULL, so the lag returns either the earlier count (the baseline row exists)
        // or the default 0 (the baseline row does not exist, i.e. the frame is the clamped
        // whole prefix, where the count difference is the current count itself). A count
        // difference of 0 therefore means every row of the frame is NULL.
        val countDifference = Subtract(count,
          WindowExpression(
            Lag(count, Literal(width, IntegerType), Literal(0L, LongType), false), lagSpec),
          NumericEvalContext(EvalMode.LEGACY))
        CaseWhen(Seq(
          (EqualTo(countDifference, Literal(0L, LongType)), Literal(null, LongType))),
          Some(difference))
    }

    // Keep the original alias's name, exprId, qualifier and metadata so downstream
    // references to it resolve to the replacement.
    candidate.alias.withNewChildren(Seq(result)).asInstanceOf[NamedExpression]
  }
}
