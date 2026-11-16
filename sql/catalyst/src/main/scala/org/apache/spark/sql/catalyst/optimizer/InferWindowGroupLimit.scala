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

import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, CurrentRow, DenseRank, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, IntegerLiteral, LessThan, LessThanOrEqual, Literal, NamedExpression, PredicateHelper, Rank, RowFrame, RowNumber, SizeBasedWindowFunction, SpecifiedWindowFrame, UnboundedPreceding, WindowExpression, WindowSpecDefinition}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Limit, LocalRelation, LogicalPlan, Project, Window, WindowGroupLimit}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{FILTER, WINDOW}
import org.apache.spark.sql.catalyst.util.UnsafeRowUtils

/**
 * Inserts a `WindowGroupLimit` below `Window` if the `Window` has rank-like functions
 * and the function results are further filtered by limit-like predicates, and further
 * eliminates the `Window`/`Filter` entirely when they become provably redundant after that
 * insertion (see the top-1 special case below). Example query:
 * {{{
 *   SELECT * FROM (
 *      SELECT *, ROW_NUMBER() OVER(PARTITION BY k ORDER BY a) AS rn FROM Tab1
 *   ) WHERE rn = 5;
 *
 *   SELECT * FROM (
 *      SELECT *, ROW_NUMBER() OVER(PARTITION BY k ORDER BY a) AS rn FROM Tab1
 *   ) WHERE 5 = rn;
 *
 *   SELECT * FROM (
 *      SELECT *, ROW_NUMBER() OVER(PARTITION BY k ORDER BY a) AS rn FROM Tab1
 *   ) WHERE rn < 5;
 *
 *   SELECT * FROM (
 *      SELECT *, ROW_NUMBER() OVER(PARTITION BY k ORDER BY a) AS rn FROM Tab1
 *   ) WHERE 5 > rn;
 *
 *   SELECT * FROM (
 *      SELECT *, ROW_NUMBER() OVER(PARTITION BY k ORDER BY a) AS rn FROM Tab1
 *    ) WHERE rn <= 5;
 *
 *   SELECT * FROM (
 *      SELECT *, ROW_NUMBER() OVER(PARTITION BY k ORDER BY a) AS rn FROM Tab1
 *   ) WHERE 5 >= rn;
 * }}}
 *
 * As a special case, once the `WindowGroupLimit` has truncated every partition group to at
 * most one row (`limit = 1`), a `row_number()` recomputed over that same partition/order spec
 * can only ever produce the literal `1` for the surviving row, making the `Window` and its
 * top-1 `Filter` provably dead computation. When the `Window` computes nothing else and carries
 * no hint, this rule replaces both with a `Project` that aliases the literal `1` onto the
 * original `rn` output, instead of inserting the `WindowGroupLimit` below an otherwise-unchanged
 * `Window`/`Filter`:
 * {{{
 *   Filter(rn = 1 / rn <= 1 / rn < 2)
 *     Window(row_number() AS rn, partitionSpec, orderSpec)
 *       Tab1
 * }}}
 * becomes
 * {{{
 *   Project(..., 1 AS rn)
 *     WindowGroupLimit(partitionSpec, orderSpec, row_number(), limit = 1)
 *       Tab1
 * }}}
 * A deterministic extra conjunct alongside the top-1 predicate (e.g. `rn = 1 AND v > 6`) is kept
 * as a `Filter` over the new `Project` rather than dropped, since it is evaluated on the same
 * single surviving row either way. A nondeterministic one is left in place instead (keeping the
 * `Window`/`Filter` unchanged), since relocating it would change which rows it is evaluated on.
 * This special case never applies when `partitionSpec` is empty and `limit` is below
 * `spark.sql.execution.topKSortFallbackThreshold` (the default), since that case is instead
 * rewritten straight to a `Limit` on top of the `Window`, with no `WindowGroupLimit` involved.
 * It also never applies when a `partitionSpec` expression is not binary-stable (e.g. a STRING
 * with a non-`UTF8_BINARY` collation): `WindowGroupLimitExec` groups rows by raw `UnsafeRow`
 * byte equality, so collation-equal keys with different bytes land in separate groups and each
 * keeps its own row. The `Window`/`Filter` being eliminated here is what re-merges those rows
 * back into one group today, so dropping them would let the extras leak through as `rn = 1`.
 */
object InferWindowGroupLimit extends Rule[LogicalPlan] with PredicateHelper {

  /**
   * Extract the limit value from a single predicate of the form `attr <op> literal` or
   * `literal <op> attr`, e.g. `attr = 5`, `5 = attr`, `attr < 5`, `5 > attr`, `attr <= 5` or
   * `5 >= attr`. Returns `None` if `condition` is not one of these shapes (e.g. a conjunction).
   */
  private def extractLimit(condition: Expression, attr: Attribute): Option[Int] = condition match {
    case EqualTo(IntegerLiteral(limit), e) if e.semanticEquals(attr) => Some(limit)
    case EqualTo(e, IntegerLiteral(limit)) if e.semanticEquals(attr) => Some(limit)
    case LessThan(e, IntegerLiteral(limit)) if e.semanticEquals(attr) => Some(limit - 1)
    case GreaterThan(IntegerLiteral(limit), e) if e.semanticEquals(attr) => Some(limit - 1)
    case LessThanOrEqual(e, IntegerLiteral(limit)) if e.semanticEquals(attr) => Some(limit)
    case GreaterThanOrEqual(IntegerLiteral(limit), e) if e.semanticEquals(attr) => Some(limit)
    case _ => None
  }

  /**
   * Extract all the limit values from predicates.
   */
  private def extractLimits(condition: Expression, attr: Attribute): Option[Int] = {
    val limits = splitConjunctivePredicates(condition).flatMap(extractLimit(_, attr))
    if (limits.nonEmpty) Some(limits.min) else None
  }

  /**
   * Splits a `Filter` condition into its top-1 conjunct(s) for `attr` (see `extractLimit`) and
   * the rest, e.g. `rn = 1 AND v > 6` -> `(Seq(rn = 1), Seq(v > 6))`. The first element is empty
   * if `condition` has no top-1 conjunct for `attr`.
   */
  private def splitTop1Conjuncts(
      condition: Expression, attr: Attribute): (Seq[Expression], Seq[Expression]) =
    splitConjunctivePredicates(condition).partition(c => extractLimit(c, attr).contains(1))

  /**
   * All window expressions should use the same expanding window and do not contains
   * `SizeBasedWindowFunction`, so that we can safely do the early stop.
   */
  private def isExpandingWindow(
      windowExpression: NamedExpression): Boolean = windowExpression match {
    case Alias(WindowExpression(windowFunction, WindowSpecDefinition(_, _,
    SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))), _)
      if !windowFunction.isInstanceOf[SizeBasedWindowFunction] => true
    case _ => false
  }

  private def support(windowFunction: Expression): Boolean = windowFunction match {
    case _: Rank | _: DenseRank | _: RowNumber => true
    case _ => false
  }

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (conf.windowGroupLimitThreshold == -1) return plan

    plan.transformWithPruning(_.containsAllPatterns(FILTER, WINDOW), ruleId) {
      case filter @ Filter(condition,
        window @ Window(windowExpressions, partitionSpec, orderSpec, child, _))
        if !child.isInstanceOf[WindowGroupLimit] && windowExpressions.forall(isExpandingWindow) &&
          orderSpec.nonEmpty =>
        val limits = windowExpressions.collect {
          case alias @ Alias(WindowExpression(rankLikeFunction, _), _)
            if support(rankLikeFunction) =>
            extractLimits(condition, alias.toAttribute).map((_, rankLikeFunction, alias))
        }.flatten

        if (limits.isEmpty) {
          filter
        } else {
          val (rowNumberLimits, otherLimits) = limits.partition(_._2.isInstanceOf[RowNumber])
          // Pick RowNumber first as it's cheaper to evaluate.
          val selectedLimits = if (rowNumberLimits.isEmpty) {
            otherLimits
          } else {
            rowNumberLimits
          }
          // Pick a rank-like function with the smallest limit
          selectedLimits.minBy(_._1) match {
            case (limit, rankLikeFunction, alias) if limit <= conf.windowGroupLimitThreshold &&
              child.maxRows.forall(_ > limit) =>
              if (limit > 0) {
                if (rankLikeFunction.isInstanceOf[RowNumber] &&
                  partitionSpec.isEmpty && limit < conf.topKSortFallbackThreshold) {
                  // Top n (Limit + Sort) have better performance than WindowGroupLimit if the
                  // window function is RowNumber and Window partitionSpec is empty.
                  filter.withNewChildren(Seq(Limit(Literal(limit), window)))
                } else {
                  val windowGroupLimit =
                    WindowGroupLimit(partitionSpec, orderSpec, rankLikeFunction, limit, child)
                  lazy val (top1Conjuncts, remainingConjuncts) =
                    splitTop1Conjuncts(condition, alias.toAttribute)
                  if (limit == 1 && windowExpressions.length == 1 && window.hint.isEmpty &&
                    rankLikeFunction.isInstanceOf[RowNumber] &&
                    partitionSpec.forall(e => UnsafeRowUtils.isBinaryStable(e.dataType)) &&
                    top1Conjuncts.nonEmpty && remainingConjuncts.forall(_.deterministic)) {
                    // The surviving row's row_number() can only be 1, so the top-1 conjunct(s)
                    // are always true and that part of the Filter is dead computation. Keep the
                    // alias's qualifier/metadata so the output schema is unaffected. See the
                    // class doc for the binary-stable and determinism requirements above.
                    val newAlias = Alias(Literal(1), alias.name)(
                      exprId = alias.exprId,
                      qualifier = alias.qualifier,
                      explicitMetadata = Some(alias.metadata))
                    val project = Project(child.output :+ newAlias, windowGroupLimit)
                    if (remainingConjuncts.isEmpty) {
                      project
                    } else {
                      Filter(remainingConjuncts.reduceLeft(And), project)
                    }
                  } else {
                    filter.withNewChildren(Seq(window.withNewChildren(Seq(windowGroupLimit))))
                  }
                }
              } else {
                LocalRelation(filter.output, data = Seq.empty, isStreaming = filter.isStreaming)
              }
            case _ =>
              filter
          }
        }
    }
  }
}
