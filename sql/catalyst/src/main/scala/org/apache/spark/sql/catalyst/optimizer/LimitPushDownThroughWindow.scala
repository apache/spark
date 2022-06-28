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

import org.apache.spark.sql.catalyst.expressions.{Alias, AliasHelper, Ascending, Attribute, CurrentRow, DenseRank, Expression, IntegerLiteral, LessThan, LessThanOrEqual, NamedExpression, NTile, Rank, RowFrame, RowNumber, SortOrder, SpecifiedWindowFrame, UnboundedPreceding, WindowExpression, WindowSpecDefinition}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{LIMIT, WINDOW}

/**
 * Pushes down [[Limit]] beneath WINDOW. This rule optimizes the following cases:
 * {{{
 *   SELECT *, ROW_NUMBER() OVER(ORDER BY a) AS rn FROM Tab1 LIMIT 5 ==>
 *   SELECT *, ROW_NUMBER() OVER(ORDER BY a) AS rn FROM (SELECT * FROM Tab1 ORDER BY a LIMIT 5) t
 *
 *   SELECT *, ROW_NUMBER() OVER(PARTITION BY a ORDER BY b) AS rn FROM Tab1 LIMIT 5 ==>
 *   SELECT *,
 *          row_number() OVER(partition BY a ORDER BY b) AS rn
 *   FROM   (SELECT * FROM tab1 ORDER BY a, b LIMIT 5) t
 *
 *   SELECT *
 *   FROM   (SELECT *, row_number() OVER(PARTITION BY a ORDER BY b) AS rn FROM tab1) t
 *   WHERE  rn <= 5
 *   LIMIT 5 ==>
 *   SELECT *
 *   FROM   (SELECT *,
 *                  row_number() OVER(partition BY a ORDER BY b) AS rn
 *           FROM   (SELECT * FROM tab1 ORDER BY a, b LIMIT 5) t)
 *   WHERE rn <= 5
 * }}}
 */
object LimitPushDownThroughWindow extends Rule[LogicalPlan] with AliasHelper {
  // The window frame can only be UNBOUNDED PRECEDING to CURRENT ROW.
  private def pushableWindowExprs(
      windowExpressions: Seq[NamedExpression]): Boolean = windowExpressions.forall {
    case Alias(WindowExpression(_: Rank | _: DenseRank | _: NTile | _: RowNumber,
        WindowSpecDefinition(_, _,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))), _) => true
    case _ => false
  }

  // Check if it is safe to push down limit. A supported case must meet the following criteria:
  // - It only has one pushable window function.
  // - It only has one filter condition on this window function.
  // - Window function's partition specification is the prefix of sort expressions.
  private def supportsPushDown(
      attr: Attribute, order: Seq[SortOrder], window: Window): Boolean = {
    window.windowExpressions.size == 1 &&
      getAliasMap(window.windowExpressions).get(attr).exists(a => pushableWindowExprs(a :: Nil)) &&
      (order.isEmpty || (window.partitionSpec.nonEmpty &&
        window.partitionSpec.zip(order).forall {
          case (e: Expression, s: SortOrder) => s.child.semanticEquals(e)
        }))
  }

  // Sort partition specification and order specification.
  private def sortWindowSpec(
      partitionSpec: Seq[Expression], orderSpec: Seq[SortOrder]): Seq[SortOrder] = {
    partitionSpec.map(SortOrder(_, Ascending)) ++ orderSpec
  }

  private def pushedOrder(order: Seq[SortOrder], window: Window): Seq[SortOrder] = {
    val windowSize = window.partitionSpec.size
    order.take(windowSize) ++
      sortWindowSpec(window.partitionSpec.takeRight(windowSize - order.size), window.orderSpec)
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(
    _.containsAllPatterns(WINDOW, LIMIT), ruleId) {
    // Adding an extra Limit below WINDOW when the partitionSpec of all window functions is empty.
    case Limit(limitExpr @ IntegerLiteral(limit),
        window @ Window(windowExpressions, partitionSpec, orderSpec, child))
      if pushableWindowExprs(windowExpressions) && child.maxRows.forall(_ > limit) &&
        limit < conf.topKSortFallbackThreshold =>
      // Sort is needed here because we need global sort.
      val newWindowChild =
        Limit(limitExpr, Sort(sortWindowSpec(partitionSpec, orderSpec), true, child))
      window.copy(child = newWindowChild)
    // There is a Project between LocalLimit and Window if they do not have the same output.
    case Limit(limitExpr @ IntegerLiteral(limit), project @ Project(_,
        window @ Window(windowExpressions, partitionSpec, orderSpec, child)))
      if pushableWindowExprs(windowExpressions) && child.maxRows.forall(_ > limit) &&
        limit < conf.topKSortFallbackThreshold =>
      // Sort is needed here because we need global sort.
      val newWindowChild =
        Limit(limitExpr, Sort(sortWindowSpec(partitionSpec, orderSpec), true, child))
      project.copy(child = window.copy(child = newWindowChild))

    case Limit(limitExpr @ IntegerLiteral(limit),
        f @ Filter(ExtractFilterCondition(attr, filterVal), window: Window))
      if limit < filterVal && window.maxRows.forall(_ > limit) &&
        limit < conf.topKSortFallbackThreshold && supportsPushDown(attr, Nil, window) =>
      val newWindowChild = Limit(limitExpr, Sort(pushedOrder(Nil, window), true, window.child))
      f.copy(child = window.copy(child = newWindowChild))

    case Limit(limitExpr @ IntegerLiteral(limit), s @ Sort(order, _,
        f @ Filter(ExtractFilterCondition(attr, filterVal), window: Window)))
      if limit < filterVal && window.maxRows.forall(_ > limit) &&
        limit < conf.topKSortFallbackThreshold && supportsPushDown(attr, order, window) =>
      val newWindowChild = Limit(limitExpr, Sort(pushedOrder(order, window), true, window.child))
      s.copy(child = f.copy(child = window.copy(child = newWindowChild)))
  }

  private object ExtractFilterCondition {
    def unapply(e: Expression): Option[(Attribute, Int)] = e match {
      case LessThan(attr: Attribute, IntegerLiteral(value)) =>
        Some((attr, value))
      case LessThanOrEqual(attr: Attribute, IntegerLiteral(value)) =>
        Some((attr, value + 1))
      case _ => None
    }
  }
}
