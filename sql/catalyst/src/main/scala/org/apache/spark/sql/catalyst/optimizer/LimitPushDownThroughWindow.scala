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

import org.apache.spark.sql.catalyst.expressions.{Alias, CurrentRow, DenseRank, IntegerLiteral, NamedExpression, Rank, RowFrame, RowNumber, SpecifiedWindowFrame, UnboundedPreceding, WindowExpression, WindowSpecDefinition}
import org.apache.spark.sql.catalyst.plans.logical.{Limit, LocalLimit, LogicalPlan, Project, Sort, Window}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{LIMIT, WINDOW}

/**
 * Pushes down [[LocalLimit]] beneath WINDOW. This rule optimizes the following case:
 * {{{
 *   SELECT *, ROW_NUMBER() OVER(ORDER BY a) AS rn FROM Tab1 LIMIT 5 ==>
 *   SELECT *, ROW_NUMBER() OVER(ORDER BY a) AS rn FROM (SELECT * FROM Tab1 ORDER BY a LIMIT 5) t
 * }}}
 */
object LimitPushDownThroughWindow extends Rule[LogicalPlan] {
  // The window frame of RankLike and RowNumberLike can only be UNBOUNDED PRECEDING to CURRENT ROW.
  private def supportsPushdownThroughWindow(
      windowExpressions: Seq[NamedExpression]): Boolean = windowExpressions.forall {
    case Alias(WindowExpression(_: Rank | _: DenseRank | _: RowNumber, WindowSpecDefinition(Nil, _,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))), _) => true
    case _ => false
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(
    _.containsAllPatterns(WINDOW, LIMIT), ruleId) {
    // Adding an extra Limit below WINDOW when the partitionSpec of all window functions is empty.
    case LocalLimit(limitExpr @ IntegerLiteral(limit),
        window @ Window(windowExpressions, Nil, orderSpec, child))
      if supportsPushdownThroughWindow(windowExpressions) && child.maxRows.forall(_ > limit) &&
        limit < conf.topKSortFallbackThreshold =>
      // Sort is needed here because we need global sort.
      window.copy(child = Limit(limitExpr, Sort(orderSpec, true, child)))
    // There is a Project between LocalLimit and Window if they do not have the same output.
    case LocalLimit(limitExpr @ IntegerLiteral(limit), project @ Project(_,
        window @ Window(windowExpressions, Nil, orderSpec, child)))
      if supportsPushdownThroughWindow(windowExpressions) && child.maxRows.forall(_ > limit) &&
        limit < conf.topKSortFallbackThreshold =>
      // Sort is needed here because we need global sort.
      project.copy(child = window.copy(child = Limit(limitExpr, Sort(orderSpec, true, child))))
  }
}
