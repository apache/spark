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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, CurrentRow, DenseRank, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, IntegerLiteral, LessThan, LessThanOrEqual, Literal, NamedExpression, PredicateHelper, Rank, RowFrame, RowNumber, SpecifiedWindowFrame, UnboundedPreceding, WindowExpression, WindowSpecDefinition}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Limit, LocalLimit, LogicalPlan, Project, Sort, Window}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{FILTER, LIMIT, WINDOW}
import org.apache.spark.sql.types.IntegerType

/**
 * Pushes down [[LocalLimit]] beneath WINDOW. This rule optimizes the following case:
 * {{{
 *   SELECT *, ROW_NUMBER() OVER(ORDER BY a) AS rn FROM Tab1 LIMIT 5 ==>
 *   SELECT *, ROW_NUMBER() OVER(ORDER BY a) AS rn FROM (SELECT * FROM Tab1 ORDER BY a LIMIT 5) t
 *   SELECT *, ROW_NUMBER() OVER(ORDER BY a) AS rn FROM Tab1 WHERE rn <= 5 ==>
 *   SELECT *, ROW_NUMBER() OVER(ORDER BY a) AS rn FROM (SELECT * FROM Tab1 ORDER BY a LIMIT 5) t
 * }}}
 */
object LimitPushDownThroughWindow extends Rule[LogicalPlan] with PredicateHelper {
  // The window frame of RankLike and RowNumberLike can only be UNBOUNDED PRECEDING to CURRENT ROW.
  private def supportsPushdownThroughWindow(
      windowExpressions: Seq[NamedExpression]): Boolean = windowExpressions.forall {
    case Alias(WindowExpression(_: Rank | _: DenseRank | _: RowNumber, WindowSpecDefinition(Nil, _,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))), _) => true
    case _ => false
  }

  /**
   * Extract all the limit values from predicates.
   */
  def extractLimits(condition: Expression, attr: Attribute): Seq[Int] =
    splitConjunctivePredicates(condition).collect {
      case EqualTo(Literal(limit: Int, IntegerType), e)
        if e.semanticEquals(attr) => limit
      case EqualTo(e, Literal(limit: Int, IntegerType))
        if e.semanticEquals(attr) => limit
      case LessThan(e, Literal(limit: Int, IntegerType))
        if e.semanticEquals(attr) => limit - 1
      case GreaterThan(Literal(limit: Int, IntegerType), e)
        if e.semanticEquals(attr) => limit - 1
      case LessThanOrEqual(e, Literal(limit: Int, IntegerType))
        if e.semanticEquals(attr) => limit
      case GreaterThanOrEqual(Literal(limit: Int, IntegerType), e)
        if e.semanticEquals(attr) => limit
    }

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(
    _.containsAnyPattern(WINDOW, LIMIT, FILTER), ruleId) {
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
    case filter @ Filter(condition,
        window @ Window(windowExpressions, Nil, orderSpec, child))
      if condition.getTagValue(Filter.FILTER_PUSHED_AS_LIMIT).isEmpty &&
        supportsPushdownThroughWindow(windowExpressions) && orderSpec.nonEmpty =>
      val limits = windowExpressions.collect {
        case alias: Alias => extractLimits(condition, alias.toAttribute)
      }.flatten
      if (limits.nonEmpty) {
        val limit = limits.min
        if (limit < conf.topKSortFallbackThreshold) {
          val newWindow = window.copy(child = Limit(Literal(limit), Sort(orderSpec, true, child)))
          condition.setTagValue(Filter.FILTER_PUSHED_AS_LIMIT, ())
          val newFilter = filter.copy(child = newWindow)
          newFilter
        } else {
          filter
        }
      } else {
        filter
      }
  }
}
