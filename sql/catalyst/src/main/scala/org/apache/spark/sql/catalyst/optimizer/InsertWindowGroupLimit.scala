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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, CurrentRow, DenseRank, EqualTo, Expression, ExpressionSet, GreaterThan, GreaterThanOrEqual, IntegerLiteral, LessThan, LessThanOrEqual, NamedExpression, PredicateHelper, Rank, RowFrame, RowNumber, SpecifiedWindowFrame, UnboundedPreceding, WindowExpression, WindowSpecDefinition}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation, LogicalPlan, Window, WindowGroupLimit}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{FILTER, WINDOW}

/**
 * Optimize the filter based on rank-like window function by reduce not required rows.
 * This rule optimizes the following cases:
 * {{{
 *   SELECT *, ROW_NUMBER() OVER(PARTITION BY k ORDER BY a) AS rn FROM Tab1 WHERE rn = 5
 *   SELECT *, ROW_NUMBER() OVER(PARTITION BY k ORDER BY a) AS rn FROM Tab1 WHERE 5 = rn
 *   SELECT *, ROW_NUMBER() OVER(PARTITION BY k ORDER BY a) AS rn FROM Tab1 WHERE rn < 5
 *   SELECT *, ROW_NUMBER() OVER(PARTITION BY k ORDER BY a) AS rn FROM Tab1 WHERE 5 > rn
 *   SELECT *, ROW_NUMBER() OVER(PARTITION BY k ORDER BY a) AS rn FROM Tab1 WHERE rn <= 5
 *   SELECT *, ROW_NUMBER() OVER(PARTITION BY k ORDER BY a) AS rn FROM Tab1 WHERE 5 >= rn
 * }}}
 */
object InsertWindowGroupLimit extends Rule[LogicalPlan] with PredicateHelper {

  /**
   * Extract all the limit values from predicates.
   */
  def extractLimits(condition: Expression, attr: Attribute): Option[Int] = {
    val limits = splitConjunctivePredicates(condition).collect {
      case EqualTo(IntegerLiteral(limit), e) if e.semanticEquals(attr) => limit
      case EqualTo(e, IntegerLiteral(limit)) if e.semanticEquals(attr) => limit
      case LessThan(e, IntegerLiteral(limit)) if e.semanticEquals(attr) => limit - 1
      case GreaterThan(IntegerLiteral(limit), e) if e.semanticEquals(attr) => limit - 1
      case LessThanOrEqual(e, IntegerLiteral(limit)) if e.semanticEquals(attr) => limit
      case GreaterThanOrEqual(IntegerLiteral(limit), e) if e.semanticEquals(attr) => limit
    }

    if (limits.nonEmpty) Some(limits.min) else None
  }

  private def supports(
      windowExpressions: Seq[NamedExpression]): Boolean = windowExpressions.exists {
    case Alias(WindowExpression(_: Rank | _: DenseRank | _: RowNumber, WindowSpecDefinition(_, _,
    SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))), _) => true
    case _ => false
  }

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (conf.windowGroupLimitThreshold == -1) return plan

    plan.transformWithPruning(_.containsAllPatterns(FILTER, WINDOW), ruleId) {
      case filter @ Filter(condition,
        window @ Window(windowExpressions, partitionSpec, orderSpec, child))
        if !child.isInstanceOf[WindowGroupLimit] &&
          supports(windowExpressions) && orderSpec.nonEmpty =>
        val limits = windowExpressions.collect {
          case alias @ Alias(WindowExpression(rankLikeFunction, _), _) =>
            extractLimits(condition, alias.toAttribute).map((_, rankLikeFunction))
        }.flatten

        // multiple different rank-like functions unsupported.
        if (limits.isEmpty || ExpressionSet(limits.map(_._2)).size > 1) {
          filter
        } else {
          val minLimit = limits.minBy(_._1)
          minLimit match {
            case (limit, rankLikeFunction) if limit <= conf.windowGroupLimitThreshold =>
              if (limit > 0) {
                val windowGroupLimit =
                  WindowGroupLimit(partitionSpec, orderSpec, rankLikeFunction, limit, child)
                val newWindow = window.withNewChildren(Seq(windowGroupLimit))
                val newFilter = filter.withNewChildren(Seq(newWindow))
                newFilter
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
