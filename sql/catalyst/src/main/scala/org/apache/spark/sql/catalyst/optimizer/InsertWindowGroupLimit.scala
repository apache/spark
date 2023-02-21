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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, CurrentRow, DenseRank, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, IntegerLiteral, LessThan, LessThanOrEqual, NamedExpression, PredicateHelper, Rank, RowFrame, RowNumber, SpecifiedWindowFrame, UnboundedPreceding, WindowExpression, WindowSpecDefinition}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation, LogicalPlan, Window, WindowGroupLimit}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{FILTER, WINDOW}

/**
 * Inserts a `WindowGroupLimit` below `Window` if the `Window` has rank-like functions
 * and the function results are further filtered by limit-like predicates. Example query:
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

  private def support(
      windowExpression: NamedExpression): Boolean = windowExpression match {
    case Alias(WindowExpression(_: Rank | _: DenseRank | _: RowNumber, WindowSpecDefinition(_, _,
    SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))), _) => true
    case _ => false
  }

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (conf.windowGroupLimitThreshold == -1) return plan

    plan.transformWithPruning(_.containsAllPatterns(FILTER, WINDOW), ruleId) {
      case filter @ Filter(condition,
        window @ Window(windowExpressions, partitionSpec, orderSpec, child))
        if !child.isInstanceOf[WindowGroupLimit] && windowExpressions.exists(support) &&
          orderSpec.nonEmpty =>
        val limits = windowExpressions.collect {
          case alias @ Alias(WindowExpression(rankLikeFunction, _), _) if support(alias) =>
            extractLimits(condition, alias.toAttribute).map((_, rankLikeFunction))
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
            case (limit, rankLikeFunction) if limit <= conf.windowGroupLimitThreshold =>
              if (limit > 0) {
                val windowGroupLimit =
                  WindowGroupLimit(partitionSpec, orderSpec, rankLikeFunction, limit, child)
                val newWindow = window.withNewChildren(Seq(windowGroupLimit))
                filter.withNewChildren(Seq(newWindow))
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
