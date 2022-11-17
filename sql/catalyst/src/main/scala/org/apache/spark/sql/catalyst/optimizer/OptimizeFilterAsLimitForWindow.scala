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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, CurrentRow, DenseRank, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, Literal, NamedExpression, PredicateHelper, Rank, RowFrame, RowNumber, SpecifiedWindowFrame, UnboundedPreceding, WindowExpression, WindowSpecDefinition}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation, LogicalPlan, Window}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{FILTER, WINDOW}
import org.apache.spark.sql.types.IntegerType

/**
 * Optimize the filter based on rank-like window function by reduce not required rows.
 * This rule optimizes the following cases:
 * {{{
 *   SELECT *, ROW_NUMBER() OVER(ORDER BY a) AS rn FROM Tab1 WHERE rn = 5
 *   SELECT *, ROW_NUMBER() OVER(ORDER BY a) AS rn FROM Tab1 WHERE 5 = rn
 *   SELECT *, ROW_NUMBER() OVER(ORDER BY a) AS rn FROM Tab1 WHERE rn < 5
 *   SELECT *, ROW_NUMBER() OVER(ORDER BY a) AS rn FROM Tab1 WHERE 5 > rn
 *   SELECT *, ROW_NUMBER() OVER(ORDER BY a) AS rn FROM Tab1 WHERE rn <= 5
 *   SELECT *, ROW_NUMBER() OVER(ORDER BY a) AS rn FROM Tab1 WHERE 5 >= rn
 * }}}
 */
object OptimizeFilterAsLimitForWindow extends Rule[LogicalPlan] with PredicateHelper {

  private def extractLimits(condition: Expression, attr: Attribute): Option[Int] = {
    val limits = splitConjunctivePredicates(condition).collect {
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

    if (limits.nonEmpty) Some(limits.min) else None
  }

  private def extractLimitAndRankFunction(
      condition: Expression,
      windowExpressions: Seq[NamedExpression]): Option[(Int, Expression)] = {
    val limitAndWindowFunctions = windowExpressions.collect {
      case alias @ Alias(WindowExpression(windowFunction, _), _) =>
        val limits = extractLimits(condition, alias.toAttribute)
        limits.map((_, windowFunction))
    }.filter(_.isDefined)

    if (limitAndWindowFunctions.nonEmpty) {
      limitAndWindowFunctions.sortBy(_.get._1).head
    } else {
      None
    }
  }

  private def supports(
      windowExpressions: Seq[NamedExpression]): Boolean = windowExpressions.forall {
    case Alias(WindowExpression(_: Rank | _: DenseRank | _: RowNumber, WindowSpecDefinition(_, _,
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))), _) => true
    case _ => false
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(
    _.containsAllPatterns(FILTER, WINDOW), ruleId) {
    case filter @ Filter(condition, w @ Window(windowExpressions, partitionSpec, orderSpec, _, _))
      if supports(windowExpressions) && partitionSpec.nonEmpty && orderSpec.nonEmpty =>
      extractLimitAndRankFunction(condition, windowExpressions) match {
        case Some((limit, windowFunction)) if limit > 0 =>
          val newWindow = w.copy(groupLimit = Some(limit))
          val newFilter = filter.withNewChildren(Seq(newWindow))
          newFilter
        case Some((limit, _)) if limit <= 0 =>
          LocalRelation(filter.output, data = Seq.empty, isStreaming = filter.isStreaming)
        case _ => filter
      }
  }
}
