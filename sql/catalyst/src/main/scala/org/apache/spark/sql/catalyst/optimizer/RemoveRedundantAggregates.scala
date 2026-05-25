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

import org.apache.spark.sql.catalyst.expressions.{Alias, AliasHelper, Attribute, AttributeSet}
import org.apache.spark.sql.catalyst.expressions.{EvalMode, ExpressionSet, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count, Sum}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.AGGREGATE

/**
 * Remove redundant aggregates from a query plan. A redundant aggregate is an aggregate whose
 * only goal is to keep distinct values, while its parent aggregate would ignore duplicate values.
 */
object RemoveRedundantAggregates extends Rule[LogicalPlan] with AliasHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan.transformUpWithPruning(
    _.containsPattern(AGGREGATE), ruleId) {
    case upper @ Aggregate(_, _, lower: Aggregate, _)
        if collapseSumOfCount(upper, lower).isDefined =>
      collapseSumOfCount(upper, lower).get
    case upper @ Aggregate(_, _, lower: Aggregate, _) if isLowerRedundant(upper, lower) =>
      val projectList = lower.aggregateExpressions.filter(upper.references.contains(_))
      upper.copy(child = Project(projectList, lower.child))
    case agg @ Aggregate(groupingExps, _, child, _)
        if agg.groupOnly && child.distinctKeys.exists(_.subsetOf(ExpressionSet(groupingExps))) =>
      Project(agg.aggregateExpressions, child)
  }

  /**
   * `SUM(COUNT(1))` over a finer-grained grouped aggregation can be evaluated as `SUM(1)` over
   * the input when the lower aggregation is grouped. This avoids evaluating lower grouping
   * expressions that only partition rows before their counts are added back together.
   *
   * Retaining `SUM` preserves its empty-input and ANSI overflow semantics. `TRY` is excluded
   * because the lower `COUNT` can overflow differently from a direct `TRY_SUM(1)`. A global
   * lower aggregate is excluded because it produces one row even when its input is empty.
   */
  private def collapseSumOfCount(upper: Aggregate, lower: Aggregate): Option[Aggregate] = {
    if (lower.groupingExpressions.isEmpty || !lower.groupingExpressions.forall(_.deterministic)) {
      return None
    }

    val countOutputs = AttributeSet(lower.aggregateExpressions.collect {
      case a @ Alias(
          AggregateExpression(Count(Seq(l: Literal)), _, false, None, _), _)
          if l.value != null => a.toAttribute
    })
    if (countOutputs.isEmpty) {
      return None
    }

    val upperAggs = upper.aggregateExpressions.flatMap(_.collect {
      case ae: AggregateExpression => ae
    })
    if (upperAggs.isEmpty || !upperAggs.forall {
      case AggregateExpression(Sum(a: Attribute, context), _, false, None, _) =>
        context.evalMode != EvalMode.TRY && countOutputs.contains(a)
      case _ => false
    }) {
      return None
    }

    val rewrittenExpressions = upper.aggregateExpressions.map { expr =>
      expr.transform {
        case ae @ AggregateExpression(Sum(a: Attribute, context), _, false, None, _)
            if countOutputs.contains(a) =>
          ae.copy(aggregateFunction = Sum(Literal(1L), context))
      }.asInstanceOf[NamedExpression]
    }
    val rewritten = upper.copy(aggregateExpressions = rewrittenExpressions)
    val lowerNonAggOutputs = AttributeSet(lower.aggregateExpressions
      .filter(_.deterministic)
      .filterNot(AggregateExpression.containsAggregate)
      .map(_.toAttribute))
    if (!rewritten.references.subsetOf(lowerNonAggOutputs)) {
      return None
    }

    val projectList = lower.aggregateExpressions.filter(rewritten.references.contains(_))
    Some(rewritten.copy(child = Project(projectList, lower.child)))
  }

  private def isLowerRedundant(upper: Aggregate, lower: Aggregate): Boolean = {
    val upperHasNoDuplicateSensitiveAgg = upper
      .aggregateExpressions
      .forall(expr => !expr.exists {
        case ae: AggregateExpression => isDuplicateSensitive(ae)
        case _ => false
      })

    lazy val upperRefsOnlyDeterministicNonAgg = upper.references.subsetOf(AttributeSet(
      lower
        .aggregateExpressions
        .filter(_.deterministic)
        .filterNot(AggregateExpression.containsAggregate)
        .map(_.toAttribute)
    ))

    // If the lower aggregation is global, it is not redundant because a project with
    // non-aggregate expressions is different with global aggregation in semantics.
    // E.g., if the input relation is empty, a project might be optimized to an empty
    // relation, while a global aggregation will return a single row.
    lazy val lowerIsGlobalAgg = lower.groupingExpressions.isEmpty

    upperHasNoDuplicateSensitiveAgg && upperRefsOnlyDeterministicNonAgg && !lowerIsGlobalAgg
  }

  private def isDuplicateSensitive(ae: AggregateExpression): Boolean = {
    !ae.isDistinct && !EliminateDistinct.isDuplicateAgnostic(ae.aggregateFunction)
  }
}
