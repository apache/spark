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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeSet, EvalMode, Literal,
  NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count, Max, Min, Sum}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.AGGREGATE

/**
 * Collapses a grouped `SUM(COUNT(1))` rollup to a single `SUM(1)` aggregation.
 *
 * Data source scan planning invokes this after first attempting to push the original `COUNT(1)`,
 * and before final column pruning when the collapse is needed.
 */
object CollapseGroupedSumOfCount extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan.transformUpWithPruning(
    _.containsPattern(AGGREGATE), ruleId) {
    case upper @ Aggregate(_, _, lower: Aggregate, _) =>
      collapse(upper, lower).getOrElse(upper)
  }

  /**
   * `SUM(COUNT(1))` over a finer-grained grouped aggregation can be evaluated as `SUM(1)` over
   * the input when the lower aggregation is grouped. Unfiltered non-distinct `MAX` and `MIN`
   * over retained lower grouping outputs can remain alongside the rewritten `SUM`, because
   * repeating their input values does not change their result. This avoids evaluating lower
   * grouping expressions that only partition rows before their counts are added back together.
   *
   * Retaining `SUM` preserves its empty-input and ANSI overflow semantics. `TRY` is excluded
   * because the lower `COUNT` can overflow differently from a direct `TRY_SUM(1)`. A global
   * lower aggregate is excluded because it produces one row even when its input is empty.
   */
  private def collapse(upper: Aggregate, lower: Aggregate): Option[Aggregate] = {
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
    val hasCollapsibleSum = upperAggs.exists {
      case AggregateExpression(Sum(a: Attribute, context), _, false, None, _) =>
        context.evalMode != EvalMode.TRY && countOutputs.contains(a)
      case _ => false
    }
    if (!hasCollapsibleSum || !upperAggs.forall {
      case AggregateExpression(Sum(a: Attribute, context), _, false, None, _) =>
        context.evalMode != EvalMode.TRY && countOutputs.contains(a)
      case AggregateExpression(_: Max, _, false, None, _) => true
      case AggregateExpression(_: Min, _, false, None, _) => true
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
    val discardedGroupingRefs =
      AttributeSet(lower.groupingExpressions.flatMap(_.references)) -- rewritten.references
    if (hasNondeterministicProducer(lower.child, discardedGroupingRefs)) {
      return None
    }
    Some(rewritten.copy(child = Project(projectList, lower.child)))
  }

  /**
   * The analyzer pulls a nondeterministic grouping expression, such as `rand()`, into a `Project`
   * below the aggregate and replaces the grouping key with its output attribute. Do not remove
   * that projected evaluation when the corresponding grouping key is discarded by this rule.
   */
  private def hasNondeterministicProducer(
      plan: LogicalPlan,
      attributes: AttributeSet): Boolean = plan match {
    case Project(projectList, child) =>
      val producers = projectList.filter(expr => attributes.contains(expr.toAttribute))
      producers.exists(!_.deterministic) ||
        hasNondeterministicProducer(child, AttributeSet(producers.flatMap(_.references)))
    case _ => false
  }
}
