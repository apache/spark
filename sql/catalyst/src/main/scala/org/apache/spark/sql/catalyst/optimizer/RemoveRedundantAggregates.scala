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

import org.apache.spark.sql.catalyst.expressions.{AliasHelper, Attribute, AttributeSet, Expression, ExpressionSet}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
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
    case upper @ Aggregate(_, _, lower: Aggregate, _) if isLowerRedundant(upper, lower) =>
      val projectList = lower.aggregateExpressions.filter(upper.references.contains(_))
      upper.copy(child = Project(projectList, lower.child))
    case agg @ Aggregate(groupingExps, _, child, _)
        if agg.groupOnly && child.distinctKeys.exists(_.subsetOf(ExpressionSet(groupingExps))) &&
          !NormalizeFloatingNumbers.readsNormalizedKeys(agg, agg.aggregateExpressions) =>
      Project(agg.aggregateExpressions, child)
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

    // The lower aggregation outputs its float/double grouping keys normalized, so it is not
    // redundant if the upper aggregation would read them raw without it: through an output computed
    // from a key, inside an aggregate function or a computed grouping expression, or as a grouping
    // key of an upper aggregation that does not normalize its keys. A key the upper aggregation
    // groups by directly is normalized by the upper aggregation itself.
    lazy val upperReadsNormalizedKeys = {
      val keys = NormalizeFloatingNumbers.normalizedKeys(lower)
      val (keyOutputs, otherOutputs) = lower.aggregateExpressions
        .filter(upper.references.contains(_))
        .partition(output => keys.exists(_.semanticEquals(trimAliases(output))))
      val keyAttributes = AttributeSet(keyOutputs.map(_.toAttribute))
      def readsKey(e: Expression): Boolean = e.references.intersect(keyAttributes).nonEmpty
      NormalizeFloatingNumbers.readsNormalizedKeys(lower, otherOutputs) ||
        keyAttributes.nonEmpty && (
          !NormalizeFloatingNumbers.outputsNormalizedKeys(upper) ||
          upper.groupingExpressions.exists(g => !g.isInstanceOf[Attribute] && readsKey(g)) ||
          upper.aggregateExpressions.exists(_.exists {
            case ae: AggregateExpression => readsKey(ae)
            case _ => false
          }))
    }

    upperHasNoDuplicateSensitiveAgg && upperRefsOnlyDeterministicNonAgg && !lowerIsGlobalAgg &&
      !upperReadsNormalizedKeys
  }

  private def isDuplicateSensitive(ae: AggregateExpression): Boolean = {
    !ae.isDistinct && !EliminateDistinct.isDuplicateAgnostic(ae.aggregateFunction)
  }
}
