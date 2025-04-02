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

import org.apache.spark.sql.catalyst.expressions.{AliasHelper, AttributeSet, ExpressionSet}
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
        if agg.groupOnly && child.distinctKeys.exists(_.subsetOf(ExpressionSet(groupingExps))) =>
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

    upperHasNoDuplicateSensitiveAgg && upperRefsOnlyDeterministicNonAgg
  }

  private def isDuplicateSensitive(ae: AggregateExpression): Boolean = {
    !ae.isDistinct && !EliminateDistinct.isDuplicateAgnostic(ae.aggregateFunction)
  }
}
