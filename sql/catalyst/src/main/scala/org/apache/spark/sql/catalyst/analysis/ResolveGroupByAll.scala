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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{AGGREGATE, UNRESOLVED_ATTRIBUTE}

/**
 * Resolve "group by all" in the following SQL pattern:
 *  `select col1, col2, agg_expr(...) from table group by all`.
 *
 * The all is expanded to include all non-aggregate columns in the select clause.
 */
object ResolveGroupByAll extends Rule[LogicalPlan] {

  val ALL = "ALL"

  /**
   * Returns true iff this is a GROUP BY ALL aggregate. i.e. an Aggregate expression that has
   * a single unresolved all grouping expression.
   */
  private def matchToken(a: Aggregate): Boolean = {
    if (a.groupingExpressions.size != 1) {
      return false
    }
    a.groupingExpressions.head match {
      case a: UnresolvedAttribute => a.equalsIgnoreCase(ALL)
      case _ => false
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUpWithPruning(
    _.containsAllPatterns(UNRESOLVED_ATTRIBUTE, AGGREGATE), ruleId) {
    case a: Aggregate
      if a.child.resolved && a.aggregateExpressions.forall(_.resolved) && matchToken(a) =>
      // Only makes sense to do the rewrite once all the aggregate expressions have been resolved.
      // Otherwise, we might incorrectly pull an actual aggregate expression over to the grouping
      // expression list (because we don't know they would be aggregate expressions until resolved).
      val groupingExprs = a.aggregateExpressions.filter(!_.exists(AggregateExpression.isAggregate))

      // If the grouping exprs are empty, this could either be (1) a valid global aggregate, or
      // (2) we simply fail to infer the grouping columns. As an example, in "i + sum(j)", we will
      // not automatically infer the grouping column to be "i".
      if (groupingExprs.isEmpty && a.aggregateExpressions.exists(containsAttribute)) {
        // Case (2): don't replace the ALL. We will eventually tell the user in checkAnalysis
        // that we cannot resolve the all in group by.
        a
      } else {
        // Case (1): this is a valid global aggregate.
        a.copy(groupingExpressions = groupingExprs)
      }
  }

  /**
   * Returns true if the expression includes an Attribute outside the aggregate expression part.
   * For example:
   *  "i" -> true
   *  "i + 2" -> true
   *  "i + sum(j)" -> true
   *  "sum(j)" -> false
   *  "sum(j) / 2" -> false
   */
  private def containsAttribute(expr: Expression): Boolean = expr match {
    case _ if AggregateExpression.isAggregate(expr) =>
      // Don't recurse into AggregateExpressions
      false
    case _: Attribute =>
      true
    case e =>
      e.children.exists(containsAttribute)
  }

  /**
   * A check to be used in [[CheckAnalysis]] to see if we have any unresolved group by at the
   * end of analysis, so we can tell users that we fail to infer the grouping columns.
   */
  def checkAnalysis(operator: LogicalPlan): Unit = operator match {
    case a: Aggregate if matchToken(a) =>
      if (a.aggregateExpressions.exists(_.exists(_.isInstanceOf[Attribute]))) {
        operator.failAnalysis(
          errorClass = "UNRESOLVED_ALL_IN_GROUP_BY",
          messageParameters = Map.empty)
      }
    case _ =>
  }
}
