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

import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{AGGREGATE, AGGREGATE_EXPRESSION, UNRESOLVED_STAR}

/**
 * Resolve the star in the group by statement in the following SQL pattern:
 *  `select col1, col2, agg_expr(...) from table group by *`.
 *
 * The star is expanded to include all non-aggregate columns in the select clause.
 */
object ResolveGroupByStar extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUpWithPruning(
    _.containsAllPatterns(UNRESOLVED_STAR, AGGREGATE), ruleId) {
    // Match a group by with a single unresolved star
    case a: Aggregate if a.groupingExpressions == UnresolvedStar(None) :: Nil =>
      // Only makes sense to do the rewrite once all the aggregate expressions have been resolved.
      // Otherwise, we might incorrectly pull an actual aggregate expression over to the grouping
      // expression list (because we don't know they would be aggregate expressions until resolved).
      if (a.aggregateExpressions.forall(_.resolved)) {
        val groupingExprs =
          a.aggregateExpressions.filter(!_.containsPattern(AGGREGATE_EXPRESSION))
        a.copy(groupingExpressions = groupingExprs)
      } else {
        a
      }
  }
}
