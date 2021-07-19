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

import org.apache.spark.sql.catalyst.plans.{LeftAnti, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Join, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{AGGREGATE, LEFT_SEMI_OR_ANTI_JOIN}

/**
 * Remove the redundant aggregation from left semi/anti join if the same aggregation has already
 * been done on left side.
 */
object RemoveRedundantAggregatesInLeftSemiAntiJoin extends Rule[LogicalPlan] {
  // Transform down to remove more Aggregates.
  def apply(plan: LogicalPlan): LogicalPlan = plan.transformDownWithPruning(
    _.containsAllPatterns(AGGREGATE, LEFT_SEMI_OR_ANTI_JOIN), ruleId) {
    case agg @ Aggregate(grouping, aggExps, j @ Join(left: Aggregate, _, LeftSemi | LeftAnti, _, _))
      if agg.groupOnly && left.groupOnly &&
        aggExps.forall(e => left.aggregateExpressions.exists(_.semanticEquals(e))) &&
        grouping.length == left.groupingExpressions.length &&
        grouping.zip(left.groupingExpressions).forall(e => e._1.semanticEquals(e._2)) =>
      j
  }
}
