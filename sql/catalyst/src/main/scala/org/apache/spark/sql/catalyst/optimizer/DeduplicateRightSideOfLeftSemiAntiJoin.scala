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

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.LEFT_SEMI_OR_ANTI_JOIN

/**
 * Partial deduplicate the right side of left semi/anti join.
 *
 * This rule should be applied after ColumnPruning.
 */
object DeduplicateRightSideOfLeftSemiAntiJoin extends Rule[LogicalPlan] with JoinSelectionHelper {

  /**
   * It has benefit in the following cases:
   * 1. can reduce shuffle data
   * 2. user set spark.sql.optimizer.partialAggregationOptimization.benefitRatio to 1.0
   */
  def pushPartialAggHasBenefit(
      join: Join,
      groupingExpressions: Seq[Expression],
      plan: LogicalPlan): Boolean = {
    val benefitRatio = conf.partialAggregationOptimizationBenefitRatio
    // To reduce shuffle data. For example: TPC-DS q14a, q14b
    if (!canPlanAsBroadcastHashJoin(join, conf) &&
      Aggregate(groupingExpressions, Nil, plan).stats.rowCount
        .exists(_.toDouble / plan.stats.rowCount.getOrElse(BigInt(1)).toDouble <= benefitRatio)) {
      true
    } else {
      1.0 <= benefitRatio
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.partialAggregationOptimizationEnabled) {
      plan
    } else {
      plan.transformWithPruning(_.containsPattern(LEFT_SEMI_OR_ANTI_JOIN), ruleId) {
        case j @ Join(_, _: AggregateBase, _, _, _) =>
          j
        case j @ Join(_, right, LeftSemiOrAnti(_), _, _)
            if pushPartialAggHasBenefit(j, right.output, right) =>
          j.copy(right = PartialAggregate(right.output, right.output, right))
      }
    }
  }
}
