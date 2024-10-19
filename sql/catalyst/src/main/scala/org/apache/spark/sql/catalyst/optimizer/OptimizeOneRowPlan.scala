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

import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.internal.SQLConf

/**
 * The rule is applied both normal and AQE Optimizer. It optimizes plan using max rows:
 *   - if the max rows of the child of sort is less than or equal to 1, remove the sort
 *   - if the max rows per partition of the child of local sort is less than or equal to 1,
 *     remove the local sort
 *   - if the max rows of the child of aggregate is less than or equal to 1 and its child and
 *     it's grouping only(include the rewritten distinct plan), convert aggregate to project
 *   - if the max rows of the child of aggregate is less than or equal to 1,
 *     set distinct to false in all aggregate expression
 *
 * Note: the rule should not be applied to streaming source, since the number of rows it sees is
 * just for current microbatch. It does not mean the streaming source will ever produce max 1
 * rows during lifetime of the query. Suppose the case: the streaming query has a case where
 * batch 0 runs with empty data in streaming source A which triggers the rule with Aggregate,
 * and batch 1 runs with several data in streaming source A which no longer trigger the rule.
 * In the above scenario, this could fail the query as stateful operator is expected to be planned
 * for every batches whereas here it is planned "selectively".
 */
object OptimizeOneRowPlan extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    val enableForStreaming = conf.getConf(SQLConf.STREAMING_OPTIMIZE_ONE_ROW_PLAN_ENABLED)

    plan.transformUpWithPruning(_.containsAnyPattern(SORT, AGGREGATE), ruleId) {
      case Sort(_, _, child) if child.maxRows.exists(_ <= 1L) &&
        isChildEligible(child, enableForStreaming) => child
      case Sort(_, false, child) if child.maxRowsPerPartition.exists(_ <= 1L) &&
        isChildEligible(child, enableForStreaming) => child
      case agg @ Aggregate(_, _, child) if agg.groupOnly && child.maxRows.exists(_ <= 1L) &&
        isChildEligible(child, enableForStreaming) =>
        Project(agg.aggregateExpressions, child)
      case agg: Aggregate if agg.child.maxRows.exists(_ <= 1L) &&
        isChildEligible(agg.child, enableForStreaming) =>
        agg.transformExpressions {
          case aggExpr: AggregateExpression if aggExpr.isDistinct =>
            aggExpr.copy(isDistinct = false)
        }
    }
  }

  private def isChildEligible(child: LogicalPlan, enableForStreaming: Boolean): Boolean = {
    enableForStreaming || !child.isStreaming
  }
}
