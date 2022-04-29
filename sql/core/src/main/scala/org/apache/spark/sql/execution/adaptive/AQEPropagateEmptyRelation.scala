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

package org.apache.spark.sql.execution.adaptive

import org.apache.spark.sql.catalyst.optimizer.PropagateEmptyRelationBase
import org.apache.spark.sql.catalyst.planning.ExtractSingleColumnNullAwareAntiJoin
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.TreePattern.{LOCAL_RELATION, LOGICAL_QUERY_STAGE, TRUE_OR_FALSE_LITERAL}
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.joins.HashedRelationWithAllNullKeys

/**
 * This rule runs in the AQE optimizer and optimizes more cases
 * compared to [[PropagateEmptyRelationBase]]:
 * 1. Join is single column NULL-aware anti join (NAAJ)
 *    Broadcasted [[HashedRelation]] is [[HashedRelationWithAllNullKeys]]. Eliminate join to an
 *    empty [[LocalRelation]].
 */
object AQEPropagateEmptyRelation extends PropagateEmptyRelationBase {
  override protected def isEmpty(plan: LogicalPlan): Boolean =
    super.isEmpty(plan) || getEstimatedRowCount(plan).contains(0)

  override protected def nonEmpty(plan: LogicalPlan): Boolean =
    super.nonEmpty(plan) || getEstimatedRowCount(plan).exists(_ > 0)

  // The returned value follows:
  //   - 0 means the plan must produce 0 row
  //   - positive value means an estimated row count which can be over-estimated
  //   - none means the plan has not materialized or the plan can not be estimated
  private def getEstimatedRowCount(plan: LogicalPlan): Option[BigInt] = plan match {
    case LogicalQueryStage(_, stage: QueryStageExec) if stage.isMaterialized =>
      stage.getRuntimeStatistics.rowCount

    case LogicalQueryStage(_, agg: BaseAggregateExec) if agg.groupingExpressions.nonEmpty &&
      agg.child.isInstanceOf[QueryStageExec] =>
      val stage = agg.child.asInstanceOf[QueryStageExec]
      if (stage.isMaterialized) {
        stage.getRuntimeStatistics.rowCount
      } else {
        None
      }

    case _ => None
  }

  private def isRelationWithAllNullKeys(plan: LogicalPlan): Boolean = plan match {
    case LogicalQueryStage(_, stage: BroadcastQueryStageExec) if stage.isMaterialized =>
      stage.broadcast.relationFuture.get().value == HashedRelationWithAllNullKeys
    case _ => false
  }

  private def eliminateSingleColumnNullAwareAntiJoin: PartialFunction[LogicalPlan, LogicalPlan] = {
    case j @ ExtractSingleColumnNullAwareAntiJoin(_, _) if isRelationWithAllNullKeys(j.right) =>
      empty(j)
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformUpWithPruning(
    // LOCAL_RELATION and TRUE_OR_FALSE_LITERAL pattern are matched at
    // `PropagateEmptyRelationBase.commonApplyFunc`
    // LOGICAL_QUERY_STAGE pattern is matched at `PropagateEmptyRelationBase.commonApplyFunc`
    // and `AQEPropagateEmptyRelation.eliminateSingleColumnNullAwareAntiJoin`
    // Note that, We can not specify ruleId here since the LogicalQueryStage is not immutable.
    _.containsAnyPattern(LOGICAL_QUERY_STAGE, LOCAL_RELATION, TRUE_OR_FALSE_LITERAL)) {
    eliminateSingleColumnNullAwareAntiJoin.orElse(commonApplyFunc)
  }
}
