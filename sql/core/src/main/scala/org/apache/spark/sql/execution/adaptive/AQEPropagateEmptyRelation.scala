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
    super.isEmpty(plan) || getRowCount(plan).contains(0)

  override protected def nonEmpty(plan: LogicalPlan): Boolean =
    super.nonEmpty(plan) || getRowCount(plan).exists(_ > 0)

  private def getRowCount(plan: LogicalPlan): Option[BigInt] = plan match {
    case LogicalQueryStage(_, stage: QueryStageExec) if stage.resultOption.get().isDefined =>
      stage.getRuntimeStatistics.rowCount
    case _ => None
  }

  private def isRelationWithAllNullKeys(plan: LogicalPlan): Boolean = plan match {
    case LogicalQueryStage(_, stage: BroadcastQueryStageExec)
      if stage.resultOption.get().isDefined =>
      stage.broadcast.relationFuture.get().value == HashedRelationWithAllNullKeys
    case _ => false
  }

  private def eliminateSingleColumnNullAwareAntiJoin: PartialFunction[LogicalPlan, LogicalPlan] = {
    case j @ ExtractSingleColumnNullAwareAntiJoin(_, _) if isRelationWithAllNullKeys(j.right) =>
      empty(j)
  }

  // TODO we need use transformUpWithPruning instead of transformUp
  def apply(plan: LogicalPlan): LogicalPlan = plan.transformUp {
    eliminateSingleColumnNullAwareAntiJoin.orElse(commonApplyFunc)
  }
}
