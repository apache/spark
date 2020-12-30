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

import org.apache.spark.sql.catalyst.plans.logical.{HintInfo, Join, LogicalPlan, NO_BROADCAST_HASH}
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * This optimization rule detects a join child that has a high ratio of empty partitions and
 * adds a no-broadcast-hash-join hint to avoid it being broadcast.
 */
object DemoteBroadcastHashJoin extends Rule[LogicalPlan] {

  private def shouldDemote(plan: LogicalPlan): Boolean = plan match {
    case LogicalQueryStage(_, stage: ShuffleQueryStageExec) if stage.resultOption.get().isDefined
      && stage.mapStats.isDefined =>
      val mapStats = stage.mapStats.get
      val partitionCnt = mapStats.bytesByPartitionId.length
      val nonZeroCnt = mapStats.bytesByPartitionId.count(_ > 0)
      partitionCnt > 0 && nonZeroCnt > 0 &&
        (nonZeroCnt * 1.0 / partitionCnt) < conf.nonEmptyPartitionRatioForBroadcastJoin
    case _ => false
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformDown {
    case j @ Join(left, right, _, _, hint) =>
      var newHint = hint
      if (!hint.leftHint.exists(_.strategy.isDefined) && shouldDemote(left)) {
        newHint = newHint.copy(leftHint =
          Some(hint.leftHint.getOrElse(HintInfo()).copy(strategy = Some(NO_BROADCAST_HASH))))
      }
      if (!hint.rightHint.exists(_.strategy.isDefined) && shouldDemote(right)) {
        newHint = newHint.copy(rightHint =
          Some(hint.rightHint.getOrElse(HintInfo()).copy(strategy = Some(NO_BROADCAST_HASH))))
      }
      if (newHint.ne(hint)) {
        j.copy(hint = newHint)
      } else {
        j
      }
  }
}
