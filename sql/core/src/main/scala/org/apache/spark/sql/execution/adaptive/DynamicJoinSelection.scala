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

import org.apache.spark.sql.catalyst.plans.logical.{HintInfo, Join, JoinStrategyHint, JoinStrategyHintCollection, LogicalPlan, NO_BROADCAST_HASH, PREFER_SHUFFLE_HASH}
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * This optimization rule incluede two join selection:
 *   1. detects a join child that has a high ratio of empty partitions and adds a
 *      no_broadcast_hash_join hint to avoid it being broadcast.
 *   2. detects a join child that every partition size less than local map threshold and adds a
 *      prefer_shuffled_hash hint to encourage being shuffle hash join instead of sort merge join.
 */
object DynamicJoinSelection extends Rule[LogicalPlan] {

  private def shouldDemoteBroadcastHashJoin(stage: ShuffleQueryStageExec): Boolean = {
    val mapStats = stage.mapStats.get
    val partitionCnt = mapStats.bytesByPartitionId.length
    val nonZeroCnt = mapStats.bytesByPartitionId.count(_ > 0)
    partitionCnt > 0 && nonZeroCnt > 0 &&
      (nonZeroCnt * 1.0 / partitionCnt) < conf.nonEmptyPartitionRatioForBroadcastJoin
  }

  private def preferShuffledHashJoin(stage: ShuffleQueryStageExec): Boolean = {
    val localMapThreshold = conf.shuffleHashJoinLocalMapThreshold
    stage.mapStats.get.bytesByPartitionId.forall(_ <= localMapThreshold)
  }

  private def selectJoinStrategy(plan: LogicalPlan): Option[JoinStrategyHint] = plan match {
    case LogicalQueryStage(_, stage: ShuffleQueryStageExec) if stage.resultOption.get().isDefined
      && stage.mapStats.isDefined =>
      var strategies = Seq[JoinStrategyHint]()
      if (shouldDemoteBroadcastHashJoin(stage)) {
        strategies = strategies ++ Seq(NO_BROADCAST_HASH)
      }
      if (preferShuffledHashJoin(stage)) {
        strategies = strategies ++ Seq(PREFER_SHUFFLE_HASH)
      }
      Some(JoinStrategyHintCollection(strategies))

    case _ => None
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformDown {
    case j @ Join(left, right, _, _, hint) =>
      var newHint = hint
      if (!hint.leftHint.exists(_.strategy.isDefined)) {
        selectJoinStrategy(left).foreach { strategy =>
          newHint = newHint.copy(leftHint =
            Some(hint.leftHint.getOrElse(HintInfo()).copy(strategy = Some(strategy))))
        }
      }
      if (!hint.rightHint.exists(_.strategy.isDefined)) {
        selectJoinStrategy(right).foreach { strategy =>
          newHint = newHint.copy(rightHint =
            Some(hint.rightHint.getOrElse(HintInfo()).copy(strategy = Some(strategy))))
        }
      }
      if (newHint.ne(hint)) {
        j.copy(hint = newHint)
      } else {
        j
      }
  }
}
