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

import org.apache.spark.MapOutputStatistics
import org.apache.spark.sql.catalyst.optimizer.JoinSelectionHelper
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.{LeftAnti, LeftOuter, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical.{HintInfo, Join, JoinStrategyHint, LogicalPlan, NO_BROADCAST_HASH, PREFER_SHUFFLE_HASH, SHUFFLE_HASH}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf

/**
 * This optimization rule includes three join selection:
 *   1. detects a join child that has a high ratio of empty partitions and adds a
 *      NO_BROADCAST_HASH hint to avoid it being broadcast, as shuffle join is faster in this case:
 *      many tasks complete immediately since one join side is empty.
 *   2. detects a join child that every partition size is less than local map threshold and adds a
 *      PREFER_SHUFFLE_HASH hint to encourage being shuffle hash join instead of sort merge join.
 *   3. if a join satisfies both NO_BROADCAST_HASH and PREFER_SHUFFLE_HASH,
 *      then add a SHUFFLE_HASH hint.
 */
object DynamicJoinSelection extends Rule[LogicalPlan] with JoinSelectionHelper {

  private def hasManyEmptyPartitions(mapStats: MapOutputStatistics): Boolean = {
    val partitionCnt = mapStats.bytesByPartitionId.length
    val nonZeroCnt = mapStats.bytesByPartitionId.count(_ > 0)
    partitionCnt > 0 && nonZeroCnt > 0 &&
      (nonZeroCnt * 1.0 / partitionCnt) < conf.nonEmptyPartitionRatioForBroadcastJoin
  }

  private def preferShuffledHashJoin(mapStats: MapOutputStatistics): Boolean = {
    val maxShuffledHashJoinLocalMapThreshold =
      conf.getConf(SQLConf.ADAPTIVE_MAX_SHUFFLE_HASH_JOIN_LOCAL_MAP_THRESHOLD)
    val advisoryPartitionSize = conf.getConf(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES)
    advisoryPartitionSize <= maxShuffledHashJoinLocalMapThreshold &&
      mapStats.bytesByPartitionId.forall(_ <= maxShuffledHashJoinLocalMapThreshold)
  }

  private def selectJoinStrategy(
      join: Join,
      isLeft: Boolean): Option[JoinStrategyHint] = {
    val plan = if (isLeft) join.left else join.right
    plan match {
      case LogicalQueryStage(_, stage: ShuffleQueryStageExec) if stage.isMaterialized
        && stage.mapStats.isDefined =>

        val manyEmptyInPlan = hasManyEmptyPartitions(stage.mapStats.get)
        val canBroadcastPlan = (isLeft && canBuildBroadcastLeft(join.joinType)) ||
          (!isLeft && canBuildBroadcastRight(join.joinType))
        val manyEmptyInOther = (if (isLeft) join.right else join.left) match {
          case LogicalQueryStage(_, stage: ShuffleQueryStageExec) if stage.isMaterialized
            && stage.mapStats.isDefined => hasManyEmptyPartitions(stage.mapStats.get)
          case _ => false
        }

        val demoteBroadcastHash = if (manyEmptyInPlan && canBroadcastPlan) {
          join.joinType match {
            // don't demote BHJ since you cannot short circuit local join if inner (null-filled)
            // side is empty
            case LeftOuter | RightOuter | LeftAnti => false
            case _ => true
          }
        } else if (manyEmptyInOther && canBroadcastPlan) {
          // for example, LOJ, !isLeft but it's the LHS that has many empty partitions if we
          // proceed with shuffle.  But if we proceed with BHJ, the OptimizeShuffleWithLocalRead
          // will assemble partitions as they were before the shuffle and that may no longer have
          // many empty partitions and thus cannot short-circuit local join
          join.joinType match {
            case LeftOuter | RightOuter | LeftAnti => true
            case _ => false
          }
        } else {
          false
        }

        val preferShuffleHash = preferShuffledHashJoin(stage.mapStats.get)
        if (demoteBroadcastHash && preferShuffleHash) {
          Some(SHUFFLE_HASH)
        } else if (demoteBroadcastHash) {
          Some(NO_BROADCAST_HASH)
        } else if (preferShuffleHash) {
          Some(PREFER_SHUFFLE_HASH)
        } else {
          None
        }

      case _ => None
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformDown {
    case j @ ExtractEquiJoinKeys(_, _, _, _, _, _, _, hint) =>
      var newHint = hint
      if (!hint.leftHint.exists(_.strategy.isDefined)) {
        selectJoinStrategy(j, true).foreach { strategy =>
          newHint = newHint.copy(leftHint =
            Some(hint.leftHint.getOrElse(HintInfo()).copy(strategy = Some(strategy))))
        }
      }
      if (!hint.rightHint.exists(_.strategy.isDefined)) {
        selectJoinStrategy(j, false).foreach { strategy =>
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
