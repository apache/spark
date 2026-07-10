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

import scala.annotation.tailrec

import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide, JoinSelectionHelper}
import org.apache.spark.sql.catalyst.plans.LeftExistence
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{FilterExec, ProjectExec, SortExec, SparkPlan}
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, EnsureRequirements}
import org.apache.spark.sql.execution.joins.{BaseJoinExec, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.execution.window.{WindowExecBase, WindowGroupLimitExec}

/**
 * Converts a [[SortMergeJoinExec]] into a [[ShuffledHashJoinExec]] during adaptive execution when
 * a build side's materialized shuffle statistics show it is small enough for a local hash map.
 * Unlike [[DynamicJoinSelection]], this runs on the physical plan, so it can reach the input
 * shuffle through operators (aggregate, project, filter, window, etc...) sitting above it.
 *
 * The swap is shuffle-free since both joins are `ShuffledJoin`s with the same distribution and
 * partitioning; only the child sorts become unnecessary. As a shuffled hash join loses the sort
 * merge join's output ordering, [[EnsureRequirements]] is re-run to restore any ordering an
 * ancestor still needs, and AQE's [[CostEvaluator]] decides whether to adopt the converted plan.
 */
case class ReplaceSortMergeJoinToShuffledHashJoin(ensureRequirements: EnsureRequirements)
  extends Rule[SparkPlan] with JoinSelectionHelper {

  /**
   * Chooses the build side for the shuffled hash join. A side is eligible only if it is allowed
   * as a build side for this join type and its input shuffle is small enough to build a local
   * hash map. When both sides are eligible, the smaller one (by total shuffle bytes) is chosen.
   */
  private def selectBuildSide(
      smj: SortMergeJoinExec,
      left: ShuffleQueryStageExec,
      right: ShuffleQueryStageExec): Option[BuildSide] = {
    val canBuildLeft = canBuildShuffledHashJoinLeft(smj.joinType) &&
      preferShuffledHashJoin(left.mapStats.get)
    val canBuildRight = canBuildShuffledHashJoinRight(smj.joinType) &&
      preferShuffledHashJoin(right.mapStats.get)
    if (canBuildLeft && canBuildRight) {
      if (left.mapStats.get.bytesByPartitionId.sum < right.mapStats.get.bytesByPartitionId.sum) {
        Some(BuildLeft)
      } else {
        Some(BuildRight)
      }
    } else if (canBuildLeft) {
      Some(BuildLeft)
    } else if (canBuildRight) {
      Some(BuildRight)
    } else {
      None
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.convertSortMergeJoinToShuffledHashJoinEnabled) {
      return plan
    }
    val optimizedPlan = plan.transformUp {
      case smj @ SortMergeJoinExec(leftKeys, rightKeys, joinType, condition,
        ExtractShuffleStage(left), ExtractShuffleStage(right), false) =>
        selectBuildSide(smj, left, right) match {
          case Some(buildSide) =>
            ShuffledHashJoinExec(leftKeys, rightKeys, joinType, buildSide, condition,
              stripSort(smj.left), stripSort(smj.right))
          case None => smj
        }
    }
    if (optimizedPlan.fastEquals(plan)) {
      plan
    } else {
      // A shuffled hash join does not preserve the sort merge join's output ordering. Re-run
      // EnsureRequirements so any ordering an ancestor still needs is re-established, keeping the
      // plan valid. AQE's CostEvaluator then decides between this plan and the current one.
      ensureRequirements.apply(optimizedPlan)
    }
  }

  /**
   * Drops a top-level [[SortExec]] since a shuffled hash join does not require sorted input;
   * [[RemoveRedundantSorts]] cleans up any remaining redundant sorts afterwards.
   */
  private def stripSort(plan: SparkPlan): SparkPlan = plan match {
    case s: SortExec => s.child
    case other => other
  }

  /**
   * Finds a join child's input shuffle, looking through the [[SortExec]] and other non-shuffle,
   * non-data-inflating operators (aggregate, project, filter, window, left-existence join) above
   * it. Descent stops at the first [[ShuffleQueryStageExec]], which is thus guaranteed to be the
   * join's own input shuffle whose statistics bound (or, for a reducing aggregate, upper-bound)
   * the build side. The stage must be materialized with stats and originate from
   * [[EnsureRequirements]], so swapping the join type does not change the shuffle.
   */
  object ExtractShuffleStage {
    def unapply(plan: SparkPlan): Option[ShuffleQueryStageExec] = findShuffleStage(plan)

    @tailrec
    private def findShuffleStage(plan: SparkPlan): Option[ShuffleQueryStageExec] = plan match {
      case s: ShuffleQueryStageExec if s.isMaterialized && s.mapStats.isDefined &&
        s.shuffle.shuffleOrigin == ENSURE_REQUIREMENTS => Some(s)
      case _: ProjectExec | _: FilterExec | _: SortExec | _: BaseAggregateExec | _: WindowExecBase |
          _: WindowGroupLimitExec =>
        findShuffleStage(plan.children.head)
      case join: BaseJoinExec =>
        join.joinType match {
          case LeftExistence(_) => findShuffleStage(join.left)
          case _ => None
        }
      case _ => None
    }
  }
}
