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

import org.apache.spark.sql.catalyst.expressions.{BindReferences, Expression}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.planning.{ExtractEquiJoinKeys, ExtractRangeJoinKeys, ExtractSingleColumnNullAwareAntiJoin, RangeJoin}
import org.apache.spark.sql.catalyst.plans.{JoinType, LeftAnti}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastPartitioning, IdentityBroadcastMode}
import org.apache.spark.sql.classic.Strategy
import org.apache.spark.sql.execution.{joins, SparkPlan}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, BroadcastRangeJoinExec, HashedRelationBroadcastMode, RangeBroadcastMode}

/**
 * Strategy for plans containing [[LogicalQueryStage]] nodes:
 * 1. Transforms [[LogicalQueryStage]] to its corresponding physical plan that is either being
 *    executed or has already completed execution.
 * 2. Transforms [[Join]] which has one child relation already planned and executed as a
 *    [[BroadcastQueryStageExec]]. This is to prevent reversing a broadcast stage into a shuffle
 *    stage in case of the larger join child relation finishes before the smaller relation. Note
 *    that this rule needs to be applied before regular join strategies.
 */
object LogicalQueryStageStrategy extends Strategy {

  private def isBroadcastStageWithHashedBroadcastMode(
      plan: LogicalPlan,
      isNullAware: Boolean): Boolean = plan match {
    case LogicalQueryStage(_, bqs: BroadcastQueryStageExec) =>
      bqs.broadcast.outputPartitioning match {
        case BroadcastPartitioning(HashedRelationBroadcastMode(_, stageIsNullAware)) =>
          stageIsNullAware == isNullAware
        case _ => false
      }
    case _ => false
  }

  private def isBroadcastStageWithIdentityBroadcastMode(plan: LogicalPlan): Boolean = plan match {
    // A range-join broadcast stage reports RangeBroadcastMode (not IdentityBroadcastMode), so
    // it is naturally excluded here and routed to BroadcastRangeJoinExec by the
    // `RangeBroadcastJoinStage` case below instead of BroadcastNestedLoopJoinExec.
    case LogicalQueryStage(_, bqs: BroadcastQueryStageExec) =>
      bqs.broadcast.outputPartitioning match {
        case BroadcastPartitioning(IdentityBroadcastMode) => true
        case _ => false
      }
    case _ => false
  }

  /**
   * Extracts the [[RangeBroadcastMode]] carried by a broadcast query stage, if any.
   * Keys and the join condition come from the logical join. The mode identifies
   * the already-built range index.
   */
  private def broadcastRangeMode(plan: LogicalPlan): Option[RangeBroadcastMode] = plan match {
    case LogicalQueryStage(_, bqs: BroadcastQueryStageExec) =>
      bqs.broadcast.outputPartitioning match {
        case BroadcastPartitioning(m: RangeBroadcastMode) => Some(m)
        case _ => None
      }
    case _ => None
  }

  /**
   * A range-predicate join whose left or right child is already a range-broadcast
   * query stage. `unapply` looks up the mode once so `apply` does not search
   * for both a guard and the constructor.
   */
  private case class RangeBroadcastJoinStage(
      left: LogicalPlan,
      right: LogicalPlan,
      joinType: JoinType,
      buildSide: BuildSide,
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      condition: Expression,
      rangeJoin: RangeJoin)

  private object RangeBroadcastJoinStage {
    def unapply(plan: LogicalPlan): Option[RangeBroadcastJoinStage] = plan match {
      case j @ Join(_, _, _, Some(cond), _) =>
        j match {
          case ExtractRangeJoinKeys(left, right, leftKeys, rightKeys, joinType, rangeJoin) =>
            def fromMode(
                mode: RangeBroadcastMode,
                buildSide: BuildSide): Option[RangeBroadcastJoinStage] = {
              if (!mode.indexKind.matches(rangeJoin) ||
                  !BroadcastRangeJoinExec.supports(joinType, buildSide)) {
                None
              } else {
                val buildPlan = buildSide match {
                  case BuildLeft => left
                  case BuildRight => right
                }
                val buildKeys = buildSide match {
                  case BuildLeft => leftKeys
                  case BuildRight => rightKeys
                }
                // Same index kind is not enough: a point index of column y must not
                // serve a join on column z. Mode equality ignores nullability, and
                // BroadcastPartitioning.satisfies uses the same ==, so a stage
                // whose nullability AQE rewrote still satisfies the distribution.
                val bound = BindReferences.bindReferences(buildKeys, buildPlan.output)
                if (mode == RangeBroadcastMode(bound, mode.indexKind)) {
                  Some(RangeBroadcastJoinStage(
                    left, right, joinType, buildSide, leftKeys, rightKeys, cond, rangeJoin))
                } else {
                  None
                }
              }
            }
            broadcastRangeMode(left).flatMap(fromMode(_, BuildLeft))
              .orElse(broadcastRangeMode(right).flatMap(fromMode(_, BuildRight)))
          case _ => None
        }
      case _ => None
    }
  }

  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, otherCondition, _,
          left, right, hint)
        if isBroadcastStageWithHashedBroadcastMode(left, isNullAware = false) ||
            isBroadcastStageWithHashedBroadcastMode(right, isNullAware = false) =>
      val buildSide =
        if (isBroadcastStageWithHashedBroadcastMode(left, isNullAware = false)) {
          BuildLeft
        } else {
          BuildRight
        }
      Seq(BroadcastHashJoinExec(
        leftKeys, rightKeys, joinType, buildSide, otherCondition, planLater(left),
        planLater(right)))

    case j @ ExtractSingleColumnNullAwareAntiJoin(leftKeys, rightKeys)
        if isBroadcastStageWithHashedBroadcastMode(j.right, isNullAware = true) =>
      Seq(joins.BroadcastHashJoinExec(leftKeys, rightKeys, LeftAnti, BuildRight,
        None, planLater(j.left), planLater(j.right), isNullAwareAntiJoin = true))

    case RangeBroadcastJoinStage(stage) =>
      BroadcastRangeJoinExec(
        stage.leftKeys, stage.rightKeys, stage.joinType, stage.buildSide,
        Some(stage.condition), stage.rangeJoin,
        planLater(stage.left), planLater(stage.right)) :: Nil

    case Join(left, right, joinType, condition, _)
        if isBroadcastStageWithIdentityBroadcastMode(left) ||
            isBroadcastStageWithIdentityBroadcastMode(right) =>
      val buildSide =
        if (isBroadcastStageWithIdentityBroadcastMode(left)) BuildLeft else BuildRight
      BroadcastNestedLoopJoinExec(
        planLater(left), planLater(right), buildSide, joinType, condition) :: Nil

    case q: LogicalQueryStage =>
      q.physicalPlan :: Nil

    case _ => Nil
  }
}
