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

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.planning.{ExtractEquiJoinKeys, ExtractSingleColumnNullAwareAntiJoin}
import org.apache.spark.sql.catalyst.plans.LeftAnti
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.execution.{joins, SparkPlan}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec}

/**
 * Strategy for plans containing [[LogicalQueryStage]] nodes:
 * 1. Transforms [[LogicalQueryStage]] to its corresponding physical plan that is either being
 *    executed or has already completed execution.
 * 2. Transforms [[Join]] which has one child relation already planned and executed as a
 *    [[BroadcastQueryStageExec]]. This is to prevent reversing a broadcast stage into a shuffle
 *    stage in case of the larger join child relation finishes before the smaller relation. Note
 *    that this rule needs to applied before regular join strategies.
 */
object LogicalQueryStageStrategy extends Strategy with PredicateHelper {

  private def isBroadcastStage(plan: LogicalPlan): Boolean = plan match {
    case LogicalQueryStage(_, _: BroadcastQueryStageExec) => true
    case _ => false
  }

  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right, hint)
        if isBroadcastStage(left) || isBroadcastStage(right) =>
      val buildSide = if (isBroadcastStage(left)) BuildLeft else BuildRight
      Seq(BroadcastHashJoinExec(
        leftKeys, rightKeys, joinType, buildSide, condition, planLater(left), planLater(right)))

    case j @ ExtractSingleColumnNullAwareAntiJoin(leftKeys, rightKeys)
        if isBroadcastStage(j.right) =>
      Seq(joins.BroadcastHashJoinExec(leftKeys, rightKeys, LeftAnti, BuildRight,
        None, planLater(j.left), planLater(j.right), isNullAwareAntiJoin = true))

    case j @ Join(left, right, joinType, condition, _)
        if isBroadcastStage(left) || isBroadcastStage(right) =>
      val buildSide = if (isBroadcastStage(left)) BuildLeft else BuildRight
      BroadcastNestedLoopJoinExec(
        planLater(left), planLater(right), buildSide, joinType, condition) :: Nil

    case q: LogicalQueryStage =>
      q.physicalPlan :: Nil

    case _ => Nil
  }
}
