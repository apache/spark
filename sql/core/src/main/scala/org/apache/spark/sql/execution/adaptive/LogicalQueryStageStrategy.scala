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
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.LeftAnti
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.execution.{joins, SparkPlan}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec}
import org.apache.spark.sql.internal.SQLConf

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

    case j @ Join(left, right, joinType, condition, _)
        if isBroadcastStage(left) || isBroadcastStage(right) =>
      def createBroadcastNLJoin() = {
        val buildSide = if (isBroadcastStage(left)) BuildLeft else BuildRight
        BroadcastNestedLoopJoinExec(
          planLater(left), planLater(right), buildSide, joinType, condition) :: Nil
      }

      /**
       * See. [SPARK-32290]
       * Not in Subquery will almost certainly be planned as a Broadcast Nested Loop join,
       * which is very time consuming because it's an O(M*N) calculation.
       * But if it's a single column NotInSubquery, and buildSide data is small enough,
       * O(M*N) calculation could be optimized into O(M) using hash lookup instead of loop lookup.
       */
      if (SQLConf.get.notInSubqueryHashJoinEnabled &&
        joinType == LeftAnti &&
        isBroadcastStage(right) &&
        right.output.length == 1) {
        val (matched, _, _) =
          joins.NotInSubqueryConditionPattern.singleColumnPatternMatch(condition)
        if (matched) {
          Seq(joins.BroadcastNullAwareHashJoinExec(
            planLater(left), planLater(right), BuildRight, LeftAnti, condition))
        } else {
          createBroadcastNLJoin()
        }
      } else {
        createBroadcastNLJoin()
      }

    case q: LogicalQueryStage =>
      q.physicalPlan :: Nil

    case _ => Nil
  }
}
