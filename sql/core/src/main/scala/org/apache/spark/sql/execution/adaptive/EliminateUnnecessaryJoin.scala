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

import org.apache.spark.sql.catalyst.planning.ExtractSingleColumnNullAwareAntiJoin
import org.apache.spark.sql.catalyst.plans.{Inner, LeftAnti, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.joins.HashedRelationWithAllNullKeys

/**
 * This optimization rule detects and eliminates unnecessary Join:
 * 1. Join is single column NULL-aware anti join (NAAJ), and broadcasted [[HashedRelation]]
 *    is [[HashedRelationWithAllNullKeys]]. Eliminate join to an empty [[LocalRelation]].
 *
 * 2. Join is inner join, and either side of join is empty. Eliminate join to an empty
 *    [[LocalRelation]].
 *
 * 3. Join is left semi join
 *    3.1. Join right side is empty. Eliminate join to an empty [[LocalRelation]].
 *    3.2. Join right side is non-empty and condition is empty. Eliminate join to its left side.
 *
 * 4. Join is left anti join
 *    4.1. Join right side is empty. Eliminate join to its left side.
 *    4.2. Join right side is non-empty and condition is empty. Eliminate join to an empty
 *         [[LocalRelation]].
 *
 * This applies to all joins (sort merge join, shuffled hash join, broadcast hash join, and
 * broadcast nested loop join), because sort merge join and shuffled hash join will be changed
 * to broadcast hash join with AQE at the first place.
 */
object EliminateUnnecessaryJoin extends Rule[LogicalPlan] {

  private def isRelationWithAllNullKeys(plan: LogicalPlan) = plan match {
    case LogicalQueryStage(_, stage: BroadcastQueryStageExec)
      if stage.resultOption.get().isDefined =>
      stage.broadcast.relationFuture.get().value == HashedRelationWithAllNullKeys
    case _ => false
  }

  private def checkRowCount(plan: LogicalPlan, hasRow: Boolean): Boolean = plan match {
    case LogicalQueryStage(_, stage: QueryStageExec) if stage.resultOption.get().isDefined =>
      stage.getRuntimeStatistics.rowCount match {
        case Some(count) => hasRow == (count > 0)
        case _ => false
      }
    case _ => false
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformDown {
    case j @ ExtractSingleColumnNullAwareAntiJoin(_, _) if isRelationWithAllNullKeys(j.right) =>
      LocalRelation(j.output, data = Seq.empty, isStreaming = j.isStreaming)

    case j @ Join(_, _, Inner, _, _) if checkRowCount(j.left, hasRow = false) ||
      checkRowCount(j.right, hasRow = false) =>
      LocalRelation(j.output, data = Seq.empty, isStreaming = j.isStreaming)

    case j @ Join(_, _, LeftSemi, condition, _) =>
      if (checkRowCount(j.right, hasRow = false)) {
        LocalRelation(j.output, data = Seq.empty, isStreaming = j.isStreaming)
      } else if (condition.isEmpty && checkRowCount(j.right, hasRow = true)) {
        j.left
      } else {
        j
      }

    case j @ Join(_, _, LeftAnti, condition, _) =>
      if (checkRowCount(j.right, hasRow = false)) {
        j.left
      } else if (condition.isEmpty && checkRowCount(j.right, hasRow = true)) {
        LocalRelation(j.output, data = Seq.empty, isStreaming = j.isStreaming)
      } else {
        j
      }
  }
}
