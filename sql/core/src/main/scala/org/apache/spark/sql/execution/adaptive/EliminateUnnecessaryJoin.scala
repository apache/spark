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
import org.apache.spark.sql.catalyst.plans.{LeftAnti, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.joins.HashedRelationWithAllNullKeys

/**
 * This optimization rule detects and eliminates unnecessary Join:
 * 1. Join is single column NULL-aware anti join (NAAJ), and broadcasted [[HashedRelation]]
 *    is [[HashedRelationWithAllNullKeys]]. Eliminate join to an empty [[LocalRelation]].
 *
 * 2. Join is left semi join
 *    Join right side is non-empty and condition is empty. Eliminate join to its left side.
 *
 * 3. Join is left anti join
 *    Join right side is non-empty and condition is empty. Eliminate join to an empty
 *         [[LocalRelation]].
 */
object EliminateUnnecessaryJoin extends Rule[LogicalPlan] {

  private def isRelationWithAllNullKeys(plan: LogicalPlan) = plan match {
    case LogicalQueryStage(_, stage: BroadcastQueryStageExec)
      if stage.resultOption.get().isDefined =>
      stage.broadcast.relationFuture.get().value == HashedRelationWithAllNullKeys
    case _ => false
  }

  private def checkRowCount(plan: LogicalPlan): Boolean = plan match {
    case LogicalQueryStage(_, stage: QueryStageExec) if stage.resultOption.get().isDefined =>
      stage.getRuntimeStatistics.rowCount match {
        case Some(count) => count > 0
        case _ => false
      }

    case _ => false
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformUp {
    case j @ ExtractSingleColumnNullAwareAntiJoin(_, _) if isRelationWithAllNullKeys(j.right) =>
      LocalRelation(j.output, data = Seq.empty, isStreaming = j.isStreaming)

    case j @ Join(_, _, LeftSemi, condition, _) =>
      if (condition.isEmpty && checkRowCount(j.right)) {
        j.left
      } else {
        j
      }

    case j @ Join(_, _, LeftAnti, condition, _) =>
      if (condition.isEmpty && checkRowCount(j.right)) {
        LocalRelation(j.output, data = Seq.empty, isStreaming = j.isStreaming)
      } else {
        j
      }
  }
}
