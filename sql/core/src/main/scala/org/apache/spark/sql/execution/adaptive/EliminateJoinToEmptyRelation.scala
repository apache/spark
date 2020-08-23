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
import org.apache.spark.sql.catalyst.plans.{Inner, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.joins.{EmptyHashedRelation, HashedRelation, HashedRelationWithAllNullKeys}

/**
 * This optimization rule detects and converts a Join to an empty [[LocalRelation]]:
 * 1. Join is single column NULL-aware anti join (NAAJ), and broadcasted [[HashedRelation]]
 *    is [[HashedRelationWithAllNullKeys]].
 *
 * 2. Join is inner or left semi join, and broadcasted [[HashedRelation]]
 *    is [[EmptyHashedRelation]].
 *    This applies to all Joins (sort merge join, shuffled hash join, and broadcast hash join),
 *    because sort merge join and shuffled hash join will be changed to broadcast hash join with AQE
 *    at the first place.
 */
object EliminateJoinToEmptyRelation extends Rule[LogicalPlan] {

  private def canEliminate(plan: LogicalPlan, relation: HashedRelation): Boolean = plan match {
    case LogicalQueryStage(_, stage: BroadcastQueryStageExec) if stage.resultOption.get().isDefined
      && stage.broadcast.relationFuture.get().value == relation => true
    case _ => false
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformDown {
    case j @ ExtractSingleColumnNullAwareAntiJoin(_, _)
        if canEliminate(j.right, HashedRelationWithAllNullKeys) =>
      LocalRelation(j.output, data = Seq.empty, isStreaming = j.isStreaming)

    case j @ Join(_, _, Inner, _, _) if canEliminate(j.left, EmptyHashedRelation) ||
        canEliminate(j.right, EmptyHashedRelation) =>
      LocalRelation(j.output, data = Seq.empty, isStreaming = j.isStreaming)

    case j @ Join(_, _, LeftSemi, _, _) if canEliminate(j.right, EmptyHashedRelation) =>
      LocalRelation(j.output, data = Seq.empty, isStreaming = j.isStreaming)
  }
}
