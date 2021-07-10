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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.{LeftAnti, LeftOuter, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.{Join, JoinHint, LogicalPlan, RebalancePartitions}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{LEFT_SEMI_OR_ANTI_JOIN, OUTER_JOIN}

/**
 * This rule broadcast the stream side for left outer/semi/anti join first if stream side
 * can build broadcast and it is much smaller that other side.
 */
object BroadcastJoinOuterJoinStreamSide extends Rule[LogicalPlan] with JoinSelectionHelper {
  def apply(plan: LogicalPlan): LogicalPlan =
    plan.transformWithPruning(_.containsAnyPattern(OUTER_JOIN, LEFT_SEMI_OR_ANTI_JOIN)) {
      case j @ Join(_, Join(_, _: RebalancePartitions, LeftSemi, _, _),
        LeftOuter | LeftSemi | LeftAnti, _, _) =>
        j
      case j @ ExtractEquiJoinKeys(LeftOuter | LeftSemi | LeftAnti,
        leftKeys, _, None, left, right, hint) if leftKeys.nonEmpty && muchSmaller(left, right) &&
        !(hintToBroadcastRight(hint) || canBroadcastBySize(right, conf)) &&
        (hintToBroadcastLeft(hint) || canBroadcastBySize(left, conf)) =>
        val newRight = Join(
          right,
          RebalancePartitions(leftKeys, left, true), // Use EnsureRequirements to reuse exchange.
          LeftSemi,
          j.condition,
          JoinHint.NONE)
        j.copy(right = newRight)
    }
}
