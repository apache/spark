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

import org.apache.spark.sql.catalyst.plans.{LeftAnti, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Distinct, Join, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.LEFT_SEMI_OR_ANTI_JOIN

/**
 * Deduplicate the right side of left semi/anti join if it cannot be planed as broadcast hash join
 * and there are many duplicate values.
 * {{{
 *   SELECT a1, a2 FROM Tab1 LEFT SEMI JOIN Tab2 ON a1=b1
 *   ==>  SELECT a1, a2 FROM Tab1 LEFT SEMI JOIN (SELECT b1 Tab2 GROUP BY b1) t2 ON a1<=>b1
 * }}}
 */
object DeduplicateRightSideOfLeftSemiAntiJoin extends Rule[LogicalPlan] with JoinSelectionHelper {
  def apply(plan: LogicalPlan): LogicalPlan =
    plan.transformWithPruning(_.containsAnyPattern(LEFT_SEMI_OR_ANTI_JOIN)) {
      case j @ Join(_, right, LeftSemi | LeftAnti, _, hint)
        if conf.cboEnabled && !canBroadcastBySize(right, conf) && !hintToBroadcastRight(hint) &&
          muchSmaller(Distinct(right), right) =>
        j.copy(right = Aggregate(right.output, right.output, right))
    }
}
