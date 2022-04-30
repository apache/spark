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

import org.apache.spark.sql.catalyst.plans.LeftSemiOrAnti
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Deduplicate the right side of left semi/anti join.
 */
object DeduplicateRightSideOfLeftSemiAntiJoin extends Rule[LogicalPlan] with JoinSelectionHelper {
  def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transform {
      case j @ Join(_, agg: Aggregate, LeftSemiOrAnti(_), _, _) if agg.groupOnly =>
        j
      case j @ Join(_, _: PartialAggregate, LeftSemiOrAnti(_), _, _) =>
        j
      case j @ Join(_, right, LeftSemiOrAnti(_), _, _) if !canPlanAsBroadcastHashJoin(j, conf) =>
        j.copy(right = PartialAggregate(right.output, right.output, right))
    }
  }
}
