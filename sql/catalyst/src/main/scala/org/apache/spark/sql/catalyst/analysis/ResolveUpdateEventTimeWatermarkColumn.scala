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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.plans.logical.{EventTimeWatermark, LogicalPlan, UpdateEventTimeWatermarkColumn}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.UPDATE_EVENT_TIME_WATERMARK_COLUMN
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * Extracts the watermark delay and adds it to the UpdateEventTimeWatermarkColumn
 * logical node (if such a node is present). [[UpdateEventTimeWatermarkColumn]] node updates
 * the eventTimeColumn for upstream operators.
 *
 * If the logical plan contains a [[UpdateEventTimeWatermarkColumn]] node, but no watermark
 * has been defined, the query will fail with a compilation error.
 */
object ResolveUpdateEventTimeWatermarkColumn extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUpWithPruning(
    _.containsPattern(UPDATE_EVENT_TIME_WATERMARK_COLUMN), ruleId) {
    case u: UpdateEventTimeWatermarkColumn if u.delay.isEmpty && u.childrenResolved =>
      val existingWatermarkDelay = u.child.collect {
        case EventTimeWatermark(_, _, delay, _) => delay
      }

      if (existingWatermarkDelay.isEmpty) {
        // input dataset needs to have a event time column, we transfer the
        // watermark delay from this column to user specified eventTimeColumnName
        // in the output dataset.
        throw QueryCompilationErrors.cannotAssignEventTimeColumn()
      }

      val delay = existingWatermarkDelay.head
      u.copy(delay = Some(delay))
  }
}
