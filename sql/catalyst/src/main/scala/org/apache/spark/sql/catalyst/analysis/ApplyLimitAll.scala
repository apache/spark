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

import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, BaseEvalPython, CTERelationRef, Filter, Join, LimitAll, LogicalPlan, Offset, Project, SubqueryAlias, Union, Window}
import org.apache.spark.sql.catalyst.rules.Rule

object ApplyLimitAll extends Rule[LogicalPlan] {
  private def applyLimitAllToPlan(plan: LogicalPlan, isInLimitAll: Boolean = false): LogicalPlan = {
    plan match {
      case la: LimitAll =>
        applyLimitAllToPlan(la.child, isInLimitAll = true)
      case cteRef: CTERelationRef if isInLimitAll =>
        cteRef.copy(isUnlimitedRecursion = true)
      // Allow-list for pushing down Limit All.
      case _: Project | _: Filter | _: Join | _: Union | _: Offset |
           _: BaseEvalPython | _: Aggregate | _: Window | _: SubqueryAlias =>
        plan.withNewChildren(plan.children
          .map(child => applyLimitAllToPlan(child, isInLimitAll)))
      case other =>
        other.withNewChildren(plan.children
          .map(child => applyLimitAllToPlan(child, isInLimitAll = false)))
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = {
    applyLimitAllToPlan(plan)
  }
}
