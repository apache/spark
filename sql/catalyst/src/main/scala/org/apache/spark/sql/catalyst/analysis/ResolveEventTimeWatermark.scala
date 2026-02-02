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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.plans.logical.{EventTimeWatermark, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern
import org.apache.spark.sql.catalyst.util.AUTO_GENERATED_ALIAS

/**
 * Resolve [[UnresolvedEventTimeWatermark]] to [[EventTimeWatermark]].
 */
object ResolveEventTimeWatermark extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUpWithPruning(
    _.containsPattern(TreePattern.UNRESOLVED_EVENT_TIME_WATERMARK), ruleId) {

    case u: UnresolvedEventTimeWatermark if u.eventTimeColExpr.resolved && u.childrenResolved =>
      if (u.eventTimeColExpr.metadata.contains(AUTO_GENERATED_ALIAS) &&
          u.eventTimeColExpr.metadata.getString(AUTO_GENERATED_ALIAS) == "true") {
        throw new AnalysisException(
          errorClass = "REQUIRES_EXPLICIT_NAME_IN_WATERMARK_CLAUSE",
          messageParameters = Map("sqlExpr" -> u.eventTimeColExpr.sql)
        )
      }

      val uuid = java.util.UUID.randomUUID()

      val attrRef = u.eventTimeColExpr.toAttribute
      if (u.child.outputSet.contains(u.eventTimeColExpr)) {
        // We don't need to have projection since the attribute being referenced will be available.
        EventTimeWatermark(uuid, attrRef, u.delay, u.child)
      } else {
        // We need to inject projection as we can't find the matching column directly in the
        // child output.
        val proj = Project(Seq(u.eventTimeColExpr) ++ u.child.output, u.child)
        EventTimeWatermark(uuid, attrRef, u.delay, proj)
      }
    }
}
