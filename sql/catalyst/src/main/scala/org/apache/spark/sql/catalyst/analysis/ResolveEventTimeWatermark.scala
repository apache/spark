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
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{EventTimeWatermark, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern

/**
 * Resolve [[UnresolvedEventTimeWatermark]] to [[EventTimeWatermark]].
 */
object ResolveEventTimeWatermark extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUpWithPruning(
    _.containsPattern(TreePattern.UNRESOLVED_EVENT_TIME_WATERMARK), ruleId) {

    case u: UnresolvedEventTimeWatermark if u.eventTimeColExpr.resolved && u.childrenResolved =>
      val uuid = java.util.UUID.randomUUID()

      if (u.eventTimeColExpr.isInstanceOf[MultiAlias]) {
        throw new AnalysisException(
          errorClass = "CANNOT_USE_MULTI_ALIASES_IN_WATERMARK_CLAUSE",
          messageParameters = Map()
        )
      }

      val namedExpression = u.eventTimeColExpr match {
        case e: NamedExpression => e
        case e: Expression => UnresolvedAlias(e)
      }

      if (u.child.outputSet.contains(namedExpression)) {
        // We don't need to have projection since the attribute being referenced will be available.
        EventTimeWatermark(uuid, namedExpression.toAttribute, u.delay, u.child)
      } else {
        // We need to inject projection as we can't find the matching column directly in the
        // child output.
        val proj = Project(Seq(namedExpression, u.child.output), u.child)
        val attrRef = proj.projectList.head.toAttribute
        EventTimeWatermark(uuid, attrRef, u.delay, proj)
      }
    }
}
