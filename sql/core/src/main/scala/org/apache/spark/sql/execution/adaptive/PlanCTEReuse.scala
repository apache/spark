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

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.CTE_REUSE_EXCHANGE
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.CTEReuseExchange

/**
 * Builds the shared inner [[AdaptiveSparkPlanExec]] for each [[CTEReuseExchange]] and registers it
 * in [[AdaptiveExecutionContext.cteAQERegistry]] by `cteId`.
 *
 * Runs as a preprocessing rule next to [[PlanAdaptiveSubqueries]] so the shared inner AQE exists
 * before the later preprocessing rules and stage creation need it.
 *
 * The registry is the source of truth: it lives on the shared [[AdaptiveExecutionContext]], so the
 * SAME inner AQE instance is reused across every reference (main query and subqueries) and across
 * replans -- avoiding duplicate replanning / plan divergence for the reused subplan.
 * `createNonResultQueryStages` resolves the inner AQE from the registry (robust even on re-plan
 * paths that drop this rule).
 */
case class PlanCTEReuse(adaptiveExecutionContext: AdaptiveExecutionContext)
  extends Rule[SparkPlan] {

  def apply(plan: SparkPlan): SparkPlan = {
    if (!plan.containsPattern(CTE_REUSE_EXCHANGE)) {
      return plan
    }
    // Only `children` are traversed here; `CTEReuseExchange` is a leaf whose `subplan` (holding any
    // nested CTE) lives in `innerChildren`, so nested references are handled by the recursive
    // `applyInternal` below rather than by this transform.
    plan.transformWithPruning(_.containsPattern(CTE_REUSE_EXCHANGE)) {
      case re: CTEReuseExchange =>
        // Build and register the shared inner AQE for this cteId so stage creation resolves it from
        // cteAQERegistry; the node is returned unchanged. re.subplan is already physical, so
        // applyInternal wraps it in an inner AQE and recursively preprocesses any nested CTE or
        // subquery in the body.
        adaptiveExecutionContext.cteAQERegistry.getOrElseUpdate(re.cteId, {
          InsertAdaptiveSparkPlan(adaptiveExecutionContext).applyInternal(
            re.subplan, isSubquery = true) match {
            case aqe: AdaptiveSparkPlanExec => aqe
            case _ =>
              // AQE not applicable (should not happen: the subplan roots at a LOCAL_SHUFFLE_FOR_CTE
              // shuffle and isSubquery bypasses shouldApplyAQE). Back the whole query off AQE via
              // InsertAdaptiveSparkPlan's catch, which runs the non-AQE unwrap path.
              throw CTEAdaptiveNotSupportedException(re.subplan.logicalLink.orNull)
          }
        })
        re
    }
  }
}
