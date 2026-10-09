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
 * Builds the shared inner [[AdaptiveSparkPlanExec]] for each [[CTEReuseExchange]], registers it in
 * [[AdaptiveExecutionContext.cteAQERegistry]] by `cteId`, and attaches it onto the node
 * (`CTEReuseExchange.innerAQE`).
 *
 * Runs as a preprocessing rule next to [[PlanAdaptiveSubqueries]] so the inner AQE exists before
 * the Photon rules run: the Photon dry run reads the CTE subplan's real photonization estimate
 * (`initialPlanForExplain`) off the node instead of guessing, which is what lets it decide
 * accurately whether a broadcast hash join over a CTE reference photonizes.
 *
 * The registry is the source of truth: it lives on the shared [[AdaptiveExecutionContext]], so the
 * SAME inner AQE instance is reused across every reference (main query and subqueries) and across
 * replans -- avoiding duplicate replanning / plan divergence for the reused subplan. The node's
 * `innerAQE` is just a reference to that shared instance, re-attached each preprocessing pass
 * (preprocessing rules also run in `reOptimize`, where `SparkStrategies` regenerates fresh
 * `CTEReuseExchange` nodes with no AQE). `createNonResultQueryStages` still resolves the inner AQE
 * from the registry (robust even on re-plan paths that drop this rule); the node's `innerAQE`
 * exists only so the Photon compiler can read the estimate off the node.
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
        // Get-or-build the shared inner AQE for this cteId, then attach it onto the node so the
        // Photon compiler can read the estimate off the node.
        val innerAQE = adaptiveExecutionContext.cteAQERegistry.getOrElseUpdate(re.cteId, {
          // `re.subplan` is already a physical `ShuffleExchangeLike`, so -- unlike
          // `PlanAdaptiveSubqueries`, which compiles a logical subquery -- there is no
          // `createSparkPlan` step; `applyInternal` wraps it into an inner AQE and recursively
          // preprocesses it (handling any nested CTE / subquery inside the CTE body).
          InsertAdaptiveSparkPlan(adaptiveExecutionContext).applyInternal(
            re.subplan, isSubquery = true) match {
            case aqe: AdaptiveSparkPlanExec => aqe
            case _ =>
              // AQE was not applied to the CTE subplan (should not happen: the subplan roots at a
              // LOCAL_SHUFFLE_FOR_CTE shuffle, which always makes AQE applicable, and `isSubquery`
              // bypasses the `shouldApplyAQE` check). Back the whole query off AQE rather than
              // failing at execution time: `InsertAdaptiveSparkPlan` catches this and runs the
              // non-AQE path, where `UnwrapCTEReuseExchange` handles the CTE reuse.
              throw CTEAdaptiveNotSupportedException(re.subplan.logicalLink.orNull)
          }
        })
        re.copy(innerAQE = Some(innerAQE))
    }
  }
}
