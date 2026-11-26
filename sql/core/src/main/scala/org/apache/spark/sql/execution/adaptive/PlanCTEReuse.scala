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
import org.apache.spark.sql.catalyst.trees.TreePattern.{CTE_REUSE_EXCHANGE, PLAN_EXPRESSION}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.{CTEReuseExchange, ShuffleExchangeLike}

/**
 * Builds the shared inner [[AdaptiveSparkPlanExec]] for each [[CTEReuseExchange]] and registers it
 * in [[AdaptiveExecutionContext.cteAQERegistry]] by `cteId`.
 *
 * Runs as the last AQE preprocessing rule, after [[PlanAdaptiveSubqueries]], so the shared inner
 * AQE is registered before stage creation looks it up.
 *
 * The registry is the source of truth: it lives on the shared [[AdaptiveExecutionContext]], so the
 * SAME inner AQE instance is reused across every reference (main query and subqueries) and across
 * replans -- avoiding duplicate replanning / plan divergence for the reused subplan.
 * `createNonResultQueryStages` resolves the inner AQE from the registry (robust even on re-plan
 * paths that drop this rule).
 *
 * The rule also replaces each reference's `subplan` with its own preprocessed form, whose
 * subqueries are planned, when the subplan holds a subquery or a nested CTE reference. The query
 * stage that replaces the reference keeps the reference's canonical form, which is the canonical
 * form of its `subplan`, and a query stage's canonical form must not hold the logical plans of
 * unplanned subqueries. The preprocessed subplan is metadata: the stage runs the shared inner AQE.
 * So the inner AQE is built only for the first reference of a `cteId`; every later reference only
 * runs the preprocessing rules on its own subplan.
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
    val registry = adaptiveExecutionContext.cteAQERegistry
    plan.transformWithPruning(_.containsPattern(CTE_REUSE_EXCHANGE)) {
      case re: CTEReuseExchange
          if registry.contains(re.cteId) &&
            !re.subplan.containsAnyPattern(PLAN_EXPRESSION, CTE_REUSE_EXCHANGE) =>
        // The shared inner AQE exists, and this reference's subplan has nothing to preprocess.
        re
      case re: CTEReuseExchange if registry.contains(re.cteId) =>
        // The shared inner AQE exists, so this reference needs only its own preprocessed subplan:
        // run the preprocessing rules on it, planning its subqueries and any nested CTE, without
        // building another inner AQE. A subquery that cannot be planned adaptively backs the whole
        // query off AQE, as it does when the inner AQE is built below.
        // TODO: This compiles the body's subqueries again for every later reference of the cteId,
        // only to give that reference its canonical form, although the stage runs the shared
        // inner AQE. Planning each shared body, its subqueries included, once per plan-reuse id
        // is deferred to a follow-up.
        val preprocessed = try {
          InsertAdaptiveSparkPlan(adaptiveExecutionContext).preprocess(re.subplan)._1
        } catch {
          case SubqueryAdaptiveNotSupportedException(_) =>
            throw CTEAdaptiveNotSupportedException(re.subplan.logicalLink.orNull)
        }
        withSubplan(re, preprocessed)
      case re: CTEReuseExchange =>
        // Build and register the shared inner AQE for this cteId so stage creation resolves it from
        // cteAQERegistry. re.subplan is already physical, so applyInternal wraps it in an inner AQE
        // and recursively preprocesses any nested CTE or subquery in the body.
        val innerAQE = InsertAdaptiveSparkPlan(adaptiveExecutionContext).applyInternal(
          re.subplan, isSubquery = true) match {
          case aqe: AdaptiveSparkPlanExec => aqe
          case _ =>
            // applyInternal returns the subplan without an inner AQE when the subplan does not
            // support adaptive execution, or when a subquery or nested CTE reference in the body
            // cannot be planned adaptively, a back-off it catches itself. Carry that back-off to
            // InsertAdaptiveSparkPlan's catch, which runs the whole query without AQE, through the
            // non-AQE unwrap path.
            throw CTEAdaptiveNotSupportedException(re.subplan.logicalLink.orNull)
        }
        registry.putIfAbsent(re.cteId, innerAQE)
        withSubplan(re, innerAQE.inputPlan)
    }
  }

  // The reference with `preprocessed` as its subplan, when that is still the shared shuffle.
  private def withSubplan(re: CTEReuseExchange, preprocessed: SparkPlan): CTEReuseExchange =
    preprocessed match {
      case s: ShuffleExchangeLike => re.copy(subplan = s)
      case _ => re
    }
}
