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

package org.apache.spark.sql.catalyst.analysis.resolver

import org.apache.spark.sql.catalyst.{QueryPlanningTracker, SQLConfHelper}
import org.apache.spark.sql.catalyst.analysis.{AnalysisContext, CleanupAliases}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf

/**
 * Wrapper class for [[Resolver]] and single-pass resolution. This class encapsulates single-pass
 * resolution, rewriting and validation of resolved plan. The plan rewrite is necessary in order to
 * either fully resolve the plan or stay compatible with the fixed-point analyzer.
 */
class ResolverRunner(
    resolver: Resolver,
    extendedResolutionChecks: Seq[LogicalPlan => Unit] = Seq.empty
) extends ResolverMetricTracker
    with SQLConfHelper {

  /**
   * Sequence of post-resolution rules that should be applied on the result of single-pass
   * resolution.
   */
  private val planRewriteRules: Seq[Rule[LogicalPlan]] = Seq(
    PruneMetadataColumns,
    CleanupAliases
  )

  /**
   * `planRewriter` is used to rewrite the plan and the subqueries inside by applying
   * `planRewriteRules`.
   */
  private val planRewriter = new PlanRewriter(planRewriteRules)

  /**
   * Entry point for the resolver. This method performs following 4 steps:
   *  - Resolves the plan in a bottom-up using [[Resolver]], single-pass manner.
   *  - Rewrites the plan using rules configured in the [[planRewriter]].
   *  - Validates the final result internally using [[ResolutionValidator]].
   *  - Validates the final result using [[extendedResolutionChecks]].
   */
  def resolve(
      plan: LogicalPlan,
      analyzerBridgeState: Option[AnalyzerBridgeState] = None,
      tracker: QueryPlanningTracker = new QueryPlanningTracker): LogicalPlan =
    recordMetrics(tracker) {
      AnalysisContext.withNewAnalysisContext {
        val resolvedPlan = resolver.lookupMetadataAndResolve(plan, analyzerBridgeState)

        val rewrittenPlan = planRewriter.rewriteWithSubqueries(resolvedPlan)

        if (conf.getConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_VALIDATION_ENABLED)) {
          val validator = new ResolutionValidator
          validator.validatePlan(rewrittenPlan)
        }

        for (rule <- extendedResolutionChecks) {
          rule(rewrittenPlan)
        }

        rewrittenPlan
      }
    }
}
