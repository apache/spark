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

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.analysis.{AnalysisContext, CleanupAliases}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf

/**
 * Wrapper class for [[Resolver]] and single-pass resolution. This class encapsulates single-pass
 * resolution and post-processing of resolved plan. This post-processing is necessary in order to
 * either fully resolve the plan or stay compatible with the fixed-point analyzer.
 */
class ResolverRunner(
    resolver: Resolver,
    extendedResolutionChecks: Seq[LogicalPlan => Unit] = Seq.empty
) extends SQLConfHelper {

  private val resolutionPostProcessingExecutor = new RuleExecutor[LogicalPlan] {
    override def batches: Seq[Batch] = Seq(
      Batch("Post-process", Once, CleanupAliases)
    )
  }

  /**
   * Entry point for the resolver. This method performs following 3 steps:
   *  - Resolves the plan in a bottom-up, single-pass manner.
   *  - Validates the result of single-pass resolution.
   *  - Applies necessary post-processing rules.
   */
  def resolve(
      plan: LogicalPlan,
      analyzerBridgeState: Option[AnalyzerBridgeState] = None): LogicalPlan = {
    AnalysisContext.withNewAnalysisContext {
      val resolvedPlan = resolver.lookupMetadataAndResolve(plan, analyzerBridgeState)
      if (conf.getConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_VALIDATION_ENABLED)) {
        val validator = new ResolutionValidator
        validator.validatePlan(resolvedPlan)
      }
      finishResolution(resolvedPlan)
    }
  }

  /**
   * This method performs necessary post-processing rules that aren't suitable for single-pass
   * resolver. We apply these rules after the single-pass has finished resolution to stay
   * compatible with fixed-point analyzer.
   */
  private def finishResolution(plan: LogicalPlan): LogicalPlan = {
    val planWithPostProcessing = resolutionPostProcessingExecutor.execute(plan)

    for (rule <- extendedResolutionChecks) {
      rule(planWithPostProcessing)
    }
    planWithPostProcessing
  }
}
