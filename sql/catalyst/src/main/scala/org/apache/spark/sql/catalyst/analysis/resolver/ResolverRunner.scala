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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.{MetricKey, QueryPlanningTracker, SQLConfHelper}
import org.apache.spark.sql.catalyst.analysis.AnalysisContext
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.internal.SQLConf

/**
 * Wrapper class for [[Resolver]] and single-pass resolution. This class encapsulates single-pass
 * resolution, rewriting and validation of resolved plan.
 */
class ResolverRunner(
    resolver: Resolver,
    catalogObjectGuard: CatalogObjectGuard,
    extendedResolutionChecks: Seq[LogicalPlan => Unit] = Seq.empty
) extends ResolverMetricTracker
    with SQLConfHelper {

  /**
   * Resolution checks that should only run in single-pass mode.
   * These checks are not part of the legacy analyzer's `extendedResolutionChecks`.
   */
  private val singlePassOnlyResolutionChecks: Seq[LogicalPlan => Unit] = Seq(
    NonDeterministicExpressionCheck
  )

  /**
   * `resolutionCheckRunner` is used to run `extendedResolutionChecks` and
   * `singlePassOnlyResolutionChecks` on the resolved plan.
   */
  private val resolutionCheckRunner =
    new ResolutionCheckRunner(extendedResolutionChecks ++ singlePassOnlyResolutionChecks)

  /**
   * Main entry point into the single-pass resolution process that does not run
   * [[CatalogObjectGuard]] and which always returns a [[LogicalPlan]] as a result for convenience.
   */
  def resolve(
      plan: LogicalPlan,
      analyzerBridgeState: Option[AnalyzerBridgeState],
      tracker: QueryPlanningTracker
  ): LogicalPlan = {
    // BEGIN-EDGE
    resolve(
      plan = plan,
      checkCatalogObjects = false,
      analyzerBridgeState = analyzerBridgeState,
      tracker = tracker
    ) match {
      case ResolverRunnerResultResolvedPlan(resolvedPlan) =>
        resolvedPlan
      case _ =>
        throw SparkException.internalError(
          "ResolverRunner.resolve(..., checkCatalogObjects = false, ...) should return a " +
          "resolved plan."
        )
    }
  }

  /**
   * Main entry point into the single-pass resolution process. This method handles all the steps
   * of single-pass resolution, as described in [[runResolution]].
   *
   * If `checkCatalogObjects` is set to `true`, it will first check if the plan can be processed
   * by the single-pass Analyzer using the [[CatalogObjectGuard]]. This check is holistic and
   * handles nested dependencies like tables and views. This is only possible given that the UC
   * metadata for referenced objects is available from the top-level call to UC.
   */
  def resolve(
      plan: LogicalPlan,
      checkCatalogObjects: Boolean,
      analyzerBridgeState: Option[AnalyzerBridgeState],
      tracker: QueryPlanningTracker
  ): ResolverRunnerResult = {
    recordTopLevelMetrics(tracker) {
      recordProfileAndLatency("resolve", MetricKey.SINGLE_PASS_ANALYZER_TOTAL_LATENCY) {
        AnalysisContext.withNewAnalysisContext {
          if (checkCatalogObjects) {
            catalogObjectGuard(plan) match {
              case CatalogObjectGuardResultPlanSupported() =>
                runResolution(plan, analyzerBridgeState)
              case CatalogObjectGuardResultPlanNotSupported(reason) =>
                ResolverRunnerResultPlanNotSupported(reason)
              case CatalogObjectGuardResultUnrecoverableException(exception) =>
                ResolverRunnerResultUnrecoverableException(exception)
            }
          } else {
            runResolution(plan, analyzerBridgeState)
          }
        }
      }
    }
  }

  /**
   * Performs the plan analysis:
   *  - Resolves the plan in a bottom-up using [[Resolver]], single-pass manner.
   *  - Validates the final result internally using [[ResolutionValidator]].
   *  - Validates the final result using [[extendedResolutionChecks]].
   *  - Marks the plan with [[SINGLE_PASS_ANALYSIS_MARKER]] tag if requested.
   *  - Sets the plan as analyzed to stay compatible with fixed-point Analyzer - some rules would
   *    not expect to traverse the plan twice given that we are performing an iterative analysis of
   *    classic DataFrames with a fallback to fixed-point.
   */
  private def runResolution(
      plan: LogicalPlan,
      analyzerBridgeState: Option[AnalyzerBridgeState] = None): ResolverRunnerResult = {
    // END-EDGE
    val resolvedPlan = resolver.lookupMetadataAndResolve(plan, analyzerBridgeState)

    runValidator(resolvedPlan)

    resolutionCheckRunner.runWithSubqueries(resolvedPlan)

    markPlan(resolvedPlan)

    resolvedPlan.setAnalyzed()

    ResolverRunnerResultResolvedPlan(resolvedPlan)
  }

  private def runValidator(plan: LogicalPlan): Unit = {
    if (conf.getConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_VALIDATION_ENABLED)) {
      // BEGIN-EDGE
      recordProfileAndLatency("runValidator", MetricKey.SINGLE_PASS_ANALYZER_VALIDATOR_LATENCY) {
        // END-EDGE
        val validator = new ResolutionValidator
        validator.validatePlan(plan)
      } // EDGE
    }
  }

  private def markPlan(plan: LogicalPlan): Unit = {
    if (conf.getConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_MARK_RESOLVED_PLAN)) {
      plan.setTagValue(ResolverRunner.SINGLE_PASS_ANALYSIS_MARKER, true)
    }
  }
}

object ResolverRunner {

  /**
   * This tag is put on the fully resolved plan by the single-pass Analyzer to validate the
   * single-pass resolution fact in unit tests.
   */
  val SINGLE_PASS_ANALYSIS_MARKER = TreeNodeTag[Boolean]("single_pass_analysis_marker")
}
