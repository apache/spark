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

import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.{QueryPlanningTracker, SQLConfHelper}
import org.apache.spark.sql.catalyst.analysis.{AnalysisContext, Analyzer}
import org.apache.spark.sql.catalyst.plans.NormalizePlan
import org.apache.spark.sql.catalyst.plans.logical.{AnalysisHelper, LogicalPlan}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf

/**
 * The HybridAnalyzer routes the unresolved logical plan between the legacy Analyzer and
 * a single-pass Analyzer when the query that we are processing is being run from unit tests
 * depending on the testing flags set and the structure of this unresolved logical plan:
 *   - If the "spark.sql.analyzer.singlePassResolver.soloRunEnabled" is "true", the
 *      [[HybridAnalyzer]] will unconditionally run the single-pass Analyzer, which would
 *      usually result in some unexpected behavior and failures. This flag is used only for
 *      development.
 *   - If the "spark.sql.analyzer.singlePassResolver.dualRunEnabled" is "true", the
 *      [[HybridAnalyzer]] will invoke the legacy analyzer and optionally _also_ the fixed-point
 *      one depending on the structure of the unresolved plan. This decision is based on which
 *      features are supported by the single-pass Analyzer, and the checking is implemented in
 *      the [[ResolverGuard]]. After that we validate the results using the following
 *      logic:
 *        - If the fixed-point Analyzer fails and the single-pass one succeeds, we throw an
 *          appropriate exception (please check the
 *          [[QueryCompilationErrors.fixedPointFailedSinglePassSucceeded]] method)
 *        - If both the fixed-point and the single-pass Analyzers failed, we throw the exception
 *          from the fixed-point Analyzer.
 *        - If the single-pass Analyzer failed, we throw an exception from its failure.
 *        - If both the fixed-point and the single-pass Analyzers succeeded, we compare the logical
 *          plans and output schemas, and return the resolved plan from the fixed-point Analyzer.
 *   - Otherwise we run the legacy analyzer.
 * */
class HybridAnalyzer(
    legacyAnalyzer: Analyzer,
    resolverGuard: ResolverGuard,
    resolver: Resolver,
    checkSupportedSinglePassFeatures: Boolean = true)
    extends SQLConfHelper {
  private var singlePassResolutionDuration: Option[Long] = None
  private var fixedPointResolutionDuration: Option[Long] = None

  def apply(plan: LogicalPlan, tracker: QueryPlanningTracker): LogicalPlan = {
    val dualRun =
      conf.getConf(SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER) &&
      checkResolverGuard(plan)

    withTrackedAnalyzerBridgeState(dualRun) {
      if (dualRun) {
        resolveInDualRun(plan, tracker)
      } else if (conf.getConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED)) {
        resolveInSinglePass(plan)
      } else {
        resolveInFixedPoint(plan, tracker)
      }
    }
  }

  def getSinglePassResolutionDuration: Option[Long] = singlePassResolutionDuration

  def getFixedPointResolutionDuration: Option[Long] = fixedPointResolutionDuration

  /**
   * Call `body` in the context of tracked [[AnalyzerBridgeState]]. Set the new bridge state
   * depending on whether we are in dual-run mode or not:
   * - If [[dualRun]] and [[ANALYZER_SINGLE_PASS_RESOLVER_RELATION_BRIDGING_ENABLED]] are true,
   *   create and set a new [[AnalyzerBridgeState]].
   * - Otherwise, reset [[AnalyzerBridgeState]].
   *
   * Finally, set the bridge state back to the previous one after the `body` is executed to avoid
   * disrupting the possible upper-level [[Analyzer]] invocation in case it's recursive
   * [[Analyzer]] call.
   * */
  private def withTrackedAnalyzerBridgeState(dualRun: Boolean)(
      body: => LogicalPlan): LogicalPlan = {
    val bridgeRelations = dualRun && conf.getConf(
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_RELATION_BRIDGING_ENABLED
      )

    val prevSinglePassResolverBridgeState = AnalysisContext.get.getSinglePassResolverBridgeState

    AnalysisContext.get.setSinglePassResolverBridgeState(if (bridgeRelations) {
      Some(new AnalyzerBridgeState)
    } else {
      None
    })

    try {
      body
    } finally {
      AnalysisContext.get.setSinglePassResolverBridgeState(prevSinglePassResolverBridgeState)
    }
  }

  /**
   * This method is used to run both the legacy Analyzer and single-pass Analyzer,
   * and then compare the results or check the errors. For more context please check the
   * [[HybridAnalyzer]] scaladoc.
   * */
  private def resolveInDualRun(plan: LogicalPlan, tracker: QueryPlanningTracker): LogicalPlan = {
    var fixedPointException: Option[Throwable] = None
    val fixedPointResult = try {
      val (resolutionDuration, result) = recordDuration {
        Some(resolveInFixedPoint(plan, tracker))
      }
      fixedPointResolutionDuration = Some(resolutionDuration)
      result
    } catch {
      case NonFatal(e) =>
        fixedPointException = Some(e)
        None
    }

    var singlePassException: Option[Throwable] = None
    val singlePassResult = try {
      val (resolutionDuration, result) = recordDuration {
        Some(resolveInSinglePass(plan))
      }
      singlePassResolutionDuration = Some(resolutionDuration)
      result
    } catch {
      case NonFatal(e) =>
        singlePassException = Some(e)
        None
    }

    fixedPointException match {
      case Some(fixedPointEx) =>
        singlePassException match {
          case Some(_) =>
            throw fixedPointEx
          case None =>
            throw QueryCompilationErrors.fixedPointFailedSinglePassSucceeded(
              singlePassResult.get,
              fixedPointEx
            )
        }
      case None =>
        singlePassException match {
          case Some(singlePassEx: ExplicitlyUnsupportedResolverFeature)
              if checkSupportedSinglePassFeatures =>
            fixedPointResult.get
          case Some(singlePassEx) =>
            throw singlePassEx
          case None =>
            validateLogicalPlans(fixedPointResult.get, singlePassResult.get)
            if (conf.getConf(SQLConf.ANALYZER_DUAL_RUN_RETURN_SINGLE_PASS_RESULT)) {
              singlePassResult.get
            } else {
              fixedPointResult.get
            }
        }
    }
  }

  /**
   * This method is used to run the single-pass Analyzer which will return the resolved plan
   * or throw an exception if the resolution fails. Both cases are handled in the caller method.
   * */
  private def resolveInSinglePass(plan: LogicalPlan): LogicalPlan = {
    val resolvedPlan = resolver.lookupMetadataAndResolve(
      plan,
      analyzerBridgeState = AnalysisContext.get.getSinglePassResolverBridgeState
    )
    if (conf.getConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_VALIDATION_ENABLED)) {
      val validator = new ResolutionValidator
      validator.validatePlan(resolvedPlan)
    }
    resolvedPlan
  }

  /**
   * This method is used to run the legacy Analyzer which will return the resolved plan
   * or throw an exception if the resolution fails. Both cases are handled in the caller method.
   * */
  private def resolveInFixedPoint(plan: LogicalPlan, tracker: QueryPlanningTracker): LogicalPlan = {
    val resolvedPlan = legacyAnalyzer.executeAndTrack(plan, tracker)
    QueryPlanningTracker.withTracker(tracker) {
      legacyAnalyzer.checkAnalysis(resolvedPlan)
    }
    resolvedPlan
  }

  private def validateLogicalPlans(fixedPointResult: LogicalPlan, singlePassResult: LogicalPlan) = {
    if (fixedPointResult.schema != singlePassResult.schema) {
      throw QueryCompilationErrors.hybridAnalyzerOutputSchemaComparisonMismatch(
        fixedPointResult.schema,
        singlePassResult.schema
      )
    }
    if (normalizePlan(fixedPointResult) != normalizePlan(singlePassResult)) {
      throw QueryCompilationErrors.hybridAnalyzerLogicalPlanComparisonMismatch(
        fixedPointResult,
        singlePassResult
      )
    }
  }

  private def normalizePlan(plan: LogicalPlan) = AnalysisHelper.allowInvokingTransformsInAnalyzer {
    NormalizePlan(plan)
  }

  private def checkResolverGuard(plan: LogicalPlan): Boolean =
    !checkSupportedSinglePassFeatures || resolverGuard.apply(plan)

  private def recordDuration[T](thunk: => T): (Long, T) = {
    val start = System.nanoTime()
    val res = thunk
    (System.nanoTime() - start, res)
  }
}
