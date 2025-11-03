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

import java.util.Random

import org.apache.spark.sql.catalyst.{QueryPlanningTracker, SQLConfHelper}
import org.apache.spark.sql.catalyst.analysis.{AnalysisContext, Analyzer}
import org.apache.spark.sql.catalyst.plans.NormalizePlan
import org.apache.spark.sql.catalyst.plans.logical.{AnalysisHelper, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf

/**
 * The HybridAnalyzer routes the unresolved logical plan between the legacy Analyzer and
 * a single-pass Analyzer when the query that we are processing is being run from unit tests
 * depending on the testing flags set and the structure of this unresolved logical plan:
 *   - If the "spark.sql.analyzer.singlePassResolver.enabled" is "true", the [[HybridAnalyzer]]
 *      will unconditionally run the single-pass Analyzer, which would usually result in some
 *      unexpected behavior and failures. This flag is used only for development.
 *   - If the "spark.sql.analyzer.singlePassResolver.dualRunEnabled" is "true", the
 *      [[HybridAnalyzer]] will invoke the legacy analyzer and optionally _also_ the fixed-point
 *      one depending on the structure of the unresolved plan. This decision is based on which
 *      features are supported by the single-pass Analyzer, and the checking is implemented in the
 *      [[ResolverGuard]]. It's also determined if the query should be run in dual run mode by the
 *      [[SQLConf.ANALYZER_DUAL_RUN_SAMPLE_RATE]] flag value.
 *      If [[SQLConf.ANALYZER_LOG_ERRORS_INSTEAD_OF_THROWING_IN_DUAL_RUNS]] is enabled we tag the
 *      query with appropriate tag. After that we validate the results using the following logic:
 *        - If the fixed-point Analyzer fails and the single-pass one succeeds, we throw an
 *          appropriate exception (please check the
 *          [[QueryCompilationErrors.fixedPointFailedSinglePassSucceeded]] method). If
 *          [[SQLConf.ANALYZER_DUAL_RUN_LOGGING]] is enabled, we tag the query, log the message and
 *          throw the exception from the fixed-point Analyzer.
 *        - If both the fixed-point and the single-pass Analyzers failed, we throw the exception
 *          from the fixed-point Analyzer.
 *        - If the single-pass Analyzer failed, we throw an exception from its failure. If
 *          [[SQLConf.ANALYZER_DUAL_RUN_LOGGING]] is enabled, we tag the query, log the message and
 *          return the resolved plan from the fixed-point Analyzer.
 *        - If both the fixed-point and the single-pass Analyzers succeeded, we compare the logical
 *          plans and output schemas, and return the resolved plan from the fixed-point Analyzer.
 *          If [[SQLConf.ANALYZER_DUAL_RUN_LOGGING]] is enabled we also tag the query.
 *   - Otherwise we run the legacy analyzer.
 * */
class HybridAnalyzer(
    legacyAnalyzer: Analyzer,
    resolverGuard: ResolverGuard,
    resolver: Resolver,
    extendedResolutionChecks: Seq[LogicalPlan => Unit] = Seq.empty,
    extendedRewriteRules: Seq[Rule[LogicalPlan]] = Seq.empty,
    exposeExplicitlyUnsupportedResolverFeature: Boolean = false)
    extends SQLConfHelper {
  private var singlePassResolutionDuration: Option[Long] = None
  private var fixedPointResolutionDuration: Option[Long] = None
  private val resolverRunner = new ResolverRunner(
    resolver = resolver,
    extendedResolutionChecks = extendedResolutionChecks,
    extendedRewriteRules = extendedRewriteRules
  )
  private val sampleRateGenerator = new Random()

  def apply(plan: LogicalPlan, tracker: QueryPlanningTracker): LogicalPlan = {
    val dualRun =
      conf.getConf(SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER) &&
      checkDualRunSampleRate() &&
      checkResolverGuard(plan)

    withTrackedAnalyzerBridgeState(dualRun) {
      if (dualRun) {
        resolveInDualRun(plan, tracker)
      } else if (conf.getConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED)) {
        resolveInSinglePass(plan, tracker)
      } else if (conf.getConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY)) {
        resolveInSinglePassTentatively(plan, tracker)
      } else {
        resolveInFixedPoint(plan, tracker)
      }
    }
  }

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
      case e: Throwable =>
        fixedPointException = Some(e)
        None
    }

    var singlePassException: Option[Throwable] = None
    val singlePassResult = try {
      val (resolutionDuration, result) = recordDuration {
        Some(resolveInSinglePass(plan, tracker))
      }
      singlePassResolutionDuration = Some(resolutionDuration)
      result
    } catch {
      case e: Throwable =>
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
              fixedPointException.get
            )
        }
      case None =>
        singlePassException match {
          case Some(singlePassEx: ExplicitlyUnsupportedResolverFeature) =>
            if (exposeExplicitlyUnsupportedResolverFeature) {
              throw singlePassEx
            }
            fixedPointResult.get
          case Some(singlePassEx) =>
            throw singlePassEx
          case None =>
            validateLogicalPlans(
              fixedPointResult = fixedPointResult.get,
              singlePassResult = singlePassResult.get
            )
            if (conf.getConf(SQLConf.ANALYZER_DUAL_RUN_RETURN_SINGLE_PASS_RESULT)) {
              singlePassResult.get
            } else {
              fixedPointResult.get
            }
        }
    }
  }

  /**
   * Run the single-pass Analyzer, but fall back to the fixed-point if
   * [[ExplicitlyUnsupportedResolverFeature]] is thrown.
   */
  private def resolveInSinglePassTentatively(
      plan: LogicalPlan,
      tracker: QueryPlanningTracker): LogicalPlan = {
    val singlePassResult = if (checkResolverGuard(plan)) {
      try {
        Some(resolveInSinglePass(plan, tracker))
      } catch {
        case _: ExplicitlyUnsupportedResolverFeature =>
          None
      }
    } else {
      None
    }

    singlePassResult match {
      case Some(result) =>
        result
      case None =>
        resolveInFixedPoint(plan, tracker)
    }
  }

  /**
   * This method is used to run the single-pass Analyzer which will return the resolved plan
   * or throw an exception if the resolution fails. Both cases are handled in the caller method.
   * */
  private def resolveInSinglePass(plan: LogicalPlan, tracker: QueryPlanningTracker): LogicalPlan =
    resolverRunner.resolve(
      plan = plan,
      analyzerBridgeState = AnalysisContext.get.getSinglePassResolverBridgeState,
      tracker = tracker
    )

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

  private def checkDualRunSampleRate(): Boolean = {
    sampleRateGenerator.nextDouble() < conf.getConf(SQLConf.ANALYZER_DUAL_RUN_SAMPLE_RATE)
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

  private def checkResolverGuard(plan: LogicalPlan): Boolean = {
    try {
      resolverGuard.apply(plan)
    } catch {
      case e: Throwable
          if !conf.getConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_EXPOSE_RESOLVER_GUARD_FAILURE) =>
        false
    }
  }

  /**
   * Normalizes the logical plan using [[NormalizePlan]].
   *
   * This method is marked as protected to be overridden in [[HybridAnalyzerSuite]] to test a
   * hypothetical situation case when [[NormalizePlan]] would throw an exception.
   */
  protected[sql] def normalizePlan(plan: LogicalPlan) =
    AnalysisHelper.allowInvokingTransformsInAnalyzer {
      NormalizePlan(plan)
    }

  private def recordDuration[T](thunk: => T): (Long, T) = {
    val start = System.nanoTime()
    val res = thunk
    (System.nanoTime() - start, res)
  }
}

object HybridAnalyzer {

  /**
   * Creates a new [[HybridAnalyzer]] instance from the legacy [[Analyzer]]. Currently most of the
   * necessary objects reside within the [[Analyzer]] itself. This is until it's replaced by the
   * new [[Resolver]].
   */
  def fromLegacyAnalyzer(
      legacyAnalyzer: Analyzer,
      exposeExplicitlyUnsupportedResolverFeature: Boolean = false): HybridAnalyzer = {
    new HybridAnalyzer(
      legacyAnalyzer = legacyAnalyzer,
      resolverGuard = new ResolverGuard(legacyAnalyzer.catalogManager),
      resolver = new Resolver(
        catalogManager = legacyAnalyzer.catalogManager,
        extensions = legacyAnalyzer.singlePassResolverExtensions,
        metadataResolverExtensions = legacyAnalyzer.singlePassMetadataResolverExtensions,
        externalRelationResolution = Some(legacyAnalyzer.getRelationResolution)
      ),
      extendedResolutionChecks = legacyAnalyzer.singlePassExtendedResolutionChecks,
      extendedRewriteRules = legacyAnalyzer.singlePassPostHocResolutionRules,
      exposeExplicitlyUnsupportedResolverFeature = exposeExplicitlyUnsupportedResolverFeature
    )
  }
}
