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

import org.apache.spark.internal.Logging
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
 *   - If the "spark.sql.analyzer.singlePassResolver.enabled" is "true", the [[HybridAnalyzer]]
 *      will unconditionally run the single-pass Analyzer, which would usually result in some
 *      unexpected behavior and failures. This flag is used only for development.
 *   - If the "spark.sql.analyzer.singlePassResolver.dualRunEnabled" is "true", the
 *      [[HybridAnalyzer]] will invoke the legacy analyzer and optionally _also_ the fixed-point
 *      one depending on the structure of the unresolved plan. This decision is based on which
 *      features are supported by the single-pass Analyzer, and the checking is implemented in the
 *      [[ResolverGuard]]. It's also determined if the query should be run in dual run mode by the
 *      [[SQLConf.ANALYZER_DUAL_RUN_SAMPLE_RATE]] flag value.
 *      After that we validate the results using the following logic:
 *        - If the fixed-point Analyzer fails and the single-pass one succeeds, we throw an
 *          appropriate exception (please check the
 *          [[QueryCompilationErrors.fixedPointFailedSinglePassSucceeded]] method).
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
    tracker: QueryPlanningTracker,
    extendedResolutionChecks: Seq[LogicalPlan => Unit] = Seq.empty)
    extends SQLConfHelper
    with Logging {
  private var singlePassResolutionDuration: Option[Long] = None
  private var fixedPointResolutionDuration: Option[Long] = None
  private val sampleRateGenerator = new Random()

  def apply(plan: LogicalPlan): LogicalPlan = {
    val dualRun =
      conf.getConf(SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER) &&
      !conf.getConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED) &&
      !conf.getConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY) &&
      checkResolverGuard(plan) &&
      checkDualRunSampleRate()

    withTrackedAnalyzerBridgeState(dualRun) {
      if (conf.getConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED)) {
        resolveInSinglePass(plan)
      } else if (conf.getConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY)) {
        resolveInSinglePassTentatively(plan)
      } else if (dualRun) {
        resolveInDualRun(plan)
      } else {
        resolveInFixedPoint(plan)
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
  private def resolveInDualRun(plan: LogicalPlan): LogicalPlan = {
    var fixedPointException: Option[Throwable] = None
    val fixedPointResult = try {
      val (resolutionDuration, result) = recordDuration {
        Some(resolveInFixedPoint(plan))
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
        Some(resolveInSinglePass(plan, inDualRun = true))
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
              fixedPointEx
            )
        }
      case None =>
        singlePassException match {
          case Some(_: ExplicitlyUnsupportedResolverFeature) =>
            fixedPointResult.get
          case Some(singlePassEx) =>
            throw QueryCompilationErrors.singlePassFailedFixedPointSucceeded(
              fixedPointResult.get,
              singlePassEx
            )
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
   * [[resolveInSinglePassTentatively]] tries to run the single-pass [[ResolverRunner]] first, and
   * falls back to the fixed-point [[Analyzer]] if the plan is not supported or the single-pass
   * resolution fails.
   *
   * The plan is validated by the [[ResolverGuard]] to avoid running the single-pass analyzer on
   * unsupported plans. Running the single-pass analyzer on unsupported plans would result in extra
   * table metadata lookups, extra CPU/RAM overhead, and potentially unexpected behavior.
   *
   * Some exceptions thrown from the single-pass Analyzer are considered unrecoverable - for
   * example, exceptions thrown from the metadata resolver. These will be just
   * rethrown without falling back to the fixed-point Analyzer.
   */
  private def resolveInSinglePassTentatively(plan: LogicalPlan): LogicalPlan = {
    logInfo(log"trying to run single-pass analyzer in fallback (tentative) mode")

    val singlePassResult = if (checkResolverGuard(plan)) {
      tryRunSinglePassInTentativeMode(plan)
    } else {
      None
    }

    singlePassResult match {
      case Some(resolvedPlan) =>
        logInfo(log"single-pass analyzer successfully resolved the query plan")
        resolvedPlan

      case None =>
        resolveInFixedPoint(plan)
    }
  }

  /**
   * Invoke the single-pass Analyzer in tentative mode with proper error handling and log any
   * unexpected issues.
   *
   * See [[resolveInSinglePassTentatively]] for more details.
   */
  private def tryRunSinglePassInTentativeMode(plan: LogicalPlan): Option[LogicalPlan] = {
    try {
      Some(resolveInSinglePass(plan))
    } catch {
      case ex: ExplicitlyUnsupportedResolverFeature =>
        logInfo(
          "single-pass analyzer is falling back to the fixed-point analyzer because " +
          s"of an ExplicitlyUnsupportedResolverFeature: $ex"
        )

        None
      case ex: Throwable =>
        logInfo(
          "single-pass analyzer is falling back to the fixed-point analyzer because " +
          s"of an exception: $ex"
        )

        None
    }
  }

  /**
   * This method is used to run the single-pass Analyzer which will return the resolved plan
   * or throw an exception if the resolution fails. Both cases are handled in the caller method.
   *
   * When this method is called when single-pass is resolved in dual-runs, heavy resolution checks
   * are performed only in testing mode to avoid regressing the production workload performance.
   */
  private def resolveInSinglePass(plan: LogicalPlan, inDualRun: Boolean = false): LogicalPlan = {
    val resolverRunner = new ResolverRunner(
      resolver = resolver,
      extendedResolutionChecks = extendedResolutionChecks
    )

    resolverRunner.resolve(
      plan = plan,
      analyzerBridgeState = AnalysisContext.get.getSinglePassResolverBridgeState,
      tracker = tracker
    )
  }

  /**
   * This method is used to run the legacy Analyzer which will return the resolved plan
   * or throw an exception if the resolution fails. Both cases are handled in the caller method.
   * */
  private def resolveInFixedPoint(plan: LogicalPlan): LogicalPlan = {
    val resolvedPlan = legacyAnalyzer.executeAndTrack(plan, tracker)
    QueryPlanningTracker.withTracker(tracker) {
      legacyAnalyzer.checkAnalysis(resolvedPlan)
    }
    resolvedPlan
  }

  private def checkDualRunSampleRate(): Boolean = {
    sampleRateGenerator.nextDouble() < conf.getConf(SQLConf.ANALYZER_DUAL_RUN_SAMPLE_RATE)
  }

  private def validateLogicalPlans(
      fixedPointResult: LogicalPlan,
      singlePassResult: LogicalPlan): Unit = {
    if (fixedPointResult.schema != singlePassResult.schema) {
      throw QueryCompilationErrors.hybridAnalyzerOutputSchemaComparisonMismatch(
        fixedPointResult.schema,
        singlePassResult.schema
      )
    }

    compareLogicalPlans(
      fixedPointResult = fixedPointResult,
      singlePassResult = singlePassResult
    )
  }

  private def compareLogicalPlans(
      fixedPointResult: LogicalPlan,
      singlePassResult: LogicalPlan): Unit = {
    val fixedPointNormalizedResult = normalizePlan(fixedPointResult)
    val singlePassNormalizedResult = normalizePlan(singlePassResult)
    if (fixedPointNormalizedResult != singlePassNormalizedResult) {
      throw QueryCompilationErrors.hybridAnalyzerLogicalPlanComparisonMismatch(
        fixedPointOutput = fixedPointNormalizedResult,
        singlePassOutput = singlePassNormalizedResult
      )
    }
  }

  private def checkResolverGuard(plan: LogicalPlan): Boolean = {
    try {
      resolverGuard.apply(plan).planUnsupportedReason.isEmpty
    } catch {
      case _: Throwable
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
   * Confs that views should inherit from current session rather then from their descriptor.
   */
  val SESSION_LEVEL_CONFS_FOR_VIEWS: Seq[String] = Seq(
    SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key,
    SQLConf.ANALYZER_DUAL_RUN_RETURN_SINGLE_PASS_RESULT.key,
    SQLConf.ANALYZER_DUAL_RUN_SAMPLE_RATE.key,
    SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key,
    SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key,
    SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_EXPOSE_RESOLVER_GUARD_FAILURE.key,
    SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_PREVENT_USING_ALIASES_FROM_NON_DIRECT_CHILDREN.key,
    SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_RELATION_BRIDGING_ENABLED.key,
    SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_RUN_EXTENDED_RESOLUTION_CHECKS.key,
    SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_THROW_FROM_RESOLVER_GUARD.key,
    SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_VALIDATION_ENABLED.key
  )

  /**
   * Creates a new [[HybridAnalyzer]] instance from the legacy [[Analyzer]]. Currently most of the
   * necessary objects reside within the [[Analyzer]] itself. This is until it's replaced by the
   * new [[Resolver]].
   */
  def fromLegacyAnalyzer(
      legacyAnalyzer: Analyzer,
      tracker: QueryPlanningTracker): HybridAnalyzer = {
    val relationResolution = legacyAnalyzer.getRelationResolution
    new HybridAnalyzer(
      legacyAnalyzer = legacyAnalyzer,
      resolverGuard = new ResolverGuard(
        catalogManager = legacyAnalyzer.catalogManager,
        tracker = Some(tracker)
      ),
      resolver = new Resolver(
        catalogManager = legacyAnalyzer.catalogManager,
        sharedRelationCache = legacyAnalyzer.sharedRelationCache,
        extensions = legacyAnalyzer.singlePassResolverExtensions,
        metadataResolverExtensions = legacyAnalyzer.singlePassMetadataResolverExtensions,
        externalRelationResolution = Some(relationResolution),
        extendedRewriteRules = legacyAnalyzer.singlePassPostHocResolutionRules,
        tracker = Some(tracker)
      ),
      tracker = tracker,
      extendedResolutionChecks = legacyAnalyzer.singlePassExtendedResolutionChecks
    )
  }
}
