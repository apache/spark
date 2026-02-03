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

import com.databricks.spark.util.LoggingGuard
import com.databricks.sql.DatabricksSQLConf

import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.LOGICAL_PLAN
import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.sql.catalyst.{MetricKey, QueryPlanningTracker, SQLConfHelper}
import org.apache.spark.sql.catalyst.analysis.{AnalysisContext, Analyzer}
import org.apache.spark.sql.catalyst.plans.NormalizePlan
import org.apache.spark.sql.catalyst.plans.logical.{AnalysisHelper, LogicalPlan}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

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
    catalogObjectGuard: CatalogObjectGuard,
    resolver: Resolver,
    tracker: QueryPlanningTracker,
    extendedResolutionChecks: Seq[LogicalPlan => Unit] = Seq.empty,
    heavyExtendedResolutionChecks: Seq[LogicalPlan => Unit] = Seq.empty)
    extends SQLConfHelper
    with Logging {
  private var singlePassResolutionDuration: Option[Long] = None
  private var fixedPointResolutionDuration: Option[Long] = None
  private val sampleRateGenerator = new Random()

  def apply(plan: LogicalPlan): LogicalPlan = {
    recordHybridAnalyzerMetrics() // EDGE
    val dualRun =
      conf.getConf(SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER) &&
      !conf.getConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED) &&
      !conf.getConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY) &&
      checkResolverGuardAndUpdateResolutionGap(plan, isDualRun = true) &&
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

  // BEGIN-EDGE
  def getSinglePassResolutionDuration: Option[Long] = singlePassResolutionDuration

  def getFixedPointResolutionDuration: Option[Long] = fixedPointResolutionDuration
  // END-EDGE

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
    val logDualRunResult =
      conf.getConf(SQLConf.ANALYZER_LOG_ERRORS_INSTEAD_OF_THROWING_IN_DUAL_RUNS)

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
        val loggingGuardHandle = LoggingGuard.create(loggingEnabled = false)
        loggingGuardHandle.runWith {
          Some(resolveInSinglePass(plan, inDualRun = true))
        }
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
            handleFixedPointFailedSinglePassSucceeded(
              logDualRunResult = logDualRunResult,
              singlePassResult = singlePassResult.get,
              fixedPointException = fixedPointEx
            )
        }
      case None =>
        singlePassException match {
          case Some(singlePassEx: ExplicitlyUnsupportedResolverFeature) =>
            handleExplicitlyUnsupportedResolverFeature(
              logDualRunResult = logDualRunResult,
              singlePassException = singlePassEx,
              fixedPointResult = fixedPointResult.get
            )
          case Some(singlePassEx) =>
            handleSinglePassFailedFixedPointSucceeded(
              logDualRunResult = logDualRunResult,
              singlePassException = singlePassEx,
              fixedPointResult = fixedPointResult.get
            )
          case None =>
            if (validateLogicalPlans(
                fixedPointResult = fixedPointResult.get,
                singlePassResult = singlePassResult.get,
                logDualRunResult = logDualRunResult
              )) {
              handleDualRunSucceeded(
                fixedPointResult = fixedPointResult.get,
                singlePassResult = singlePassResult.get,
                logDualRunResult = logDualRunResult
              )
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
   * The tentative mode can optionally be randomly sampled using the
   * [[ANALYZER_SINGLE_PASS_RESOLVER_TENTATIVE_MODE_SAMPLE_RATE]] setting.
   *
   * The plan is validated holistically by the [[ResolverGuard]] and by the [[CatalogObjectGuard]]
   * to avoid running the single-pass analyzer on unsupported plans. Running the single-pass
   * analyzer on unsupported plans would result in extra table metadata lookups, extra CPU/RAM
   * overhead, and potentially unexpected behavior.
   *
   * Since the plan is validated holistically by the [[ResolverGuard]] and by the
   * [[CatalogObjectGuard]], [[ExplicitlyUnsupportedResolverFeature]] thrown from the single-pass
   * Analyzer is considered a bug, so we expose it in testing mode.
   *
   * Some exceptions thrown from the single-pass Analyzer are considered unrecoverable - for
   * example, exceptions thrown from the [[UnityCatalogMetadataResolver]]. These will be just
   * rethrown without falling back to the fixed-point Analyzer. See [[CatalogObjectGuard.apply]]
   * for more details.
   *
   * See [[AnalyzerFallbackSuite]] for the real-world examples of fallback mechanism.
   */
  private def resolveInSinglePassTentatively(plan: LogicalPlan): LogicalPlan = {
    logInfo(log"trying to run single-pass analyzer in fallback (tentative) mode")

    val singlePassResult = if (checkResolverGuardAndUpdateResolutionGap(plan, isDualRun = false)) {
      if (checkTentativeModeSampleRate()) {
        tryRunSinglePassInTentativeMode(plan)
      } else {
        updateTentativeModeTelemetry(
          MetricKey.SINGLE_PASS_ANALYZER_TENTATIVE_MODE_FALLBACK_DUE_TO_SAMPLE_RATE_LIMIT
        )

        None
      }
    } else {
      None
    }

    singlePassResult match {
      case Some(ResolverRunnerResultResolvedPlan(resolvedPlan)) =>
        updateTentativeModeTelemetry(MetricKey.SINGLE_PASS_ANALYZER_TENTATIVE_MODE_SUCCEEDED)

        logInfo(log"single-pass analyzer successfully resolved the query plan")

        resolvedPlan

      case Some(ResolverRunnerResultUnrecoverableException(ex)) =>
        updateTentativeModeTelemetry(
          MetricKey.SINGLE_PASS_ANALYZER_TENTATIVE_MODE_UNRECOVERABLE_EXCEPTION,
          Some(ex)
        )

        throw ex

      case Some(ResolverRunnerResultPlanNotSupported(reason)) =>
        updateTentativeModeTelemetry(
          MetricKey.SINGLE_PASS_ANALYZER_TENTATIVE_MODE_FALLBACK_DUE_TO_UNSUPPORTED_PLAN,
          gap = Some(reason)
        )

        logInfo(
          log"single-pass analyzer is falling back to the fixed-point analyzer because " +
          log"plan is not supported: ${MDC(LOGICAL_PLAN, reason)}"
        )

        resolveInFixedPoint(plan)

      case None =>
        resolveInFixedPoint(plan)
    }
  }

  /**
   * Invoke the single-pass Analyzer in tentative mode with proper error handling based on the flags
   * set and log any unexpected issues.
   *
   * See [[resolveInSinglePassTentatively]] for more details.
   */
  private def tryRunSinglePassInTentativeMode(plan: LogicalPlan): Option[ResolverRunnerResult] = {
    val exposeExplicitlyUnsupportedFeature = conf.getConf(
      SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_EXPOSE_EXPLICITLY_UNSUPPORTED_FEATURE_IN_TENTATIVE_MODE
    )
    val exposeFailures = conf.getConf(
      SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_EXPOSE_FAILURES_IN_TENTATIVE_MODE
    )

    try {
      Some(resolveInSinglePassWithCatalogObjectsCheck(plan))
    } catch {
      case ex: ExplicitlyUnsupportedResolverFeature =>
        if (exposeExplicitlyUnsupportedFeature) {
          throw ex
        }

        logInfo(
          "single-pass analyzer is falling back to the fixed-point analyzer because " +
          s"of an ExplicitlyUnsupportedResolverFeature: $ex"
        )

        updateTentativeModeTelemetry(
          // scalastyle:off line.size.limit
          MetricKey.SINGLE_PASS_ANALYZER_TENTATIVE_MODE_FALLBACK_DUE_TO_EXPLICITLY_UNSUPPORTED_FEATURE,
          // scalastyle:on line.size.limit
          Some(ex)
        )

        None
      case ex: Throwable =>
        if (exposeFailures) {
          throw ex
        }

        logInfo(
          "single-pass analyzer is falling back to the fixed-point analyzer because " +
          s"of an exception: $ex"
        )

        updateTentativeModeTelemetry(
          MetricKey.SINGLE_PASS_ANALYZER_TENTATIVE_MODE_FALLBACK_DUE_TO_EXCEPTION,
          Some(ex)
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
    val resolutionChecks = if (!Utils.isTesting && inDualRun) {
      extendedResolutionChecks
    } else {
      extendedResolutionChecks ++ heavyExtendedResolutionChecks
    }

    val resolverRunner = new ResolverRunner(
      resolver = resolver,
      catalogObjectGuard = catalogObjectGuard,
      extendedResolutionChecks = resolutionChecks
    )

    resolverRunner.resolve(
      plan = plan,
      analyzerBridgeState = AnalysisContext.get.getSinglePassResolverBridgeState,
      tracker = tracker
    )
  }

  /**
   * This method is used to run the single-pass Analyzer which will return the resolved plan
   * or throw an exception if the resolution fails. Both cases are handled in the caller method.
   *
   * If [[SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_CHECK_CATALOG_OBJECTS_IN_TENTATIVE_MODE]] is set to
   * `true`, it will first check if the plan can be processed by the single-pass Analyzer using the
   * [[CatalogObjectGuard]].
   *
   * This method is call when single-pass is resolved in fallback mode and thus always performs
   * heavy resolution checks to stay compatible with the legacy result.
   */
  private def resolveInSinglePassWithCatalogObjectsCheck(
      plan: LogicalPlan): ResolverRunnerResult = {
    val resolverRunner = new ResolverRunner(
      resolver = resolver,
      catalogObjectGuard = catalogObjectGuard,
      extendedResolutionChecks = extendedResolutionChecks ++ heavyExtendedResolutionChecks
    )

    resolverRunner.resolve(
      plan = plan,
      checkCatalogObjects = conf.getConf(
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_CHECK_CATALOG_OBJECTS_IN_TENTATIVE_MODE
      ),
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

  private def checkTentativeModeSampleRate(): Boolean = {
    sampleRateGenerator.nextDouble() < conf.getConf(
      SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_TENTATIVE_MODE_SAMPLE_RATE
    )
  }

  private def handleFixedPointFailedSinglePassSucceeded(
      logDualRunResult: Boolean,
      singlePassResult: LogicalPlan,
      fixedPointException: Throwable) = {
    val wrappedException = QueryCompilationErrors.fixedPointFailedSinglePassSucceeded(
      singlePassResult,
      fixedPointException
    )
    if (logDualRunResult) {
      updateDualRunTelemetry(
        MetricKey.SINGLE_PASS_ANALYZER_DUAL_RUN_MODE_FIXED_POINT_FAILED_SINGLE_PASS_SUCCEEDED,
        Some(wrappedException)
      )
      throw fixedPointException
    } else {
      throw wrappedException
    }
  }

  private def handleExplicitlyUnsupportedResolverFeature(
      logDualRunResult: Boolean,
      singlePassException: Throwable,
      fixedPointResult: LogicalPlan): LogicalPlan = {
    if (!conf.getConf(
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_EXPOSE_EXPLICITLY_UNSUPPORTED_FEATURE_IN_DUAL_RUNS
      )) {
      fixedPointResult
    } else if (logDualRunResult) {
      updateDualRunTelemetry(
        MetricKey.SINGLE_PASS_ANALYZER_DUAL_RUN_MODE_SINGLE_PASS_FAILED_FIXED_POINT_SUCCEEDED,
        Some(singlePassException)
      )
      fixedPointResult
    } else {
      throw singlePassException
    }
  }

  private def handleSinglePassFailedFixedPointSucceeded(
      logDualRunResult: Boolean,
      singlePassException: Throwable,
      fixedPointResult: LogicalPlan): LogicalPlan = {
    if (logDualRunResult) {
      updateDualRunTelemetry(
        MetricKey.SINGLE_PASS_ANALYZER_DUAL_RUN_MODE_SINGLE_PASS_FAILED_FIXED_POINT_SUCCEEDED,
        Some(singlePassException)
      )
      fixedPointResult
    } else {
      throw QueryCompilationErrors.singlePassFailedFixedPointSucceeded(
        fixedPointResult = fixedPointResult,
        singlePassException = singlePassException
      )
    }
  }

  private def validateLogicalPlans(
      fixedPointResult: LogicalPlan,
      singlePassResult: LogicalPlan,
      logDualRunResult: Boolean) = {
    if (fixedPointResult.schema != singlePassResult.schema) {
      handleSchemaMismatch(
        logDualRunResult = logDualRunResult,
        fixedPointResult = fixedPointResult,
        singlePassResult = singlePassResult
      )
      false
    } else {
      compareLogicalPlans(
        logDualRunResult = logDualRunResult,
        fixedPointResult = fixedPointResult,
        singlePassResult = singlePassResult
      )
    }
  }

  private def handleSchemaMismatch(
      logDualRunResult: Boolean,
      singlePassResult: LogicalPlan,
      fixedPointResult: LogicalPlan): Unit = {
    val outputSchemaMismatchException =
      QueryCompilationErrors.hybridAnalyzerOutputSchemaComparisonMismatch(
        fixedPointResult.schema,
        singlePassResult.schema
      )
    if (logDualRunResult) {
      updateDualRunTelemetry(
        MetricKey.SINGLE_PASS_ANALYZER_DUAL_RUN_MODE_SCHEMA_MISMATCH,
        Some(outputSchemaMismatchException)
      )
    } else {
      throw outputSchemaMismatchException
    }
  }

  private def compareLogicalPlans(
      logDualRunResult: Boolean,
      fixedPointResult: LogicalPlan,
      singlePassResult: LogicalPlan): Boolean = {
    try {
      val fixedPointNormalizedResult = normalizePlan(fixedPointResult)
      val singlePassNormalizedResult = normalizePlan(singlePassResult)
      if (fixedPointNormalizedResult == singlePassNormalizedResult) {
        true
      } else {
        handleLogicalPlanMismatch(
          logDualRunResult = logDualRunResult,
          fixedPointResult = fixedPointNormalizedResult,
          singlePassResult = singlePassNormalizedResult
        )
        false
      }
    } catch {
      case e: Throwable =>
        handlePlanNormalizationFailure(
          logDualRunResult = logDualRunResult,
          exception = e
        )
        false
    }
  }

  private def handleLogicalPlanMismatch(
      logDualRunResult: Boolean,
      singlePassResult: LogicalPlan,
      fixedPointResult: LogicalPlan): Unit = {
    val (fixedPointTrimmedPlan, singlePassTrimmedPlan) = LogicalPlanDifference(
      lhsPlan = fixedPointResult,
      rhsPlan = singlePassResult,
      contextSize = conf.getConf(
        DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_LOGICAL_PLAN_DIFF_CONTEXT_SIZE
      )
    )
    val (fixedPointPlanString, singlePassPlanString) =
      if (conf.getConf(DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_TRIM_LOGICAL_PLAN_DIFF)) {
        (fixedPointTrimmedPlan, singlePassTrimmedPlan)
      } else {
        (fixedPointResult.toString, singlePassResult.toString)
      }
    val logicalPlanMismatchException =
      QueryCompilationErrors.hybridAnalyzerLogicalPlanComparisonMismatch(
        fixedPointOutput = fixedPointPlanString,
        singlePassOutput = singlePassPlanString
      )
    if (logDualRunResult) {
      updateDualRunTelemetry(
        MetricKey.SINGLE_PASS_ANALYZER_DUAL_RUN_MODE_LOGICAL_PLAN_MISMATCH,
        Some(logicalPlanMismatchException)
      )
    } else {
      throw logicalPlanMismatchException
    }
  }

  private def handlePlanNormalizationFailure(
      logDualRunResult: Boolean,
      exception: Throwable): Unit = {
    if (logDualRunResult) {
      updateDualRunTelemetry(
        MetricKey.SINGLE_PASS_ANALYZER_DUAL_RUN_MODE_SINGLE_PASS_FAILED_FIXED_POINT_SUCCEEDED,
        Some(exception)
      )
    } else {
      throw exception
    }
  }

  private def handleDualRunSucceeded(
      logDualRunResult: Boolean,
      singlePassResult: LogicalPlan,
      fixedPointResult: LogicalPlan) = {
    if (logDualRunResult) {
      updateDualRunTelemetry(
        MetricKey.SINGLE_PASS_ANALYZER_DUAL_RUN_MODE_SUCCEEDED
      )
    }
    if (conf.getConf(SQLConf.ANALYZER_DUAL_RUN_RETURN_SINGLE_PASS_RESULT)) {
      singlePassResult
    } else {
      fixedPointResult
    }
  }

  private def updateDualRunTelemetry(
      metricKey: MetricKey.MetricKey,
      exception: Option[Throwable] = None,
      gap: Option[String] = None): Unit = {
    updateTelemetry(
      metricKeys = Seq(MetricKey.SINGLE_PASS_ANALYZER_DUAL_RUN_MODE, metricKey),
      exception = exception,
      gap = gap
    )
  }

  private def updateTentativeModeTelemetry(
      metricKey: MetricKey.MetricKey,
      exception: Option[Throwable] = None,
      gap: Option[String] = None): Unit = {
    updateTelemetry(
      metricKeys = Seq(MetricKey.SINGLE_PASS_ANALYZER_TENTATIVE_MODE, metricKey),
      exception = exception,
      gap = gap
    )
  }

  private def updateTelemetry(
      metricKeys: Seq[MetricKey.MetricKey],
      exception: Option[Throwable] = None,
      gap: Option[String] = None): Unit = {
    val currentAnalyzerNumberOfInvocations =
      tracker.getMetric(MetricKey.HYBRID_ANALYZER_NUMBER_OF_INVOCATIONS).sum
    metricKeys.foreach(tracker.setNumericMetric(_, currentAnalyzerNumberOfInvocations))
    exception match {
      case Some(exception) =>
        tracker.setSinglePassAnalyzerException(exception)
      case None =>
    }
    gap match {
      case Some(reason) =>
        tracker.setSinglePassAnalyzerResolutionGap(reason)
      case None =>
    }
  }

  private def checkResolverGuardAndUpdateResolutionGap(
      plan: LogicalPlan,
      isDualRun: Boolean): Boolean = {
    try {
      resolverGuard.apply(plan).planUnsupportedReason match {
        case Some(reason) =>
          handlePlanUnsupportedByResolverGuard(reason, isDualRun)
          false
        case None =>
          true
      }
    } catch {
      case e: Throwable
          if !conf.getConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_EXPOSE_RESOLVER_GUARD_FAILURE) =>
        tracker.setSinglePassAnalyzerResolutionGap("ResolverGuard failure")
        false
    }
  }

  private def handlePlanUnsupportedByResolverGuard(reason: String, isDualRun: Boolean): Unit = {
    if (isDualRun) {
      updateDualRunTelemetry(
        MetricKey.SINGLE_PASS_ANALYZER_DUAL_RUN_MODE_NOT_ATTEMPTED_DUE_TO_UNSUPPORTED_PLAN,
        gap = Some(reason)
      )
    } else {
      updateTentativeModeTelemetry(
        MetricKey.SINGLE_PASS_ANALYZER_TENTATIVE_MODE_FALLBACK_DUE_TO_UNSUPPORTED_PLAN,
        gap = Some(reason)
      )
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
  // BEGIN-EDGE

  private def recordHybridAnalyzerMetrics(): Unit = {
    recordEffectiveFeature(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY)

    if (!conf.getConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY)) {
      recordEffectiveFeature(SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER)
    }

    tracker.incrementMetric(MetricKey.HYBRID_ANALYZER_NUMBER_OF_INVOCATIONS, 1.0)
  }

  /**
   * Record whether a feature is effectively enabled or not based on the value of the given flag.
   * This is used for better metrics on InsightRamp dashboard.
   */
  private def recordEffectiveFeature(feature: ConfigEntry[Boolean]): Unit = {
    tracker.recordEffectiveFeature(feature, conf.getConf(feature))
  }
  // END-EDGE
}

object HybridAnalyzer {

  /**
   * Confs that views should inherit from current session rather then from their descriptor.
   */
  val SESSION_LEVEL_CONFS_FOR_VIEWS: Seq[String] = Seq(
    // scalastyle:off line.size.limit
    // General single-pass analyzer configs.
    SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key,
    SQLConf.ANALYZER_DUAL_RUN_RETURN_SINGLE_PASS_RESULT.key,
    SQLConf.ANALYZER_DUAL_RUN_SAMPLE_RATE.key,
    SQLConf.ANALYZER_LOG_ERRORS_INSTEAD_OF_THROWING_IN_DUAL_RUNS.key,
    SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_CHECK_CATALOG_OBJECTS_IN_TENTATIVE_MODE.key,
    SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key,
    SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key,
    SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_EXPOSE_EXPLICITLY_UNSUPPORTED_FEATURE_IN_DUAL_RUNS.key,
    SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_EXPOSE_EXPLICITLY_UNSUPPORTED_FEATURE_IN_TENTATIVE_MODE.key,
    SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_EXPOSE_FAILURES_IN_TENTATIVE_MODE.key,
    SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_EXPOSE_RESOLVER_GUARD_FAILURE.key,
    SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_MARK_RESOLVED_PLAN.key,
    SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_PREVENT_USING_ALIASES_FROM_NON_DIRECT_CHILDREN.key,
    SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_RELATION_BRIDGING_ENABLED.key,
    SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_RESOLVE_UC_METADATA_FOR_TEMP_VIEWS_IN_TENTATIVE_MODE.key,
    SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_RUN_EXTENDED_RESOLUTION_CHECKS.key,
    SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_TENTATIVE_MODE_SAMPLE_RATE.key,
    SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_THROW_FROM_RESOLVER_GUARD.key,
    SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_VALIDATION_ENABLED.key,
    // Single-pass feature enablement.
    DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_EXPERIMENTAL_FEATURES.key,
    DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_EXPERIMENTAL_FUNCTIONS.key,
    DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_EXPLAIN_NODE_RESOLUTION.key,
    DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_EXTENDED_STAR_USE_CASES_RESOLUTION.key,
    DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_EXTRACT_VALUE_RESOLUTION.key,
    DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_GET_STRUCT_FIELD_RESOLUTION.key,
    DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_GROUPING_ANALYTICS_RESOLUTION.key,
    DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_HIGHER_ORDER_FUNCTIONS_RESOLUTION.key,
    DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_RECURSIVE_CTE_RESOLUTION.key,
    DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_NAME_PLACEHOLDER_RESOLUTION.key,
    DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_PARAMETER_RESOLUTION.key,
    DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_PIVOT_RESOLUTION.key,
    DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_REPARTITION_BY_EXPRESSION_RESOLUTION.key,
    DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_SCRIPTING_VARIABLE_RESOLUTION.key,
    DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_TRUSTED_PLAN_RESOLUTION.key,
    DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_TEMP_VARIABLE_RESOLUTION.key,
    DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_TVF_RESOLUTION.key,
    DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_UNPIVOT_RESOLUTION.key,
    DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_WINDOW_RESOLUTION.key,
    // Plan difference logging should be consistent across the entire query.
    DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_LOGICAL_PLAN_DIFF_CONTEXT_SIZE.key,
    DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_TRIM_LOGICAL_PLAN_DIFF.key,
    // Flag for compatibility with fixed-point analyzer.
    DatabricksSQLConf.DEDUPLICATE_SOURCE_WINDOW_EXPRESSIONS.key,
    // LCA on implicit aliases should be disabled across the entire query.
    DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ALLOW_LCA_ON_IMPLICIT_ALIAS.key
    // scalastyle:on line.size.limit
  )

  val METRIC_KEYS = Set(
    MetricKey.SINGLE_PASS_ANALYZER_DUAL_RUN_MODE,
    MetricKey.SINGLE_PASS_ANALYZER_DUAL_RUN_MODE_SUCCEEDED,
    MetricKey.SINGLE_PASS_ANALYZER_DUAL_RUN_MODE_NOT_ATTEMPTED_DUE_TO_UNSUPPORTED_PLAN,
    MetricKey.SINGLE_PASS_ANALYZER_DUAL_RUN_MODE_FIXED_POINT_FAILED_SINGLE_PASS_SUCCEEDED,
    MetricKey.SINGLE_PASS_ANALYZER_DUAL_RUN_MODE_SINGLE_PASS_FAILED_FIXED_POINT_SUCCEEDED,
    MetricKey.SINGLE_PASS_ANALYZER_DUAL_RUN_MODE_SCHEMA_MISMATCH,
    MetricKey.SINGLE_PASS_ANALYZER_DUAL_RUN_MODE_LOGICAL_PLAN_MISMATCH,
    MetricKey.SINGLE_PASS_ANALYZER_DUAL_RUN_MODE_EXCEPTION_MISMATCH,
    MetricKey.SINGLE_PASS_ANALYZER_RESOLVER_GUARD_DETECTED_SECOND_PASS_ANALYSIS,
    MetricKey.SINGLE_PASS_ANALYZER_TENTATIVE_MODE,
    MetricKey.SINGLE_PASS_ANALYZER_TENTATIVE_MODE_SUCCEEDED,
    MetricKey.SINGLE_PASS_ANALYZER_TENTATIVE_MODE_UNRECOVERABLE_EXCEPTION,
    MetricKey.SINGLE_PASS_ANALYZER_TENTATIVE_MODE_FALLBACK_DUE_TO_UNSUPPORTED_PLAN,
    MetricKey.SINGLE_PASS_ANALYZER_TENTATIVE_MODE_FALLBACK_DUE_TO_EXCEPTION,
    MetricKey.SINGLE_PASS_ANALYZER_TENTATIVE_MODE_FALLBACK_DUE_TO_EXPLICITLY_UNSUPPORTED_FEATURE,
    MetricKey.SINGLE_PASS_ANALYZER_TENTATIVE_MODE_FALLBACK_DUE_TO_SAMPLE_RATE_LIMIT
  )

  /**
   * Creates a new [[HybridAnalyzer]] instance from the legacy [[Analyzer]]. Currently most of the
   * necessary objects reside within the [[Analyzer]] itself. This is until it's replaced by the
   * new [[Resolver]].
   */
  def fromLegacyAnalyzer(
      legacyAnalyzer: Analyzer,
      tracker: QueryPlanningTracker): HybridAnalyzer = {
    new HybridAnalyzer(
      legacyAnalyzer = legacyAnalyzer,
      resolverGuard = new ResolverGuard(
        catalogManager = legacyAnalyzer.catalogManager,
        multiStatementTransactionAccessor = legacyAnalyzer.multiStatementTransactionAccessor,
        tracker = Some(tracker)
      ),
      catalogObjectGuard = new CatalogObjectGuard(
        catalogManager = legacyAnalyzer.catalogManager,
        relationResolution = legacyAnalyzer.getRelationResolution,
        functionResolution = legacyAnalyzer.getFunctionResolution,
        tracker = Some(tracker)
      ),
      resolver = new Resolver(
        catalogManager = legacyAnalyzer.catalogManager,
        sharedRelationCache = legacyAnalyzer.sharedRelationCache,
        extensions = legacyAnalyzer.singlePassResolverExtensions,
        metadataResolverExtensions = legacyAnalyzer.singlePassMetadataResolverExtensions,
        externalRelationResolution = Some(legacyAnalyzer.getRelationResolution),
        extendedRewriteRules = legacyAnalyzer.singlePassPostHocResolutionRules,
        tracker = Some(tracker)
      ),
      tracker = tracker,
      extendedResolutionChecks = legacyAnalyzer.singlePassExtendedResolutionChecks,
      heavyExtendedResolutionChecks = legacyAnalyzer.singlePassHeavyExtendedResolutionChecks
    )
  }
}
