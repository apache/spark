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

package org.apache.spark.sql.analysis.resolver

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.catalyst.{ExtendedAnalysisException, QueryPlanningTracker}
import org.apache.spark.sql.catalyst.analysis.{
  AnalysisContext,
  Analyzer,
  UnresolvedAttribute,
  UnresolvedStar
}
import org.apache.spark.sql.catalyst.analysis.resolver._
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.NormalizePlan
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, MetadataBuilder}

/**
 * Base trait for HybridAnalyzer test suites containing common test utilities and helper classes.
 */
trait HybridAnalyzerSuiteBase extends QueryTest with SharedSparkSession {
  protected val col1Integer = AttributeReference("col1", IntegerType)()
  protected val col2Integer = AttributeReference("col2", IntegerType)()
  protected val col2IntegerWithMetadata = AttributeReference(
    "col2",
    IntegerType,
    metadata = (new MetadataBuilder).putString("comment", "this is an integer").build()
  )()
  protected val unresolvedPlan: LogicalPlan = {
    Project(
      Seq(UnresolvedStar(None)),
      LocalRelation(col1Integer)
    )
  }
  protected val malformedUnresolvedPlan: LogicalPlan =
    Project(
      Seq(UnresolvedAttribute("nonexistent_col")),
      LocalRelation(col1Integer)
    )
  protected val resolvedPlan =
    Project(
      Seq(col1Integer),
      LocalRelation(Seq(col1Integer))
    )
  protected val malformedResolvedPlan: LogicalPlan =
    Project(
      Seq(col1Integer),
      Project(
        Seq(col1Integer),
        LocalRelation(Seq(col1Integer))
      )
    )

  protected def validateSinglePassResolverBridgeState(bridgeRelations: Boolean): Unit = {
    assert(bridgeRelations == AnalysisContext.get.getSinglePassResolverBridgeState.isDefined)
  }

  protected class BrokenResolver(ex: Throwable, bridgeRelations: Boolean = false)
      extends Resolver(spark.sessionState.catalogManager) {
    override def lookupMetadataAndResolve(
        plan: LogicalPlan,
        analyzerBridgeState: Option[AnalyzerBridgeState] = None): LogicalPlan = {
      validateSinglePassResolverBridgeState(bridgeRelations)
      throw ex
    }
  }

  protected class BrokenResolverGuard(catalogManager: CatalogManager)
      extends ResolverGuard(catalogManager) {
    override def apply(plan: LogicalPlan): ResolverGuardResult = {
      throw new Exception("Broken resolver guard")
    }
  }

  protected class RestrictiveResolverGuard(
      catalogManager: CatalogManager,
      reason: String = "test restriction")
      extends ResolverGuard(catalogManager) {
    override def apply(plan: LogicalPlan): ResolverGuardResult = {
      ResolverGuardResult(Some(reason))
    }
  }

  protected class ValidatingResolver(bridgeRelations: Boolean)
      extends Resolver(spark.sessionState.catalogManager) {
    override def lookupMetadataAndResolve(
        plan: LogicalPlan,
        analyzerBridgeState: Option[AnalyzerBridgeState] = None): LogicalPlan = {
      validateSinglePassResolverBridgeState(bridgeRelations)
      super.lookupMetadataAndResolve(plan, analyzerBridgeState)
    }
  }

  protected class HardCodedResolver(resolvedPlan: LogicalPlan, bridgeRelations: Boolean)
      extends Resolver(spark.sessionState.catalogManager) {
    override def lookupMetadataAndResolve(
        plan: LogicalPlan,
        analyzerBridgeState: Option[AnalyzerBridgeState] = None): LogicalPlan = {
      validateSinglePassResolverBridgeState(bridgeRelations)
      resolvedPlan
    }
  }

  protected class ValidatingAnalyzer(bridgeRelations: Boolean)
      extends Analyzer(spark.sessionState.catalogManager) {
    override def executeAndTrack(plan: LogicalPlan, tracker: QueryPlanningTracker): LogicalPlan = {
      validateSinglePassResolverBridgeState(bridgeRelations)
      super.executeAndTrack(plan, tracker)
    }
  }

  protected class BrokenAnalyzer(ex: Throwable, bridgeRelations: Boolean)
      extends Analyzer(spark.sessionState.catalogManager) {
    override def executeAndTrack(plan: LogicalPlan, tracker: QueryPlanningTracker): LogicalPlan = {
      validateSinglePassResolverBridgeState(bridgeRelations)
      throw ex
    }
  }

  protected class CustomAnalyzer(customCode: () => Unit, bridgeRelations: Boolean)
      extends Analyzer(spark.sessionState.catalogManager) {
    override def executeAndTrack(plan: LogicalPlan, tracker: QueryPlanningTracker): LogicalPlan = {
      validateSinglePassResolverBridgeState(bridgeRelations)
      customCode()
      super.executeAndTrack(plan, tracker)
    }
  }

  protected class HybridAnalyzerWithBrokenPlanNormalization(
      legacyAnalyzer: Analyzer,
      resolverGuard: ResolverGuard,
      resolver: Resolver,
      tracker: QueryPlanningTracker)
      extends HybridAnalyzer(
        legacyAnalyzer = legacyAnalyzer,
        resolverGuard = resolverGuard,
        resolver = resolver,
        tracker = tracker
      ) {
    override protected[sql] def normalizePlan(plan: LogicalPlan): LogicalPlan = {
      throw new Exception("Broken plan normalization")
    }
  }

  protected class BrokenCheckRule extends (LogicalPlan => Unit) {
    def apply(plan: LogicalPlan): Unit = {
      throw new Exception("Extended resolution check failed")
    }
  }

  protected def testDualRun(testName: String, testTags: Tag*)(testFun: => Any)(
      implicit pos: Position): Unit = {
    super.test(s"Dual run: $testName") {
      withSQLConf(
        SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "true",
        SQLConf.ANALYZER_DUAL_RUN_SAMPLE_RATE.key -> "1.0",
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_EXPOSE_RESOLVER_GUARD_FAILURE.key -> "true"
      ) {
        testFun
      }
    }
  }

  protected def assertPlansEqual(actualPlan: LogicalPlan, expectedPlan: LogicalPlan) = {
    assert(actualPlan.analyzed)
    assert(NormalizePlan(actualPlan) == NormalizePlan(expectedPlan))
  }
}

class HybridAnalyzerSuite extends HybridAnalyzerSuiteBase {

  testDualRun("Both fixed-point and single-pass analyzers pass") {
    val tracker = new QueryPlanningTracker
    assertPlansEqual(
      new HybridAnalyzer(
        new ValidatingAnalyzer(bridgeRelations = true),
        new ResolverGuard(spark.sessionState.catalogManager),
        new ValidatingResolver(bridgeRelations = true),
        tracker
      ).apply(unresolvedPlan),
      resolvedPlan
    )
  }

  testDualRun("Fixed-point analyzer passes, single-pass analyzer fails") {
    val tracker = new QueryPlanningTracker
    val error = intercept[ExtendedAnalysisException](
      new HybridAnalyzer(
        new ValidatingAnalyzer(bridgeRelations = true),
        new ResolverGuard(spark.sessionState.catalogManager),
        new BrokenResolver(
          QueryCompilationErrors.unsupportedSinglePassAnalyzerFeature("test"),
          bridgeRelations = true
        ),
        tracker
      ).apply(unresolvedPlan)
    )

    checkError(
      exception = error,
      condition = "HYBRID_ANALYZER_EXCEPTION.SINGLE_PASS_FAILED_FIXED_POINT_SUCCEEDED",
      parameters =
        Map("fixedPointOutput" -> resolvedPlan.toString)
    )
    checkError(
      exception = error.cause.get.asInstanceOf[AnalysisException],
      condition = "UNSUPPORTED_SINGLE_PASS_ANALYZER_FEATURE",
      parameters = Map("feature" -> "test")
    )
  }

  testDualRun("Fixed-point analyzer passes, single-pass analyzer fails with Stack Overflow") {
    val tracker = new QueryPlanningTracker
    val error = intercept[ExtendedAnalysisException](
      new HybridAnalyzer(
        new ValidatingAnalyzer(bridgeRelations = true),
        new ResolverGuard(spark.sessionState.catalogManager),
        new BrokenResolver(
          new StackOverflowError("Stack Overflow"),
          bridgeRelations = true
        ),
        tracker
      ).apply(unresolvedPlan)
    )

    checkError(
      exception = error,
      condition = "HYBRID_ANALYZER_EXCEPTION.SINGLE_PASS_FAILED_FIXED_POINT_SUCCEEDED",
      parameters =
        Map("fixedPointOutput" -> resolvedPlan.toString)
    )
    assert(error.cause.get.isInstanceOf[StackOverflowError])
  }

  testDualRun("Fixed-point analyzer fails, single-pass analyzer passes") {
    val tracker = new QueryPlanningTracker
    checkError(
      exception = intercept[AnalysisException](
        new HybridAnalyzer(
          new ValidatingAnalyzer(bridgeRelations = true),
          new ResolverGuard(spark.sessionState.catalogManager),
            new HardCodedResolver(resolvedPlan, bridgeRelations = true),
          tracker
        ).apply(malformedUnresolvedPlan)
      ),
      condition = "HYBRID_ANALYZER_EXCEPTION.FIXED_POINT_FAILED_SINGLE_PASS_SUCCEEDED",
      parameters = Map("singlePassOutput" -> resolvedPlan.toString)
    )
  }

  testDualRun("Both fixed-point and single-pass analyzers fail") {
    val tracker = new QueryPlanningTracker
    checkError(
      exception = intercept[ExtendedAnalysisException](
        new HybridAnalyzer(
          new ValidatingAnalyzer(bridgeRelations = true),
          new ResolverGuard(spark.sessionState.catalogManager),
            new ValidatingResolver(bridgeRelations = true),
          tracker
        ).apply(malformedUnresolvedPlan)
      ),
      condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
      parameters = Map(
        "objectName" -> "`nonexistent_col`",
        "proposal" -> "`col1`"
      )
    )
  }

  testDualRun("Missing metadata in output schema") {
    val tracker = new QueryPlanningTracker
    val plan: LogicalPlan =
      Project(
        Seq(UnresolvedAttribute("col2")),
        LocalRelation(col2IntegerWithMetadata)
      )
    val resolvedPlan =
      Project(
        Seq(col2Integer),
        LocalRelation(Seq(col2Integer))
      )
    checkError(
      exception = intercept[AnalysisException](
        new HybridAnalyzer(
          new ValidatingAnalyzer(bridgeRelations = true),
          new ResolverGuard(spark.sessionState.catalogManager),
            new HardCodedResolver(resolvedPlan, bridgeRelations = true),
          tracker
        ).apply(plan)
      ),
      condition = "HYBRID_ANALYZER_EXCEPTION.OUTPUT_SCHEMA_COMPARISON_MISMATCH",
      parameters = Map(
        "singlePassOutputSchema" -> "(col2,IntegerType,true,{})",
        "fixedPointOutputSchema" -> "(col2,IntegerType,true,{\"comment\":\"this is an integer\"})"
      )
    )
  }

  testDualRun("Broken plan normalization") {
    val tracker = new QueryPlanningTracker
    intercept[Exception] {
      new HybridAnalyzerWithBrokenPlanNormalization(
        new ValidatingAnalyzer(bridgeRelations = true),
        new ResolverGuard(spark.sessionState.catalogManager),
        new HardCodedResolver(resolvedPlan, bridgeRelations = true),
        tracker
      ).apply(unresolvedPlan)
    }
  }

  testDualRun("Broken resolver guard") {
    val tracker = new QueryPlanningTracker
    intercept[Exception] {
      new HybridAnalyzer(
        new ValidatingAnalyzer(bridgeRelations = true),
        new BrokenResolverGuard(spark.sessionState.catalogManager),
        new HardCodedResolver(resolvedPlan, bridgeRelations = true),
        tracker
      ).apply(unresolvedPlan)
    }
  }

  testDualRun("Broken resolver guard - failure not exposed") {
    withSQLConf(
      SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_EXPOSE_RESOLVER_GUARD_FAILURE.key -> "false"
    ) {
      val tracker = new QueryPlanningTracker
      assertPlansEqual(
        new HybridAnalyzer(
          new ValidatingAnalyzer(bridgeRelations = false),
          new BrokenResolverGuard(spark.sessionState.catalogManager),
            new HardCodedResolver(resolvedPlan, bridgeRelations = false),
          tracker
        ).apply(unresolvedPlan),
        resolvedPlan
      )
    }
  }

  testDualRun("Explicitly unsupported resolver feature") {
    val tracker = new QueryPlanningTracker
    assertPlansEqual(
      new HybridAnalyzer(
        new ValidatingAnalyzer(bridgeRelations = true),
        new ResolverGuard(spark.sessionState.catalogManager),
        new BrokenResolver(
          new ExplicitlyUnsupportedResolverFeature("FAILURE"),
          bridgeRelations = true
        ),
        tracker
      ).apply(unresolvedPlan),
      resolvedPlan
    )
  }

  test("Fixed-point only run") {
    val plan = Project(
      Seq(UnresolvedStar(None)),
      LocalRelation(col1Integer)
    )
    val resolvedPlan = Project(
      Seq(col1Integer),
      LocalRelation(Seq(col1Integer))
    )
    val tracker = new QueryPlanningTracker
    assertPlansEqual(
      withSQLConf(
        SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false",
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false"
      ) {
        new HybridAnalyzer(
          new ValidatingAnalyzer(bridgeRelations = false),
          new ResolverGuard(spark.sessionState.catalogManager),
            new BrokenResolver(
            new Exception("Single-pass resolver should not be invoked"),
            bridgeRelations = false
          ),
          tracker
        ).apply(plan)
      },
      resolvedPlan
    )
  }

  test("Single-pass only run") {
    val plan = Project(
      Seq(UnresolvedStar(None)),
      LocalRelation(col1Integer)
    )
    val resolvedPlan = Project(
      Seq(col1Integer),
      LocalRelation(Seq(col1Integer))
    )
    val tracker = new QueryPlanningTracker
    assertPlansEqual(
      withSQLConf(
        SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false",
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true"
      ) {
        new HybridAnalyzer(
          new BrokenAnalyzer(
            new Exception("Fixed-point analyzer should not be invoked"),
            bridgeRelations = false
          ),
          new ResolverGuard(spark.sessionState.catalogManager),
            new ValidatingResolver(bridgeRelations = false),
          tracker
        ).apply(plan)
      },
      resolvedPlan
    )
  }

  testDualRun("Nested invocations") {
    val plan = Project(
      Seq(UnresolvedStar(None)),
      LocalRelation(col1Integer)
    )
    val resolvedPlan = Project(
      Seq(col1Integer),
      LocalRelation(Seq(col1Integer))
    )

    val nestedAnalysis = () => {
      val nestedTracker = new QueryPlanningTracker
      assertPlansEqual(
        withSQLConf(
          SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false",
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true"
        ) {
          new HybridAnalyzer(
            new BrokenAnalyzer(
              new Exception("Fixed-point analyzer should not be invoked"),
              bridgeRelations = false
            ),
            new ResolverGuard(spark.sessionState.catalogManager),
                new ValidatingResolver(bridgeRelations = false),
            nestedTracker
          ).apply(plan)
        },
        resolvedPlan
      )
    }

    val tracker = new QueryPlanningTracker
    assertPlansEqual(
      new HybridAnalyzer(
        new CustomAnalyzer(
          customCode = () => { nestedAnalysis() },
          bridgeRelations = true
        ),
        new ResolverGuard(spark.sessionState.catalogManager),
        new ValidatingResolver(bridgeRelations = true),
        tracker
      ).apply(plan),
      resolvedPlan
    )
  }

  testDualRun("Extended resolution checks are enabled/disabled") {
    val plan: LogicalPlan = {
      Project(
        Seq(UnresolvedStar(None)),
        LocalRelation(col1Integer)
      )
    }
    val resolvedPlan =
      Project(
        Seq(col1Integer),
        LocalRelation(Seq(col1Integer))
      )

    val tracker1 = new QueryPlanningTracker
    intercept[Exception] {
      new HybridAnalyzer(
        legacyAnalyzer = new ValidatingAnalyzer(bridgeRelations = true),
        resolverGuard = new ResolverGuard(spark.sessionState.catalogManager),
        resolver = new ValidatingResolver(bridgeRelations = true),
        tracker = tracker1,
        extendedResolutionChecks = Seq(new BrokenCheckRule)
      ).apply(plan)
    }

    withSQLConf(
      SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_RUN_EXTENDED_RESOLUTION_CHECKS.key -> "false"
    ) {
      val tracker2 = new QueryPlanningTracker
      assertPlansEqual(
        new HybridAnalyzer(
          legacyAnalyzer = new ValidatingAnalyzer(bridgeRelations = true),
          resolverGuard = new ResolverGuard(spark.sessionState.catalogManager),
          resolver = new ValidatingResolver(bridgeRelations = true),
          tracker = tracker2,
          extendedResolutionChecks = Seq(new BrokenCheckRule)
        ).apply(plan),
        resolvedPlan
      )
    }
  }

  testDualRun("Extended resolution checks invocation count in dual-run mode") {
    var singlePassCheckCounter = 0

    val singlePassCounterCheckRule = new (LogicalPlan => Unit) {
      def apply(plan: LogicalPlan): Unit = {
        singlePassCheckCounter += 1
      }
    }

    val tracker = new QueryPlanningTracker
    assertPlansEqual(
      new HybridAnalyzer(
        legacyAnalyzer = new ValidatingAnalyzer(bridgeRelations = true),
        resolverGuard = new ResolverGuard(spark.sessionState.catalogManager),
        resolver = new ValidatingResolver(bridgeRelations = true),
        tracker = tracker,
        extendedResolutionChecks = Seq(singlePassCounterCheckRule)
      ).apply(unresolvedPlan),
      resolvedPlan
    )

    assert(
      singlePassCheckCounter == 1,
      s"Expected check to be invoked 1 time, but was invoked $singlePassCheckCounter times"
    )
  }

  test("Extended resolution checks invocation count in single-pass only mode") {
    var checkCounter = 0

    val counterCheckRule = new (LogicalPlan => Unit) {
      def apply(plan: LogicalPlan): Unit = {
        checkCounter += 1
      }
    }

    val tracker = new QueryPlanningTracker
    assertPlansEqual(
      withSQLConf(
        SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false",
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true"
      ) {
        new HybridAnalyzer(
          legacyAnalyzer = new BrokenAnalyzer(
            new Exception("Fixed-point analyzer should not be invoked"),
            bridgeRelations = false
          ),
          resolverGuard = new ResolverGuard(spark.sessionState.catalogManager),
          resolver = new ValidatingResolver(bridgeRelations = false),
          tracker = tracker,
          extendedResolutionChecks = Seq(counterCheckRule)
        ).apply(unresolvedPlan)
      },
      resolvedPlan
    )

    assert(
      checkCounter == 1,
      s"Expected check to be invoked 1 time, but was invoked $checkCounter times"
    )
  }
}
