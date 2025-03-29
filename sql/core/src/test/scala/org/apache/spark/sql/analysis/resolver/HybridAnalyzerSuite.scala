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
import org.apache.spark.sql.catalyst.{
  AliasIdentifier,
  ExtendedAnalysisException,
  QueryPlanningTracker
}
import org.apache.spark.sql.catalyst.analysis.{
  AnalysisContext,
  Analyzer,
  UnresolvedAttribute,
  UnresolvedStar
}
import org.apache.spark.sql.catalyst.analysis.resolver.{
  AnalyzerBridgeState,
  ExplicitlyUnsupportedResolverFeature,
  HybridAnalyzer,
  Resolver,
  ResolverGuard
}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.NormalizePlan
import org.apache.spark.sql.catalyst.plans.logical.{
  LocalRelation,
  LogicalPlan,
  Project,
  SubqueryAlias
}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, MetadataBuilder}

class HybridAnalyzerSuite extends QueryTest with SharedSparkSession {
  private val col1Integer = AttributeReference("col1", IntegerType)()
  private val col2Integer = AttributeReference("col2", IntegerType)()
  private val col2IntegerWithMetadata = AttributeReference(
    "col2",
    IntegerType,
    metadata = (new MetadataBuilder).putString("comment", "this is an integer").build()
  )()

  private def validateSinglePassResolverBridgeState(bridgeRelations: Boolean): Unit = {
    assert(bridgeRelations == AnalysisContext.get.getSinglePassResolverBridgeState.isDefined)
  }

  private class BrokenResolver(ex: Throwable, bridgeRelations: Boolean)
      extends Resolver(spark.sessionState.catalogManager) {
    override def lookupMetadataAndResolve(
        plan: LogicalPlan,
        analyzerBridgeState: Option[AnalyzerBridgeState] = None): LogicalPlan = {
      validateSinglePassResolverBridgeState(bridgeRelations)
      throw ex
    }
  }

  private class ValidatingResolver(bridgeRelations: Boolean)
      extends Resolver(spark.sessionState.catalogManager) {
    override def lookupMetadataAndResolve(
        plan: LogicalPlan,
        analyzerBridgeState: Option[AnalyzerBridgeState] = None): LogicalPlan = {
      validateSinglePassResolverBridgeState(bridgeRelations)
      super.lookupMetadataAndResolve(plan, analyzerBridgeState)
    }
  }

  private class HardCodedResolver(resolvedPlan: LogicalPlan, bridgeRelations: Boolean)
      extends Resolver(spark.sessionState.catalogManager) {
    override def lookupMetadataAndResolve(
        plan: LogicalPlan,
        analyzerBridgeState: Option[AnalyzerBridgeState] = None): LogicalPlan = {
      validateSinglePassResolverBridgeState(bridgeRelations)
      resolvedPlan
    }
  }

  private class ValidatingAnalyzer(bridgeRelations: Boolean)
      extends Analyzer(spark.sessionState.catalogManager) {
    override def executeAndTrack(plan: LogicalPlan, tracker: QueryPlanningTracker): LogicalPlan = {
      validateSinglePassResolverBridgeState(bridgeRelations)
      super.executeAndTrack(plan, tracker)
    }
  }

  private class BrokenAnalyzer(ex: Throwable, bridgeRelations: Boolean)
      extends Analyzer(spark.sessionState.catalogManager) {
    override def executeAndTrack(plan: LogicalPlan, tracker: QueryPlanningTracker): LogicalPlan = {
      validateSinglePassResolverBridgeState(bridgeRelations)
      throw ex
    }
  }

  private class CustomAnalyzer(customCode: () => Unit, bridgeRelations: Boolean)
      extends Analyzer(spark.sessionState.catalogManager) {
    override def executeAndTrack(plan: LogicalPlan, tracker: QueryPlanningTracker): LogicalPlan = {
      validateSinglePassResolverBridgeState(bridgeRelations)
      customCode()
      super.executeAndTrack(plan, tracker)
    }
  }

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(
      implicit pos: Position): Unit = {
    super.test(testName) {
      withSQLConf(
        SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "true"
      ) {
        testFun
      }
    }
  }

  test("Both fixed-point and single-pass analyzers pass") {
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
    assertPlansEqual(
      new HybridAnalyzer(
        new ValidatingAnalyzer(bridgeRelations = true),
        new ResolverGuard(spark.sessionState.catalogManager),
        new ValidatingResolver(bridgeRelations = true)
      ).apply(plan, new QueryPlanningTracker),
      resolvedPlan
    )
  }

  test("Fixed-point analyzer passes, single-pass analyzer fails") {
    val plan: LogicalPlan =
      Project(Seq(UnresolvedStar(None)), LocalRelation(col1Integer))
    checkError(
      exception = intercept[AnalysisException](
        new HybridAnalyzer(
          new ValidatingAnalyzer(bridgeRelations = true),
          new ResolverGuard(spark.sessionState.catalogManager),
          new BrokenResolver(
            QueryCompilationErrors.unsupportedSinglePassAnalyzerFeature("test"),
            bridgeRelations = true
          )
        ).apply(plan, new QueryPlanningTracker)
      ),
      condition = "UNSUPPORTED_SINGLE_PASS_ANALYZER_FEATURE",
      parameters = Map("feature" -> "test")
    )
  }

  test("Fixed-point analyzer fails, single-pass analyzer passes") {
    val plan: LogicalPlan =
      Project(
        Seq(UnresolvedAttribute("nonexistent_col")),
        LocalRelation(col1Integer)
      )
    val resolvedPlan =
      Project(
        Seq(col1Integer),
        LocalRelation(Seq(col1Integer))
      )
    checkError(
      exception = intercept[AnalysisException](
        new HybridAnalyzer(
          new ValidatingAnalyzer(bridgeRelations = true),
          new ResolverGuard(spark.sessionState.catalogManager),
          new HardCodedResolver(resolvedPlan, bridgeRelations = true)
        ).apply(plan, new QueryPlanningTracker)
      ),
      condition = "HYBRID_ANALYZER_EXCEPTION.FIXED_POINT_FAILED_SINGLE_PASS_SUCCEEDED",
      parameters = Map("singlePassOutput" -> resolvedPlan.toString)
    )
  }

  test("Both fixed-point and single-pass analyzers fail") {
    val plan: LogicalPlan =
      Project(
        Seq(UnresolvedAttribute("nonexistent_col")),
        LocalRelation(col1Integer)
      )
    checkError(
      exception = intercept[ExtendedAnalysisException](
        new HybridAnalyzer(
          new ValidatingAnalyzer(bridgeRelations = true),
          new ResolverGuard(spark.sessionState.catalogManager),
          new ValidatingResolver(bridgeRelations = true)
        ).apply(plan, new QueryPlanningTracker)
      ),
      condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
      parameters = Map(
        "objectName" -> "`nonexistent_col`",
        "proposal" -> "`col1`"
      )
    )
  }

  test("Plan mismatch") {
    val plan: LogicalPlan =
      Project(
        Seq(UnresolvedAttribute("col1")),
        SubqueryAlias(
          AliasIdentifier("t", Seq.empty),
          LocalRelation(Seq(col1Integer))
        )
      )
    val resolvedPlan =
      Project(
        Seq(col1Integer),
        LocalRelation(Seq(col1Integer))
      )
    val expectedResolvedPlan =
      Project(
        Seq(col1Integer),
        SubqueryAlias(
          AliasIdentifier("t", Seq.empty),
          LocalRelation(Seq(col1Integer))
        )
      )
    checkError(
      exception = intercept[AnalysisException](
        new HybridAnalyzer(
          new ValidatingAnalyzer(bridgeRelations = true),
          new ResolverGuard(spark.sessionState.catalogManager),
          new HardCodedResolver(resolvedPlan, bridgeRelations = true)
        ).apply(plan, new QueryPlanningTracker)
      ),
      condition = "HYBRID_ANALYZER_EXCEPTION.LOGICAL_PLAN_COMPARISON_MISMATCH",
      parameters = Map(
        "singlePassOutput" -> resolvedPlan.toString,
        "fixedPointOutput" -> expectedResolvedPlan.toString
      )
    )
  }

  test("Missing metadata in output schema") {
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
          new HardCodedResolver(resolvedPlan, bridgeRelations = true)
        ).apply(plan, new QueryPlanningTracker)
      ),
      condition = "HYBRID_ANALYZER_EXCEPTION.OUTPUT_SCHEMA_COMPARISON_MISMATCH",
      parameters = Map(
        "singlePassOutputSchema" -> "(col2,IntegerType,true,{})",
        "fixedPointOutputSchema" -> "(col2,IntegerType,true,{\"comment\":\"this is an integer\"})"
      )
    )
  }

  test("Explicitly unsupported resolver feature") {
    val plan: LogicalPlan = {
      Project(
        Seq(UnresolvedStar(None)),
        LocalRelation(col1Integer)
      )
    }
    checkAnswer(
      new HybridAnalyzer(
        new ValidatingAnalyzer(bridgeRelations = true),
        new ResolverGuard(spark.sessionState.catalogManager),
        new BrokenResolver(
          new ExplicitlyUnsupportedResolverFeature("FAILURE"),
          bridgeRelations = true
        )
      ).apply(plan, new QueryPlanningTracker),
      plan
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
    assertPlansEqual(
      withSQLConf(
        SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false"
      ) {
        new HybridAnalyzer(
          new ValidatingAnalyzer(bridgeRelations = false),
          new ResolverGuard(spark.sessionState.catalogManager),
          new BrokenResolver(
            new Exception("Single-pass resolver should not be invoked"),
            bridgeRelations = false
          )
        ).apply(plan, new QueryPlanningTracker)
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
    assertPlansEqual(
      withSQLConf(
        SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false",
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true"
      ) {
        new HybridAnalyzer(
          new BrokenAnalyzer(
            new Exception("Fixed-point analyzer should not be invoked"),
            bridgeRelations = false
          ),
          new ResolverGuard(spark.sessionState.catalogManager),
          new ValidatingResolver(bridgeRelations = false)
        ).apply(plan, new QueryPlanningTracker)
      },
      resolvedPlan
    )
  }

  test("Nested invocations") {
    val plan = Project(
      Seq(UnresolvedStar(None)),
      LocalRelation(col1Integer)
    )
    val resolvedPlan = Project(
      Seq(col1Integer),
      LocalRelation(Seq(col1Integer))
    )

    val nestedAnalysis = () => {
      assertPlansEqual(
        withSQLConf(
          SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false",
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true"
        ) {
          new HybridAnalyzer(
            new BrokenAnalyzer(
              new Exception("Fixed-point analyzer should not be invoked"),
              bridgeRelations = false
            ),
            new ResolverGuard(spark.sessionState.catalogManager),
            new ValidatingResolver(bridgeRelations = false)
          ).apply(plan, new QueryPlanningTracker)
        },
        resolvedPlan
      )
    }

    assertPlansEqual(
      new HybridAnalyzer(
        new CustomAnalyzer(
          customCode = () => { nestedAnalysis() },
          bridgeRelations = true
        ),
        new ResolverGuard(spark.sessionState.catalogManager),
        new ValidatingResolver(bridgeRelations = true)
      ).apply(plan, new QueryPlanningTracker),
      resolvedPlan
    )
  }

  private def assertPlansEqual(actualPlan: LogicalPlan, expectedPlan: LogicalPlan) = {
    assert(NormalizePlan(actualPlan) == NormalizePlan(expectedPlan))
  }
}
