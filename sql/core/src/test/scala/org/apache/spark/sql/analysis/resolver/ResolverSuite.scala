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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.analysis.resolver.{
  LogicalPlanResolver,
  Resolver,
  ResolverExtension
}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, PipeSetInput}
import org.apache.spark.sql.catalyst.plans.NormalizePlan
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.IntegerType

class ResolverSuite extends SharedSparkSession {
  private val col1Integer = AttributeReference("col1", IntegerType)()

  test("Node matched the extension") {
    val resolver = createResolver(
      Seq(
        new NoopResolver,
        new TestRelationResolver
      )
    )

    val result = resolver.lookupMetadataAndResolve(
      Project(
        Seq(UnresolvedAttribute("col1")),
        TestRelation(resolutionDone = false, output = Seq(col1Integer))
      )
    )
    assertPlansEqual(
      result,
      Project(
        Seq(col1Integer),
        TestRelation(resolutionDone = true, output = Seq(col1Integer))
      )
    )
  }

  test("Node didn't match the extension") {
    val resolver = createResolver(
      Seq(
        new NoopResolver,
        new TestRelationResolver
      )
    )

    checkError(
      exception = intercept[AnalysisException](
        resolver.lookupMetadataAndResolve(
          Project(
            Seq(UnresolvedAttribute("col1")),
            UnknownRelation(output = Seq(col1Integer))
          )
        )
      ),
      condition = "UNSUPPORTED_SINGLE_PASS_ANALYZER_FEATURE",
      parameters = Map(
        "feature" -> ("class " +
        "org.apache.spark.sql.analysis.resolver.UnknownRelation operator resolution")
      )
    )
  }

  test("Ambiguous extensions") {
    val resolver = createResolver(
      Seq(
        new NoopResolver,
        new TestRelationResolver,
        new TestRelationOtherResolver
      )
    )

    checkError(
      exception = intercept[AnalysisException](
        resolver.lookupMetadataAndResolve(
          Project(
            Seq(UnresolvedAttribute("col1")),
            TestRelation(resolutionDone = false, output = Seq(col1Integer))
          )
        )
      ),
      condition = "AMBIGUOUS_RESOLVER_EXTENSION",
      parameters = Map(
        "operator" -> "org.apache.spark.sql.analysis.resolver.TestRelation",
        "extensions" -> "TestRelationResolver, TestRelationOtherResolver"
      )
    )
  }

  test("SPARK-59146: pipe SET cleanup follows extended rewrite rules") {
    var extendedRuleSawPipeSetInput = false
    val extendedRewriteRule = new Rule[LogicalPlan] {
      override def apply(plan: LogicalPlan): LogicalPlan = {
        if (plan.exists(_.isInstanceOf[PipeSetInput])) {
          extendedRuleSawPipeSetInput = true
        }
        plan.transformUp {
          case pipeSetInput: PipeSetInput => pipeSetInput.child
        }
      }
    }
    val resolver = new Resolver(
      catalogManager = spark.sessionState.catalogManager,
      sharedRelationCache = spark.sharedState.relationCache,
      extendedRewriteRules = Seq(extendedRewriteRule))

    val result = resolver.lookupMetadataAndResolve(spark.sessionState.sqlParser.parsePlan(
      "VALUES (1, 10) AS t(a, b) |> SET a = a + 1 " +
        "|> INNER JOIN VALUES (2, 20) AS u(a, c) USING (a) " +
        "|> SELECT a"))
    val hiddenOutput = result.collect {
      case project: Project =>
        project.getTagValue(Project.hiddenOutputTag).toSeq.flatten
    }.flatten

    assert(extendedRuleSawPipeSetInput)
    assert(!result.exists(_.isInstanceOf[PipeSetInput]))
    assert(!hiddenOutput.exists(_.pipeSetRetained))
    assert(hiddenOutput.exists { attribute =>
      attribute.name == "a" && attribute.qualifier == Seq("u")
    })
  }

  private def createResolver(extensions: Seq[ResolverExtension] = Seq.empty): Resolver = {
    new Resolver(
      spark.sessionState.catalogManager,
      spark.sharedState.relationCache,
      extensions)
  }

  private class TestRelationResolver extends ResolverExtension {
    var timesCalled = 0

    override def resolveOperator(
        operator: LogicalPlan,
        resolver: LogicalPlanResolver): Option[LogicalPlan] = operator match {
      case testNode: TestRelation if countTimesCalled() =>
        Some(testNode.copy(resolutionDone = true))
      case _ =>
        None
    }

    private def countTimesCalled(): Boolean = {
      timesCalled += 1
      assert(timesCalled == 1)
      true
    }
  }

  private class TestRelationOtherResolver extends ResolverExtension {
    override def resolveOperator(
        operator: LogicalPlan,
        resolver: LogicalPlanResolver): Option[LogicalPlan] = operator match {
      case testNode: TestRelation =>
        Some(testNode)
      case _ =>
        None
    }
  }

  private class NoopResolver extends ResolverExtension {
    override def resolveOperator(
        operator: LogicalPlan,
        resolver: LogicalPlanResolver): Option[LogicalPlan] = operator match {
      case node: LogicalPlan if false =>
        assert(false)
        Some(node)
      case _ =>
        None
    }
  }

  private def assertPlansEqual(actualPlan: LogicalPlan, expectedPlan: LogicalPlan) = {
    assert(NormalizePlan(actualPlan) == NormalizePlan(expectedPlan))
  }
}

private case class TestRelation(resolutionDone: Boolean, override val output: Seq[Attribute])
    extends LeafNode {}

private case class UnknownRelation(override val output: Seq[Attribute]) extends LeafNode {}
