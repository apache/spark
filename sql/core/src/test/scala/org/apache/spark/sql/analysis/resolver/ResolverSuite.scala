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

import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.analysis.resolver.{Resolver, ResolverExtension}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.IntegerType

class ResolverSuite extends QueryTest with SharedSparkSession {
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
    assert(
      result == Project(
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
        "org.apache.spark.sql.analysis.resolver.ResolverSuite$UnknownRelation operator resolution")
      )
    )
  }

  test("Ambiguous extensions") {
    val resolver = createResolver(
      Seq(
        new NoopResolver,
        new TestRelationResolver,
        new TestRelationBrokenResolver
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
        "operator" -> "org.apache.spark.sql.analysis.resolver.ResolverSuite$TestRelation",
        "extensions" -> "TestRelationResolver, TestRelationBrokenResolver"
      )
    )
  }

  private def createResolver(extensions: Seq[ResolverExtension] = Seq.empty): Resolver = {
    new Resolver(spark.sessionState.catalogManager, extensions)
  }

  private class TestRelationResolver extends ResolverExtension {
    var timesCalled = 0

    override def resolveOperator: PartialFunction[LogicalPlan, LogicalPlan] = {
      case testNode: TestRelation if countTimesCalled() =>
        testNode.copy(resolutionDone = true)
    }

    private def countTimesCalled(): Boolean = {
      timesCalled += 1
      assert(timesCalled == 1)
      true
    }
  }

  private class TestRelationBrokenResolver extends ResolverExtension {
    override def resolveOperator: PartialFunction[LogicalPlan, LogicalPlan] = {
      case testNode: TestRelation =>
        assert(false)
        testNode
    }
  }

  private class NoopResolver extends ResolverExtension {
    override def resolveOperator: PartialFunction[LogicalPlan, LogicalPlan] = {
      case node: LogicalPlan if false =>
        assert(false)
        node
    }
  }

  private case class TestRelation(
      resolutionDone: Boolean,
      override val output: Seq[Attribute],
      override val children: Seq[LogicalPlan] = Seq.empty)
      extends LogicalPlan {
    override protected def withNewChildrenInternal(
        newChildren: IndexedSeq[LogicalPlan]): TestRelation =
      copy(children = newChildren)
  }

  private case class UnknownRelation(
      override val output: Seq[Attribute],
      override val children: Seq[LogicalPlan] = Seq.empty)
      extends LogicalPlan {
    override protected def withNewChildrenInternal(
        newChildren: IndexedSeq[LogicalPlan]): UnknownRelation =
      copy(children = newChildren)
  }
}
