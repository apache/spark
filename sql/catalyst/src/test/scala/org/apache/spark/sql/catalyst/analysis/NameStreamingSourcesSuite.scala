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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Project, SubqueryAlias, Union}
import org.apache.spark.sql.catalyst.streaming.{FlowAssigned, StreamingRelationV2, StreamingSourceIdentifyingName, Unassigned, UserProvided}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Unit tests for the NameStreamingSources analyzer rule.
 */
class NameStreamingSourcesSuite extends AnalysisTest {

  private def createStreamingRelation(name: String = "source"): LocalRelation = {
    LocalRelation(
      Seq(AttributeReference("id", IntegerType)()),
      Nil,
      isStreaming = true
    )
  }

  private def createBatchRelation(): LocalRelation = {
    LocalRelation(
      Seq(AttributeReference("id", IntegerType)()),
      Nil,
      isStreaming = false
    )
  }

  private def createStreamingRelationV2(
      sourceName: StreamingSourceIdentifyingName = Unassigned): StreamingRelationV2 = {
    val output = Seq(AttributeReference("id", IntegerType)())
    StreamingRelationV2(
      None, "test", null,
      new CaseInsensitiveStringMap(java.util.Collections.emptyMap()),
      output, None, None, None, sourceName)
  }

  test("skips non-streaming plans") {
    val batchPlan = createBatchRelation()
    val result = NameStreamingSources.apply(batchPlan)

    // Non-streaming plans should be returned unchanged
    assert(result eq batchPlan)
  }

  test("unwraps NamedStreamingRelation with resolved child") {
    val streamingPlan = createStreamingRelation()
    val wrapped = NamedStreamingRelation(streamingPlan, Unassigned)

    val result = NameStreamingSources.apply(wrapped)

    // With Unassigned name and resolved child, wrapper should be removed
    assert(result eq streamingPlan)
  }

  test("preserves user-provided names in enforcement check") {
    withSQLConf(SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "true") {
      val source1 = NamedStreamingRelation(createStreamingRelation(), Unassigned)
        .withUserProvidedName(Some("my_source"))
      val source2 = NamedStreamingRelation(createStreamingRelation(), Unassigned)
        .withUserProvidedName(Some("other_source"))
      val streamingPlan = Union(Seq(source1, source2))

      // Should not throw when all sources are user-named
      val result = NameStreamingSources.apply(streamingPlan)

      // Verify both sources were processed
      val namedSources = streamingPlan.collect {
        case n: NamedStreamingRelation => n
      }
      assert(namedSources.size == 2)
      assert(namedSources.forall(_.sourceIdentifyingName.isInstanceOf[UserProvided]))
    }
  }

  test("throws error when enforcement enabled and sources unnamed") {
    withSQLConf(SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "true") {
      val source = NamedStreamingRelation(createStreamingRelation(), Unassigned)
      val streamingPlan = Project(source.output, source)

      checkError(
        exception = intercept[AnalysisException] {
          NameStreamingSources.apply(streamingPlan)
        },
        condition =
          "STREAMING_QUERY_EVOLUTION_ERROR.UNNAMED_STREAMING_SOURCES_WITH_ENFORCEMENT",
        parameters = Map("sourceInfo" -> "(?s).*"),
        matchPVals = true
      )
    }
  }

  test("throws error for mixed named and unnamed sources with enforcement") {
    withSQLConf(SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "true") {
      val namedSource = NamedStreamingRelation(createStreamingRelation(), Unassigned)
        .withUserProvidedName(Some("named_source"))
      val unnamedSource = NamedStreamingRelation(createStreamingRelation(), Unassigned)
      val streamingPlan = Union(Seq(namedSource, unnamedSource))

      // Should error because there's an unnamed source when enforcement is on
      checkError(
        exception = intercept[AnalysisException] {
          NameStreamingSources.apply(streamingPlan)
        },
        condition =
          "STREAMING_QUERY_EVOLUTION_ERROR.UNNAMED_STREAMING_SOURCES_WITH_ENFORCEMENT",
        parameters = Map("sourceInfo" -> "(?s).*"),
        matchPVals = true
      )
    }
  }

  test("no error when enforcement disabled and sources unnamed") {
    withSQLConf(SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "false") {
      val source = NamedStreamingRelation(createStreamingRelation(), Unassigned)

      // Should not throw - enforcement is off
      val result = NameStreamingSources.apply(source)

      // The resolved child should be unwrapped
      assert(!result.isInstanceOf[NamedStreamingRelation])
    }
  }

  test("propagates name to StreamingRelationV2") {
    withSQLConf(SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "true") {
      val streamingV2 = createStreamingRelationV2()
      val wrapped = NamedStreamingRelation(streamingV2, UserProvided("my_source"))

      val result = NameStreamingSources.apply(wrapped)

      result match {
        case s: StreamingRelationV2 =>
          assert(s.sourceIdentifyingName == UserProvided("my_source"))
        case other =>
          fail(s"Expected StreamingRelationV2 but got ${other.getClass.getSimpleName}")
      }
    }
  }

  // ===================================
  // Additional test cases
  // ===================================

  test("preserves FlowAssigned names in enforcement check") {
    withSQLConf(SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "true") {
      val source1 = NamedStreamingRelation(createStreamingRelation(), FlowAssigned("flow_source_1"))
      val source2 = NamedStreamingRelation(createStreamingRelation(), FlowAssigned("flow_source_2"))
      val streamingPlan = Union(Seq(source1, source2))

      // Should not throw when all sources are flow-assigned
      val result = NameStreamingSources.apply(streamingPlan)

      // Plan should be transformed without error
      assert(result != null)
    }
  }

  test("propagates FlowAssigned name to StreamingRelationV2") {
    withSQLConf(SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "true") {
      val streamingV2 = createStreamingRelationV2()
      val wrapped = NamedStreamingRelation(streamingV2, FlowAssigned("flow_source"))

      val result = NameStreamingSources.apply(wrapped)

      result match {
        case s: StreamingRelationV2 =>
          assert(s.sourceIdentifyingName == FlowAssigned("flow_source"))
        case other =>
          fail(s"Expected StreamingRelationV2 but got ${other.getClass.getSimpleName}")
      }
    }
  }

  test("propagates name through SubqueryAlias") {
    withSQLConf(SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "true") {
      val streamingV2 = createStreamingRelationV2()
      val aliased = SubqueryAlias("my_alias", streamingV2)
      val wrapped = NamedStreamingRelation(aliased, UserProvided("aliased_source"))

      val result = NameStreamingSources.apply(wrapped)

      result match {
        case SubqueryAlias(_, inner: StreamingRelationV2) =>
          assert(inner.sourceIdentifyingName == UserProvided("aliased_source"))
        case other =>
          fail(s"Expected SubqueryAlias wrapping StreamingRelationV2 but got $other")
      }
    }
  }

  test("error message contains source position for unnamed sources") {
    withSQLConf(SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "true") {
      val unnamedSource = NamedStreamingRelation(createStreamingRelation(), Unassigned)
      val streamingPlan = Project(unnamedSource.output, unnamedSource)

      // Error should contain position information
      checkError(
        exception = intercept[AnalysisException] {
          NameStreamingSources.apply(streamingPlan)
        },
        condition =
          "STREAMING_QUERY_EVOLUTION_ERROR.UNNAMED_STREAMING_SOURCES_WITH_ENFORCEMENT",
        parameters = Map("sourceInfo" -> "(?s).*Leaf position 0.*"),
        matchPVals = true
      )
    }
  }

  test("error message lists all unnamed sources with their positions") {
    withSQLConf(SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "true") {
      val unnamed1 = NamedStreamingRelation(createStreamingRelation(), Unassigned)
      val unnamed2 = NamedStreamingRelation(createStreamingRelation(), Unassigned)
      val unnamed3 = NamedStreamingRelation(createStreamingRelation(), Unassigned)
      val streamingPlan = Union(Seq(unnamed1, unnamed2, unnamed3))

      // Error should contain positions for all unnamed sources
      checkError(
        exception = intercept[AnalysisException] {
          NameStreamingSources.apply(streamingPlan)
        },
        condition =
          "STREAMING_QUERY_EVOLUTION_ERROR.UNNAMED_STREAMING_SOURCES_WITH_ENFORCEMENT",
        parameters = Map(
          "sourceInfo" -> "(?s).*Leaf position 0.*Leaf position 1.*Leaf position 2.*"),
        matchPVals = true
      )
    }
  }

  test("mixed UserProvided and FlowAssigned names pass enforcement") {
    withSQLConf(SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "true") {
      val userNamed = NamedStreamingRelation(createStreamingRelation(), UserProvided("user_src"))
      val flowNamed = NamedStreamingRelation(createStreamingRelation(), FlowAssigned("flow_src"))
      val streamingPlan = Union(Seq(userNamed, flowNamed))

      // Should not throw - both are explicitly named
      val result = NameStreamingSources.apply(streamingPlan)
      assert(result != null)
    }
  }

  test("nested unions with all named sources pass enforcement") {
    withSQLConf(SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "true") {
      val source1 = NamedStreamingRelation(createStreamingRelation(), UserProvided("source1"))
      val source2 = NamedStreamingRelation(createStreamingRelation(), UserProvided("source2"))
      val source3 = NamedStreamingRelation(createStreamingRelation(), UserProvided("source3"))

      val innerUnion = Union(Seq(source1, source2))
      val outerUnion = Union(Seq(innerUnion, source3))

      // Should not throw
      val result = NameStreamingSources.apply(outerUnion)
      assert(result != null)
    }
  }

  test("deeply nested streaming relation with Unassigned is unwrapped") {
    val streamingPlan = createStreamingRelation()
    val wrapped = NamedStreamingRelation(streamingPlan, Unassigned)
    val projected = Project(wrapped.output, wrapped)

    val result = NameStreamingSources.apply(projected)

    // The NamedStreamingRelation should be unwrapped
    val hasNamedRelation = result.collect {
      case _: NamedStreamingRelation => true
    }.nonEmpty
    assert(!hasNamedRelation, "NamedStreamingRelation should be unwrapped")
  }

  test("batch plan with streaming subquery is processed") {
    // A batch plan containing a streaming relation should still be processed
    val streamingSource = NamedStreamingRelation(createStreamingRelation(), Unassigned)
    val batchOuter = Project(streamingSource.output, streamingSource)

    // The plan is streaming because it contains a streaming relation
    assert(batchOuter.isStreaming)

    val result = NameStreamingSources.apply(batchOuter)

    // Should unwrap the NamedStreamingRelation
    val hasNamedRelation = result.collect {
      case _: NamedStreamingRelation => true
    }.nonEmpty
    assert(!hasNamedRelation)
  }

  // ===================================
  // Tests for config disabled behavior
  // ===================================

  test("config disabled - unwraps Unassigned NamedStreamingRelation") {
    withSQLConf(
      SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "false"
    ) {
      val streamingV2 = createStreamingRelationV2()
      val wrapped = NamedStreamingRelation(streamingV2, Unassigned)

      val result = NameStreamingSources.apply(wrapped)

      // Should unwrap without error when name is Unassigned
      result match {
        case s: StreamingRelationV2 =>
          assert(s.sourceIdentifyingName == Unassigned)
        case other =>
          fail(s"Expected StreamingRelationV2 but got ${other.getClass.getSimpleName}")
      }
    }
  }

  test("config disabled - throws error for UserProvided name") {
    withSQLConf(
      SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "false"
    ) {
      val streamingV2 = createStreamingRelationV2()
      val wrapped = NamedStreamingRelation(streamingV2, UserProvided("my_source"))

      checkError(
        exception = intercept[AnalysisException] {
          NameStreamingSources.apply(wrapped)
        },
        condition = "STREAMING_QUERY_EVOLUTION_ERROR.SOURCE_NAMING_NOT_SUPPORTED",
        parameters = Map("name" -> "my_source"))
    }
  }

  test("config disabled - throws error for FlowAssigned name") {
    withSQLConf(
      SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "false"
    ) {
      val streamingV2 = createStreamingRelationV2()
      val wrapped = NamedStreamingRelation(streamingV2, FlowAssigned("flow_source"))

      checkError(
        exception = intercept[AnalysisException] {
          NameStreamingSources.apply(wrapped)
        },
        condition = "STREAMING_QUERY_EVOLUTION_ERROR.SOURCE_NAMING_NOT_SUPPORTED",
        parameters = Map("name" -> "flow_source"))
    }
  }

  test("config disabled - no enforcement error even with unnamed sources") {
    withSQLConf(
      SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "false"
    ) {
      val source = NamedStreamingRelation(createStreamingRelation(), Unassigned)
      val streamingPlan = Project(source.output, source)

      // Should NOT throw - feature is disabled so enforcement is skipped
      val result = NameStreamingSources.apply(streamingPlan)
      assert(!result.isInstanceOf[NamedStreamingRelation])
    }
  }

  test("config disabled - unwraps all Unassigned NamedStreamingRelation nodes") {
    withSQLConf(
      SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "false"
    ) {
      val source1 = NamedStreamingRelation(createStreamingRelation(), Unassigned)
      val source2 = NamedStreamingRelation(createStreamingRelation(), Unassigned)
      val streamingPlan = Union(Seq(source1, source2))

      val result = NameStreamingSources.apply(streamingPlan)

      // All NamedStreamingRelation nodes should be unwrapped
      val hasNamedRelation = result.collect {
        case _: NamedStreamingRelation => true
      }.nonEmpty
      assert(!hasNamedRelation, "All NamedStreamingRelation nodes should be unwrapped")
    }
  }
}
