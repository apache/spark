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
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Project, SequentialUnion}
import org.apache.spark.sql.errors.DataTypeErrorsBase

class SequentialUnionAnalysisSuite extends AnalysisTest with DataTypeErrorsBase {

  private val testRelation1 = LocalRelation($"a".int, $"b".int)
  private val testRelation2 = LocalRelation($"a".int, $"b".int)
  private val testRelation3 = LocalRelation($"a".int, $"b".int)

  test("FlattenSequentialUnion - single level") {
    val union = SequentialUnion(testRelation1, testRelation2)
    val result = FlattenSequentialUnion(union)

    assert(result.isInstanceOf[SequentialUnion])
    val su = result.asInstanceOf[SequentialUnion]
    assert(su.children.length == 2)
  }

  test("FlattenSequentialUnion - nested unions get flattened") {
    val innerUnion = SequentialUnion(testRelation1, testRelation2)
    val outerUnion = SequentialUnion(innerUnion, testRelation3)

    val result = FlattenSequentialUnion(outerUnion)

    assert(result.isInstanceOf[SequentialUnion])
    val su = result.asInstanceOf[SequentialUnion]
    assert(su.children.length == 3, "Should flatten to 3 children")
    assert(!su.children.exists(_.isInstanceOf[SequentialUnion]), "No nested SequentialUnions")
  }

  test("FlattenSequentialUnion - deeply nested unions") {
    val union1 = SequentialUnion(testRelation1, testRelation2)
    val union2 = SequentialUnion(union1, testRelation3)

    val result = FlattenSequentialUnion(union2)

    val su = result.asInstanceOf[SequentialUnion]
    assert(su.children.length == 3)
  }

  test("FlattenSequentialUnion - multiple nested unions at same level") {
    val union1 = SequentialUnion(testRelation1, testRelation2)
    val union2 = SequentialUnion(testRelation2, testRelation3)
    val outerUnion = SequentialUnion(Seq(union1, union2))

    val result = FlattenSequentialUnion(outerUnion)

    val su = result.asInstanceOf[SequentialUnion]
    assert(su.children.length == 4, "Should have 4 flattened children")
  }

  test("FlattenSequentialUnion - preserves byName parameter") {
    val innerUnion = SequentialUnion(testRelation1, testRelation2)
    val outerUnion = SequentialUnion(Seq(innerUnion, testRelation3), byName = true)

    val result = FlattenSequentialUnion(outerUnion)

    val su = result.asInstanceOf[SequentialUnion]
    assert(su.byName == true, "byName parameter should be preserved")
  }

  test("FlattenSequentialUnion - preserves allowMissingCol parameter") {
    val innerUnion = SequentialUnion(testRelation1, testRelation2)
    val outerUnion = SequentialUnion(
      Seq(innerUnion, testRelation3),
      byName = true,
      allowMissingCol = true)

    val result = FlattenSequentialUnion(outerUnion)

    val su = result.asInstanceOf[SequentialUnion]
    assert(su.allowMissingCol == true, "allowMissingCol parameter should be preserved")
  }

  test("ValidateSequentialUnion - valid plan passes") {
    // Create streaming relations
    val streamingRelation1 = testRelation1.copy(isStreaming = true)
    val streamingRelation2 = testRelation2.copy(isStreaming = true)

    val union = SequentialUnion(streamingRelation1, streamingRelation2)

    // Should not throw exception
    ValidateSequentialUnion(union)
  }

  test("ValidateSequentialUnion - rejects non-streaming children") {
    // Non-streaming relations
    val union = SequentialUnion(testRelation1, testRelation2)

    checkError(
      exception = intercept[AnalysisException] {
        ValidateSequentialUnion(union)
      },
      condition = "NOT_STREAMING_DATASET",
      parameters = Map("operator" -> "SequentialUnion"))
  }

  test("ValidateSequentialUnion - rejects nested SequentialUnion") {
    val streamingRelation1 = testRelation1.copy(isStreaming = true)
    val streamingRelation2 = testRelation2.copy(isStreaming = true)
    val streamingRelation3 = testRelation3.copy(isStreaming = true)

    val innerUnion = SequentialUnion(streamingRelation1, streamingRelation2)
    val outerUnion = SequentialUnion(innerUnion, streamingRelation3)

    checkError(
      exception = intercept[AnalysisException] {
        ValidateSequentialUnion(outerUnion)
      },
      condition = "NESTED_SEQUENTIAL_UNION",
      parameters = Map(
        "hint" -> "Use chained followedBy calls instead: df1.followedBy(df2).followedBy(df3)"))
  }

  test("ValidateSequentialUnion - rejects indirectly nested SequentialUnion through Project") {
    val streamingRelation1 = testRelation1.copy(isStreaming = true)
    val streamingRelation2 = testRelation2.copy(isStreaming = true)
    val streamingRelation3 = testRelation3.copy(isStreaming = true)

    // Create a nested SequentialUnion wrapped in a Project
    val innerUnion = SequentialUnion(streamingRelation1, streamingRelation2)
    val projectOverUnion = Project(Seq($"a", $"b"), innerUnion)
    val outerUnion = SequentialUnion(projectOverUnion, streamingRelation3)

    checkError(
      exception = intercept[AnalysisException] {
        ValidateSequentialUnion(outerUnion)
      },
      condition = "NESTED_SEQUENTIAL_UNION",
      parameters = Map(
        "hint" -> "Use chained followedBy calls instead: df1.followedBy(df2).followedBy(df3)"))
  }

  test("ValidateSequentialUnion - three or more children allowed") {
    val streamingRelation1 = testRelation1.copy(isStreaming = true)
    val streamingRelation2 = testRelation2.copy(isStreaming = true)
    val streamingRelation3 = testRelation3.copy(isStreaming = true)

    val union = SequentialUnion(Seq(streamingRelation1, streamingRelation2, streamingRelation3))

    // Should not throw exception
    ValidateSequentialUnion(union)
  }
}
