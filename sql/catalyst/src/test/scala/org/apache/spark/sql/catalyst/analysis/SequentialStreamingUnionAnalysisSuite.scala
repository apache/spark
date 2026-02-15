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
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LocalRelation, Project, SequentialStreamingUnion}
import org.apache.spark.sql.errors.DataTypeErrorsBase

class SequentialStreamingUnionAnalysisSuite extends AnalysisTest with DataTypeErrorsBase {

  private val testRelation1 = LocalRelation($"a".int, $"b".int)
  private val testRelation2 = LocalRelation($"a".int, $"b".int)
  private val testRelation3 = LocalRelation($"a".int, $"b".int)

  test("FlattenSequentialStreamingUnion - single level") {
    val union = SequentialStreamingUnion(testRelation1, testRelation2)
    val result = FlattenSequentialStreamingUnion(union)

    assert(result.isInstanceOf[SequentialStreamingUnion])
    val su = result.asInstanceOf[SequentialStreamingUnion]
    assert(su.children.length == 2)
  }

  test("FlattenSequentialStreamingUnion - nested unions get flattened") {
    val innerUnion = SequentialStreamingUnion(testRelation1, testRelation2)
    val outerUnion = SequentialStreamingUnion(innerUnion, testRelation3)

    val result = FlattenSequentialStreamingUnion(outerUnion)

    assert(result.isInstanceOf[SequentialStreamingUnion])
    val su = result.asInstanceOf[SequentialStreamingUnion]
    assert(su.children.length == 3, "Should flatten to 3 children")
    assert(!su.children.exists(_.isInstanceOf[SequentialStreamingUnion]),
      "No nested SequentialStreamingUnions")
  }

  test("FlattenSequentialStreamingUnion - deeply nested unions") {
    val union1 = SequentialStreamingUnion(testRelation1, testRelation2)
    val union2 = SequentialStreamingUnion(union1, testRelation3)

    val result = FlattenSequentialStreamingUnion(union2)

    val su = result.asInstanceOf[SequentialStreamingUnion]
    assert(su.children.length == 3)
  }

  test("FlattenSequentialStreamingUnion - multiple nested unions at same level") {
    val union1 = SequentialStreamingUnion(testRelation1, testRelation2)
    val union2 = SequentialStreamingUnion(testRelation2, testRelation3)
    val outerUnion = SequentialStreamingUnion(
      Seq(union1, union2), byName = false, allowMissingCol = false)

    val result = FlattenSequentialStreamingUnion(outerUnion)

    val su = result.asInstanceOf[SequentialStreamingUnion]
    assert(su.children.length == 4, "Should have 4 flattened children")
  }

  test("FlattenSequentialStreamingUnion - handles multi-level direct nesting") {
    // Test flattening when we have multiple levels of direct SequentialStreamingUnion nesting
    // e.g., df1.followedBy(df2).followedBy(df3).followedBy(df4)
    val union1 = SequentialStreamingUnion(testRelation1, testRelation2)
    val union2 = SequentialStreamingUnion(union1, testRelation3)
    val testRelation4 = LocalRelation($"a".int, $"b".int)
    val union3 = SequentialStreamingUnion(union2, testRelation4)

    val result = FlattenSequentialStreamingUnion(union3)

    val su = result.asInstanceOf[SequentialStreamingUnion]
    // Should recursively flatten all levels: rel1, rel2, rel3, rel4
    assert(su.children.length == 4, "Should recursively flatten all nested levels")
    assert(!su.children.exists(_.isInstanceOf[SequentialStreamingUnion]),
      "No SequentialStreamingUnion children should remain")
  }

  test("FlattenSequentialStreamingUnion - preserves byName parameter") {
    val innerUnion = SequentialStreamingUnion(testRelation1, testRelation2)
    val outerUnion = SequentialStreamingUnion(
      Seq(innerUnion, testRelation3), byName = true, allowMissingCol = false)

    val result = FlattenSequentialStreamingUnion(outerUnion)

    val su = result.asInstanceOf[SequentialStreamingUnion]
    assert(su.byName == true, "byName parameter should be preserved")
  }

  test("FlattenSequentialStreamingUnion - preserves allowMissingCol parameter") {
    val innerUnion = SequentialStreamingUnion(testRelation1, testRelation2)
    val outerUnion = SequentialStreamingUnion(
      Seq(innerUnion, testRelation3),
      byName = true,
      allowMissingCol = true)

    val result = FlattenSequentialStreamingUnion(outerUnion)

    val su = result.asInstanceOf[SequentialStreamingUnion]
    assert(su.allowMissingCol == true, "allowMissingCol parameter should be preserved")
  }

  test("ValidateSequentialStreamingUnion - valid plan passes") {
    // Create streaming relations
    val streamingRelation1 = testRelation1.copy(isStreaming = true)
    val streamingRelation2 = testRelation2.copy(isStreaming = true)

    val union = SequentialStreamingUnion(streamingRelation1, streamingRelation2)

    // Should not throw exception
    ValidateSequentialStreamingUnion(union)
  }

  test("ValidateSequentialStreamingUnion - rejects non-streaming children") {
    // Non-streaming relations
    val union = SequentialStreamingUnion(testRelation1, testRelation2)

    checkError(
      exception = intercept[AnalysisException] {
        ValidateSequentialStreamingUnion(union)
      },
      condition = "NOT_STREAMING_DATASET",
      parameters = Map("operator" -> "SequentialStreamingUnion"))
  }

  test("ValidateSequentialStreamingUnion - rejects directly nested unions") {
    val streamingRelation1 = testRelation1.copy(isStreaming = true)
    val streamingRelation2 = testRelation2.copy(isStreaming = true)
    val streamingRelation3 = testRelation3.copy(isStreaming = true)

    // Manually create a nested union without running flatten
    // (In practice, flatten would handle this, but validation catches it as a safeguard)
    val innerUnion = SequentialStreamingUnion(streamingRelation1, streamingRelation2)
    val outerUnion = SequentialStreamingUnion(innerUnion, streamingRelation3)

    checkError(
      exception = intercept[AnalysisException] {
        ValidateSequentialStreamingUnion(outerUnion)
      },
      condition = "NESTED_SEQUENTIAL_STREAMING_UNION",
      parameters = Map(
        "hint" -> "Use chained followedBy calls instead: df1.followedBy(df2).followedBy(df3)"))
  }

  test("ValidateSequentialStreamingUnion - rejects nested unions through other operators") {
    val streamingRelation1 = testRelation1.copy(isStreaming = true)
    val streamingRelation2 = testRelation2.copy(isStreaming = true)
    val streamingRelation3 = testRelation3.copy(isStreaming = true)

    // Create a nested union through a Project operator
    // e.g., from df1.select("a", "b").followedBy(df2)
    val innerUnion = SequentialStreamingUnion(streamingRelation1, streamingRelation2)
    val projectOverUnion = Project(Seq($"a", $"b"), innerUnion)
    val outerUnion = SequentialStreamingUnion(projectOverUnion, streamingRelation3)

    checkError(
      exception = intercept[AnalysisException] {
        ValidateSequentialStreamingUnion(outerUnion)
      },
      condition = "NESTED_SEQUENTIAL_STREAMING_UNION",
      parameters = Map(
        "hint" -> "Use chained followedBy calls instead: df1.followedBy(df2).followedBy(df3)"))
  }

  test("ValidateSequentialStreamingUnion - three or more children allowed") {
    val streamingRelation1 = testRelation1.copy(isStreaming = true)
    val streamingRelation2 = testRelation2.copy(isStreaming = true)
    val streamingRelation3 = testRelation3.copy(isStreaming = true)

    val union = SequentialStreamingUnion(
      Seq(streamingRelation1, streamingRelation2, streamingRelation3),
      byName = false,
      allowMissingCol = false)

    // Should not throw exception
    ValidateSequentialStreamingUnion(union)
  }

  test("ValidateSequentialStreamingUnion - rejects stateful children") {
    import org.apache.spark.sql.catalyst.expressions.aggregate.Count
    val streamingRelation1 = testRelation1.copy(isStreaming = true)
    val streamingRelation2 = testRelation2.copy(isStreaming = true)

    // Create an aggregation (stateful operation) as a child
    val agg = Aggregate(Seq($"a"), Seq($"a", Count($"b").toAggregateExpression().as("count")),
      streamingRelation2)

    val union = SequentialStreamingUnion(streamingRelation1, agg)

    checkError(
      exception = intercept[AnalysisException] {
        ValidateSequentialStreamingUnion(union)
      },
      condition = "STATEFUL_CHILDREN_NOT_SUPPORTED_IN_SEQUENTIAL_STREAMING_UNION",
      parameters = Map.empty)
  }

  test("ValidateSequentialStreamingUnion - rejects indirect stateful descendants") {
    import org.apache.spark.sql.catalyst.expressions.aggregate.Count
    val streamingRelation1 = testRelation1.copy(isStreaming = true)
    val streamingRelation2 = testRelation2.copy(isStreaming = true)

    // Create an aggregation wrapped in a Project (indirect stateful descendant)
    val agg = Aggregate(Seq($"a"), Seq($"a", Count($"b").toAggregateExpression().as("count")),
      streamingRelation2)
    val projectOverAgg = Project(Seq($"a"), agg)

    val union = SequentialStreamingUnion(streamingRelation1, projectOverAgg)

    checkError(
      exception = intercept[AnalysisException] {
        ValidateSequentialStreamingUnion(union)
      },
      condition = "STATEFUL_CHILDREN_NOT_SUPPORTED_IN_SEQUENTIAL_STREAMING_UNION",
      parameters = Map.empty)
  }

  test("ValidateSequentialStreamingUnion - allows non-stateful operations like select") {
    val streamingRelation1 = testRelation1.copy(isStreaming = true)
    val streamingRelation2 = testRelation2.copy(isStreaming = true)
    val streamingRelation3 = testRelation3.copy(isStreaming = true)

    // Project (select) is NOT a stateful operation, so this should be valid
    val project1 = Project(Seq($"a"), streamingRelation1)
    val project2 = Project(Seq($"a", $"b"), streamingRelation2)
    val project3 = Project(Seq($"a"), streamingRelation3)

    val union = SequentialStreamingUnion(
      Seq(project1, project2, project3),
      byName = false,
      allowMissingCol = false)

    // Should not throw exception - Project is not a stateful operation
    ValidateSequentialStreamingUnion(union)
  }

  test("Chained followedBy with select operations - full analyzer flow") {
    val streamingRelation1 = testRelation1.copy(isStreaming = true)
    val streamingRelation2 = testRelation2.copy(isStreaming = true)
    val streamingRelation3 = testRelation3.copy(isStreaming = true)

    // Simulate: df1.select("a").followedBy(df2.select("a", "b")).followedBy(df3.select("a"))
    val project1 = Project(Seq($"a"), streamingRelation1)
    val project2 = Project(Seq($"a", $"b"), streamingRelation2)
    val project3 = Project(Seq($"a"), streamingRelation3)

    // This creates the nested structure from chaining:
    // df1.select("a").followedBy(df2.select("a", "b"))
    val innerUnion = SequentialStreamingUnion(project1, project2)
    // .followedBy(df3.select("a"))
    val outerUnion = SequentialStreamingUnion(innerUnion, project3)

    // Step 1: Flatten the nested unions
    val flattened = FlattenSequentialStreamingUnion(outerUnion)
    val flattenedUnion = flattened.asInstanceOf[SequentialStreamingUnion]

    // Verify flattening worked
    assert(flattenedUnion.children.length == 3, "Should flatten to 3 children")
    assert(!flattenedUnion.children.exists(_.isInstanceOf[SequentialStreamingUnion]),
      "No nested SequentialStreamingUnions after flattening")

    // Step 2: Validate the flattened plan
    // Should not throw exception - Project is not stateful, all are streaming
    ValidateSequentialStreamingUnion(flattenedUnion)

    // Verify the children are the expected Project nodes
    assert(flattenedUnion.children(0).isInstanceOf[Project])
    assert(flattenedUnion.children(1).isInstanceOf[Project])
    assert(flattenedUnion.children(2).isInstanceOf[Project])
  }
}
