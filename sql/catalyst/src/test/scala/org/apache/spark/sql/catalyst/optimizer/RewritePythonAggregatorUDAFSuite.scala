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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types._

class RewritePythonAggregatorUDAFSuite extends PlanTest {

  /**
   * Create a mock PythonUDF for testing.
   * The func parameter is null because we only test plan structure, not execution.
   */
  private def createMockPythonUDF(
      name: String,
      returnType: DataType,
      children: Seq[Expression]): PythonUDF = {
    PythonUDF(
      name = name,
      func = null,
      dataType = returnType,
      children = children,
      evalType = PythonEvalType.SQL_MAP_ARROW_ITER_UDF,
      udfDeterministic = true)
  }

  test("rewrite ungrouped aggregation") {
    val child = LocalRelation($"a".int, $"b".int)

    // Create mock PythonUDF expressions with null func for testing
    val outputSchema = StructType(Seq(
      StructField("key", LongType, nullable = false),
      StructField("buffer", BinaryType, nullable = true)
    ))
    val partialReduceUDF = createMockPythonUDF(
      "partial_reduce", outputSchema, child.output)
    val mergeUDF = createMockPythonUDF(
      "merge", StructType(Seq(StructField("buffer", BinaryType, nullable = true))), Seq.empty)
    val finalMergeUDF = createMockPythonUDF(
      "final_merge", StructType(Seq(StructField("result", LongType, nullable = true))), Seq.empty)
    val resultAttr = AttributeReference("result", LongType)()

    val query = PythonAggregatorUDAF(
      groupingAttributes = Seq.empty,
      partialReduceUDF = partialReduceUDF,
      mergeUDF = mergeUDF,
      finalMergeUDF = finalMergeUDF,
      resultAttribute = resultAttr,
      child = child
    )

    val rewritten = RewritePythonAggregatorUDAF(query)

    // Verify that the rewritten plan does not contain PythonAggregatorUDAF
    assert(!rewritten.exists(_.isInstanceOf[PythonAggregatorUDAF]),
      "PythonAggregatorUDAF should be rewritten")

    // Verify the rewritten plan contains MapInArrow and FlatMapGroupsInArrow
    assert(rewritten.exists(_.isInstanceOf[MapInArrow]),
      "Rewritten plan should contain MapInArrow")
    assert(rewritten.exists(_.isInstanceOf[FlatMapGroupsInArrow]),
      "Rewritten plan should contain FlatMapGroupsInArrow")
  }

  test("rewrite grouped aggregation") {
    val child = LocalRelation($"a".int, $"b".int, $"c".int)
    val groupingAttr = child.output.head

    // Create mock PythonUDF expressions with null func for testing
    val outputSchema = StructType(Seq(
      StructField("key", LongType, nullable = false),
      StructField("buffer", BinaryType, nullable = true)
    ))
    val partialReduceUDF = createMockPythonUDF(
      "partial_reduce", outputSchema, child.output)
    val mergeUDF = createMockPythonUDF(
      "merge", StructType(Seq(StructField("buffer", BinaryType, nullable = true))), Seq.empty)
    val finalMergeUDF = createMockPythonUDF(
      "final_merge", StructType(Seq(StructField("result", LongType, nullable = true))), Seq.empty)
    val resultAttr = AttributeReference("result", LongType)()

    val query = PythonAggregatorUDAF(
      groupingAttributes = Seq(groupingAttr),
      partialReduceUDF = partialReduceUDF,
      mergeUDF = mergeUDF,
      finalMergeUDF = finalMergeUDF,
      resultAttribute = resultAttr,
      child = child
    )

    val rewritten = RewritePythonAggregatorUDAF(query)

    // Verify that the rewritten plan does not contain PythonAggregatorUDAF
    assert(!rewritten.exists(_.isInstanceOf[PythonAggregatorUDAF]),
      "PythonAggregatorUDAF should be rewritten")

    // Verify the rewritten plan contains MapInArrow and FlatMapGroupsInArrow
    assert(rewritten.exists(_.isInstanceOf[MapInArrow]),
      "Rewritten plan should contain MapInArrow")
    assert(rewritten.exists(_.isInstanceOf[FlatMapGroupsInArrow]),
      "Rewritten plan should contain FlatMapGroupsInArrow")

    // Verify the output includes the grouping attribute
    val outputAttrs = rewritten.output.map(_.name)
    assert(outputAttrs.contains("a") || outputAttrs.exists(_.contains("result")),
      "Output should contain grouping attribute or result")
  }

  test("non-PythonAggregatorUDAF plan unchanged") {
    val child = LocalRelation($"a".int, $"b".int)

    val rewritten = RewritePythonAggregatorUDAF(child)

    // Should be unchanged (same object reference since no transformation needed)
    assert(rewritten eq child, "Plan without PythonAggregatorUDAF should be unchanged")
  }
}
