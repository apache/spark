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

package org.apache.spark.graphframes

import org.apache.spark.graphframes.lib.Pregel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.graphframes.GraphFrameInternals

/**
 * Unit tests for GraphFrameInternals.extractColumnReferences.
 *
 * These tests verify that column references are correctly extracted from various expression
 * patterns, which is critical for the Pregel dst join optimization. The optimization skips a join
 * when dst columns are not referenced, so we must correctly identify all column references.
 */
class GraphFrameInternalsSuite extends SparkFunSuite with GraphFrameTestSparkContext {

  // ============================================================================
  // Basic Column References
  // ============================================================================

  test("extractColumnReferences - simple dot notation") {
    val refs = GraphFrameInternals.extractColumnReferences(spark, col("src.id"))
    assert(refs === Map("src" -> Set("id")))
  }

  test("extractColumnReferences - Pregel.src helper") {
    val refs = GraphFrameInternals.extractColumnReferences(spark, Pregel.src("rank"))
    assert(refs === Map("src" -> Set("rank")))
  }

  test("extractColumnReferences - Pregel.dst helper") {
    val refs = GraphFrameInternals.extractColumnReferences(spark, Pregel.dst("value"))
    assert(refs === Map("dst" -> Set("value")))
  }

  test("extractColumnReferences - Pregel.edge helper") {
    val refs = GraphFrameInternals.extractColumnReferences(spark, Pregel.edge("weight"))
    assert(refs === Map("edge" -> Set("weight")))
  }

  test("extractColumnReferences - whole struct reference") {
    val refs = GraphFrameInternals.extractColumnReferences(spark, col("dst"))
    assert(refs === Map("dst" -> Set()))
  }

  // ============================================================================
  // Bracket Notation
  // ============================================================================

  test("extractColumnReferences - bracket notation") {
    val refs = GraphFrameInternals.extractColumnReferences(spark, col("src")("id"))
    assert(refs === Map("src" -> Set("id")))
  }

  // ============================================================================
  // Complex Expressions with Multiple References
  // ============================================================================

  test("extractColumnReferences - arithmetic with multiple refs from same prefix") {
    val refs =
      GraphFrameInternals.extractColumnReferences(
        spark,
        Pregel.src("rank") / Pregel.src("outDegree"))
    assert(refs === Map("src" -> Set("rank", "outDegree")))
  }

  test("extractColumnReferences - expression with src, dst, and edge") {
    val refs = GraphFrameInternals.extractColumnReferences(
      spark,
      Pregel.src("value") + Pregel.dst("value") + Pregel.edge("weight"))
    assert(refs === Map("src" -> Set("value"), "dst" -> Set("value"), "edge" -> Set("weight")))
  }

  test("extractColumnReferences - when/case expression") {
    val refs = GraphFrameInternals.extractColumnReferences(
      spark,
      when(Pregel.dst("value") > Pregel.src("value"), Pregel.edge("weight")))
    assert(refs.contains("dst"), "Should detect dst reference")
    assert(refs.contains("src"), "Should detect src reference")
    assert(refs.contains("edge"), "Should detect edge reference")
  }

  test("extractColumnReferences - coalesce with multiple refs") {
    val refs =
      GraphFrameInternals.extractColumnReferences(
        spark,
        coalesce(col("dst.value"), col("src.default")))
    assert(refs.contains("dst"))
    assert(refs.contains("src"))
  }

  // ============================================================================
  // Column Used as Map/Array Key - Critical Cases!
  // These verify that foreach traversal catches column refs used as arguments
  // ============================================================================

  test("extractColumnReferences - column used as map key via element_at") {
    // element_at(col("edge.weights"), col("dst.name"))
    // Should detect BOTH "edge" and "dst" references
    val refs =
      GraphFrameInternals.extractColumnReferences(
        spark,
        element_at(col("edge.weights"), col("dst.name")))
    assert(refs.contains("edge"), "Should detect edge reference (the map)")
    assert(refs.contains("dst"), "Should detect dst reference (used as map key)")
  }

  test("extractColumnReferences - column used as array index via element_at") {
    // element_at(col("edge.values"), col("dst.index"))
    // Should detect BOTH "edge" and "dst" references
    val refs =
      GraphFrameInternals.extractColumnReferences(
        spark,
        element_at(col("edge.values"), col("dst.index")))
    assert(refs.contains("edge"), "Should detect edge reference (the array)")
    assert(refs.contains("dst"), "Should detect dst reference (used as array index)")
  }

  test("extractColumnReferences - column in nested function call") {
    // concat(col("src.prefix"), col("dst.suffix"))
    val refs =
      GraphFrameInternals.extractColumnReferences(
        spark,
        concat(col("src.prefix"), col("dst.suffix")))
    assert(refs.contains("src"))
    assert(refs.contains("dst"))
  }

  test("extractColumnReferences - column in aggregate-like expression") {
    // greatest(col("src.value"), col("dst.value"), col("edge.weight"))
    val refs = GraphFrameInternals.extractColumnReferences(
      spark,
      greatest(col("src.value"), col("dst.value"), col("edge.weight")))
    assert(refs.contains("src"))
    assert(refs.contains("dst"))
    assert(refs.contains("edge"))
  }

  // ============================================================================
  // Deeply Nested Struct Access
  // ============================================================================

  test("extractColumnReferences - deeply nested struct via bracket notation") {
    // col("dst")("location")("city") - three levels deep
    // Should still detect "dst" as a prefix
    val refs = GraphFrameInternals.extractColumnReferences(spark, col("dst")("location")("city"))
    assert(refs.contains("dst"), "Should detect dst prefix even for deeply nested access")
  }

  test("extractColumnReferences - two-level nesting via dot notation") {
    // col("dst.location.city") - parsed as UnresolvedAttribute(Seq("dst", "location", "city"))
    val refs = GraphFrameInternals.extractColumnReferences(spark, col("dst.location.city"))
    assert(refs.contains("dst"))
    // We should get "location" as the first-level field (we only parse one level deep)
    assert(refs("dst").contains("location"))
  }

  // ============================================================================
  // Edge Cases - No Column References
  // ============================================================================

  test("extractColumnReferences - literal only expression") {
    val refs = GraphFrameInternals.extractColumnReferences(spark, lit(42))
    assert(refs.isEmpty)
  }

  test("extractColumnReferences - null literal") {
    val refs = GraphFrameInternals.extractColumnReferences(spark, lit(null))
    assert(refs.isEmpty)
  }

  test("extractColumnReferences - complex math with no columns") {
    val refs = GraphFrameInternals.extractColumnReferences(spark, lit(1) + lit(2) * lit(3))
    assert(refs.isEmpty)
  }

  test("extractColumnReferences - string literal") {
    val refs = GraphFrameInternals.extractColumnReferences(spark, lit("hello"))
    assert(refs.isEmpty)
  }

  // ============================================================================
  // Mixed Expressions - Columns and Literals
  // ============================================================================

  test("extractColumnReferences - column plus literal") {
    val refs = GraphFrameInternals.extractColumnReferences(spark, col("src.value") + lit(10))
    assert(refs === Map("src" -> Set("value")))
  }

  test("extractColumnReferences - conditional with literal fallback") {
    val refs = GraphFrameInternals.extractColumnReferences(
      spark,
      when(col("dst.flag"), col("src.value")).otherwise(lit(0)))
    assert(refs.contains("dst"))
    assert(refs.contains("src"))
  }
}
