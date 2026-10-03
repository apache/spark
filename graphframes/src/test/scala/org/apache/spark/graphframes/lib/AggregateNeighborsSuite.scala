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

package org.apache.spark.graphframes.lib

import org.apache.spark.graphframes.GraphFrame
import org.apache.spark.graphframes.GraphFrameTestSparkContext
import org.apache.spark.graphframes.SparkFunSuite
import org.apache.spark.sql.functions._

class AggregateNeighborsSuite extends SparkFunSuite with GraphFrameTestSparkContext {

  test("find all paths between two vertices using AggregateNeighbors") {
    // Create a simple graph: 1 -> 2 -> 3 -> 4, and also 1 -> 3
    val vertices =
      spark.createDataFrame(Seq((1L, "A"), (2L, "B"), (3L, "C"), (4L, "D"))).toDF("id", "name")

    val edges = spark
      .createDataFrame(
        Seq((1L, 2L, "edge1"), (2L, 3L, "edge2"), (3L, 4L, "edge3"), (1L, 3L, "edge4")))
      .toDF("src", "dst", "edgeAttr")

    val graph = GraphFrame(vertices, edges)

    // We want to find all paths from vertex 1 to vertex 4
    val sourceId = 1L
    val targetId = 4L

    // Use AggregateNeighbors to find paths
    // We'll track paths as strings in an accumulator
    val agg = graph.aggregateNeighbors
      .setStartingVertices(col("id") === sourceId)
      .setMaxHops(5) // Enough to reach vertex 4
      .setTargetCondition(
        AggregateNeighbors.dstAttr("id") === lit(targetId)
      ) // Using dst_id from the internal join
      .addAccumulator(
        "path",
        // Initialize accumulator: start with source vertex ID
        lit(sourceId.toString),
        // Update accumulator: append current destination vertex ID
        concat(col("path"), lit("->"), col("dst_id").cast("string")))
      .setRequiredVertexAttributes(Seq("id", "name"))
      .setRequiredEdgeAttributes(Seq("edgeAttr"))
      .run()

    // The result should contain paths from source to target
    // Let's collect and analyze the results
    val results = agg.collect()

    // Expected paths:
    // Path 1: 1 -> 2 -> 3 -> 4
    // Path 2: 1 -> 3 -> 4
    val expectedPaths = Set(s"$sourceId->2->3->$targetId", s"$sourceId->3->$targetId")

    // Extract paths from results
    val actualPaths = results.map { row =>
      // The path accumulator should be in the result
      row.getAs[String]("path")
    }.toSet

    // Verify we found the correct number of paths
    assert(results.length === 2, s"Expected 2 paths but found ${results.length}")

    // Verify each expected path is present
    expectedPaths.foreach { expectedPath =>
      assert(
        actualPaths.contains(expectedPath),
        s"Path $expectedPath not found in results: $actualPaths")
    }

    // Also verify that each result has the correct target vertex ID
    results.foreach { row =>
      val id = row.getAs[Long]("id")
      assert(id === targetId, s"Result should have target vertex ID $targetId, but found $id")
    }
  }

  test("AggregateNeighbors with multiple accumulators") {
    // Create a simple graph
    val vertices = spark.createDataFrame(Seq((1L, 10), (2L, 20), (3L, 30))).toDF("id", "value")

    val edges = spark
      .createDataFrame(Seq((1L, 2L, 5.0), (2L, 3L, 6.0), (1L, 3L, 7.0)))
      .toDF("src", "dst", "weight")

    val graph = GraphFrame(vertices, edges)

    // Test with multiple accumulators: sum of values and product of weights
    val sourceId = 1L
    val targetId = 3L

    val agg = graph.aggregateNeighbors
      .setStartingVertices(col("id") === sourceId)
      .setMaxHops(3)
      .setTargetCondition(AggregateNeighbors.dstAttr("id") === lit(targetId))
      .addAccumulator(
        "sum_values",
        lit(0L),
        col("sum_values") + AggregateNeighbors.dstAttr("value"))
      .addAccumulator(
        "product_weights",
        lit(1.0),
        col("product_weights") * AggregateNeighbors.edgeAttr("weight"))
      .setRequiredVertexAttributes(Seq("id", "value"))
      .setRequiredEdgeAttributes(Seq("weight"))
      .run()

    val results = agg.collect()

    // There should be 2 paths from 1 to 3
    assert(results.length === 2)

    // Sort results by product_weights for consistent checking
    val sortedResults = results.sortBy(row => row.getAs[Double]("product_weights"))

    // Path 1: 1 -> 3 directly
    val directPath = sortedResults(0)
    assert(directPath.getAs[Long]("sum_values") === 30L) // Only vertex 3's value (30)
    assert(math.abs(directPath.getAs[Double]("product_weights") - 7.0) < 0.001)

    // Path 2: 1 -> 2 -> 3
    val indirectPath = sortedResults(1)
    assert(indirectPath.getAs[Long]("sum_values") === 50L) // 20 + 30 = 50
    assert(
      math.abs(indirectPath.getAs[Double]("product_weights") - 30.0) < 0.001
    ) // 5.0 * 6.0 = 30.0
  }

  test("AggregateNeighbors with stopping condition") {
    val vertices = spark.createDataFrame(Seq((1L, "A"), (2L, "B"), (3L, "C"))).toDF("id", "name")

    val edges = spark
      .createDataFrame(Seq((1L, 2L), (2L, 3L), (3L, 1L), (1L, 3L), (2L, 1L)))
      .toDF("src", "dst")

    val graph = GraphFrame(vertices, edges)

    // We want to stop when we revisit a vertex (to avoid infinite loops)
    // We'll track visited vertices in an accumulator
    val sourceId = 1L

    val agg = graph.aggregateNeighbors
      .setStartingVertices(col("id") === sourceId)
      .setMaxHops(10)
      .setStoppingCondition(
        // Stop when we've already visited the destination vertex
        array_contains(col("visited_vertices"), AggregateNeighbors.dstAttr("id")))
      .addAccumulator(
        "visited_vertices",
        array(lit(sourceId)),
        // Add the current destination vertex to the visited list
        array_append(col("visited_vertices"), AggregateNeighbors.dstAttr("id")))
      .addAccumulator("path_length", lit(0L), col("path_length") + lit(1L))
      .setRequiredVertexAttributes(Seq("id"))
      .run()

    val results = agg.collect()

    // We should get results for each vertex reachable without cycles
    // Since we stop when revisiting a vertex, we should get:
    // 1 - 3 - 1
    // 1 - 2 - 1
    // 1 - 2 - 3 - 1
    // Count results
    assert(results.length === 3)

    // Check path lengths
    assert(results.map(_.getAs[Long]("path_length")).toSet == Set(2, 3))
    assert(results.forall(r => r.getAs[Long]("path_length") == r.getAs[Int]("hop")))
  }

  test("AggregateNeighbors with edge filter") {
    // Create a graph with different types of edges
    val vertices =
      spark.createDataFrame(Seq((1L, "A"), (2L, "B"), (3L, "C"), (4L, "D"))).toDF("id", "name")

    val edges = spark
      .createDataFrame(
        Seq(
          (1L, 2L, "allowed"),
          (2L, 3L, "allowed"),
          (3L, 4L, "allowed"),
          (1L, 3L, "blocked"),
          (2L, 4L, "blocked")))
      .toDF("src", "dst", "type")

    val graph = GraphFrame(vertices, edges)

    // Only traverse edges with type "allowed"
    val sourceId = 1L
    val targetId = 4L

    val agg = graph.aggregateNeighbors
      .setStartingVertices(col("id") === sourceId)
      .setMaxHops(5)
      .setTargetCondition(AggregateNeighbors.dstAttr("id") === lit(targetId))
      .setEdgeFilter(AggregateNeighbors.edgeAttr("type") === lit("allowed"))
      .addAccumulator(
        "path",
        lit(sourceId.toString),
        concat(col("path"), lit("->"), AggregateNeighbors.dstAttr("id").cast("string")))
      .setRequiredEdgeAttributes(Seq("type"))
      .setRequiredVertexAttributes(Seq("id"))
      .run()

    val results = agg.collect()

    // With the edge filter, only allowed edges should be traversed
    // Allowed path: 1 -> 2 -> 3 -> 4
    // The direct path 1 -> 3 uses a "blocked" edge, so it shouldn't be found
    // The path 1 -> 2 -> 4 uses a "blocked" edge for the last hop

    // So only one path should be found
    assert(results.length === 1)

    val actualPath = results(0).getAs[String]("path")
    val expectedPath = s"$sourceId->2->3->$targetId"
    assert(actualPath === expectedPath)
  }

  test("AggregateNeighbors with empty graph") {
    // Create an empty graph with no vertices and no edges
    val vertices = spark.createDataFrame(Seq.empty[(Long, String)]).toDF("id", "name")
    val edges = spark.createDataFrame(Seq.empty[(Long, Long)]).toDF("src", "dst")

    val graph = GraphFrame(vertices, edges)

    // Run aggregate neighbors with arbitrary starting vertex
    val agg = graph.aggregateNeighbors
      .setStartingVertices(col("id") === 1L)
      .setStoppingCondition(lit(false))
      .setMaxHops(3)
      .addAccumulator("count", lit(0L), col("count") + lit(1L))
      .run()

    val results = agg.collect()

    // Should return empty results without errors
    assert(results.length === 0)
  }

  test("AggregateNeighbors with disconnected vertices") {
    // Create a graph with disconnected components
    // Component 1: 1 -> 2
    // Component 2: 3 -> 4 (disconnected from component 1)
    val vertices = spark
      .createDataFrame(Seq((1L, "A"), (2L, "B"), (3L, "C"), (4L, "D")))
      .toDF("id", "name")

    val edges = spark
      .createDataFrame(Seq((1L, 2L, "edge1"), (3L, 4L, "edge2")))
      .toDF("src", "dst", "edgeAttr")

    val graph = GraphFrame(vertices, edges)

    // Start from vertex 1, try to reach vertex 4 (disconnected)
    val sourceId = 1L
    val targetId = 4L

    val agg = graph.aggregateNeighbors
      .setStartingVertices(col("id") === sourceId)
      .setMaxHops(5)
      .setTargetCondition(AggregateNeighbors.dstAttr("id") === lit(targetId))
      .addAccumulator(
        "path",
        lit(sourceId.toString),
        concat(col("path"), lit("->"), AggregateNeighbors.dstAttr("id").cast("string")))
      .setRequiredVertexAttributes(Seq("id"))
      .setRequiredEdgeAttributes(Seq("edgeAttr"))
      .run()

    val results = agg.collect()

    // No path should exist to disconnected vertex 4
    assert(results.length === 0)

    // Now test within connected component
    val agg2 = graph.aggregateNeighbors
      .setStartingVertices(col("id") === sourceId)
      .setMaxHops(5)
      .setTargetCondition(AggregateNeighbors.dstAttr("id") === lit(2L))
      .addAccumulator(
        "path",
        lit(sourceId.toString),
        concat(col("path"), lit("->"), AggregateNeighbors.dstAttr("id").cast("string")))
      .setRequiredVertexAttributes(Seq("id"))
      .setRequiredEdgeAttributes(Seq("edgeAttr"))
      .run()

    val results2 = agg2.collect()

    // Path should exist within connected component
    assert(results2.length === 1)
    assert(results2(0).getAs[String]("path") === s"$sourceId->2")
  }

  test("AggregateNeighbors with self-loops") {
    // Create a graph with a self-loop
    val vertices =
      spark.createDataFrame(Seq((1L, "A"), (2L, "B"), (3L, "C"))).toDF("id", "name")

    val edges = spark
      .createDataFrame(
        Seq(
          (1L, 2L, "edge1"),
          (2L, 2L, "self-loop"), // Self-loop on vertex 2
          (2L, 3L, "edge2")))
      .toDF("src", "dst", "edgeAttr")

    val graph = GraphFrame(vertices, edges)

    val sourceId = 1L
    val targetId = 3L

    // Test without stopping condition to see self-loop behavior
    val agg = graph.aggregateNeighbors
      .setStartingVertices(col("id") === sourceId)
      .setMaxHops(3)
      .setTargetCondition(AggregateNeighbors.dstAttr("id") === lit(targetId))
      .addAccumulator(
        "path",
        lit(sourceId.toString),
        concat(col("path"), lit("->"), AggregateNeighbors.dstAttr("id").cast("string")))
      .setRequiredVertexAttributes(Seq("id"))
      .setRequiredEdgeAttributes(Seq("edgeAttr"))
      .run()

    val results = agg.collect()

    // Should find at least one path to target
    assert(results.length >= 1)

    // Verify direct path exists
    val paths = results.map(_.getAs[String]("path")).toSet
    assert(paths.contains(s"$sourceId->2->3"))
  }

  test("AggregateNeighbors with multiple edge types") {
    // Create a graph with multiple edge types
    val vertices = spark
      .createDataFrame(Seq((1L, "A"), (2L, "B"), (3L, "C"), (4L, "D")))
      .toDF("id", "name")

    val edges = spark
      .createDataFrame(
        Seq(
          (1L, 2L, "friend"),
          (2L, 3L, "colleague"),
          (3L, 4L, "friend"),
          (1L, 3L, "colleague"),
          (2L, 4L, "friend")))
      .toDF("src", "dst", "edgeType")

    val graph = GraphFrame(vertices, edges)

    val sourceId = 1L
    val targetId = 4L

    // Test with edge type filter for "friend" edges only
    val agg = graph.aggregateNeighbors
      .setStartingVertices(col("id") === sourceId)
      .setMaxHops(5)
      .setTargetCondition(AggregateNeighbors.dstAttr("id") === lit(targetId))
      .setEdgeFilter(AggregateNeighbors.edgeAttr("edgeType") === lit("friend"))
      .addAccumulator(
        "path",
        lit(sourceId.toString),
        concat(col("path"), lit("->"), AggregateNeighbors.dstAttr("id").cast("string")))
      .setRequiredVertexAttributes(Seq("id"))
      .setRequiredEdgeAttributes(Seq("edgeType"))
      .run()

    val results = agg.collect()

    // With friend-only filter, should find paths using only friend edges
    // Path: 1 -> 2 -> 4 (all friend edges)
    assert(results.length === 1)
    assert(results(0).getAs[String]("path") === s"$sourceId->2->$targetId")

    // Test without filter - should find all paths
    val agg2 = graph.aggregateNeighbors
      .setStartingVertices(col("id") === sourceId)
      .setMaxHops(5)
      .setTargetCondition(AggregateNeighbors.dstAttr("id") === lit(targetId))
      .addAccumulator(
        "path",
        lit(sourceId.toString),
        concat(col("path"), lit("->"), AggregateNeighbors.dstAttr("id").cast("string")))
      .setRequiredVertexAttributes(Seq("id"))
      .setRequiredEdgeAttributes(Seq("edgeType"))
      .run()

    val results2 = agg2.collect()

    // Should find multiple paths without filtering
    assert(results2.length > 1)
  }

  test("AggregateNeighbors with large vertex degrees") {
    // Create a star graph: center vertex (1) connected to many leaf vertices
    val numLeaves = 100

    // Center vertex
    val centerVertex = (1L, "center")

    // Leaf vertices (2 to numLeaves+1)
    val leafVertices = (2L to (numLeaves + 1).toLong).map(id => (id, s"leaf_$id"))

    val vertices = spark
      .createDataFrame(centerVertex +: leafVertices)
      .toDF("id", "name")

    // Edges from center to all leaves
    val edges = leafVertices
      .map { case (leafId, _) =>
        (1L, leafId, "connects")
      }

    val edgesDF = spark
      .createDataFrame(edges)
      .toDF("src", "dst", "edgeType")

    val graph = GraphFrame(vertices, edgesDF)

    val sourceId = 1L

    // Test aggregation from high-degree vertex
    val agg = graph.aggregateNeighbors
      .setStartingVertices(col("id") === sourceId)
      .setMaxHops(2)
      .addAccumulator("count", lit(0L), col("count") + lit(1L))
      .setTargetCondition(
        AggregateNeighbors.dstAttr("id").isInCollection((2L to (numLeaves + 1).toLong).toSeq))
      .setRequiredVertexAttributes(Seq("id"))
      .setRequiredEdgeAttributes(Seq("edgeType"))
      .run()

    val results = agg.collect()

    // Should find all leaf vertices without performance issues
    assert(results.length === numLeaves)

    // Verify all accumulators are correctly computed
    results.foreach { row =>
      assert(row.getAs[Long]("count") === 1L)
    }
  }
}
