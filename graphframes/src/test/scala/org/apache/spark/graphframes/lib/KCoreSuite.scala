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

import org.apache.spark.sql.types.DataTypes
import org.apache.spark.graphframes._
import org.apache.spark.graphframes.examples.Graphs

class KCoreSuite extends SparkFunSuite with GraphFrameTestSparkContext {
  test("empty graph") {
    val empty = Graphs.empty[Int]
    val result = empty.kCore.run()
    assert(result.count() === 0L)
    result.unpersist()
  }

  test("single vertex") {
    val v = spark.createDataFrame(Seq((0L, "a"))).toDF("id", "name")
    // Create an empty dataframe with the proper columns.
    val e = spark.createDataFrame(Seq.empty[(Long, Long)]).toDF("src", "dst")
    val g = GraphFrame(v, e)
    val result = g.kCore.run()
    TestUtils.checkColumnType(result.schema, "kcore", DataTypes.IntegerType)
    assert(result.count() === 1)
    val rows = result.collect()
    assert(rows.head.getAs[Int]("kcore") === 0)
    result.unpersist()
  }

  test("two connected vertices") {
    val v = spark.createDataFrame(Seq((0L, "a"), (1L, "b"))).toDF("id", "name")
    val e = spark.createDataFrame(Seq((0L, 1L))).toDF("src", "dst")
    val g = GraphFrame(v, e)
    val result = g.kCore.run()
    TestUtils.checkColumnType(result.schema, "kcore", DataTypes.IntegerType)
    assert(result.count() === 2)
    val rows = result.collect()
    // Both vertices should have k-core value of 1
    rows.foreach { row =>
      assert(row.getAs[Int]("kcore") === 1)
    }
    result.unpersist()
  }

  test("triangle graph") {
    val v = spark.createDataFrame(Seq((0L, "a"), (1L, "b"), (2L, "c"))).toDF("id", "name")
    val e = spark.createDataFrame(Seq((0L, 1L), (1L, 2L), (2L, 0L))).toDF("src", "dst")
    val g = GraphFrame(v, e)
    val result = g.kCore.run()
    TestUtils.checkColumnType(result.schema, "kcore", DataTypes.IntegerType)
    assert(result.count() === 3)
    val rows = result.collect()
    // All vertices should have k-core value of 2
    rows.foreach { row =>
      assert(row.getAs[Int]("kcore") === 2)
    }
    result.unpersist()
  }

  test("star graph") {
    val v = spark
      .createDataFrame(Seq((0L, "center"), (1L, "leaf1"), (2L, "leaf2"), (3L, "leaf3")))
      .toDF("id", "name")
    val e = spark.createDataFrame(Seq((0L, 1L), (0L, 2L), (0L, 3L))).toDF("src", "dst")
    val g = GraphFrame(v, e)
    val result = g.kCore.run()
    TestUtils.checkColumnType(result.schema, "kcore", DataTypes.IntegerType)
    assert(result.count() === 4)
    val rows = result.collect()
    // All vertices have k-core = 1: despite the center having degree 3, each leaf has
    // only one neighbor so no 2-core can form, pulling everything down to 1.
    rows.foreach { row =>
      assert(row.getAs[Int]("kcore") === 1)
    }
    result.unpersist()
  }

  test("chain graph") {
    // Open chain: 0 - 1 - 2.
    // No 2-core exists: endpoints have degree 1, so the whole graph is only a 1-core.
    // All vertices get kcore = 1.
    val v = spark.createDataFrame(Seq((0L, "a"), (1L, "b"), (2L, "c"))).toDF("id", "name")
    val e = spark.createDataFrame(Seq((0L, 1L), (1L, 2L))).toDF("src", "dst")
    val g = GraphFrame(v, e)
    val result = g.kCore.run()
    TestUtils.checkColumnType(result.schema, "kcore", DataTypes.IntegerType)
    assert(result.count() === 3)
    val rows = result.collect()
    rows.foreach { row =>
      assert(row.getAs[Int]("kcore") === 1)
    }
    result.unpersist()
  }

  test("disconnected vertices") {
    val v = spark.createDataFrame(Seq((0L, "a"), (1L, "b"), (2L, "c"))).toDF("id", "name")
    val e = spark.createDataFrame(Seq.empty[(Long, Long)]).toDF("src", "dst")
    val g = GraphFrame(v, e)
    val result = g.kCore.run()
    TestUtils.checkColumnType(result.schema, "kcore", DataTypes.IntegerType)
    assert(result.count() === 3)
    val rows = result.collect()
    // All vertices should have k-core value of 0
    rows.foreach { row =>
      assert(row.getAs[Int]("kcore") === 0)
    }
    result.unpersist()
  }

  test("friends graph") {
    val friends = Graphs.friends
    val result = friends.kCore.run()
    TestUtils.checkColumnType(result.schema, "kcore", DataTypes.IntegerType)
    assert(result.count() === friends.vertices.count())
    // In the friends graph, all vertices except 'g' should have k-core >= 1
    // 'g' is isolated, so it should have k-core 0
    val rows = result.collect()
    rows.foreach { row =>
      val id = row.getAs[String]("id")
      val kcore = row.getAs[Int]("kcore")
      if (id == "g") {
        assert(kcore === 0)
      } else {
        assert(kcore >= 1)
      }
    }
    result.unpersist()
  }

  test("medium graph with varying k-core values") {
    // Create a graph with 25 vertices and varying degrees to get different k-core values
    val v =
      spark.createDataFrame((0L until 25L).map(id => (id, s"vertex_$id"))).toDF("id", "name")

    // Create edges to form a graph with diverse connectivity
    val edges = Seq(
      // High degree cluster around vertex 0 (should have high k-core)
      (0L, 1L),
      (0L, 2L),
      (0L, 3L),
      (0L, 4L),
      (0L, 5L),
      (1L, 2L),
      (1L, 3L),
      (2L, 3L),
      (2L, 4L),
      (3L, 4L),
      (1L, 6L),
      (2L, 7L),
      (3L, 8L),
      (4L, 9L),
      (5L, 10L),

      // Medium degree cluster around vertex 11
      (11L, 12L),
      (11L, 13L),
      (11L, 14L),
      (12L, 13L),
      (12L, 15L),
      (13L, 14L),
      (13L, 16L),
      (14L, 17L),

      // Chain structure (lower k-core values)
      (18L, 19L),
      (19L, 20L),
      (20L, 21L),
      (21L, 22L),

      // Some additional connections to create more varied structure
      (6L, 12L),
      (7L, 13L),
      (8L, 14L),
      (9L, 15L),
      (10L, 16L),

      // Isolated vertices or low-degree vertices
      (23L, 24L))

    val e = spark.createDataFrame(edges).toDF("src", "dst")
    val g = GraphFrame(v, e)
    val result = g.kCore.run()
    TestUtils.checkColumnType(result.schema, "kcore", DataTypes.IntegerType)
    assert(result.count() === 25)

    val rows = result.collect()
    // Check that we have a range of k-core values
    val kcoreValues = rows.map(_.getAs[Int]("kcore")).distinct.sorted
    assert(kcoreValues.length > 2, "Should have at least 3 distinct k-core values")

    // Verify specific expected patterns
    val kcoreMap = rows.map(row => row.getAs[Long]("id") -> row.getAs[Int]("kcore")).toMap

    // Vertices in the highly connected cluster should have higher k-core values
    assert(kcoreMap(0L) >= 3, "Central vertex should have high k-core")
    assert(kcoreMap(1L) >= 3, "Well-connected vertex should have high k-core")

    // Leaf nodes should have lower k-core values
    assert(kcoreMap(18L) <= 2, "Leaf node should have low k-core")
    assert(kcoreMap(23L) <= 1, "Low-degree node should have very low k-core")

    result.unpersist()
  }

  test("graph with clear hierarchical k-core structure") {
    // Create a graph designed to have clear k-core layers
    val v = spark.createDataFrame((0L until 30L).map(id => (id, s"v$id"))).toDF("id", "name")

    // Build edges to create a hierarchical structure:
    // Core (k=5): vertices 0-4 - fully connected
    // Next layer (k=3): vertices 5-14 - each connects to multiple core vertices
    // Outer layer (k=1): vertices 15-29 - sparse connections
    val coreEdges = for {
      i <- 0 until 5
      j <- (i + 1) until 5
    } yield (i.toLong, j.toLong)

    val midLayerEdges = Seq(
      (5L, 0L),
      (5L, 1L),
      (5L, 2L), // Connect to core
      (6L, 0L),
      (6L, 1L),
      (6L, 3L),
      (7L, 1L),
      (7L, 2L),
      (7L, 4L),
      (8L, 0L),
      (8L, 3L),
      (8L, 4L),
      (9L, 1L),
      (9L, 2L),
      (9L, 3L),
      (10L, 0L),
      (10L, 4L),
      (11L, 2L),
      (11L, 3L),
      (12L, 1L),
      (12L, 4L),
      (13L, 0L),
      (13L, 2L),
      (14L, 3L),
      (14L, 4L))

    val outerEdges = Seq(
      (15L, 5L),
      (16L, 6L),
      (17L, 7L),
      (18L, 8L),
      (19L, 9L),
      (20L, 10L),
      (21L, 11L),
      (22L, 12L),
      (23L, 13L),
      (24L, 14L),
      (25L, 15L),
      (26L, 16L),
      (27L, 17L),
      (28L, 18L),
      (29L, 19L))

    val allEdges = coreEdges ++ midLayerEdges ++ outerEdges
    val e = spark.createDataFrame(allEdges).toDF("src", "dst")
    val g = GraphFrame(v, e)
    val result = g.kCore.run()
    TestUtils.checkColumnType(result.schema, "kcore", DataTypes.IntegerType)
    assert(result.count() === 30)

    val rows = result.collect()
    val kcoreMap = rows.map(row => row.getAs[Long]("id") -> row.getAs[Int]("kcore")).toMap

    // Validate hierarchical structure
    // Core vertices (0-4) should have highest k-core
    (0L to 4L).foreach { id =>
      assert(kcoreMap(id) >= 4, s"Core vertex $id should have high k-core, got ${kcoreMap(id)}")
    }

    // Mid-layer vertices (5-14) should have medium k-core
    (5L to 14L).foreach { id =>
      assert(
        kcoreMap(id) >= 2,
        s"Mid-layer vertex $id should have medium k-core, got ${kcoreMap(id)}")
      assert(
        kcoreMap(id) <= 4,
        s"Mid-layer vertex $id should not have too high k-core, got ${kcoreMap(id)}")
    }

    // Outer vertices (15-29) should have low k-core
    (15L to 29L).foreach { id =>
      assert(kcoreMap(id) <= 2, s"Outer vertex $id should have low k-core, got ${kcoreMap(id)}")
    }

    result.unpersist()
  }

  test("triangle with tail - exact kcore values") {
    // This graph has vertices where degree != kcore, which is important to test correctness:
    // it would catch a buggy implementation that converges too early (e.g. after one superstep), which
    // would return kcore = degree for all vertices and pass simpler tests.
    //
    // Undirected graph:
    //
    //   Triangle: 1 - 2 - 3 - 1   (kcore = 2, they form the 2-core)
    //   Tail:     1 - 4 - 5       (kcore = 1, pendant chain excluded from the 2-core)
    //
    // Degrees: 1→3, 2→2, 3→2, 4→2, 5→1  (degree != kcore for vertices 1 and 4)
    val v = spark
      .createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"), (4L, "d"), (5L, "e")))
      .toDF("id", "name")
    val e = spark
      .createDataFrame(Seq((1L, 2L), (2L, 3L), (3L, 1L), (1L, 4L), (4L, 5L)))
      .toDF("src", "dst")
    val g = GraphFrame(v, e)
    val result = g.kCore.run()
    TestUtils.checkColumnType(result.schema, "kcore", DataTypes.IntegerType)
    assert(result.count() === 5)
    val kcoreMap = result.collect().map(r => r.getAs[Long]("id") -> r.getAs[Int]("kcore")).toMap
    // Triangle vertices form the 2-core
    assert(kcoreMap(1L) === 2)
    assert(kcoreMap(2L) === 2)
    assert(kcoreMap(3L) === 2)
    // Tail vertices are only in the 1-core
    assert(kcoreMap(4L) === 1)
    assert(kcoreMap(5L) === 1)
    result.unpersist()
  }
}
