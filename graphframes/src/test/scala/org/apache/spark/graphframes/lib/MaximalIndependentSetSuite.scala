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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.graphframes._
import org.apache.spark.graphframes.examples.Graphs

class MaximalIndependentSetSuite extends SparkFunSuite with GraphFrameTestSparkContext {
  test("isolated vertices should be included in MIS") {
    // Create a graph with isolated vertices
    val vertices =
      spark.createDataFrame(Seq((0L, "a"), (1L, "b"), (2L, "c"), (3L, "d"))).toDF("id", "name")

    // Only connect vertices 0 and 1
    val edges = spark.createDataFrame(Seq((0L, 1L, "edge1"))).toDF("src", "dst", "name")

    val graph = GraphFrame(vertices, edges)
    val mis = graph.maximalIndependentSet.run(seed = 12345L)

    // Check that all vertices are in the MIS (since 2 and 3 are isolated)
    val misIds = mis.select("id").collect().map(_.getLong(0)).toSet
    assert(misIds.size == 3, "MIS should contain 2 isolated vertices and one of linked")
    assert(misIds.contains(2L), "Isolated vertex 2 should be in MIS")
    assert(misIds.contains(3L), "Isolated vertex 3 should be in MIS")

    mis.unpersist()
  }

  def isIndependent(graph: GraphFrame, mis: DataFrame): Boolean = {
    graph.edges
      .join(mis, col(GraphFrame.SRC) === col(GraphFrame.ID))
      .select(col(GraphFrame.DST))
      .join(mis, col(GraphFrame.DST) === col(GraphFrame.ID))
      .count() == 0
  }

  def isMaximal(graph: GraphFrame, mis: DataFrame): Boolean = {
    val undirectedG = graph.asUndirected()
    val verticesNotInMIS = undirectedG.vertices.join(mis, Seq(GraphFrame.ID), "left_anti")

    val verticesWithEdgesToMIS = undirectedG.edges
      .join(mis, col(GraphFrame.ID) === col(GraphFrame.DST))
      .select(GraphFrame.SRC)
      .distinct()

    val countVerticesNotInMIS = verticesNotInMIS.count()
    val countVerticesWithEdgesToMIS = verticesWithEdgesToMIS.count()

    countVerticesNotInMIS == countVerticesWithEdgesToMIS
  }

  test("correct MIS, seed 12345") {
    val graph = Graphs.friends

    val mis = graph.maximalIndependentSet.run(seed = 12345L)

    assert(isIndependent(graph, mis))
    assert(isMaximal(graph, mis))

    mis.unpersist()
  }

  test("correct MIS, seed 23456") {
    val graph = Graphs.friends

    val mis = graph.maximalIndependentSet.run(seed = 23456L)

    assert(isIndependent(graph, mis))
    assert(isMaximal(graph, mis))

    mis.unpersist()
  }

  test("MIS on empty graph") {
    val emptyGraph = Graphs.empty[Long]
    val mis = emptyGraph.maximalIndependentSet.run(seed = 12345L)
    assert(mis.count() == 0, "MIS of empty graph should be empty")
    mis.unpersist()
  }

  test("MIS on single vertex graph") {
    val vertices = spark.createDataFrame(Seq((0L, "vertex"))).toDF("id", "name")
    val edges = spark.createDataFrame(Seq.empty[(Long, Long)]).toDF("src", "dst")
    val graph = GraphFrame(vertices, edges)

    val mis = graph.maximalIndependentSet.run(seed = 12345L)
    assert(mis.count() == 1, "MIS of single vertex graph should contain one vertex")

    val misId = mis.select("id").collect()(0).getLong(0)
    assert(misId == 0L, "MIS should contain vertex with id 0")

    mis.unpersist()
  }

  test("MIS on disconnected vertices") {
    val n = 5L
    val vertices = spark.range(n).toDF("id")
    val edges = spark.createDataFrame(Seq.empty[(Long, Long)]).toDF("src", "dst")
    val graph = GraphFrame(vertices, edges)

    val mis = graph.maximalIndependentSet.run(seed = 12345L)
    assert(mis.count() == n, s"MIS should contain all $n vertices for disconnected graph")

    mis.unpersist()
  }

  test("MIS on complete graph of 5 vertices") {
    val vertices = spark.range(5).toDF("id")
    val edges = for {
      i <- 0L until 5L
      j <- (i + 1L) until 5L
    } yield (i, j)
    val edgeDF = spark.createDataFrame(edges).toDF("src", "dst")
    val graph = GraphFrame(vertices, edgeDF)

    val mis = graph.maximalIndependentSet.run(seed = 12345L)
    assert(mis.count() == 1, "MIS of complete graph should contain exactly one vertex")

    mis.unpersist()
  }
}
