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

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.test.SharedSparkSession

class GraphFrameSuite extends QueryTest with SharedSparkSession {

  import testImplicits._

  private def graph: GraphFrame = {
    val vertices = Seq((1L, "a"), (2L, "b"), (3L, "c"), (4L, "isolated"))
      .toDF("id", "name")
    val edges = Seq((1L, 2L, "friend"), (2L, 3L, "follow"), (2L, 1L, "friend"))
      .toDF("src", "dst", "relationship")
    GraphFrame(vertices, edges)
  }

  test("construction validates required columns") {
    val vertices = Seq((1L, "a")).toDF("id", "name")
    val edges = Seq((1L, 1L)).toDF("src", "dst")
    GraphFrame(vertices, edges)

    val missingId = intercept[IllegalArgumentException] {
      GraphFrame(vertices.withColumnRenamed("id", "vertex"), edges)
    }
    assert(missingId.getMessage.contains("Vertex ID column 'id' is missing"))

    val missingDestination = intercept[IllegalArgumentException] {
      GraphFrame(vertices, edges.withColumnRenamed("dst", "destination"))
    }
    assert(missingDestination.getMessage.contains("Destination vertex ID column 'dst' is missing"))
  }

  test("degree DataFrames preserve GraphFrames schemas") {
    checkAnswer(graph.outDegrees, Seq(Row(1L, 1), Row(2L, 2)))
    checkAnswer(graph.inDegrees, Seq(Row(1L, 1), Row(2L, 1), Row(3L, 1)))
    checkAnswer(graph.degrees, Seq(Row(1L, 2), Row(2L, 3), Row(3L, 1)))
  }

  test("triplets contain complete source, edge, and destination rows") {
    val result = graph.triplets.select(
      col("src.id"),
      col("src.name"),
      col("edge.relationship"),
      col("dst.id"),
      col("dst.name"))

    checkAnswer(
      result,
      Seq(
        Row(1L, "a", "friend", 2L, "b"),
        Row(2L, "b", "follow", 3L, "c"),
        Row(2L, "b", "friend", 1L, "a")))
  }

  test("relational graph transforms retain attributes") {
    val filtered = graph.filterVertices(col("id") <= 2L)
    checkAnswer(filtered.vertices, Seq(Row(1L, "a"), Row(2L, "b")))
    checkAnswer(
      filtered.edges,
      Seq(Row(1L, 2L, "friend"), Row(2L, 1L, "friend")))

    checkAnswer(
      graph.filterEdges(col("relationship") === "follow").edges,
      Seq(Row(2L, 3L, "follow")))
    checkAnswer(graph.dropIsolatedVertices().vertices.select("id"), Seq(Row(1L), Row(2L), Row(3L)))
    checkAnswer(
      graph.reverse.edges,
      Seq(
        Row(2L, 1L, "friend"),
        Row(3L, 2L, "follow"),
        Row(1L, 2L, "friend")))
  }

  test("validate rejects duplicate vertices and unknown edge endpoints") {
    graph.validate()

    val duplicateVertices = Seq((1L, "a"), (1L, "duplicate")).toDF("id", "name")
    val noEdges = Seq.empty[(Long, Long)].toDF("src", "dst")
    assertThrows[InvalidGraphException](GraphFrame(duplicateVertices, noEdges).validate())

    val vertices = Seq(1L).toDF("id")
    val unknownEndpoint = Seq((1L, 2L)).toDF("src", "dst")
    assertThrows[InvalidGraphException](GraphFrame(vertices, unknownEndpoint).validate())
  }

  test("fromEdges derives distinct vertices") {
    val edges = Seq((1L, 2L), (2L, 3L), (1L, 2L)).toDF("src", "dst")
    val derived = GraphFrame.fromEdges(edges)
    try {
      checkAnswer(derived.vertices, Seq(Row(1L), Row(2L), Row(3L)))
      checkAnswer(derived.edges, edges.collect().toSeq)
    } finally {
      derived.vertices.unpersist()
    }
  }
}
