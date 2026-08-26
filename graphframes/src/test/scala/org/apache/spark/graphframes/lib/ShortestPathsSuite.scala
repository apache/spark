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
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.graphframes._
import org.apache.spark.graphframes.GraphFrame.quote

class ShortestPathsSuite extends SparkFunSuite with GraphFrameTestSparkContext {

  test("Simple test") {
    doTest()
  }

  def doTest(vertices: Option[DataFrame] = Option.empty): Unit = {
    val spark = this.spark
    import spark.implicits._

    val edgeSeq = Seq((1, 2), (1, 5), (2, 3), (2, 5), (3, 4), (4, 5), (4, 6))
      .flatMap { case e =>
        Seq(e, e.swap)
      }
      .map { case (src, dst) => (src.toLong, dst.toLong) }
    val edges = spark.createDataFrame(edgeSeq).toDF("src", "dst")
    val graph = vertices.map(GraphFrame(_, edges)).getOrElse(GraphFrame.fromEdges(edges))

    // Ground truth
    val shortestPaths = Seq(
      (1, Map(1 -> 0, 4 -> 2)),
      (2, Map(1 -> 1, 4 -> 2)),
      (3, Map(1 -> 2, 4 -> 1)),
      (4, Map(1 -> 2, 4 -> 0)),
      (5, Map(1 -> 1, 4 -> 1)),
      (6, Map(1 -> 3, 4 -> 1))).toDF("id", "distances")
    val expectedCols = vertices.map(_.columns.toSeq).getOrElse(Seq("id")) :+ "distances"
    val expected = vertices
      .foldLeft(shortestPaths) { case (shortestPaths, vertices) =>
        shortestPaths.join(vertices, "id")
      }
      .select(expectedCols.map(quote).map(col): _*)
      .collect()
      .toSet

    val landmarks = Seq(1, 4).map(_.toLong)
    val v2 = graph.shortestPaths.landmarks(landmarks).run()

    TestUtils.testSchemaInvariants(graph, v2)
    TestUtils.checkColumnType(
      v2.schema,
      "distances",
      DataTypes.createMapType(v2.schema("id").dataType, DataTypes.IntegerType, false))
    val results = v2.collect().toSet
    assert(results === expected)
    v2.unpersist()
    ()
  }

  test("Simple test with GraphFrames") {
    val edgeSeq = Seq((1, 2), (1, 5), (2, 3), (2, 5), (3, 4), (4, 5), (4, 6))
      .flatMap { case e =>
        Seq(e, e.swap)
      }
      .map { case (src, dst) => (src.toLong, dst.toLong) }
    val edges = spark.createDataFrame(edgeSeq).toDF("src", "dst")
    val graph = GraphFrame.fromEdges(edges)

    // Ground truth
    val shortestPaths = Set(
      (1, Map(1 -> 0, 4 -> 2)),
      (2, Map(1 -> 1, 4 -> 2)),
      (3, Map(1 -> 2, 4 -> 1)),
      (4, Map(1 -> 2, 4 -> 0)),
      (5, Map(1 -> 1, 4 -> 1)),
      (6, Map(1 -> 3, 4 -> 1)))

    val landmarks = Seq(1, 4).map(_.toLong)
    val v2 = graph.shortestPaths.landmarks(landmarks).setAlgorithm("graphframes").run()

    TestUtils.testSchemaInvariants(graph, v2)
    TestUtils.checkColumnType(
      v2.schema,
      "distances",
      DataTypes.createMapType(v2.schema("id").dataType, DataTypes.IntegerType, true))
    val newVs = v2.select("id", "distances").collect().toSeq
    val results = newVs.map {
      case Row(id: Long, spMap: Map[Long, Int] @unchecked) =>
        (id, spMap)
      case _ => throw new GraphFramesUnreachableException()
    }
    assert(results.toSet === shortestPaths)
    v2.unpersist()
  }

  test("friends graph") {
    val friends = examples.Graphs.friends
    val v = friends.shortestPaths.landmarks(Seq("a", "d")).run()
    val expected = Set[(String, Map[String, Int])](
      ("a", Map("a" -> 0, "d" -> 2)),
      ("b", Map.empty),
      ("c", Map.empty),
      ("d", Map("a" -> 1, "d" -> 0)),
      ("e", Map("a" -> 2, "d" -> 1)),
      ("f", Map.empty),
      ("g", Map.empty))
    val results = v
      .select("id", "distances")
      .collect()
      .map {
        case Row(id: String, spMap: Map[String, Int] @unchecked) =>
          (id, spMap)
        case _ => throw new GraphFramesUnreachableException()
      }
      .toSet
    assert(results === expected)
    v.unpersist()
  }

  test("friends graph with GraphFrames") {
    val friends = examples.Graphs.friends
    val v = friends.shortestPaths.landmarks(Seq("a", "d")).setAlgorithm("graphframes").run()
    val expected = Set[(String, Map[String, Int])](
      ("a", Map("a" -> 0, "d" -> 2)),
      ("b", Map.empty),
      ("c", Map.empty),
      ("d", Map("a" -> 1, "d" -> 0)),
      ("e", Map("a" -> 2, "d" -> 1)),
      ("f", Map.empty),
      ("g", Map.empty))
    val results = v
      .select("id", "distances")
      .collect()
      .map {
        case Row(id: String, spMap: Map[String, Int] @unchecked) =>
          (id, spMap)
        case _ => throw new GraphFramesUnreachableException()
      }
      .toSet
    assert(results === expected)
    v.unpersist()
  }
  test("Test vertices with column name") {
    val verticeSeq =
      Seq((1L, "one"), (2L, "two"), (3L, "three"), (4L, "four"), (5L, "five"), (6L, "six"))
    val vertices = sqlContext.createDataFrame(verticeSeq).toDF("id", "name")
    doTest(Some(vertices))
  }

  test("Test vertices with dot column name") {
    val verticeSeq =
      Seq((1L, "one"), (2L, "two"), (3L, "three"), (4L, "four"), (5L, "five"), (6L, "six"))
    val vertices = sqlContext.createDataFrame(verticeSeq).toDF("id", "a.name")
    doTest(Some(vertices))
  }

  test("Test vertices with backquote in column name") {
    val verticeSeq =
      Seq((1L, "one"), (2L, "two"), (3L, "three"), (4L, "four"), (5L, "five"), (6L, "six"))
    val vertices = sqlContext.createDataFrame(verticeSeq).toDF("id", "a `name`")
    doTest(Some(vertices))
  }

}
