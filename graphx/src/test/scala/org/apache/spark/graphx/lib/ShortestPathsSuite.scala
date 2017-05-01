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

package org.apache.spark.graphx.lib

import org.apache.spark.SparkFunSuite
import org.apache.spark.graphx._

class ShortestPathsSuite extends SparkFunSuite with LocalSparkContext {

  test("Shortest Path Computations") {
    withSpark { sc =>
      val shortestPaths = Set(
        (1, Map(1 -> 0, 4 -> 2)), (2, Map(1 -> 1, 4 -> 2)), (3, Map(1 -> 2, 4 -> 1)),
        (4, Map(1 -> 2, 4 -> 0)), (5, Map(1 -> 1, 4 -> 1)), (6, Map(1 -> 3, 4 -> 1)))
      val edgeSeq = Seq((1, 2), (1, 5), (2, 3), (2, 5), (3, 4), (4, 5), (4, 6)).flatMap {
        case e => Seq(e, e.swap)
      }
      val edges = sc.parallelize(edgeSeq).map { case (v1, v2) => (v1.toLong, v2.toLong) }
      val graph = Graph.fromEdgeTuples(edges, 1)
      val landmarks = Seq(1, 4).map(_.toLong)
      val results = ShortestPaths.run(graph, landmarks).vertices.collect.map {
        case (v, spMap) => (v, spMap.mapValues(i => i))
      }
      assert(results.toSet === shortestPaths)
    }
  }

  test("Shortest Path single source all paths in weighted graph") {
    withSpark { sc =>
      val edgeArr: Array[Edge[Double]] = Array(Edge(2, 1, 7),
        Edge(2, 4, 2),
        Edge(3, 2, 4),
        Edge(3, 6, 3),
        Edge(4, 1, 1),
        Edge(5, 2, 2),
        Edge(5, 3, 3),
        Edge(5, 6, 8),
        Edge(5, 7, 2),
        Edge(7, 6, 4),
        Edge(7, 4, 1))

      val edges = sc.parallelize(edgeArr)
      val graph = Graph.fromEdges(edges, true)
      val results = ShortestPaths.run(graph, 5, true).vertices.collect.map {
        case(id, distAndPaths) => distAndPaths._2.map{
          path => (distAndPaths._1, path)
        }
      }.flatMap(pair => pair)

      val shortestPaths = Set(
        (3.0, List[VertexId](5, 7, 4)),
        (4.0, List[VertexId](5, 7, 4, 1)),
        (0.0, List[VertexId](5)),
        (6.0, List[VertexId](5, 7, 6)),
        (6.0, List[VertexId](5, 3, 6)),
        (2.0, List[VertexId](5, 2)),
        (3.0, List[VertexId](5, 3)),
        (2.0, List[VertexId](5, 7))
      )

      assert(results.toSet === shortestPaths)
    }
  }

  test("Shortest Path single source all paths in unweighted graph") {
    withSpark { sc =>
      val edgeArr: Array[Edge[Double]] = Array(Edge(2, 1, 7),
        Edge(2, 4, 2),
        Edge(3, 2, 4),
        Edge(3, 6, 3),
        Edge(4, 1, 1),
        Edge(5, 2, 2),
        Edge(5, 3, 3),
        Edge(5, 6, 8),
        Edge(5, 7, 2),
        Edge(7, 6, 4),
        Edge(7, 4, 1))

      val edges = sc.parallelize(edgeArr)
      val graph = Graph.fromEdges(edges, true)
      val results = ShortestPaths.run(graph, 5, false).vertices.collect.map {
        case(id, distAndPaths) => distAndPaths._2.map{
          path => (distAndPaths._1, path)
        }
      }.flatMap(pair => pair)

      val shortestPaths = Set(
        (2.0, List[VertexId](5, 2, 4)),
        (2.0, List[VertexId](5, 7, 4)),
        (2.0, List[VertexId](5, 2, 1)),
        (0.0, List[VertexId](5)),
        (1.0, List[VertexId](5, 6)),
        (1.0, List[VertexId](5, 2)),
        (1.0, List[VertexId](5, 3)),
        (1.0, List[VertexId](5, 7))
      )

      assert(results.toSet === shortestPaths)
    }
  }
}

