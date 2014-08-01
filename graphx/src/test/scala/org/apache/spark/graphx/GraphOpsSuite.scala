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

package org.apache.spark.graphx

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph._
import org.apache.spark.graphx.impl.EdgePartition
import org.apache.spark.rdd._
import org.scalatest.FunSuite

class GraphOpsSuite extends FunSuite with LocalSparkContext {

  test("joinVertices") {
    withSpark { sc =>
      val vertices =
        sc.parallelize(Seq[(VertexId, String)]((1, "one"), (2, "two"), (3, "three")), 2)
      val edges = sc.parallelize((Seq(Edge(1, 2, "onetwo"))))
      val g: Graph[String, String] = Graph(vertices, edges)

      val tbl = sc.parallelize(Seq[(VertexId, Int)]((1, 10), (2, 20)))
      val g1 = g.joinVertices(tbl) { (vid: VertexId, attr: String, u: Int) => attr + u }

      val v = g1.vertices.collect().toSet
      assert(v === Set((1, "one10"), (2, "two20"), (3, "three")))
    }
  }

  test("collectNeighborIds") {
    withSpark { sc =>
      val graph = getCycleGraph(sc, 100)
      val nbrs = graph.collectNeighborIds(EdgeDirection.Either).cache()
      assert(nbrs.count === 100)
      assert(graph.numVertices === nbrs.count)
      nbrs.collect.foreach { case (vid, nbrs) => assert(nbrs.size === 2) }
      nbrs.collect.foreach {
        case (vid, nbrs) =>
          val s = nbrs.toSet
          assert(s.contains((vid + 1) % 100))
          assert(s.contains(if (vid > 0) vid - 1 else 99))
      }
    }
  }

  test ("filter") {
    withSpark { sc =>
      val n = 5
      val vertices = sc.parallelize((0 to n).map(x => (x:VertexId, x)))
      val edges = sc.parallelize((1 to n).map(x => Edge(0, x, x)))
      val graph: Graph[Int, Int] = Graph(vertices, edges).cache()
      val filteredGraph = graph.filter(
        graph => {
          val degrees: VertexRDD[Int] = graph.outDegrees
          graph.outerJoinVertices(degrees) {(vid, data, deg) => deg.getOrElse(0)}
        },
        vpred = (vid: VertexId, deg:Int) => deg > 0
      ).cache()

      val v = filteredGraph.vertices.collect().toSet
      assert(v === Set((0,0)))

      // the map is necessary because of object-reuse in the edge iterator
      val e = filteredGraph.edges.map(e => Edge(e.srcId, e.dstId, e.attr)).collect().toSet
      assert(e.isEmpty)
    }
  }

  test("collectEdgesCycleDirectionOut") {
    withSpark { sc =>
      val graph = getCycleGraph(sc, 100)
      val edges = graph.collectEdges(EdgeDirection.Out).cache()
      assert(edges.count == 100)
      edges.collect.foreach { case (vid, edges) => assert(edges.size == 1) }
      edges.collect.foreach {
        case (vid, edges) =>
          val s = edges.toSet
          val edgeDstIds = s.map(e => e.dstId)
          assert(edgeDstIds.contains((vid + 1) % 100))
      }
    }
  }

  test("collectEdgesCycleDirectionIn") {
    withSpark { sc =>
      val graph = getCycleGraph(sc, 100)
      val edges = graph.collectEdges(EdgeDirection.In).cache()
      assert(edges.count == 100)
      edges.collect.foreach { case (vid, edges) => assert(edges.size == 1) }
      edges.collect.foreach {
        case (vid, edges) =>
          val s = edges.toSet
          val edgeSrcIds = s.map(e => e.srcId)
          assert(edgeSrcIds.contains(if (vid > 0) vid - 1 else 99))
      }
    }
  }

  test("collectEdgesCycleDirectionEither") {
    withSpark { sc =>
      val graph = getCycleGraph(sc, 100)
      val edges = graph.collectEdges(EdgeDirection.Either).cache()
      assert(edges.count == 100)
      edges.collect.foreach { case (vid, edges) => assert(edges.size == 2) }
      edges.collect.foreach {
        case (vid, edges) =>
          val s = edges.toSet
          val edgeIds = s.map(e => if (vid != e.srcId) e.srcId else e.dstId)
          assert(edgeIds.contains((vid + 1) % 100))
          assert(edgeIds.contains(if (vid > 0) vid - 1 else 99))
      }
    }
  }

  test("collectEdgesChainDirectionOut") {
    withSpark { sc =>
      val graph = getChainGraph(sc, 50)
      val edges = graph.collectEdges(EdgeDirection.Out).cache()
      assert(edges.count == 49)
      edges.collect.foreach { case (vid, edges) => assert(edges.size == 1) }
      edges.collect.foreach {
        case (vid, edges) =>
          val s = edges.toSet
          val edgeDstIds = s.map(e => e.dstId)
          assert(edgeDstIds.contains(vid + 1))
      }
    }
  }

  test("collectEdgesChainDirectionIn") {
    withSpark { sc =>
      val graph = getChainGraph(sc, 50)
      val edges = graph.collectEdges(EdgeDirection.In).cache()
      // We expect only 49 because collectEdges does not return vertices that do
      // not have any edges in the specified direction.
      assert(edges.count == 49)
      edges.collect.foreach { case (vid, edges) => assert(edges.size == 1) }
      edges.collect.foreach {
        case (vid, edges) =>
          val s = edges.toSet
          val edgeDstIds = s.map(e => e.srcId)
          assert(edgeDstIds.contains((vid - 1) % 100))
      }
    }
  }

  test("collectEdgesChainDirectionEither") {
    withSpark { sc =>
      val graph = getChainGraph(sc, 50)
      val edges = graph.collectEdges(EdgeDirection.Either).cache()
      // We expect only 49 because collectEdges does not return vertices that do
      // not have any edges in the specified direction.
      assert(edges.count === 50)
      edges.collect.foreach {
        case (vid, edges) => if (vid > 0 && vid < 49) {
          assert(edges.size == 2)
        } else {
          assert(edges.size == 1)
        }
      }
      edges.collect.foreach {
        case (vid, edges) =>
          val s = edges.toSet
          val edgeIds = s.map(e => if (vid != e.srcId) e.srcId else e.dstId)
          if (vid == 0) { assert(edgeIds.contains(1)) }
          else if (vid == 49) { assert(edgeIds.contains(48)) }
          else {
            assert(edgeIds.contains(vid + 1))
            assert(edgeIds.contains(vid - 1))
          }
      }
    }
  }

  private def getCycleGraph(sc: SparkContext, numVertices: Int): Graph[Double, Int] = {
    val cycle = (0 until numVertices).map(x => (x, (x + 1) % numVertices))
    getGraphFromSeq(sc, cycle)
  }

  private def getChainGraph(sc: SparkContext, numVertices: Int): Graph[Double, Int] = {
    val chain = (0 until numVertices - 1).map(x => (x, (x + 1)))
    getGraphFromSeq(sc, chain)
  }

  private def getGraphFromSeq(sc: SparkContext, seq: IndexedSeq[(Int, Int)]): Graph[Double, Int] = {
    val rawEdges = sc.parallelize(seq, 3).map { case (s, d) => (s.toLong, d.toLong) }
    Graph.fromEdgeTuples(rawEdges, 1.0).cache()
  }
}
