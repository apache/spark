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

import org.apache.spark.{SparkContext, SparkFunSuite}
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd._


class ConnectedComponentsWithDegreeSuite extends SparkFunSuite with LocalSparkContext {

  test("Grid Connected Components") {
    withSpark { sc =>
      val n = 5
      val gridGraph = GraphGenerators.gridGraph(sc, n, n)
      val dccGraph = gridGraph.connectedComponentsWithDegree()
      val dccIds = dccGraph.vertices.map { case (vid, dccId) => dccId }.distinct().collect()
      assert(dccIds.length === 1)
      assert(dccIds.head._1 === n + 1)
      assert(dccIds.head._2 === 4)
    }
  } // end of Grid connected components


  test("Reverse Grid Connected Components") {
    withSpark { sc =>
      val n = 5
      val gridGraph = GraphGenerators.gridGraph(sc, n, n).reverse
      val dccGraph = gridGraph.connectedComponentsWithDegree()
      val dccIds = dccGraph.vertices.map { case (vid, dccId) => dccId }.distinct().collect()
      assert(dccIds.length === 1)
      assert(dccIds.head._1 === n + 1)
      assert(dccIds.head._2 === 4)
    }
  } // end of Grid connected components


  test("Chain Connected Components") {
    withSpark { sc =>
      val chain1 = (0 until 9).map(x => (x, x + 1))
      val chain2 = (10 until 20).map(x => (x, x + 1))
      val rawEdges = sc.parallelize(chain1 ++ chain2, 3).map { case (s, d) => (s.toLong, d.toLong) }
      val twoChains = Graph.fromEdgeTuples(rawEdges, 1.0)
      val dccGraph = twoChains.connectedComponentsWithDegree()
      val vertices = dccGraph.vertices.collect()
      for ( (id, dcc) <- vertices ) {
        if (id < 10) {
          assert(dcc._1 === 1)
          assert(dcc._2 === 2)
        } else {
          assert(dcc._1 === 11)
          assert(dcc._2 === 2)
        }
      }
    }
  } // end of chain connected components

  test("Reverse Chain Connected Components") {
    withSpark { sc =>
      val chain1 = (0 until 9).map(x => (x, x + 1))
      val chain2 = (10 until 20).map(x => (x, x + 1))
      val rawEdges = sc.parallelize(chain1 ++ chain2, 3).map { case (s, d) => (s.toLong, d.toLong) }
      val twoChains = Graph.fromEdgeTuples(rawEdges, true).reverse
      val dccGraph = twoChains.connectedComponentsWithDegree()
      val vertices = dccGraph.vertices.collect()
      for ( (id, dcc) <- vertices ) {
        if (id < 10) {
          assert(dcc._1 === 1)
          assert(dcc._2 === 2)
        } else {
          assert(dcc._1 === 11)
          assert(dcc._2 === 2)
        }
      }
    }
  } // end of reverse chain connected components

  test("Connected Components on a Toy Connected Graph") {
    withSpark { sc =>
      // Create an RDD for the vertices
      val users: RDD[(VertexId, (String, String))] =
        sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
          (5L, ("franklin", "prof")), (2L, ("istoica", "prof")),
          (4L, ("peter", "student"))))
      // Create an RDD for edges
      val relationships: RDD[Edge[String]] =
        sc.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
          Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi"),
          Edge(4L, 0L, "student"), Edge(5L, 0L, "colleague")))
      // Edges are:
      //   2 ---> 5 ---> 3
      //          | \
      //          V   \|
      //   4 ---> 0    7
      //
      // Define a default user in case there are relationship with missing user
      val defaultUser = ("John Doe", "Missing")
      // Build the initial Graph
      val graph = Graph(users, relationships, defaultUser)
      val dccGraph = graph.connectedComponentsWithDegree()
      val vertices = dccGraph.vertices.collect()
      for ( (id, dcc) <- vertices ) {
        // vertex 5 has the max degree, which equals to 4
        assert(dcc._1 === 5)
        assert(dcc._2 === 4)
      }
    }
  } // end of toy connected components

}
