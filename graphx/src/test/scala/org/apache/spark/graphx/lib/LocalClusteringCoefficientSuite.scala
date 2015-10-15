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

import org.apache.spark.graphx._
import org.scalatest.FunSuite

class LocalClusteringCoefficientSuite extends FunSuite with LocalSparkContext {

  def approEqual(a: Double, b: Double): Boolean = {
    (a - b).abs < 1e-20
  }

  test("test in a complete graph") {
    withSpark { sc =>
      val edges = Array( 0L->1L, 1L->2L, 2L->0L )
      val revEdges = edges.map { case (a, b) => (b, a) }
      val rawEdges = sc.parallelize(edges ++ revEdges, 2)
      val graph = Graph.fromEdgeTuples(rawEdges, true).cache()
      val lccCount = graph.localClusteringCoefficient()
      val verts = lccCount.vertices
      verts.collect.foreach { case (_, count) => assert(approEqual(count, 1.0)) }
    }
  }

  test("test in a triangle with one bi-directed edges") {
    withSpark { sc =>
      val rawEdges = sc.parallelize(Array( 0L->1L, 1L->2L, 2L->1L, 2L->0L ), 2)
      val graph = Graph.fromEdgeTuples(rawEdges, true).cache()
      val lccCount = graph.localClusteringCoefficient()
      val verts = lccCount.vertices
      verts.collect.foreach { case (vid, count) =>
          if (vid == 0) {
            assert(approEqual(count, 1.0))
          } else {
            assert(approEqual(count, 0.5))
          }
      }
    }
  }

  test("test in a graph with only self-edges") {
    withSpark { sc =>
      val rawEdges = sc.parallelize(Array(0L -> 0L, 1L -> 1L, 2L -> 2L, 3L -> 3L), 2)
      val graph = Graph.fromEdgeTuples(rawEdges, true).cache()
      val lccCount = graph.localClusteringCoefficient()
      val verts = lccCount.vertices
      verts.collect.foreach { case (vid, count) => assert(approEqual(count, 0.0)) }
    }
  }

  test("test in a graph with duplicated edges") {
    withSpark { sc =>
      val edges = Array( 0L->1L, 1L->2L, 2L->0L )
      val rawEdges = sc.parallelize(edges ++ edges, 2)
      val graph = Graph.fromEdgeTuples(rawEdges, true).cache()
      val lccCount = graph.localClusteringCoefficient()
      val verts = lccCount.vertices
      verts.collect.foreach { case (vid, count) => assert(approEqual(count, 1.0)) }
    }
  }
}
