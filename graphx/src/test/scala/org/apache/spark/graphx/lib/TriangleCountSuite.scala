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
import org.apache.spark.graphx.PartitionStrategy.RandomVertexCut


class TriangleCountSuite extends SparkFunSuite with LocalSparkContext {

  test("Count a single triangle") {
    withSpark { sc =>
      val rawEdges = sc.parallelize(Array( 0L->1L, 1L->2L, 2L->0L ), 2)
      val graph = Graph.fromEdgeTuples(rawEdges, true).cache()
      val triangleCount = graph.triangleCount()
      val verts = triangleCount.vertices
      verts.collect.foreach { case (vid, count) => assert(count === 1) }
    }
  }

  test("Count two triangles") {
    withSpark { sc =>
      val triangles = Array(0L -> 1L, 1L -> 2L, 2L -> 0L) ++
        Array(0L -> -1L, -1L -> -2L, -2L -> 0L)
      val rawEdges = sc.parallelize(triangles, 2)
      val graph = Graph.fromEdgeTuples(rawEdges, true).cache()
      val triangleCount = graph.triangleCount()
      val verts = triangleCount.vertices
      verts.collect().foreach { case (vid, count) =>
        if (vid == 0) {
          assert(count === 2)
        } else {
          assert(count === 1)
        }
      }
    }
  }

  test("Count two triangles with bi-directed edges") {
    withSpark { sc =>
      val triangles =
        Array(0L -> 1L, 1L -> 2L, 2L -> 0L) ++
        Array(0L -> -1L, -1L -> -2L, -2L -> 0L)
      val revTriangles = triangles.map { case (a, b) => (b, a) }
      val rawEdges = sc.parallelize(triangles ++ revTriangles, 2)
      val graph = Graph.fromEdgeTuples(rawEdges, true).cache()
      val triangleCount = graph.triangleCount()
      val verts = triangleCount.vertices
      verts.collect().foreach { case (vid, count) =>
        if (vid == 0) {
          assert(count === 4)
        } else {
          assert(count === 2)
        }
      }
    }
  }

  test("Count a single triangle with duplicate edges") {
    withSpark { sc =>
      val rawEdges = sc.parallelize(Array(0L -> 1L, 1L -> 2L, 2L -> 0L) ++
        Array(0L -> 1L, 1L -> 2L, 2L -> 0L), 2)
      val graph = Graph.fromEdgeTuples(rawEdges, true, uniqueEdges = Some(RandomVertexCut)).cache()
      val triangleCount = graph.triangleCount()
      val verts = triangleCount.vertices
      verts.collect.foreach { case (vid, count) => assert(count === 1) }
    }
  }

}
