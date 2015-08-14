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
import org.apache.spark.rdd.RDD

class ClosenessCentralitySuite extends SparkFunSuite with LocalSparkContext {

  test("Closeness Centrality Computations") {
    withSpark { sc =>
      val closenessCentrality = Set(
        (1, .4), (2, .4), (3, .4),
        (4, .4), (5, .4), (0, .4))

      // Create an RDD for the vertices
      val vertices: RDD[(VertexId, Double)] = sc.parallelize(Seq(
        (0L, 1.0),
        (1L, 1.0),
        (2L, 1.0),
        (3L, 1.0),
        (4L, 1.0),
        (5L, 1.0)))

      // Create an RDD for edges
      val edges: RDD[Edge[Double]] = sc.parallelize(Seq(
        Edge(0L, 1L, 0.0),
        Edge(1L, 2L, 0.0),
        Edge(2L, 3L, 0.0),
        Edge(3L, 4L, 0.0),
        Edge(4L, 5L, 0.0),
        Edge(5L, 0L, 0.0)))

      // Build the initial Graph
      val graph = Graph(vertices, edges)

      val results = ClosenessCentrality.run(graph).vertices.map(a => (a._1, a._2)).collect().toSet[(VertexId, Double)]

      assert(results === closenessCentrality)
    }
  }

}

