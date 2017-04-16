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
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import scala.language.reflectiveCalls
import scala.language.implicitConversions

/**
 * Compute the sum of the distance of each vertex's shortest paths to all other vertices in the graph
 *
 * The algorithm is relatively straightforward and can be computed in three steps:
 *
 * <ul>
 * <li> Compute all shortest paths through each vertex using [[ShortestPaths]] algorithm.
 * <li> Compute the average shortest path length from vertex V to all other vertices captured in [[ShortestPaths.SPMap]].
 * <li> Compute the reciprocal of the average shortest path length for vertex V.
 * </ul>
 */
object ClosenessCentrality {

  /**
   * Calculate the closeness centrality of each vertex in the input graph.
   *
   * @tparam VD the vertex attribute type (discarded in the computation)
   * @tparam ED the edge attribute type (preserved in the computation)
   *
   * @param graph the graph for which to compute the closeness centrality
   *
   * @return a graph with vertex attributes containing its closeness centrality
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Graph[Double, ED] = {
    Graph(ShortestPaths.run(graph, graph.vertices.map { vx => vx._1 }.collect())
      .vertices.map {
      vx => (vx._1, {
        val dx = 1.0 / vx._2.values.seq.avg
        if (dx.isNaN | dx.isNegInfinity | dx.isPosInfinity) 0.0 else dx
      })
    }: RDD[(VertexId, Double)], graph.edges)
  }

  def average[T](ts: Iterable[T])(implicit num: Numeric[T]) = {
    num.toDouble(ts.sum) / ts.size
  }

  implicit def iterableWithAvg[T: Numeric](data: Iterable[T]): Object {def avg: Double} = new {
    def avg = average(data)
  }
}
