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

import scala.reflect.ClassTag

import org.apache.spark.graphx._

/** Connected components algorithm based on degree propagation. */
object ConnectedComponentsWithDegree {
  /**
   * Compute the connected component membership of each vertex and return a graph with the vertex
   * value containing the largest degree in the connected component and the corresponding vertex
   * id. If several vertices have the same largest degree, the one with lowest id is chosen.
   *
   * @tparam VD the vertex attribute type (discarded in the computation)
   * @tparam ED the edge attribute type (preserved in the computation)
   * @param graph the graph for which to compute the connected components
   * @param maxIterations the maximum number of iterations to run for
   * @return a graph with vertex attributes containing the largest degree and the corresponding id
   *         in each connected component
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED],
                                      maxIterations: Int): Graph[(VertexId, Int), ED] = {
    require(maxIterations > 0, s"Maximum of iterations must be greater than 0," +
      s" but got ${maxIterations}")
    val degGraph = graph.outerJoinVertices(graph.degrees)(
      (vid, vd, degOpt) => (vid, degOpt.getOrElse(0)))

    // First compare the degree, and then the vertex id. The greater the degree is, the greater
    // the tuple is. And if two tuples have the same degree, the less the vertex id is, the greater
    // the tuple is.
    def cmp(a: (VertexId, Int), b: (VertexId, Int)): Int = {
      if (a._2 > b._2) {
        1
      } else if (a._2 < b._2) {
        -1
      } else if (a._1 < b._1) {
        1
      } else if (a._1 > b._1) {
        -1
      } else {
        0
      }
    }

    // return the greater tuple
    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (cmp(a, b) >= 0) {
        a
      } else {
        b
      }
    }

    val pregelGraph = Pregel(graph = degGraph,
      initialMsg = (Long.MaxValue, Int.MinValue),
      maxIterations = maxIterations,
      activeDirection = EdgeDirection.Either)(
      vprog = (id, attr, msg) => max(attr, msg),
      sendMsg = edge => {
        cmp(edge.srcAttr, edge.dstAttr) match {
          case 1 => Iterator((edge.dstId, edge.srcAttr))
          case -1 => Iterator((edge.srcId, edge.dstAttr))
          case 0 => Iterator.empty
        }
      },
      mergeMsg = max
    )
    degGraph.unpersist()
    pregelGraph
  } // end of connectedComponentswithdegree

  /**
   * Compute the connected component membership of each vertex and return a graph with the vertex
   * value containing the largest degree in the connected component and the corresponding vertex
   * id. If several vertices have the same largest degree, the one with lowest id is chosen.
   *
   * @tparam VD the vertex attribute type (discarded in the computation)
   * @tparam ED the edge attribute type (preserved in the computation)
   * @param graph the graph for which to compute the connected components
   * @return a graph with vertex attributes containing the largest degree and the corresponding id
   *         in each connected component
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Graph[(VertexId, Int), ED] = {
    run(graph, Int.MaxValue)
  }
}
