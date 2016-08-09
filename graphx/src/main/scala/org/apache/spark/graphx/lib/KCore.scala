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

import scala.language.implicitConversions
import scala.reflect.ClassTag

import org.apache.spark.graphx._



/** KCore algorithm. */
object KCore {
  /**
   * Compute the KCore membership of each vertex and return a graph with the vertex
   * value containing a boolean that is true if this vertex belongs to the KCore of
   * the original graph.
    *
    * General idea:
    * 1. each node starts with his initial degree
    * 2. if his current degree is less than K than:
    *   2a. he changes its degree to 0
    *   2b. he sends a message to his neighbours to reduce 1 from their degree
    *   2c. he sends a message to himself to be marked as "removed" - degree = -1
    *
   *
   * @tparam VD the vertex attribute type (discarded in the computation)
   * @tparam ED the edge attribute type (preserved in the computation)
   * @param graph the graph for which to compute the KCore
   * @param k the k of the KCore algorithm
   * @param maxIterations the maximum number of iterations to run for
   * @return a graph with vertex attributes containing a boolean that
   *         represents if this vertex belongs to the KCore
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED],
                                      k: Int,
                                      maxIterations: Int = Int.MaxValue): Graph[Boolean, ED] = {
    require(maxIterations > 0, s"Maximum of iterations must be greater than 0," +
      s" but got ${maxIterations}")

    val kGraph = Graph[Int, ED](graph.degrees, graph.edges)
    def vprog(vid: VertexId, vd: Int, i: Int) = {
      val ret = if ((vd - i) >= k) vd - i
                else if (vd > 0) 0
                else -1

      ret
    }

    def sendMessage(edge: EdgeTriplet[Int, ED]): Iterator[(VertexId, Int)] = {
      val lst = if (edge.srcAttr == 0) {
        List((edge.dstId, (1, edge.dstAttr)), (edge.srcId, (0, edge.srcAttr)))
      } else if (edge.dstAttr == 0) {
        List((edge.srcId, (1, edge.srcAttr)), (edge.dstId, (0, edge.dstAttr)))
      } else List.empty

      // no need to send msg to nodes that have already been "removed"
      val lstFiltered = lst.filter(_._2._2>=0).map(t => (t._1, t._2._1))

      lstFiltered.iterator
    }

    val initialMessage = 0
    val pregelGraph = Pregel(kGraph, initialMessage,
      maxIterations, EdgeDirection.Either)(
      vprog = vprog,
      sendMsg = sendMessage,
      mergeMsg = (a, b) => a + b)
    kGraph.unpersist()

    pregelGraph.mapVertices[Boolean]((id: VertexId, d: Int) => d>0)

  } // end of KCore

}
