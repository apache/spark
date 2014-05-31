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

object ShortestPaths {
  type SPMap = Map[VertexId, Int] // map of landmarks -> minimum distance to landmark
  def SPMap(x: (VertexId, Int)*) = Map(x: _*)
  def increment(spmap: SPMap): SPMap = spmap.map { case (v, d) => v -> (d + 1) }
  def plus(spmap1: SPMap, spmap2: SPMap): SPMap =
    (spmap1.keySet ++ spmap2.keySet).map{
      k => k -> scala.math.min(spmap1.getOrElse(k, Int.MaxValue), spmap2.getOrElse(k, Int.MaxValue))
    }.toMap

  /**
   * Compute the shortest paths to each landmark for each vertex and
   * return an RDD with the map of landmarks to their shortest-path
   * lengths.
   *
   * @tparam VD the shortest paths map for the vertex
   * @tparam ED the incremented shortest-paths map of the originating
   * vertex (discarded in the computation)
   *
   * @param graph the graph for which to compute the shortest paths
   * @param landmarks the list of landmark vertex ids
   *
   * @return a graph with vertex attributes containing a map of the
   * shortest paths to each landmark
   */
  def run[VD, ED](graph: Graph[VD, ED], landmarks: Seq[VertexId])
    (implicit m1: Manifest[VD], m2: Manifest[ED]): Graph[SPMap, SPMap] = {

    val spGraph = graph
      .mapVertices{ (vid, attr) =>
        if (landmarks.contains(vid)) SPMap(vid -> 0)
        else SPMap()
      }
      .mapTriplets{ edge => edge.srcAttr }

    val initialMessage = SPMap()

    def vertexProgram(id: VertexId, attr: SPMap, msg: SPMap): SPMap = {
      plus(attr, msg)
    }

    def sendMessage(edge: EdgeTriplet[SPMap, SPMap]): Iterator[(VertexId, SPMap)] = {
      val newAttr = increment(edge.srcAttr)
      if (edge.dstAttr != plus(newAttr, edge.dstAttr)) Iterator((edge.dstId, newAttr))
      else Iterator.empty
    }

    def messageCombiner(s1: SPMap, s2: SPMap): SPMap = {
      plus(s1, s2)
    }

    Pregel(spGraph, initialMessage)(
      vertexProgram, sendMessage, messageCombiner)
  }

}
