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
import scala.reflect.ClassTag

/**
 * Computes shortest paths between two vertext, returning a graph where each
 * vertex attribute is a map containing the shortest-path distance and
 * ordered list of vertex in shortest path
 * edge attribute will be used as weight here
 */
object TwoVertexShortestPath {
  /** Stores a map from the vertex id of a landmark to the distance to that landmark. */
  type SPMap = Map[VertexId, (Long, Seq[VertexId])]

  private def makeMap(x: (VertexId, (Long, Seq[VertexId]))*): SPMap = Map(x: _*)

  private def incrementMap(spmap: SPMap, weight: Long, srcVid: VertexId): SPMap =
     spmap.map { case (k, v) => k -> (v._1 + weight, (srcVid +: v._2)) }

  private def addMaps(spmap1: SPMap, spmap2: SPMap): SPMap =
    // combine keyset
    (spmap1.keySet ++ spmap2.keySet).map {
      k => {
         if (spmap1.getOrElse(k, (Long.MaxValue, Seq()))._1 <
              spmap2.getOrElse(k, (Long.MaxValue, Seq(0)))._1) {
                k -> spmap1(k)
          }
         else {
               k -> spmap2(k)
         }
           }
    }.toMap

  /**
   * Computes shortest paths between two vertices.
   *
   * The edge attribute is Long which, will be used as weight
   *
   * @param graph the graph for which to compute the shortest paths
   * @param fromVid is the vertexId from spf needs to be computed
   * @param toVid is the vertexId to which spf needs to be conputed
   *
   * @return a graph where each vertex attribute is a map containing
   * the shortest-path distance and (path array) to* toVid.
   */
  def run[VD: ClassTag](graph: Graph[VD, Long],
                       fromVid: VertexId,
                       toVid: VertexId): Graph[SPMap, Long] = {
    // arrt is Int here
    // vid of destination is inserted in path
    val spGraph = graph.mapVertices { (vid, attr) =>
      if (vid == toVid) makeMap(vid -> (0, Seq(toVid))) else makeMap()
    }

    val initialMessage = makeMap()

    def vertexProgram(id: VertexId, attr: SPMap, msg: SPMap): SPMap = {
      addMaps(attr, msg)
    }

    // edge.attr is the edge attribute we treat that as weight
    def sendMessage(edge: EdgeTriplet[SPMap, Long]): Iterator[(VertexId, SPMap)] = {
      val newAttr = incrementMap(edge.dstAttr, edge.attr, edge.srcId)
      if (edge.srcAttr != addMaps(newAttr, edge.srcAttr)) Iterator((edge.srcId, newAttr))
      else Iterator.empty
    }

    Pregel(spGraph, initialMessage)(vertexProgram, sendMessage, addMaps)
  }
}
