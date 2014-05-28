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

/** LPA algorithm. */
object LPA {
  /**
   * Run LPA (label propogation algorithm) for detecting communities in networks using the pregel framework.
   * 
   * Each node in the network is initially assigned to its own community.  At every super step 
   * nodes send their community affiliation to all neighbors and update their state to the mode 
   * community affiliation of incomming messages.  
   *
   * LPA is a standard community detection algorithm for graphs.  It is very inexpensive 
   * computationally, although (1) convergence is not guaranteed and (2) one can end up with
   * trivial solutions (all nodes are identified into a single community).
   *
   * @tparam VD the vertex attribute type (discarded in the computation)
   * @tparam ED the edge attribute type (not used in the computation)
   *
   * @param graph the graph for which to compute the community affiliation
   * @param maxSteps the number of supersteps of LPA to be performed
   *
   * @return a graph with vertex attributes containing the label of community affiliation
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], maxSteps: Int): Graph[VertexId, Long]{
    val lpaGraph = graph.mapVertices { case (vid, _) => vid }
    def sendMessage(edge: EdgeTriplet[VertexId, ED]) = {
      Iterator((e.srcId, Map(e.dstAttr -> 1L)),(e.dstId, Map(e.srcAttr -> 1L)))
    }
    def mergeMessage(count1: Map[VertexId, Long], count2: Map[VertexId, Long]): Map[VertexId, Long] = {
      (count1.keySet ++ count2.keySet).map { i =>
        val count1Val = count1.getOrElse(i,0L)
        val count2Val = count2.getOrElse(i,0L)
	i -> (count1Val +count2Val)
	}.toMap
    }
    def vertexProgram(vid: VertexId, attr: Long, message: Map[VertexId, Long])={
      if (message.isEmpty) attr else message.maxBy{_._2}._1),
    }
    val initialMessage = Map[VertexId,Long]()
    Pregel(lpaGraph, initialMessage, maxIterations = maxSteps)(
      vprog = vertexProgram,
      sendMsg = sendMessage,
      mergeMsg = mergeMessage)
  }
}
