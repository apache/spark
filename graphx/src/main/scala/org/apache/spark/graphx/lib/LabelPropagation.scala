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
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap

/** Label Propagation algorithm. */
object LabelPropagation {
  /**
   * Run static Label Propagation for detecting communities in networks.
   *
   * Each node in the network is initially assigned to its own community. At every superstep, nodes
   * send their community affiliation to all neighbors and update their state to the mode community
   * affiliation of incoming messages. Ties are broken by choosing the smallest label.
   *
   * LPA is a standard community detection algorithm for graphs. It is very inexpensive
   * computationally, although (1) convergence is not guaranteed and (2) one can end up with
   * trivial solutions (all nodes are identified into a single community).
   *
   * @tparam ED the edge attribute type (not used in the computation)
   *
   * @param graph the graph for which to compute the community affiliation
   * @param maxSteps the number of supersteps of LPA to be performed. Because this is a static
   * implementation, the algorithm will run for exactly this many supersteps.
   *
   * @return a graph with vertex attributes containing the label of community affiliation
   */
  def run[VD, ED: ClassTag](graph: Graph[VD, ED], maxSteps: Int): Graph[VertexId, ED] = {
    require(maxSteps > 0, s"Maximum of steps must be greater than 0, but got ${maxSteps}")

    val lpaGraph = graph.mapVertices { case (vid, _) => vid }
    type LabelCounts = GraphXPrimitiveKeyOpenHashMap[VertexId, Long]
    def labelCount(label: VertexId): LabelCounts = {
      val counts = new LabelCounts(1)
      counts.update(label, 1L)
      counts
    }
    def sendMessage(e: EdgeTriplet[VertexId, ED]): Iterator[(VertexId, LabelCounts)] = {
      Iterator((e.srcId, labelCount(e.dstAttr)), (e.dstId, labelCount(e.srcAttr)))
    }
    // Merges in place: adds count2 into count1 and returns count1. This is safe because
    // sendMessage always creates new maps and vertexProgram does not modify its message.
    // Plain while loops over the internal arrays avoid a tuple and boxed values per entry.
    def mergeMessage(count1: LabelCounts, count2: LabelCounts): LabelCounts = {
      val labels = count2.keySet
      var pos = labels.nextPos(0)
      while (pos >= 0) {
        count1.setMerge(labels.getValue(pos), count2._values(pos), _ + _)
        pos = labels.nextPos(pos + 1)
      }
      count1
    }
    // The most frequent label; ties are broken by the smallest label.
    def vertexProgram(vid: VertexId, attr: Long, message: LabelCounts): VertexId = {
      val labels = message.keySet
      var best = attr
      var bestCount = 0L
      var pos = labels.nextPos(0)
      while (pos >= 0) {
        val label = labels.getValue(pos)
        val count = message._values(pos)
        if (count > bestCount || (count == bestCount && label < best)) {
          best = label
          bestCount = count
        }
        pos = labels.nextPos(pos + 1)
      }
      best
    }
    val initialMessage = new LabelCounts(1)
    Pregel(lpaGraph, initialMessage, maxIterations = maxSteps)(
      vprog = vertexProgram,
      sendMsg = sendMessage,
      mergeMsg = mergeMessage)
  }
}
