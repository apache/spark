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
 * Computes all the direct and indirect connections of each vertex on a graph up to a
 * maximum degree.
 */
object VertexConnections {
  /**
   * @tparam ED the edge attribute type (not used in the computation)
   *
   * @param maxDegree the maximum number of Pregel iterations to be performed
   * @param graph the graph for which to compute the connections
   * @param removeDegree0 if true, the returned graph will not contain vertices with degree 0
   *
   * @return a graph where each vertex attribute is a map containing all the connections and
   *         the degree of each connection
   */
  def apply[VD: ClassTag, ED: ClassTag](
      graph: Graph[VD, ED],
      maxDegree: Int = Int.MaxValue,
      removeDegree0: Boolean = true)
      : Graph[Map[Int, Set[VD]], ED] =
  {
    type RelationsMap = Map[Int, Set[VD]]

    def makeMap(x: (Int, Set[VD])*): RelationsMap = (Map(x: _*))

    /**
     * Add two maps from different vertices
     */
    def addMaps(rel1: collection.mutable.Map[Int, Set[VD]],
        rel2: collection.mutable.Map[Int, Set[VD]]): RelationsMap = {
      val firstValues: Set[VD] = rel1.values.toSet.flatten

      val notIncludedMap: collection.mutable.Map[Int, Set[VD]] = rel2.map{case (key, value) =>
        val notIncluded: Set[VD] = value -- firstValues
        key -> notIncluded
      }

      notIncludedMap.foreach{case (key, value) =>
         if (key > 0) {
          val finalSet: Set[VD] = rel1.getOrElse(key + 1, Set.empty) ++ value
          if (finalSet.size > 0) rel1.put(key + 1, finalSet)
        }
      }

      Map(rel1.toSeq: _*)
    }

    /**
     * This function merges two maps from the same vertex.
     */
    def mergeMaps(rel1: RelationsMap, rel2: RelationsMap): RelationsMap = {
      val firstValues: Set[VD] = rel1.values.toSet.flatten

      val notIncludedMap: RelationsMap = rel2.map{case (key, value) =>
        val notIncluded: Set[VD] = value -- firstValues
        key -> notIncluded
      }

      val finalSet = (rel1.keys ++ notIncludedMap.keys).map { k =>
        (k, rel1.getOrElse(k, Set.empty) ++ notIncludedMap.getOrElse(k, Set.empty))
      }

      finalSet.toMap
    }

    /**
     * Called on every vertex to merge all the inbound messages after each Pregel iteration
     */
    def vertexProgram(id: VertexId, attr: RelationsMap, msg: RelationsMap): RelationsMap = {
      mergeMaps(attr, msg)
    }

    /**
     * Function applied to all of the edges that received messages in the current iteration
     */
    def sendMessage(edge: EdgeTriplet[RelationsMap, _]): Iterator[(VertexId, RelationsMap)] = {
      val copySrc = collection.mutable.Map(edge.srcAttr.toSeq: _*)
      val copyDst = collection.mutable.Map(edge.dstAttr.toSeq: _*)

      copySrc.put(1, copySrc.getOrElse(1, Set.empty) ++ copyDst(0))
      copyDst.put(1, copyDst.getOrElse(1, Set.empty) ++ copySrc(0))

      val newSrcAttr = addMaps(copySrc, copyDst)
      val newDstAttr = addMaps(copyDst, copySrc)

      if (edge.srcAttr != newSrcAttr || edge.dstAttr != newDstAttr) {
        Iterator((edge.srcId, newSrcAttr), (edge.dstId, newDstAttr))
      }
      else Iterator.empty
    }

    val initialMessage = makeMap()

    val relGraph: Graph[RelationsMap, ED] =
      if (!removeDegree0) {
        graph.mapVertices{ (vid, attr) => makeMap((0, Set(attr))) }
      } else {
        graph.filter(graph => {
          val degrees: VertexRDD[Int] = graph.degrees
          graph.outerJoinVertices(degrees) {(vid, data, deg) => deg.getOrElse(0)}
        },
        vpred = (vid: VertexId, deg: Int) => deg > 0
        ).mapVertices{ (vid, attr) => makeMap((0, Set(attr))) }
      }

    Pregel(relGraph, initialMessage, maxDegree)(vertexProgram, sendMessage, mergeMaps)
  }
}
