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
import org.apache.spark.rdd.RDD

/** Computes unweighted all-pairs shortest paths. */
object AllPairsShortestPaths {

  /**
   * Computes unweighted all-pairs shortest paths, returning an RDD containing the shortest-path
   * distance between all pairs of reachable vertices. The algorithm is similar to distance-vector
   * routing and runs in `O(|V|)` iterations.
   *
   * @param graph the graph for which to compute the all-pairs shortest paths
   * @return an RDD with the shortest-path distance for each pair of reachable vertices. Distances
   * are represented as pairs of the form `(srcId, (dstId, distance))`.
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): RDD[(VertexId, (VertexId, Int))] = {
    val g = graph
      // Remove redundant edges
      .groupEdges((a, b) => a)
      // Initialize vertices with the distance to themselves
      .mapVertices((id, attr) => Map(id -> 0))

    def unionMapsWithMin(a: Map[VertexId, Int], b: Map[VertexId, Int]): Map[VertexId, Int] = {
      (a.keySet ++ b.keySet).map { v =>
        val aDist = a.getOrElse(v, Int.MaxValue)
        val bDist = b.getOrElse(v, Int.MaxValue)
        v -> (if (aDist < bDist) aDist else bDist)
      }.toMap
    }

    // For sending update messages from a to b
    def diffMaps(a: Map[VertexId, Int], b: Map[VertexId, Int])
      : Map[VertexId, Int] = {
      (a.keySet ++ b.keySet).flatMap { v =>
        if (a.contains(v)) {
          val aDist = a(v)
          val bDist = b.getOrElse(v, Int.MaxValue)
          if (aDist + 1 < bDist) Some(v -> (aDist + 1)) else None
        } else {
          None
        }
      }.toMap
    }

    def sendMsg(et: EdgeTriplet[Map[VertexId, Int], ED])
      : Iterator[(VertexId, Map[VertexId, Int])] = {
      val msgToSrc = diffMaps(et.dstAttr, et.srcAttr)
      val msgToDst = diffMaps(et.srcAttr, et.dstAttr)
      Iterator((et.srcId, msgToSrc), (et.dstId, msgToDst)).filter(_._2.nonEmpty)
    }

    val maps = Pregel(g, Map.empty[VertexId, Int])(
      (id, a, b) => unionMapsWithMin(a, b), sendMsg, unionMapsWithMin)

    maps.vertices.flatMap { case (src, map) => map.iterator.map(kv => (src, kv)) }
  }
}
