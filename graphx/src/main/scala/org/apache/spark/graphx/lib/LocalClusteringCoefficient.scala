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

import scala.collection.mutable.ListBuffer

/**
 * Local clustering coefficient algorithm
 *
 * In a directed graph G=(V, E), we define the neighbourhood N_i of a vertex v_i as
 * N_i={v_j: e_ij \in E or e_ji \in E}
 *
 * The local clustering coefficient C_i of a vertex v_i is then defined as
 * C_i = |{e_jk: v_j, v_k \in N_i, e_jk \in E}| / (K_i * (K_i - 1))
 * where K_i=|N_i| is the number of neighbors of v_i
 *
 * Note that the input graph must have been partitioned using
 * [[org.apache.spark.graphx.Graph#partitionBy]].
 */
object LocalClusteringCoefficient {
  /**
   * Compute the local clustering coefficient for each vertex and
   * return a graph with vertex value representing the local clustering coefficient of that vertex
   *
   * @param graph the graph for which to compute the connected components
   *
   * @return a graph with vertex attributes containing
   *         the local clustering coefficient of that vertex
   *
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Graph[Double, ED] = {
    // Remove redundant edges
    val g = graph.groupEdges((a, b) => a).cache()

    // Construct set representations of the neighborhoods
    // ()
    val nbrSets: VertexRDD[VertexSet] =
      g.collectNeighborIds(EdgeDirection.Either).mapValues { (vid, nbrs) =>
        val set = new VertexSet(4)
        var i = 0
        while (i < nbrs.size) {
          // prevent self cycle
          if(nbrs(i) != vid) {
            set.add(nbrs(i))
          }
          i += 1
        }
        set
      }

    // join the sets with the graph
    val setGraph: Graph[VertexSet, ED] = g.outerJoinVertices(nbrSets) {
      (vid, _, optSet) => optSet.getOrElse(null)
    }

    // Edge function computes intersection of smaller vertex with larger vertex
    def edgeFunc(et: EdgeTriplet[VertexSet, ED]): Iterator[(VertexId, Double)] = {
      assert(et.srcAttr != null)
      assert(et.dstAttr != null)
      val (smallSet, largeSet) = if (et.srcAttr.size < et.dstAttr.size) {
        (et.srcAttr, et.dstAttr)
      } else {
        (et.dstAttr, et.srcAttr)
      }
      val iter = smallSet.iterator
      val buf = new ListBuffer[(VertexId, Double)]
      while (iter.hasNext) {
        val vid = iter.next()
        if (vid != et.srcId && vid != et.dstId && largeSet.contains(vid)) {
          buf += ((vid, 1.0))
        }
      }
      buf.toIterator
    }

    // compute the intersection along edges
    val counters: VertexRDD[Double] = setGraph.mapReduceTriplets(edgeFunc, _ + _)

    // count number of neighbors for each vertex
    var nbNumMap = Map[VertexId, Int]()
    nbrSets.collect().foreach { case (vid, nbVal) =>
      nbNumMap += (vid -> nbVal.size)
    }

    // Merge counters with the graph
    g.outerJoinVertices(counters) {
      (vid, _, optCounter: Option[Double]) =>
        val dblCount: Double = optCounter.getOrElse(0)
        val nbNum = nbNumMap(vid)
        if (nbNum > 1) {
          dblCount / (nbNum * (nbNum - 1))
        }
        else {
          0
        }
    }
  }
}
