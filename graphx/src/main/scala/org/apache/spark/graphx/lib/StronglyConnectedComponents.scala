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

/** Strongly connected components algorithm implementation. */
object StronglyConnectedComponents {

  /**
   * Compute the strongly connected component (SCC) of each vertex and return a graph with the
   * vertex value containing the lowest vertex id in the SCC containing that vertex.
   *
   * @tparam VD the vertex attribute type (discarded in the computation)
   * @tparam ED the edge attribute type (preserved in the computation)
   *
   * @param graph the graph for which to compute the SCC
   *
   * @return a graph with vertex attributes containing the smallest vertex id in each SCC
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], numIter: Int): Graph[VertexId, ED] = {
    require(numIter > 0, s"Number of iterations must be greater than 0," +
      s" but got ${numIter}")

    // graph we are going to work with in our iterations
    var sccWorkGraph = graph.mapVertices { case (vid, _) => (vid, false) }.cache()

    // the SCC label, and the vertices to be removed in iteration
    var sccVertexLabel = graph.vertices.sparkContext.emptyRDD[(VertexId, VertexId)]

    var numVertices = sccWorkGraph.numVertices
    var iter = 0
    while (sccWorkGraph.numVertices > 0 && iter < numIter) {
      iter += 1
      do {
        numVertices = sccWorkGraph.numVertices

        val labeledByOutDegree = sccWorkGraph.outerJoinVertices(sccWorkGraph.outDegrees) {
          (vid, data, degreeOpt) => if (degreeOpt.isDefined) data else (vid, true)
        }

        val labeledByInDegree = labeledByOutDegree.outerJoinVertices(sccWorkGraph.inDegrees) {
          (vid, data, degreeOpt) => if (degreeOpt.isDefined) data else (vid, true)
        }.cache()

        // helper variables to unpersist
        val prevSccVertexLabel = sccVertexLabel

        // get all vertices to be removed
        val finalVertices = labeledByInDegree.vertices
          .filter { case (vid, (scc, isFinal)) => isFinal}
          .mapValues { (vid, data) => data._1}

        // combine new vertex with the SCC label
        sccVertexLabel = finalVertices.union(sccVertexLabel).cache()
        sccVertexLabel.count()    // materialize

        prevSccVertexLabel.unpersist(blocking = false)

        // helper variables to unpersist
        val prevSccWorkGraph = sccWorkGraph

        // only keep vertices that are not final
        sccWorkGraph = labeledByInDegree.subgraph(vpred = (vid, data) => !data._2).cache()

        // materialize vertices and edges
        sccWorkGraph.numVertices
        sccWorkGraph.numEdges

        // unpersist helper variables
        prevSccWorkGraph.unpersist(blocking = false)
        prevSccWorkGraph.edges.unpersist(blocking = false)
        labeledByInDegree.unpersist(blocking = false)
        labeledByInDegree.edges.unpersist(blocking = false)
        labeledByOutDegree.unpersist(blocking = false)
        labeledByOutDegree.edges.unpersist(blocking = false)

      } while (sccWorkGraph.numVertices < numVertices)

      // if iter < numIter at this point sccGraph that is returned
      // will not be recomputed and pregel executions are pointless
      if (iter < numIter) {
        val phase1 = sccWorkGraph.mapVertices{case(vid, (color, isFinal)) => (vid, isFinal)}

        // collect min of all my neighbor's scc values, update if it's smaller than mine
        // then notify any neighbors with scc values larger than mine
        val phase2 = Pregel[(VertexId, Boolean), ED, VertexId](
          phase1, Long.MaxValue, activeDirection = EdgeDirection.Out)(
          (vid, myScc, neighborScc) => (math.min(myScc._1, neighborScc), myScc._2),
          e => {
            if (e.srcAttr._1 < e.dstAttr._1) {
              Iterator((e.dstId, e.srcAttr._1))
            } else {
              Iterator()
            }
          },
          (vid1, vid2) => math.min(vid1, vid2))

        // start at root of SCCs. Traverse values in reverse, notify all my neighbors
        // do not propagate if colors do not match!

        sccWorkGraph.unpersist(blocking = false)
        sccWorkGraph.edges.unpersist(blocking = false)

        sccWorkGraph = Pregel[(VertexId, Boolean), ED, Boolean](
          phase2, false, activeDirection = EdgeDirection.In)(
          // vertex is final if it is the root of a color
          // or it has the same color as a neighbor that is final
          (vid, myScc, existsSameColorFinalNeighbor) => {
            val isColorRoot = vid == myScc._1
            (myScc._1, myScc._2 || isColorRoot || existsSameColorFinalNeighbor)
          },
          // activate neighbor if they are not final, you are, and you have the same color
          e => {
            val sameColor = e.dstAttr._1 == e.srcAttr._1
            val onlyDstIsFinal = e.dstAttr._2 && !e.srcAttr._2
            if (sameColor && onlyDstIsFinal) {
              Iterator((e.srcId, e.dstAttr._2))
            } else {
              Iterator()
            }
          },
          (final1, final2) => final1 || final2).cache()
        phase1.unpersist(blocking = false)
        phase1.edges.unpersist(blocking = false)
        phase2.unpersist(blocking = false)
        phase2.edges.unpersist(blocking = false)
      }
    }

    sccWorkGraph.unpersist(blocking = false)
    sccWorkGraph.edges.unpersist(blocking = false)

    val graphWithId = graph.mapVertices{case(vid, _) => vid}

    val sccGraph = graphWithId.joinVertices(sccVertexLabel) {(_, _, label) => label}.cache()

    sccGraph.numVertices
    sccGraph.numEdges

    sccVertexLabel.unpersist(blocking = false)
    graphWithId.unpersist(blocking = false)
    graphWithId.edges.unpersist(blocking = false)

    sccGraph
  }
}
