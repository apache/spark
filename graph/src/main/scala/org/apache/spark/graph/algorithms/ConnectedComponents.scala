package org.apache.spark.graph.algorithms

import org.apache.spark.graph._


object ConnectedComponents {
  /**
   * Compute the connected component membership of each vertex and
   * return an RDD with the vertex value containing the lowest vertex
   * id in the connected component containing that vertex.
   *
   * @tparam VD the vertex attribute type (discarded in the
   * computation)
   * @tparam ED the edge attribute type (preserved in the computation)
   *
   * @param graph the graph for which to compute the connected
   * components
   *
   * @return a graph with vertex attributes containing the smallest
   * vertex in each connected component
   */
  def run[VD: Manifest, ED: Manifest](graph: Graph[VD, ED]): Graph[Vid, ED] = {
    val ccGraph = graph.mapVertices { case (vid, _) => vid }

    def sendMessage(edge: EdgeTriplet[Vid, ED]) = {
      if (edge.srcAttr < edge.dstAttr) {
        Iterator((edge.dstId, edge.srcAttr))
      } else if (edge.srcAttr > edge.dstAttr) {
        Iterator((edge.srcId, edge.dstAttr))
      } else {
        Iterator.empty
      }
    }
    val initialMessage = Long.MaxValue
    Pregel(ccGraph, initialMessage)(
      vprog = (id, attr, msg) => math.min(attr, msg),
      sendMsg = sendMessage,
      mergeMsg = (a, b) => math.min(a, b))
  } // end of connectedComponents
}
