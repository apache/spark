package org.apache.spark.graphx.lib

import scala.reflect.ClassTag

import org.apache.spark.graphx._

/** Connected components algorithm. */
object ConnectedComponents {
  /**
   * Compute the connected component membership of each vertex and return a graph with the vertex
   * value containing the lowest vertex id in the connected component containing that vertex.
   *
   * @tparam VD the vertex attribute type (discarded in the computation)
   * @tparam ED the edge attribute type (preserved in the computation)
   *
   * @param graph the graph for which to compute the connected components
   * @param undirected compute reachability ignoring edge direction.
   *
   * @return a graph with vertex attributes containing the smallest vertex in each
   *         connected component
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], undirected: Boolean = true):
    Graph[VertexID, ED] = {
    val ccGraph = graph.mapVertices { case (vid, _) => vid }
    if (undirected) {
      def sendMessage(edge: EdgeTriplet[VertexID, ED]) = {
        if (edge.srcAttr < edge.dstAttr) {
          Iterator((edge.dstId, edge.srcAttr))
        } else if (edge.srcAttr > edge.dstAttr) {
          Iterator((edge.srcId, edge.dstAttr))
        } else {
          Iterator.empty
        }
      }
      val initialMessage = Long.MaxValue
      Pregel(ccGraph, initialMessage, activeDirection = EdgeDirection.Both)(
        vprog = (id, attr, msg) => math.min(attr, msg),
        sendMsg = sendMessage,
        mergeMsg = (a, b) => math.min(a, b))
    } else {
      def sendMessage(edge: EdgeTriplet[VertexID, ED]) = {
        if (edge.srcAttr < edge.dstAttr) {
          Iterator((edge.dstId, edge.srcAttr))
        } else {
          Iterator.empty
        }
      }
      val initialMessage = Long.MaxValue
      Pregel(ccGraph, initialMessage, activeDirection = EdgeDirection.Out)(
        vprog = (id, attr, msg) => math.min(attr, msg),
        sendMsg = sendMessage,
        mergeMsg = (a, b) => math.min(a, b))
    }
  } // end of connectedComponents
}
