package org.apache.spark.graph

import org.apache.spark.rdd.RDD


/**
 * This object implements the Pregel bulk-synchronous
 * message-passing API.
 */
object Pregel {


  /**
   * Execute the Pregel program.
   *
   * @tparam VD the vertex data type
   * @tparam ED the edge data type
   * @tparam A the Pregel message type
   *
   * @param vprog a user supplied function that acts as the vertex program for
   *              the Pregel computation. It takes the vertex ID of the vertex it is running on,
   *              the accompanying data for that vertex, and the incoming data and returns the
   *              new vertex value.
   * @param sendMsg a user supplied function that takes the current vertex ID and an EdgeTriplet
   *                between the vertex and one of its neighbors and produces a message to send
   *                to that neighbor.
   * @param mergeMsg a user supplied function that takes two incoming messages of type A and merges
   *                 them into a single message of type A. ''This function must be commutative and
   *                 associative.''
   * @param initialMsg the message each vertex will receive at the beginning of the
   *                   first iteration.
   * @param numIter the number of iterations to run this computation for.
   *
   * @return the resulting graph at the end of the computation
   *
   */
  def iterate[VD: ClassManifest, ED: ClassManifest, A: ClassManifest](graph: Graph[VD, ED])(
      vprog: (Vid, VD, A) => VD,
      sendMsg: (Vid, EdgeTriplet[VD, ED]) => Option[A],
      mergeMsg: (A, A) => A,
      initialMsg: A,
      numIter: Int)
    : Graph[VD, ED] = {

    var g = graph
    //var g = graph.cache()
    var i = 0

    def mapF(vid: Vid, edge: EdgeTriplet[VD,ED]) = sendMsg(edge.otherVertexId(vid), edge)

    // Receive the first set of messages
    g.mapVertices( (vid, vdata) => vprog(vid, vdata, initialMsg))

    while (i < numIter) {
      // compute the messages
      val messages = g.aggregateNeighbors(mapF, mergeMsg, EdgeDirection.In)
      // receive the messages
      g = g.joinVertices(messages)(vprog)
      // count the iteration
      i += 1
    }
    // Return the final graph
    g
  }
}
