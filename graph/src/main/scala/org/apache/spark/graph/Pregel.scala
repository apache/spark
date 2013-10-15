package org.apache.spark.graph

import org.apache.spark.rdd.RDD


object Pregel {

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
