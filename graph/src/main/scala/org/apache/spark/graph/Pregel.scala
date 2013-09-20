package org.apache.spark.graph

import org.apache.spark.rdd.RDD


object Pregel {

  def iterate[VD: ClassManifest, ED: ClassManifest, A: ClassManifest](graph: Graph[VD, ED])(
      vprog: (Vertex[VD], A) => VD,
      sendMsg: (Vid, EdgeTriplet[VD, ED]) => Option[A],
      mergeMsg: (A, A) => A,
      initialMsg: A,
      numIter: Int)
    : Graph[VD, ED] = {

    var g = graph
    //var g = graph.cache()
    var i = 0

    def mapF(vid: Vid, edge: EdgeTriplet[VD,ED]) = sendMsg(edge.otherVertex(vid).id, edge)

    def runProg(vertexWithMsgs: Vertex[(VD, Option[A])]): VD = {
      val (vData, msg) = vertexWithMsgs.data
      val v = Vertex(vertexWithMsgs.id, vData)
      msg match {
        case Some(m) => vprog(v, m)
        case None => v.data
      }
    }

    var graphWithMsgs: Graph[(VD, Option[A]), ED] =
      g.mapVertices(v => (v.data, Some(initialMsg)))

    while (i < numIter) {
      val newGraph: Graph[VD, ED] = graphWithMsgs.mapVertices(runProg).cache()
      graphWithMsgs = newGraph.aggregateNeighbors(mapF, mergeMsg, EdgeDirection.In)
      i += 1
    }
    graphWithMsgs.mapVertices(vertexWithMsgs => vertexWithMsgs.data match {
      case (vData, _) => vData
    })
  }
}
