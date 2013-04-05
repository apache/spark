package spark.graph

import scala.collection.JavaConversions._
import spark.RDD


object Pregel {

  def iterate[VD: ClassManifest, ED: ClassManifest, A: ClassManifest](
    rawGraph: Graph[VD, ED])(
    vprog: ( Vertex[VD], A) => VD,
    sendMsg: (Vid, EdgeWithVertices[VD, ED]) => Option[A],
    mergeMsg: (A, A) => A,
    numIter: Int) : Graph[VD, ED] = {

    var graph = rawGraph.cache
    var i = 0

    def reverseGather(vid: Vid, edge: EdgeWithVertices[VD,ED]) =
      sendMsg(edge.otherVertex(vid).id, edge)

    while (i < numIter) {

      val msgs: RDD[(Vid, A)] =
        graph.mapReduceNeighborhoodFilter(reverseGather, mergeMsg, EdgeDirection.In)

      def runProg(v: Vertex[VD], msg: Option[A]): VD =
        if(msg.isEmpty) v.data else vprog(v, msg.get)

      graph = graph.updateVertices(msgs, runProg).cache()

      i += 1
    }
    graph

  }

}
