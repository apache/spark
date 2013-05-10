package spark.graph

import scala.collection.JavaConversions._
import spark.RDD


object Pregel {

  def iterate[VD: ClassManifest, ED: ClassManifest, A: ClassManifest](graph: Graph[VD, ED])(
    vprog: ( Vertex[VD], A) => VD,
    sendMsg: (Vid, EdgeWithVertices[VD, ED]) => Option[A],
    mergeMsg: (A, A) => A,
    initialMsg: A,
    numIter: Int) : Graph[VD, ED] = {

    var g = graph.cache
    var i = 0

    def mapF(vid: Vid, edge: EdgeWithVertices[VD,ED]) = sendMsg(edge.otherVertex(vid).id, edge)

    def runProg(v: Vertex[VD], msg: Option[A]): VD = {
      if (msg.isEmpty) v.data else vprog(v, msg.get)
    }

    var msgs: RDD[(Vid, A)] = g.vertices.map{ v => (v.id, initialMsg) }

    while (i < numIter) {
      g = g.leftJoinVertices(msgs, runProg).cache()
      msgs = g.aggregateNeighbors(mapF, mergeMsg, EdgeDirection.In)
      i += 1
    }
    g
  }

}
