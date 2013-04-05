package spark.graph

import scala.collection.JavaConversions._
import spark.RDD


object Pregel {

  def iterate[VD: ClassManifest, ED: ClassManifest, A: ClassManifest](
    rawGraph: Graph[VD, ED])(
    vprog: ( Vertex[VD], A) => VD,
    sendMsg: (Vid, EdgeWithVertices[VD, ED]) => Option[A],
    mergeMsg: (A, A) => A,
    initialMsg: A,
    numIter: Int) : Graph[VD, ED] = {

    var graph = rawGraph.cache
    var i = 0

    def reverseGather(vid: Vid, edge: EdgeWithVertices[VD,ED]) =
      sendMsg(edge.otherVertex(vid).id, edge)

    var msgs: RDD[(Vid, A)] = graph.vertices.map{ v => (v.id, initialMsg) }

    while (i < numIter) {

      def runProg(v: Vertex[VD], msg: Option[A]): VD =
        if(msg.isEmpty) v.data else vprog(v, msg.get)

      graph = graph.updateVertices(msgs, runProg).cache()

      msgs = graph.flatMapReduceNeighborhood(reverseGather, mergeMsg, EdgeDirection.In)

      i += 1
    }
    graph

  }


  def iterateOriginal[VD: ClassManifest, ED: ClassManifest, A: ClassManifest](
    rawGraph: Graph[VD, ED])(
    vprog: ( Vertex[VD], A, Seq[Vid]) => Seq[(Vid, A)],
    mergeMsg: (A, A) => A,
    numIter: Int) : Graph[VD, ED] = {

    var graph = rawGraph.cache
    var i = 0

    val outNbrIds : RDD[(Vid, Array[Vid])] = graph.collectNeighborIds(EdgeDirection.Out)

    /// Todo implement
    /// vprog takes the vertex, the message (A), and list of out neighbor ids

    graph

  }

}
