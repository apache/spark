package spark.graph


class GraphLab {

  def iterateGAS[A: Manifest, VD: Manifest, ED: Manifest](
    graph: Graph[VD, ED],
    gather: (Vid, EdgeWithVertices[VD, ED]) => A,
    merge: (A, A) => A,
    default: A,
    apply: (Vertex[VD], A) => VD,
    numIter: Int,
    gatherEdges: EdgeDirection = EdgeDirection.In) = {

    val g = new Graph[(VD, A), ED](graph.rawVertices.map(v => (v, default)), graph.rawEdges)

    var i = 0
    while (i < numIter) {

      val gatherTable = g.mapPartitions { case(vmap, iter) =>
        val edgeSansAcc = new EdgeWithVertices[VD, ED]()
        iter.map { edge: EdgeWithVertices[(VD, A), ED] =>
          edgeSansAcc.data = edge.data
          edgeSansAcc.src.data = edge.src.data._1
          edgeSansAcc.dst.data = edge.dst.data._1
          edgeSansAcc.src.id = edge.src.id
          edgeSansAcc.dst.id = edge.dst.id
          if (gatherEdges == EdgeDirection.In || gatherEdges == EdgeDirection.Both) {
            edge.dst.data._2 = merge(edge.dst.data._2, gather(edgeSansAcc.dst.id, edgeSansAcc))
          }
          if (gatherEdges == EdgeDirection.Out || gatherEdges == EdgeDirection.Both) {
            edge.src.data._2 = merge(edge.src.data._2, gather(edgeSansAcc.src.id, edgeSansAcc))
          }
        }

        vmap.int2ObjectEntrySet().fastIterator().map{ case (vid, (vdata, acc)) => (vid, acc) }
      }.reduceByKey(graph.vertexPartitioner, false)

      gatherTable

      i += 1
    }
  }

}
