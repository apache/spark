package spark.graph

import scala.collection.JavaConversions._
import spark.RDD


object GraphLab {

  // def iterateGA[VD: ClassManifest, ED: ClassManifest, A: ClassManifest](
  //   rawGraph: Graph[VD, ED])(
  //   gather: (Vid, EdgeWithVertices[VD, ED]) => A,
  //   merge: (A, A) => A,
  //   default: A,
  //   apply: (Vertex[VD], A) => VD,
  //   numIter: Int,
  //   gatherDirection: EdgeDirection.EdgeDirection = EdgeDirection.In) : Graph[VD, ED] = {

  //   var graph = rawGraph.cache()

  //   var i = 0
  //   while (i < numIter) {

  //     val accUpdates: RDD[(Vid, A)] =
  //       graph.mapReduceNeighborhood(gather, merge, default, gatherDirection)

  //     def applyFunc(v: Vertex[VD], update: Option[A]): VD = { apply(v, update.get) }
  //     graph = graph.updateVertices(accUpdates, applyFunc).cache()

  //     i += 1
  //   }
  //   graph
  // }

  def iterateGA[VD: ClassManifest, ED: ClassManifest, A: ClassManifest](graph: Graph[VD, ED])(
    gatherFunc: (Vid, EdgeWithVertices[VD, ED]) => A,
    mergeFunc: (A, A) => A,
    applyFunc: (Vertex[VD], Option[A]) => VD,
    numIter: Int,
    gatherDirection: EdgeDirection = EdgeDirection.In) : Graph[VD, ED] = {

    var g = graph.cache()

    def someGather(vid: Vid, edge: EdgeWithVertices[VD, ED]) = Some(gatherFunc(vid, edge))

    var i = 0
    while (i < numIter) {

      val accUpdates: RDD[(Vid, A)] =
        g.flatMapReduceNeighborhood(someGather, mergeFunc, gatherDirection)

      g = g.updateVertices(accUpdates, applyFunc).cache()

      i += 1
    }
    g
  }

  def iterateGAS[VD: ClassManifest, ED: ClassManifest, A: ClassManifest](rawGraph: Graph[VD, ED])(
    gatherFunc: (Vid, EdgeWithVertices[VD, ED]) => A,
    mergeFunc: (A, A) => A,
    applyFunc: (Vertex[VD], Option[A]) => VD,
    scatterFunc: (Vid, EdgeWithVertices[VD, ED]) => Boolean,
    numIter: Int,
    gatherDirection: EdgeDirection = EdgeDirection.In,
    scatterDirection: EdgeDirection = EdgeDirection.Out) : Graph[VD, ED] = {

    var graph = rawGraph.mapVertices{ case Vertex(id,data) => Vertex(id, (true, data)) }.cache()

    def gather(vid: Vid, e: EdgeWithVertices[(Boolean, VD), ED]) = {
      if(e.vertex(vid).data._1) {
        val edge = new EdgeWithVertices[VD,ED]
        edge.src = Vertex(e.src.id, e.src.data._2)
        edge.dst = Vertex(e.dst.id, e.dst.data._2)
        Some(gatherFunc(vid, edge))
      } else {
        None
      }
    }

    def apply(v: Vertex[(Boolean, VD)], accum: Option[A]) = {
      if(v.data._1) (true, applyFunc(Vertex(v.id, v.data._2), accum))
      else (false, v.data._2)
    }

    def scatter(rawVid: Vid, e: EdgeWithVertices[(Boolean, VD),ED]) = {
      val vid = e.otherVertex(rawVid).id
      if(e.vertex(vid).data._1) {
        val edge = new EdgeWithVertices[VD,ED]
        edge.src = Vertex(e.src.id, e.src.data._2)
        edge.dst = Vertex(e.dst.id, e.dst.data._2)
        Some(scatterFunc(vid, edge))
      } else {
        None
      }
    }

    def applyActive(v: Vertex[(Boolean, VD)], accum: Option[Boolean]) =
      (accum.getOrElse(false), v.data._2)

    var i = 0
    var numActive = graph.numVertices
    while (i < numIter && numActive > 0) {

      val accUpdates: RDD[(Vid, A)] =
        graph.flatMapReduceNeighborhood(gather, mergeFunc, gatherDirection)

      graph = graph.updateVertices(accUpdates, apply).cache()

      // Scatter is basically a gather in the opposite direction so we reverse the edge direction
      val activeVertices: RDD[(Vid, Boolean)] =
        graph.flatMapReduceNeighborhood(scatter, _ || _, scatterDirection.reverse)

      graph = graph.updateVertices(activeVertices, applyActive).cache()

      numActive = graph.vertices.map(v => if (v.data._1) 1 else 0).reduce( _ + _ )
      println("Number active vertices: " + numActive)
      i += 1
    }
    graph.mapVertices(v => Vertex(v.id, v.data._2))
  }
}
