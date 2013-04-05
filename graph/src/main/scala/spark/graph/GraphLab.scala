package spark.graph

import scala.collection.JavaConversions._
import spark.RDD


object GraphLab {

  def iterateGAS[VD: ClassManifest, ED: ClassManifest, A: ClassManifest](
    rawGraph: Graph[VD, ED])(
    gather: (Vid, EdgeWithVertices[VD, ED]) => A,
    merge: (A, A) => A,
    default: A,
    apply: (Vertex[VD], A) => VD,
    numIter: Int,
    gatherDirection: EdgeDirection.EdgeDirection = EdgeDirection.In) : Graph[VD, ED] = {

    var graph = rawGraph.cache()

    var i = 0
    while (i < numIter) {

      val accUpdates: RDD[(Vid, A)] =
        graph.mapReduceNeighborhood(gather, merge, default, gatherDirection)

      def applyFunc(v: Vertex[VD], update: Option[A]): VD = { apply(v, update.get) }
      graph = graph.updateVertices(accUpdates, applyFunc).cache()

      i += 1
    }
    graph
  }


  def iterateGASOption[VD: ClassManifest, ED: ClassManifest, A: ClassManifest](
    rawGraph: Graph[VD, ED])(
    gather: (Vid, EdgeWithVertices[VD, ED]) => A,
    merge: (A, A) => A,
    apply: (Vertex[VD], Option[A]) => VD,
    numIter: Int,
    gatherDirection: EdgeDirection.EdgeDirection = EdgeDirection.In) : Graph[VD, ED] = {

    var graph = rawGraph.cache()

    def someGather(vid: Vid, edge: EdgeWithVertices[VD,ED]) = Some(gather(vid, edge))

    var i = 0
    while (i < numIter) {

      val accUpdates: RDD[(Vid, A)] =
        graph.mapReduceNeighborhoodFilter(someGather, merge, gatherDirection)

      def applyFunc(v: Vertex[VD], update: Option[A]): VD = { apply(v, update) }
      graph = graph.updateVertices(accUpdates, applyFunc).cache()

      i += 1
    }
    graph
  }



}
