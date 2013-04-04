package spark.graph

import scala.collection.JavaConversions._
import spark.RDD


object GraphLab {

  def iterateGAS[VD: ClassManifest, ED: ClassManifest, A: ClassManifest](
      graph: Graph[VD, ED])(
      gather: (Vid, EdgeWithVertices[VD, ED]) => A,
      merge: (A, A) => A,
      default: A,
      apply: (Vertex[VD], A) => VD,
      numIter: Int,
      gatherDirection: EdgeDirection.EdgeDirection = EdgeDirection.In)
    : Graph[VD, ED] = {

    var g = graph.cache()

    var i = 0
    while (i < numIter) {

      val accUpdates: RDD[(Vid, A)] = g.mapReduceNeighborhood(
        gather, merge, default, gatherDirection)

      def applyFunc(v: Vertex[VD], update: Option[A]): VD = { apply(v, update.get) }
      g = g.updateVertices(accUpdates, applyFunc).cache()

      i += 1
    }

    g
  }

}
