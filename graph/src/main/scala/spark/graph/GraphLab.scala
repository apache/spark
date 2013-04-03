package spark.graph

import scala.collection.JavaConversions._

import spark.RDD


class GraphLab {

  def iterateGAS[A: ClassManifest, VD: ClassManifest, ED: ClassManifest](
    graph: Graph[VD, ED],
    gather: (Vid, EdgeWithVertices[VD, ED]) => A,
    merge: (A, A) => A,
    default: A,
    apply: (Vertex[VD], A) => VD,
    numIter: Int,
    gatherEdges: EdgeDirection.EdgeDirection = EdgeDirection.In) = {

    var g = graph.mapVertices(v => Vertex(v.id, VDataWithAcc(v.data, default))).cache()

    var i = 0
    while (i < numIter) {

      val accUpdates: RDD[(Vid, A)] = g.mapPartitions({ case(vmap, iter) =>
        val edgeSansAcc = new EdgeWithVertices[VD, ED]()
        iter.map { edge: EdgeWithVertices[VDataWithAcc[VD, A], ED] =>
          edgeSansAcc.data = edge.data
          edgeSansAcc.src.data = edge.src.data.vdata
          edgeSansAcc.dst.data = edge.dst.data.vdata
          edgeSansAcc.src.id = edge.src.id
          edgeSansAcc.dst.id = edge.dst.id
          if (gatherEdges == EdgeDirection.In || gatherEdges == EdgeDirection.Both) {
            edge.dst.data.acc = merge(edge.dst.data.acc, gather(edgeSansAcc.dst.id, edgeSansAcc))
          }
          if (gatherEdges == EdgeDirection.Out || gatherEdges == EdgeDirection.Both) {
            edge.src.data.acc = merge(edge.src.data.acc, gather(edgeSansAcc.src.id, edgeSansAcc))
          }
        }

        vmap.int2ObjectEntrySet().fastIterator().map{ entry =>
          (entry.getIntKey(), entry.getValue().acc)
        }
      })(classManifest[(Int, A)])

      def applyFunc(v: Vertex[VDataWithAcc[VD, A]], updates: Seq[A]): VDataWithAcc[VD, A] = {
        VDataWithAcc(apply(Vertex(v.id, v.data.vdata), updates.reduce(merge)), default)
      }
      g = g.updateVertices(accUpdates, applyFunc).cache()

      i += 1
    }
  }

}


private[graph]
sealed case class VDataWithAcc[VD: ClassManifest, A](var vdata: VD, var acc: A)
