package org.apache.spark.graph

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

class GraphOps[VD: ClassManifest, ED: ClassManifest](g: Graph[VD, ED]) {

  lazy val numEdges: Long = g.edges.count()

  lazy val numVertices: Long = g.vertices.count()

  lazy val inDegrees: RDD[(Vid, Int)] = degreesRDD(EdgeDirection.In)

  lazy val outDegrees: RDD[(Vid, Int)] = degreesRDD(EdgeDirection.Out)

  lazy val degrees: RDD[(Vid, Int)] = degreesRDD(EdgeDirection.Both)

  def collectNeighborIds(edgeDirection: EdgeDirection) : RDD[(Vid, Array[Vid])] = {
    val nbrs = g.aggregateNeighbors[Array[Vid]](
      (vid, edge) => Some(Array(edge.otherVertex(vid).id)),
      (a, b) => a ++ b,
      edgeDirection)

    g.vertices.leftOuterJoin(nbrs).mapValues{
      case (_, Some(nbrs)) => nbrs
      case (_, None) => Array.empty[Vid]
    }
  }

  private def degreesRDD(edgeDirection: EdgeDirection): RDD[(Vid, Int)] = {
    g.aggregateNeighbors((vid, edge) => Some(1), _+_, edgeDirection)
  }
}
