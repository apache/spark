package org.apache.spark.graph

import org.apache.spark.rdd.RDD


class GraphOps[VD: ClassManifest, ED: ClassManifest](g: Graph[VD, ED]) {

  lazy val numEdges: Long = g.edges.count()

  lazy val numVertices: Long = g.vertices.count()

  lazy val inDegrees: RDD[(Vid, Int)] = degreesRDD(EdgeDirection.In)

  lazy val outDegrees: RDD[(Vid, Int)] = degreesRDD(EdgeDirection.Out)

  lazy val degrees: RDD[(Vid, Int)] = degreesRDD(EdgeDirection.Both)

  def collectNeighborIds(edgeDirection: EdgeDirection) : RDD[(Vid, Array[Vid])] = {
    val graph: Graph[(VD, Option[Array[Vid]]), ED] = g.aggregateNeighbors(
      (vid, edge) => Some(Array(edge.otherVertex(vid).id)),
      (a, b) => a ++ b,
      edgeDirection)
    graph.vertices.map(v => {
      val (_, neighborIds) = v.data
      (v.id, neighborIds.getOrElse(Array()))
    })
  }

  private def degreesRDD(edgeDirection: EdgeDirection): RDD[(Vid, Int)] = {
    val degreeGraph: Graph[(VD, Option[Int]), ED] =
      g.aggregateNeighbors((vid, edge) => Some(1), _+_, edgeDirection)
    degreeGraph.vertices.map(v => {
      val (_, degree) = v.data
      (v.id, degree.getOrElse(0))
    })
  }
}
