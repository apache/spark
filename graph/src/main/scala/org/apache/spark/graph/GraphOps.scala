package org.apache.spark.graph

import org.apache.spark.rdd.RDD


class GraphOps[VD: ClassManifest, ED: ClassManifest](g: Graph[VD, ED]) {

  lazy val numEdges: Long = g.edges.count()

  lazy val numVertices: Long = g.vertices.count()

  lazy val inDegrees: RDD[(Vid, Int)] = {
    g.aggregateNeighbors((vid, edge) => Some(1), _+_, EdgeDirection.In)
  }

  lazy val outDegrees: RDD[(Vid, Int)] = {
    g.aggregateNeighbors((vid, edge) => Some(1), _+_, EdgeDirection.Out)
  }

  lazy val degrees: RDD[(Vid, Int)] = {
    g.aggregateNeighbors((vid, edge) => Some(1), _+_, EdgeDirection.Both)
  }

  def collectNeighborIds(edgeDirection: EdgeDirection) : RDD[(Vid, Array[Vid])] = {
    g.aggregateNeighbors(
      (vid, edge) => Some(Array(edge.otherVertex(vid).id)),
      (a, b) => a ++ b,
      edgeDirection)
  }
}
