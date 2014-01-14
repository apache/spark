package org.apache.spark.graphx

/**
 * A single directed edge consisting of a source id, target id,
 * and the data associated with the edge.
 *
 * @tparam ED type of the edge attribute
 *
 * @param srcId The vertex id of the source vertex
 * @param dstId The vertex id of the target vertex
 * @param attr The attribute associated with the edge
 */
case class Edge[@specialized(Char, Int, Boolean, Byte, Long, Float, Double) ED] (
    var srcId: VertexID = 0,
    var dstId: VertexID = 0,
    var attr: ED = null.asInstanceOf[ED])
  extends Serializable {

  /**
   * Given one vertex in the edge return the other vertex.
   *
   * @param vid the id one of the two vertices on the edge.
   * @return the id of the other vertex on the edge.
   */
  def otherVertexId(vid: VertexID): VertexID =
    if (srcId == vid) dstId else { assert(dstId == vid); srcId }

  /**
   * Return the relative direction of the edge to the corresponding
   * vertex.
   *
   * @param vid the id of one of the two vertices in the edge.
   * @return the relative direction of the edge to the corresponding
   * vertex.
   */
  def relativeDirection(vid: VertexID): EdgeDirection =
    if (vid == srcId) EdgeDirection.Out else { assert(vid == dstId); EdgeDirection.In }
}

object Edge {
  def lexicographicOrdering[ED] = new Ordering[Edge[ED]] {
    override def compare(a: Edge[ED], b: Edge[ED]): Int =
      (if (a.srcId != b.srcId) a.srcId - b.srcId else a.dstId - b.dstId).toInt
  }
}
