package org.apache.spark.graphx

import org.apache.spark.graphx.impl.VertexPartition

/**
 * An edge triplet represents two vertices and edge along with their
 * attributes.
 *
 * @tparam VD the type of the vertex attribute.
 * @tparam ED the type of the edge attribute
 *
 * @todo specialize edge triplet for basic types, though when I last
 * tried specializing I got a warning about inherenting from a type
 * that is not a trait.
 */
class EdgeTriplet[VD, ED] extends Edge[ED] {
  /**
   * The source vertex attribute
   */
  var srcAttr: VD = _ //nullValue[VD]

  /**
   * The destination vertex attribute
   */
  var dstAttr: VD = _ //nullValue[VD]

  /**
   * Set the edge properties of this triplet.
   */
  protected[spark] def set(other: Edge[ED]): EdgeTriplet[VD,ED] = {
    srcId = other.srcId
    dstId = other.dstId
    attr = other.attr
    this
  }

  /**
   * Given one vertex in the edge return the other vertex.
   *
   * @param vid the id one of the two vertices on the edge.
   * @return the attribute for the other vertex on the edge.
   */
  def otherVertexAttr(vid: VertexID): VD =
    if (srcId == vid) dstAttr else { assert(dstId == vid); srcAttr }

  /**
   * Get the vertex object for the given vertex in the edge.
   *
   * @param vid the id of one of the two vertices on the edge
   * @return the attr for the vertex with that id.
   */
  def vertexAttr(vid: VertexID): VD =
    if (srcId == vid) srcAttr else { assert(dstId == vid); dstAttr }

  override def toString() = ((srcId, srcAttr), (dstId, dstAttr), attr).toString()
}
