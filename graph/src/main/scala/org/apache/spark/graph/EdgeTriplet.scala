package org.apache.spark.graph

/**
 * An edge triplet represents two vertices and edge along with their attributes.
 *
 * @tparam VD the type of the vertex attribute.
 * @tparam ED the type of the edge attribute
 */
class EdgeTriplet[@specialized(Char, Int, Boolean, Byte, Long, Float, Double) VD,
                  @specialized(Char, Int, Boolean, Byte, Long, Float, Double) ED] {
  /**
   * The vertex (id and attribute) corresponding to the source vertex.
   */
  var src: Vertex[VD] = _

  /**
   * The vertex (id and attribute) corresponding to the target vertex.
   */
  var dst: Vertex[VD] = _

  /**
   * The attribute associated with the edge.
   */
  var data: ED = _

  /**
   * Given one vertex in the edge return the other vertex.
   *
   * @param vid the id one of the two vertices on the edge.
   * @return the other vertex on the edge.
   */
  def otherVertex(vid: Vid): Vertex[VD] =
    if (src.id == vid) dst else { assert(dst.id == vid); src }

  /**
   * Get the vertex object for the given vertex in the edge.
   *
   * @param vid the id of one of the two vertices on the edge
   * @return the vertex object with that id.
   */
  def vertex(vid: Vid): Vertex[VD] =
    if (src.id == vid) src else { assert(dst.id == vid); dst }

  /**
   * Return the relative direction of the edge to the corresponding vertex.
   *
   * @param vid the id of one of the two vertices in the edge.
   * @return the relative direction of the edge to the corresponding vertex.
   */
  def relativeDirection(vid: Vid): EdgeDirection =
    if (vid == src.id) EdgeDirection.Out else { assert(vid == dst.id); EdgeDirection.In }

}
