package org.apache.spark.graph


/**
 * A single directed edge consisting of a source id, target id,
 * and the data associated with the Edgee.
 *
 * @tparam ED type of the edge attribute
 */
case class Edge[@specialized(Char, Int, Boolean, Byte, Long, Float, Double) ED] (
  var srcId: Vid = 0,
  var dstId: Vid = 0,
  var attr: ED = nullValue[ED]) {

  /**
   * Given one vertex in the edge return the other vertex.
   *
   * @param vid the id one of the two vertices on the edge.
   * @return the id of the other vertex on the edge.
   */
  def otherVertexId(vid: Vid): Vid =
    if (srcId == vid) dstId else { assert(dstId == vid); srcId }


  /**
   * Return the relative direction of the edge to the corresponding vertex.
   *
   * @param vid the id of one of the two vertices in the edge.
   * @return the relative direction of the edge to the corresponding vertex.
   */
  def relativeDirection(vid: Vid): EdgeDirection =
    if (vid == srcId) EdgeDirection.Out else { assert(vid == dstId); EdgeDirection.In }

}
