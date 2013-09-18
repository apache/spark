package org.apache.spark.graph

/**
 * A graph vertex consists of a vertex id and attribute.
 *
 * @tparam VD the type of the vertex attribute.
 */
case class Vertex[@specialized(Char, Int, Boolean, Byte, Long, Float, Double) VD] (
  var id: Vid = 0,
  var data: VD = nullValue[VD]) {

  def this(tuple: (Vid, VD)) = this(tuple._1, tuple._2)

  def tuple = (id, data)
}
