package spark.graph


case class Vertex[@specialized(Char, Int, Boolean, Byte, Long, Float, Double) VD] (
  var id: Vid = 0,
  var data: VD = nullValue[VD]) {

  def this(tuple: Tuple2[Vid, VD]) = this(tuple._1, tuple._2)

  def tuple = (id, data)
}
