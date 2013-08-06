package spark

package object graph {

  type Vid = Int
  type Pid = Int

  type VertexHashMap[T] = it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap[T]

  /**
   * Return the default null-like value for a data type T.
   */
  def nullValue[T] = null.asInstanceOf[T]


  private[graph]
  case class MutableTuple2[@specialized(Char, Int, Boolean, Byte, Long, Float, Double) U,
                           @specialized(Char, Int, Boolean, Byte, Long, Float, Double) V](
    var _1: U, var _2: V)

}
