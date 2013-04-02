package spark

package object graph {

  type Vid = Int
  type Pid = Int
  type Status = Boolean
  type VertexHashMap = it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap

  /**
   * Return the default null-like value for a data type T.
   */
  def nullValue[T] = null.asInstanceOf[T]

}
