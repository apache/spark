package spark

package object graph {

  type Vid = Int
  type Pid = Int

  type VertexHashMap[T] = it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap[T]

  /**
   * Return the default null-like value for a data type T.
   */
  def nullValue[T] = null.asInstanceOf[T]

}
