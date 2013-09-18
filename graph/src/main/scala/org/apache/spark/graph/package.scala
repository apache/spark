package org.apache.spark

package object graph {

  type Vid = Long
  type Pid = Int

  type VertexHashMap[T] = it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap[T]
  type VertexSet = it.unimi.dsi.fastutil.longs.LongOpenHashSet
  type VertexArrayList = it.unimi.dsi.fastutil.longs.LongArrayList

  /**
   * Return the default null-like value for a data type T.
   */
  def nullValue[T] = null.asInstanceOf[T]


  private[graph]
  case class MutableTuple2[@specialized(Char, Int, Boolean, Byte, Long, Float, Double) U,
                           @specialized(Char, Int, Boolean, Byte, Long, Float, Double) V](
    var _1: U, var _2: V)

}
