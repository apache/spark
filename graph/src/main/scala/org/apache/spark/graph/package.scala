package org.apache.spark

import org.apache.spark.util.collection.OpenHashSet


package object graph {

  type Vid = Long
  type Pid = Int

  type VertexSet = OpenHashSet[Vid]
  type VertexArrayList = it.unimi.dsi.fastutil.longs.LongArrayList

  //  type VertexIdToIndexMap = it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap
  type VertexIdToIndexMap = OpenHashSet[Vid]

  /**
   * Return the default null-like value for a data type T.
   */
  def nullValue[T] = null.asInstanceOf[T]


  private[graph]
  case class MutableTuple2[@specialized(Char, Int, Boolean, Byte, Long, Float, Double) U,
                           @specialized(Char, Int, Boolean, Byte, Long, Float, Double) V](
    var _1: U, var _2: V)

}
