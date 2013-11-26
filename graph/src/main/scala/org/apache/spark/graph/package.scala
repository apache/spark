package org.apache.spark

import org.apache.spark.util.collection.OpenHashSet


package object graph {

  type Vid = Long

  // TODO: Consider using Char.
  type Pid = Int

  type VertexSet = OpenHashSet[Vid]

  //  type VertexIdToIndexMap = it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap
  type VertexIdToIndexMap = OpenHashSet[Vid]

  /**
   * Return the default null-like value for a data type T.
   */
  def nullValue[T] = null.asInstanceOf[T]
}
