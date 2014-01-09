package org.apache.spark

import org.apache.spark.util.collection.OpenHashSet


package object graph {

  type VertexID = Long

  // TODO: Consider using Char.
  type PartitionID = Int

  type VertexSet = OpenHashSet[VertexID]

  //  type VertexIdToIndexMap = it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap
  type VertexIdToIndexMap = OpenHashSet[VertexID]

  /**
   * Return the default null-like value for a data type T.
   */
  def nullValue[T] = null.asInstanceOf[T]
}
