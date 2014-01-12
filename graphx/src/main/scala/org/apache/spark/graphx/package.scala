package org.apache.spark

import org.apache.spark.util.collection.OpenHashSet

package object graphx {
  /**
   * A 64-bit vertex identifier that uniquely identifies a vertex within a graph. It does not need
   * to follow any ordering or any constraints other than uniqueness.
   */
  type VertexID = Long

  // TODO: Consider using Char.
  type PartitionID = Int

  private[graphx] type VertexSet = OpenHashSet[VertexID]

  /** Returns the default null-like value for a data type T. */
  private[graphx] def nullValue[T] = null.asInstanceOf[T]
}
