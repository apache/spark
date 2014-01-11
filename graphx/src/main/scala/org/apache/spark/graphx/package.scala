package org.apache.spark

import org.apache.spark.util.collection.OpenHashSet

package object graphx {
  type VertexID = Long

  // TODO: Consider using Char.
  type PartitionID = Int

  private[graphx] type VertexSet = OpenHashSet[VertexID]

  /** Returns the default null-like value for a data type T. */
  private[graphx] def nullValue[T] = null.asInstanceOf[T]
}
