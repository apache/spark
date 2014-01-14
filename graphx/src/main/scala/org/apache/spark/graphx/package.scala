package org.apache.spark

import org.apache.spark.util.collection.OpenHashSet

/** GraphX is a graph processing framework built on top of Spark. */
package object graphx {
  /**
   * A 64-bit vertex identifier that uniquely identifies a vertex within a graph. It does not need
   * to follow any ordering or any constraints other than uniqueness.
   */
  type VertexID = Long

  /** Integer identifer of a graph partition. */
  // TODO: Consider using Char.
  type PartitionID = Int

  private[graphx] type VertexSet = OpenHashSet[VertexID]
}
