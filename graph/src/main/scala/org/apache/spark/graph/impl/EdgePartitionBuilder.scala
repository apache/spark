package org.apache.spark.graph.impl

import org.apache.spark.graph._
import org.apache.spark.util.collection.PrimitiveVector


//private[graph]
class EdgePartitionBuilder[@specialized(Long, Int, Double) ED: ClassManifest] {

  val srcIds = new PrimitiveVector[Vid]
  val dstIds = new PrimitiveVector[Vid]
  var dataBuilder = new PrimitiveVector[ED]

  /** Add a new edge to the partition. */
  def add(src: Vid, dst: Vid, d: ED) {
    srcIds += src
    dstIds += dst
    dataBuilder += d
  }

  def toEdgePartition: EdgePartition[ED] = {
    new EdgePartition(srcIds.trim().array, dstIds.trim().array, dataBuilder.trim().array)
  }
}
