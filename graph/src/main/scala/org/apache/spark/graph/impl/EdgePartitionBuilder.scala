package org.apache.spark.graph.impl

import scala.collection.mutable.ArrayBuilder
import org.apache.spark.graph._


private[graph]
class EdgePartitionBuilder[@specialized(Char, Int, Boolean, Byte, Long, Float, Double) 
ED: ClassManifest]{
  val srcIds = new VertexArrayList
  val dstIds = new VertexArrayList
  var dataBuilder = ArrayBuilder.make[ED]


  /** Add a new edge to the partition. */
  def add(src: Vid, dst: Vid, d: ED) {
    srcIds.add(src)
    dstIds.add(dst)
    dataBuilder += d
  }

  def toEdgePartition: EdgePartition[ED] = {
  	new EdgePartition(srcIds.toLongArray(), dstIds.toLongArray(), dataBuilder.result())
  }
  

}




