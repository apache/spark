package org.apache.spark.graph.impl

import scala.collection.mutable.ArrayBuilder
import org.apache.spark.graph._


/**
 * A partition of edges in 3 large columnar arrays.
 */
private[graph]
class EdgePartition[@specialized(Char, Int, Boolean, Byte, Long, Float, Double) ED: ClassManifest](
  val srcIds: Array[Vid],
  val dstIds: Array[Vid],
  val data: Array[ED]
  ){

  // private var _data: Array[ED] = _
  // private var _dataBuilder = ArrayBuilder.make[ED]

  // var srcIds = new VertexArrayList
  // var dstIds = new VertexArrayList

  def reverse: EdgePartition[ED] = new EdgePartition(dstIds, srcIds, data)

  def map[ED2: ClassManifest](f: Edge[ED] => ED2): EdgePartition[ED2] = {
    val newData = new Array[ED2](data.size)
    val edge = new Edge[ED]()
    for(i <- 0 until data.size){
      edge.src  = srcIds(i)
      edge.dst  = dstIds(i)
      edge.data = data(i)
      newData(i) = f(edge) 
    }
    new EdgePartition(srcIds, dstIds, newData)
  }

  def foreach(f: Edge[ED] => Unit)  {
    val edge = new Edge[ED]
    for(i <- 0 until data.size){
      edge.src  = srcIds(i)
      edge.dst  = dstIds(i)
      edge.data = data(i)
      f(edge) 
    }
  }


  def size: Int = srcIds.size

  def iterator = new Iterator[Edge[ED]] {
    private val edge = new Edge[ED]
    private var pos = 0

    override def hasNext: Boolean = pos < EdgePartition.this.size

    override def next(): Edge[ED] = {
      edge.src = srcIds(pos)
      edge.dst = dstIds(pos)
      edge.data = data(pos)
      pos += 1
      edge
    }
  }
}


