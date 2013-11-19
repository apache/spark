package org.apache.spark.graph.impl

import org.apache.spark.graph._
import org.apache.spark.util.collection.OpenHashMap


/**
 * A partition of edges in 3 large columnar arrays.
 */
class EdgePartition[@specialized(Char, Int, Boolean, Byte, Long, Float, Double) ED: ClassManifest](
    val srcIds: Array[Vid],
    val dstIds: Array[Vid],
    val data: Array[ED]) {

  def reverse: EdgePartition[ED] = new EdgePartition(dstIds, srcIds, data)

  def map[ED2: ClassManifest](f: Edge[ED] => ED2): EdgePartition[ED2] = {
    val newData = new Array[ED2](data.size)
    val edge = new Edge[ED]()
    val size = data.size
    var i = 0
    while (i < size) {
      edge.srcId  = srcIds(i)
      edge.dstId  = dstIds(i)
      edge.attr = data(i)
      newData(i) = f(edge)
      i += 1
    }
    new EdgePartition(srcIds, dstIds, newData)
  }

  def foreach(f: Edge[ED] => Unit)  {
    val edge = new Edge[ED]
    val size = data.size
    var i = 0
    while (i < size) {
      edge.srcId = srcIds(i)
      edge.dstId = dstIds(i)
      edge.attr = data(i)
      f(edge)
      i += 1
    }
  }

  def groupEdges(merge: (ED, ED) => ED): EdgePartition[ED] = {
    val agg = new OpenHashMap[(Vid,Vid), ED] //(math.ceil((data.size + 1)/0.7).toInt)
    foreach { e => agg.setMerge((e.srcId, e.dstId), e.attr, merge) }
    val newSrcIds = new Array[Vid](agg.size)
    val newDstIds = new Array[Vid](agg.size)
    val newData = new Array[ED](agg.size)
    var i = 0
    agg.foreach { kv =>
      newSrcIds(i) = kv._1._1
      newDstIds(i) = kv._1._2
      newData(i) = kv._2
      i += 1
    }
    new EdgePartition(newSrcIds, newDstIds, newData)
  }

  def size: Int = srcIds.size

  def iterator = new Iterator[Edge[ED]] {
    private[this] val edge = new Edge[ED]
    private[this] var pos = 0

    override def hasNext: Boolean = pos < EdgePartition.this.size

    override def next(): Edge[ED] = {
      edge.srcId = srcIds(pos)
      edge.dstId = dstIds(pos)
      edge.attr = data(pos)
      pos += 1
      edge
    }
  }
}
