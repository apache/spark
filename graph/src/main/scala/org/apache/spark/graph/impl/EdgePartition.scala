package org.apache.spark.graph.impl

import scala.collection.mutable.ArrayBuilder

import it.unimi.dsi.fastutil.ints.IntArrayList

import org.apache.spark.graph._


/**
 * A partition of edges in 3 large columnar arrays.
 */
private[graph]
class EdgePartition[@specialized(Char, Int, Boolean, Byte, Long, Float, Double) ED: ClassManifest] {

  private var _data: Array[ED] = _
  private var _dataBuilder = ArrayBuilder.make[ED]

  val srcIds = new VertexArrayList
  val dstIds = new VertexArrayList

  def data: Array[ED] = _data

  /** Add a new edge to the partition. */
  def add(src: Vid, dst: Vid, d: ED) {
    srcIds.add(src)
    dstIds.add(dst)
    _dataBuilder += d
  }

  def trim() {
    srcIds.trim()
    dstIds.trim()
    _data = _dataBuilder.result()
  }

  def size: Int = srcIds.size

  def iterator = new Iterator[Edge[ED]] {
    private val edge = new Edge[ED]
    private var pos = 0

    override def hasNext: Boolean = pos < EdgePartition.this.size

    override def next(): Edge[ED] = {
      edge.src = srcIds.get(pos)
      edge.dst = dstIds.get(pos)
      edge.data = _data(pos)
      pos += 1
      edge
    }
  }
}
