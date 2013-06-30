package spark.graph.impl

import scala.collection.mutable.ArrayBuffer

import it.unimi.dsi.fastutil.ints.IntArrayList

import spark.graph._


/**
 * A partition of edges in 3 large columnar arrays.
 */
private[graph]
class EdgePartition[@specialized(Char, Int, Boolean, Byte, Long, Float, Double) ED: ClassManifest] {

  val srcIds: IntArrayList = new IntArrayList
  val dstIds: IntArrayList = new IntArrayList
  // TODO: Specialize data.
  val data: ArrayBuffer[ED] = new ArrayBuffer[ED]

  /** Add a new edge to the partition. */
  def add(src: Vid, dst: Vid, d: ED) {
    srcIds.add(src)
    dstIds.add(dst)
    data += d
  }

  def trim() {
    srcIds.trim()
    dstIds.trim()
  }

  def size: Int = srcIds.size

  def iterator = new Iterator[Edge[ED]] {
    private val edge = new Edge[ED]
    private var pos = 0

    override def hasNext: Boolean = pos < EdgePartition.this.size

    override def next(): Edge[ED] = {
      edge.src = srcIds.get(pos)
      edge.dst = dstIds.get(pos)
      edge.data = data(pos)
      pos += 1
      edge
    }
  }
}
