package org.apache.spark.graph.impl

import org.apache.spark.graph._
import org.apache.spark.util.collection.OpenHashMap

/**
 * A collection of edges stored in 3 large columnar arrays (src, dst, attribute).
 *
 * @param srcIds the source vertex id of each edge
 * @param dstIds the destination vertex id of each edge
 * @param data the attribute associated with each edge
 * @tparam ED the edge attribute type.
 */
class EdgePartition[@specialized(Char, Int, Boolean, Byte, Long, Float, Double) ED: ClassManifest](
    val srcIds: Array[Vid],
    val dstIds: Array[Vid],
    val data: Array[ED]) {

  /**
   * Reverse all the edges in this partition.
   *
   * @note No new data structures are created.
   *
   * @return a new edge partition with all edges reversed.
   */
  def reverse: EdgePartition[ED] = new EdgePartition(dstIds, srcIds, data)

  /**
   * Construct a new edge partition by applying the function f to all
   * edges in this partition.
   *
   * @param f a function from an edge to a new attribute
   * @tparam ED2 the type of the new attribute
   * @return a new edge partition with the result of the function `f`
   *         applied to each edge
   */
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

  /**
   * Apply the function f to all edges in this partition.
   *
   * @param f an external state mutating user defined function.
   */
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

  /**
   * Merge all the edges with the same src and dest id into a single
   * edge using the `merge` function
   *
   * @param merge a commutative associative merge operation
   * @return a new edge partition without duplicate edges
   */
  def groupEdges(merge: (ED, ED) => ED): EdgePartition[ED] = {
    // Aggregate all matching edges in a hashmap
    val agg = new OpenHashMap[(Vid,Vid), ED]
    foreach { e => agg.setMerge((e.srcId, e.dstId), e.attr, merge) }
    // Populate new srcId, dstId, and data, arrays
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

  /**
   * The number of edges in this partition
   *
   * @return size of the partition
   */
  def size: Int = srcIds.size

  /**
   * Get an iterator over the edges in this partition.
   *
   * @return an iterator over edges in the partition
   */
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
