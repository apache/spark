package org.apache.spark.graph.impl

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ArrayBuilder

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import org.apache.spark.graph._

/**
 * Stores the layout of replicated vertex attributes for GraphImpl. Tells each
 * partition of the vertex data where it should go.
 */
class VertexPlacement(
    eTable: RDD[(Pid, EdgePartition[ED])] forSome { type ED },
    vTable: VertexSetRDD[_]) {

  val bothAttrs: RDD[Array[Array[Vid]]] = createPid2Vid(true, true)
  val srcAttrOnly: RDD[Array[Array[Vid]]] = createPid2Vid(true, false)
  val dstAttrOnly: RDD[Array[Array[Vid]]] = createPid2Vid(false, true)
  val noAttrs: RDD[Array[Array[Vid]]] = createPid2Vid(false, false)

  def get(includeSrcAttr: Boolean, includeDstAttr: Boolean): RDD[Array[Array[Vid]]] =
    (includeSrcAttr, includeDstAttr) match {
      case (true, true) => bothAttrs
      case (true, false) => srcAttrOnly
      case (false, true) => dstAttrOnly
      case (false, false) => noAttrs
    }

  def persist(newLevel: StorageLevel) {
    bothAttrs.persist(newLevel)
    srcAttrOnly.persist(newLevel)
    dstAttrOnly.persist(newLevel)
    noAttrs.persist(newLevel)
  }

  private def createPid2Vid(
      includeSrcAttr: Boolean, includeDstAttr: Boolean): RDD[Array[Array[Vid]]] = {
    // Determine which vertices each edge partition needs by creating a mapping from vid to pid.
    val preAgg = eTable.mapPartitions { iter =>
      val (pid: Pid, edgePartition: EdgePartition[_]) = iter.next()
      val numEdges = edgePartition.size
      val vSet = new VertexSet
      if (includeSrcAttr) {  // Add src vertices to the set.
        var i = 0
        while (i < numEdges) {
          vSet.add(edgePartition.srcIds(i))
          i += 1
        }
      }
      if (includeDstAttr) {  // Add dst vertices to the set.
        var i = 0
        while (i < numEdges) {
          vSet.add(edgePartition.dstIds(i))
          i += 1
        }
      }
      vSet.iterator.map { vid => (vid, pid) }
    }
    // Aggregate the mappings to determine where each vertex should go
    val vid2pid = VertexSetRDD[Pid, ArrayBuffer[Pid]](preAgg, vTable.index,
      (p: Pid) => ArrayBuffer(p),
      (ab: ArrayBuffer[Pid], p:Pid) => {ab.append(p); ab},
      (a: ArrayBuffer[Pid], b: ArrayBuffer[Pid]) => a ++ b)
      .mapValues(a => a.toArray)
    // Within each vertex partition, reorganize the placement information into
    // columnar format keyed on the destination partition
    val numPartitions = vid2pid.partitions.size
    vid2pid.mapPartitions { iter =>
      val pid2vid = Array.fill[ArrayBuilder[Vid]](numPartitions)(ArrayBuilder.make[Vid])
      for ((vid, pids) <- iter) {
        pids.foreach { pid => pid2vid(pid) += vid }
      }
      Iterator(pid2vid.map(_.result))
    }
  }
}
