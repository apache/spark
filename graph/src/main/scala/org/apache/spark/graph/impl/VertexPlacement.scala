package org.apache.spark.graph.impl

import org.apache.spark.SparkContext._
import org.apache.spark.graph._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.collection.PrimitiveVector

/**
 * Stores the layout of replicated vertex attributes for GraphImpl. Tells each
 * partition of the vertex data where it should go.
 */
class VertexPlacement(eTable: EdgeRDD[_], vTable: VertexRDD[_]) {

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
    val vid2pid: RDD[(Vid, Pid)] = eTable.partitionsRDD.mapPartitions { iter =>
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

    val numPartitions = vTable.partitions.size
    vid2pid.partitionBy(vTable.partitioner.get).mapPartitions { iter =>
      val pid2vid = Array.fill(numPartitions)(new PrimitiveVector[Vid])
      for ((vid, pid) <- iter) {
        pid2vid(pid) += vid
      }

      Iterator(pid2vid.map(_.trim().array))
    }.cache().setName("VertexPlacement %s %s".format(includeSrcAttr, includeDstAttr))
  }
}
