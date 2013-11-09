package org.apache.spark.graph.impl

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.graph._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * Stores the layout of vertex attributes.
 */
class Vid2Pid(
    eTable: RDD[(Pid, EdgePartition[ED])] forSome { type ED },
    vTableIndex: VertexSetIndex) {

  val bothAttrs: VertexSetRDD[Array[Pid]] = createVid2Pid(true, true)
  val srcAttrOnly: VertexSetRDD[Array[Pid]] = createVid2Pid(true, false)
  val dstAttrOnly: VertexSetRDD[Array[Pid]] = createVid2Pid(false, true)
  // TODO(ankurdave): create this more efficiently
  val noAttrs: VertexSetRDD[Array[Pid]] = createVid2Pid(false, false)

  def persist(newLevel: StorageLevel) {
    bothAttrs.persist(newLevel)
    srcAttrOnly.persist(newLevel)
    dstAttrOnly.persist(newLevel)
    noAttrs.persist(newLevel)
  }

  private def createVid2Pid(
    includeSrcAttr: Boolean,
    includeDstAttr: Boolean): VertexSetRDD[Array[Pid]] = {
    val preAgg = eTable.mapPartitions { iter =>
      val (pid, edgePartition) = iter.next()
      val vSet = new VertexSet
      edgePartition.foreach(e => {
        if (includeSrcAttr) vSet.add(e.srcId)
        if (includeDstAttr) vSet.add(e.dstId)
      })
      vSet.iterator.map { vid => (vid.toLong, pid) }
    }
    VertexSetRDD[Pid, ArrayBuffer[Pid]](preAgg, vTableIndex,
      (p: Pid) => ArrayBuffer(p),
      (ab: ArrayBuffer[Pid], p:Pid) => {ab.append(p); ab},
      (a: ArrayBuffer[Pid], b: ArrayBuffer[Pid]) => a ++ b)
      .mapValues(a => a.toArray).cache()
  }
}
