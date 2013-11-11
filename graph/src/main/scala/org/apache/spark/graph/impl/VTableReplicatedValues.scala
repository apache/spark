package org.apache.spark.graph.impl

import org.apache.spark.rdd.RDD
import org.apache.spark.util.collection.OpenHashSet

import org.apache.spark.graph._
import org.apache.spark.graph.impl.MsgRDDFunctions._

/**
 * Stores the vertex attribute values after they are replicated. See
 * the description of localVidMap in [[GraphImpl]].
 */
class VTableReplicatedValues[VD: ClassManifest](
    vTable: VertexSetRDD[VD],
    vid2pid: Vid2Pid,
    localVidMap: RDD[(Pid, VertexIdToIndexMap)]) {

  val bothAttrs: RDD[(Pid, Array[VD])] =
    VTableReplicatedValues.createVTableReplicated(vTable, vid2pid, localVidMap, true, true)
  val srcAttrOnly: RDD[(Pid, Array[VD])] =
    VTableReplicatedValues.createVTableReplicated(vTable, vid2pid, localVidMap, true, false)
  val dstAttrOnly: RDD[(Pid, Array[VD])] =
    VTableReplicatedValues.createVTableReplicated(vTable, vid2pid, localVidMap, false, true)
  val noAttrs: RDD[(Pid, Array[VD])] =
    VTableReplicatedValues.createVTableReplicated(vTable, vid2pid, localVidMap, false, false)


  def get(includeSrcAttr: Boolean, includeDstAttr: Boolean): RDD[(Pid, Array[VD])] =
    (includeSrcAttr, includeDstAttr) match {
      case (true, true) => bothAttrs
      case (true, false) => srcAttrOnly
      case (false, true) => dstAttrOnly
      case (false, false) => noAttrs
    }
}



object VTableReplicatedValues {
  protected def createVTableReplicated[VD: ClassManifest](
      vTable: VertexSetRDD[VD],
      vid2pid: Vid2Pid,
      localVidMap: RDD[(Pid, VertexIdToIndexMap)],
      includeSrcAttr: Boolean,
      includeDstAttr: Boolean): RDD[(Pid, Array[VD])] = {

    // Join vid2pid and vTable, generate a shuffle dependency on the joined
    // result, and get the shuffle id so we can use it on the slave.
    val msgsByPartition = vTable.zipJoinFlatMap(vid2pid.get(includeSrcAttr, includeDstAttr)) {
      // TODO(rxin): reuse VertexBroadcastMessage
      (vid, vdata, pids) => pids.iterator.map { pid =>
        new VertexBroadcastMsg[VD](pid, vid, vdata)
      }
    }.partitionBy(localVidMap.partitioner.get).cache()

    localVidMap.zipPartitions(msgsByPartition){
      (mapIter, msgsIter) =>
      val (pid, vidToIndex) = mapIter.next()
      assert(!mapIter.hasNext)
      // Populate the vertex array using the vidToIndex map
      val vertexArray = new Array[VD](vidToIndex.capacity)
      for (msg <- msgsIter) {
        val ind = vidToIndex.getPos(msg.vid) & OpenHashSet.POSITION_MASK
        vertexArray(ind) = msg.data
      }
      Iterator((pid, vertexArray))
    }.cache()

    // @todo assert edge table has partitioner
  }

}
