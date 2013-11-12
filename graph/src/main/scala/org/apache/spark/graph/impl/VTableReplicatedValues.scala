package org.apache.spark.graph.impl

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.collection.{OpenHashSet, PrimitiveKeyOpenHashMap}

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

class VertexAttributeBlock[VD: ClassManifest](val vids: Array[Vid], val attrs: Array[VD])

object VTableReplicatedValues {
  protected def createVTableReplicated[VD: ClassManifest](
      vTable: VertexSetRDD[VD],
      vid2pid: Vid2Pid,
      localVidMap: RDD[(Pid, VertexIdToIndexMap)],
      includeSrcAttr: Boolean,
      includeDstAttr: Boolean): RDD[(Pid, Array[VD])] = {

    val pid2vid = vid2pid.getPid2Vid(includeSrcAttr, includeDstAttr)

    val msgsByPartition = pid2vid.zipPartitions(vTable.index.rdd, vTable.valuesRDD) {
      (pid2vidIter, indexIter, valuesIter) =>
      val pid2vid = pid2vidIter.next()
      val index = indexIter.next()
      val values = valuesIter.next()
      val vmap = new PrimitiveKeyOpenHashMap(index, values._1)

      // Send each partition the vertex attributes it wants
      val output = new Array[(Pid, VertexAttributeBlock[VD])](pid2vid.size)
      for (pid <- 0 until pid2vid.size) {
        val block = new VertexAttributeBlock(pid2vid(pid), pid2vid(pid).map(vid => vmap(vid)))
        output(pid) = (pid, block)
      }
      output.iterator
    }.partitionBy(localVidMap.partitioner.get).cache()

    localVidMap.zipPartitions(msgsByPartition){
      (mapIter, msgsIter) =>
      val (pid, vidToIndex) = mapIter.next()
      assert(!mapIter.hasNext)
      // Populate the vertex array using the vidToIndex map
      val vertexArray = new Array[VD](vidToIndex.capacity)
      for ((_, block) <- msgsIter) {
        for (i <- 0 until block.vids.size) {
          val vid = block.vids(i)
          val attr = block.attrs(i)
          val ind = vidToIndex.getPos(vid) & OpenHashSet.POSITION_MASK
          vertexArray(ind) = attr
        }
      }
      Iterator((pid, vertexArray))
    }.cache()
  }

}
