package org.apache.spark.graph.impl

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.collection.OpenHashSet

import org.apache.spark.graph._

/**
 * Stores the vertex attribute values after they are replicated.
 */
private[impl]
class VTableReplicated[VD: ClassManifest](
    vTable: VertexRDD[VD],
    eTable: EdgeRDD[_],
    vertexPlacement: VertexPlacement,
    prevVTableReplicated: Option[VTableReplicated[VD]] = None) {

  val bothAttrs: RDD[(Pid, VertexPartition[VD])] =
    createVTableReplicated(vTable, eTable, vertexPlacement, true, true)

  val srcAttrOnly: RDD[(Pid, VertexPartition[VD])] =
    createVTableReplicated(vTable, eTable, vertexPlacement, true, false)

  val dstAttrOnly: RDD[(Pid, VertexPartition[VD])] =
    createVTableReplicated(vTable, eTable, vertexPlacement, false, true)

  val noAttrs: RDD[(Pid, VertexPartition[VD])] =
    createVTableReplicated(vTable, eTable, vertexPlacement, false, false)

  def get(includeSrc: Boolean, includeDst: Boolean): RDD[(Pid, VertexPartition[VD])] = {
    (includeSrc, includeDst) match {
      case (true, true) => bothAttrs
      case (true, false) => srcAttrOnly
      case (false, true) => dstAttrOnly
      case (false, false) => noAttrs
    }
  }

  private def createVTableReplicated(
       vTable: VertexRDD[VD],
       eTable: EdgeRDD[_],
       vertexPlacement: VertexPlacement,
       includeSrcAttr: Boolean,
       includeDstAttr: Boolean): RDD[(Pid, VertexPartition[VD])] = {

    val placement = vertexPlacement.get(includeSrcAttr, includeDstAttr)
    val vdManifest = classManifest[VD]

    // Send each edge partition the vertex attributes it wants, as specified in
    // vertexPlacement
    val msgsByPartition = placement.zipPartitions(vTable.partitionsRDD)(VTableReplicated.buildBuffer(_, _)(vdManifest))
      .partitionBy(eTable.partitioner.get).cache()
    // TODO: Consider using a specialized shuffler.

    prevVTableReplicated match {
      case Some(vTableReplicated) =>
        val prev: RDD[(Pid, VertexPartition[VD])] =
          vTableReplicated.get(includeSrcAttr, includeDstAttr)

        prev.zipPartitions(msgsByPartition) { (vTableIter, msgsIter) =>
          val (pid, vertexPartition) = vTableIter.next()
          val (_, block) = msgsIter.next()
          val newVPart = vertexPartition.updateUsingIndex(block.iterator)(vdManifest)
          Iterator((pid, newVPart))
        }.cache()

      case None =>
        // Within each edge partition, create a local map from vid to an index into
        // the attribute array. Each map contains a superset of the vertices that it
        // will receive, because it stores vids from both the source and destination
        // of edges. It must always include both source and destination vids because
        // some operations, such as GraphImpl.mapReduceTriplets, rely on this.
        val localVidMap = eTable.partitionsRDD.mapPartitions(_.map {
          case (pid, epart) =>
            val vidToIndex = new VertexIdToIndexMap
            epart.foreach { e =>
              vidToIndex.add(e.srcId)
              vidToIndex.add(e.dstId)
            }
            (pid, vidToIndex)
        }, preservesPartitioning = true).cache()

        // Within each edge partition, place the vertex attributes received from
        // msgsByPartition into the correct locations specified in localVidMap
        localVidMap.zipPartitions(msgsByPartition) { (mapIter, msgsIter) =>
          val (pid, vidToIndex) = mapIter.next()
          assert(!mapIter.hasNext)
          // Populate the vertex array using the vidToIndex map
          val vertexArray = vdManifest.newArray(vidToIndex.capacity)
          for ((_, block) <- msgsIter) {
            for (i <- 0 until block.vids.size) {
              val vid = block.vids(i)
              val attr = block.attrs(i)
              val ind = vidToIndex.getPos(vid) & OpenHashSet.POSITION_MASK
              vertexArray(ind) = attr
            }
          }
          Iterator((pid, new VertexPartition(vidToIndex, vertexArray, vidToIndex.getBitSet)(vdManifest)))
        }.cache()
    }
  }

}


object VTableReplicated {

  def buildBuffer[VD: ClassManifest](pid2vidIter: Iterator[Array[Array[Vid]]], vertexPartIter: Iterator[VertexPartition[VD]]) = {
    val pid2vid: Array[Array[Vid]] = pid2vidIter.next()
    val vertexPart: VertexPartition[VD] = vertexPartIter.next()
    val output = new Array[(Pid, VertexAttributeBlock[VD])](pid2vid.size)
    //val output = mmm.newArray(pid2vid.size)
    for (pid <- 0 until pid2vid.size) {
      val block = new VertexAttributeBlock(
        pid2vid(pid), pid2vid(pid).map(vid => vertexPart(vid)).toArray)
      output(pid) = (pid, block)
    }
    output.iterator
  }
}

class VertexAttributeBlock[VD: ClassManifest](val vids: Array[Vid], val attrs: Array[VD]) {
  def iterator: Iterator[(Vid, VD)] = (0 until vids.size).iterator.map { i => (vids(i), attrs(i)) }
}
