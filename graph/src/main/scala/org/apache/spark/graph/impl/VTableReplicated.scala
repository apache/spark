package org.apache.spark.graph.impl

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.collection.{PrimitiveVector, OpenHashSet}

import org.apache.spark.graph._

/**
 * A view of the vertices after they are shipped to the join sites specified in
 * `vertexPlacement`. The resulting view is co-partitioned with `edges`. If `prevVTableReplicated`
 * is specified, `updatedVerts` are treated as incremental updates to the previous view. Otherwise,
 * a fresh view is created.
 *
 * The view is always cached (i.e., once it is created, it remains materialized). This avoids
 * constructing it twice if the user calls graph.triplets followed by graph.mapReduceTriplets, for
 * example.
 */
private[impl]
class VTableReplicated[VD: ClassManifest](
    updatedVerts: VertexRDD[VD],
    edges: EdgeRDD[_],
    vertexPlacement: VertexPlacement,
    prevVTableReplicated: Option[VTableReplicated[VD]] = None) {

  val bothAttrs: RDD[(Pid, VertexPartition[VD])] = createVTableReplicated(true, true)
  val srcAttrOnly: RDD[(Pid, VertexPartition[VD])] = createVTableReplicated(true, false)
  val dstAttrOnly: RDD[(Pid, VertexPartition[VD])] = createVTableReplicated(false, true)
  val noAttrs: RDD[(Pid, VertexPartition[VD])] = createVTableReplicated(false, false)

  def get(includeSrc: Boolean, includeDst: Boolean): RDD[(Pid, VertexPartition[VD])] = {
    (includeSrc, includeDst) match {
      case (true, true) => bothAttrs
      case (true, false) => srcAttrOnly
      case (false, true) => dstAttrOnly
      case (false, false) => noAttrs
    }
  }

  private def createVTableReplicated(
      includeSrcAttr: Boolean, includeDstAttr: Boolean): RDD[(Pid, VertexPartition[VD])] = {

    val placement = vertexPlacement.get(includeSrcAttr, includeDstAttr)
    val vdManifest = classManifest[VD]

    // Ship vertex attributes to edge partitions according to vertexPlacement
    val shippedVerts = placement
      .zipPartitions(updatedVerts.partitionsRDD)(VTableReplicated.buildBuffer(_, _)(vdManifest))
      .partitionBy(edges.partitioner.get).cache()
    // TODO: Consider using a specialized shuffler.

    prevVTableReplicated match {
      case Some(vTableReplicated) =>
        val prevView: RDD[(Pid, VertexPartition[VD])] =
          vTableReplicated.get(includeSrcAttr, includeDstAttr)

        // Update vTableReplicated with updatedVerts, setting staleness flags in the resulting
        // VertexPartitions
        prevView.zipPartitions(shippedVerts) { (prevViewIter, shippedVertsIter) =>
          val (pid, prevVPart) = prevViewIter.next()
          val newVPart = prevVPart.updateHideUnchanged(
            shippedVertsIter.flatMap(_._2.iterator))
          Iterator((pid, newVPart))
        }.cache().setName("VTableReplicated delta %s %s".format(includeSrcAttr, includeDstAttr))

      case None =>
        // Within each edge partition, create a local map from vid to an index into
        // the attribute array. Each map contains a superset of the vertices that it
        // will receive, because it stores vids from both the source and destination
        // of edges. It must always include both source and destination vids because
        // some operations, such as GraphImpl.mapReduceTriplets, rely on this.
        val localVidMap = edges.partitionsRDD.mapPartitions(_.map {
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
        localVidMap.zipPartitions(shippedVerts) { (mapIter, shippedVertsIter) =>
          val (pid, vidToIndex) = mapIter.next()
          assert(!mapIter.hasNext)
          // Populate the vertex array using the vidToIndex map
          val vertexArray = vdManifest.newArray(vidToIndex.capacity)
          for ((_, block) <- shippedVertsIter) {
            for (i <- 0 until block.vids.size) {
              val vid = block.vids(i)
              val attr = block.attrs(i)
              val ind = vidToIndex.getPos(vid) & OpenHashSet.POSITION_MASK
              vertexArray(ind) = attr
            }
          }
          val newVPart = new VertexPartition(
            vidToIndex, vertexArray, vidToIndex.getBitSet)(vdManifest)
          Iterator((pid, newVPart))
        }.cache().setName("VTableReplicated %s %s".format(includeSrcAttr, includeDstAttr))
    }
  }
}

object VTableReplicated {
  protected def buildBuffer[VD: ClassManifest](
      pid2vidIter: Iterator[Array[Array[Vid]]],
      vertexPartIter: Iterator[VertexPartition[VD]]) = {
    val pid2vid: Array[Array[Vid]] = pid2vidIter.next()
    val vertexPart: VertexPartition[VD] = vertexPartIter.next()

    Iterator.tabulate(pid2vid.size) { pid =>
      val vidsCandidate = pid2vid(pid)
      val size = vidsCandidate.length
      val vids = new PrimitiveVector[Vid](pid2vid(pid).size)
      val attrs = new PrimitiveVector[VD](pid2vid(pid).size)
      var i = 0
      while (i < size) {
        val vid = vidsCandidate(i)
        if (vertexPart.isDefined(vid)) {
          vids += vid
          attrs += vertexPart(vid)
        }
        i += 1
      }
      (pid, new VertexAttributeBlock(vids.trim().array, attrs.trim().array))
    }
  }
}

class VertexAttributeBlock[VD: ClassManifest](val vids: Array[Vid], val attrs: Array[VD]) {
  def iterator: Iterator[(Vid, VD)] = (0 until vids.size).iterator.map { i => (vids(i), attrs(i)) }
}
