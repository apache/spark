/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.graphx.impl

import scala.reflect.{classTag, ClassTag}

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.collection.{PrimitiveVector, OpenHashSet}

import org.apache.spark.graphx._

/**
 * A view of the vertices after they are shipped to the join sites specified in
 * `vertexPlacement`. The resulting view is co-partitioned with `edges`. If `prevViewOpt` is
 * specified, `updatedVerts` are treated as incremental updates to the previous view. Otherwise, a
 * fresh view is created.
 *
 * The view is always cached (i.e., once it is evaluated, it remains materialized). This avoids
 * constructing it twice if the user calls graph.triplets followed by graph.mapReduceTriplets, for
 * example. However, it means iterative algorithms must manually call `Graph.unpersist` on previous
 * iterations' graphs for best GC performance. See the implementation of
 * [[org.apache.spark.graphx.Pregel]] for an example.
 */
private[impl]
class ReplicatedVertexView[VD: ClassTag](
    updatedVerts: VertexRDD[VD],
    edges: EdgeRDD[_],
    routingTable: RoutingTable,
    prevViewOpt: Option[ReplicatedVertexView[VD]] = None) {

  /**
   * Within each edge partition, create a local map from vid to an index into the attribute
   * array. Each map contains a superset of the vertices that it will receive, because it stores
   * vids from both the source and destination of edges. It must always include both source and
   * destination vids because some operations, such as GraphImpl.mapReduceTriplets, rely on this.
   */
  private val localVertexIdMap: RDD[(Int, VertexIdToIndexMap)] = prevViewOpt match {
    case Some(prevView) =>
      prevView.localVertexIdMap
    case None =>
      edges.partitionsRDD.mapPartitions(_.map {
        case (pid, epart) =>
          val vidToIndex = new VertexIdToIndexMap
          epart.foreach { e =>
            vidToIndex.add(e.srcId)
            vidToIndex.add(e.dstId)
          }
          (pid, vidToIndex)
      }, preservesPartitioning = true).cache().setName("ReplicatedVertexView localVertexIdMap")
  }

  private lazy val bothAttrs: RDD[(PartitionID, VertexPartition[VD])] = create(true, true)
  private lazy val srcAttrOnly: RDD[(PartitionID, VertexPartition[VD])] = create(true, false)
  private lazy val dstAttrOnly: RDD[(PartitionID, VertexPartition[VD])] = create(false, true)
  private lazy val noAttrs: RDD[(PartitionID, VertexPartition[VD])] = create(false, false)

  def unpersist(blocking: Boolean = true): ReplicatedVertexView[VD] = {
    bothAttrs.unpersist(blocking)
    srcAttrOnly.unpersist(blocking)
    dstAttrOnly.unpersist(blocking)
    noAttrs.unpersist(blocking)
    // Don't unpersist localVertexIdMap because a future ReplicatedVertexView may be using it
    // without modification
    this
  }

  def get(includeSrc: Boolean, includeDst: Boolean): RDD[(PartitionID, VertexPartition[VD])] = {
    (includeSrc, includeDst) match {
      case (true, true) => bothAttrs
      case (true, false) => srcAttrOnly
      case (false, true) => dstAttrOnly
      case (false, false) => noAttrs
    }
  }

  def get(
      includeSrc: Boolean,
      includeDst: Boolean,
      actives: VertexRDD[_]): RDD[(PartitionID, VertexPartition[VD])] = {
    // Ship active sets to edge partitions using vertexPlacement, but ignoring includeSrc and
    // includeDst. These flags govern attribute shipping, but the activeness of a vertex must be
    // shipped to all edges mentioning that vertex, regardless of whether the vertex attribute is
    // also shipped there.
    val shippedActives = routingTable.get(true, true)
      .zipPartitions(actives.partitionsRDD)(ReplicatedVertexView.buildActiveBuffer(_, _))
      .partitionBy(edges.partitioner.get)
    // Update the view with shippedActives, setting activeness flags in the resulting
    // VertexPartitions
    get(includeSrc, includeDst).zipPartitions(shippedActives) { (viewIter, shippedActivesIter) =>
      val (pid, vPart) = viewIter.next()
      val newPart = vPart.replaceActives(shippedActivesIter.flatMap(_._2.iterator))
      Iterator((pid, newPart))
    }
  }

  private def create(includeSrc: Boolean, includeDst: Boolean)
    : RDD[(PartitionID, VertexPartition[VD])] = {
    val vdTag = classTag[VD]

    // Ship vertex attributes to edge partitions according to vertexPlacement
    val verts = updatedVerts.partitionsRDD
    val shippedVerts = routingTable.get(includeSrc, includeDst)
      .zipPartitions(verts)(ReplicatedVertexView.buildBuffer(_, _)(vdTag))
      .partitionBy(edges.partitioner.get)
    // TODO: Consider using a specialized shuffler.

    prevViewOpt match {
      case Some(prevView) =>
        // Update prevView with shippedVerts, setting staleness flags in the resulting
        // VertexPartitions
        prevView.get(includeSrc, includeDst).zipPartitions(shippedVerts) {
          (prevViewIter, shippedVertsIter) =>
            val (pid, prevVPart) = prevViewIter.next()
            val newVPart = prevVPart.innerJoinKeepLeft(shippedVertsIter.flatMap(_._2.iterator))
            Iterator((pid, newVPart))
        }.cache().setName("ReplicatedVertexView delta %s %s".format(includeSrc, includeDst))

      case None =>
        // Within each edge partition, place the shipped vertex attributes into the correct
        // locations specified in localVertexIdMap
        localVertexIdMap.zipPartitions(shippedVerts) { (mapIter, shippedVertsIter) =>
          val (pid, vidToIndex) = mapIter.next()
          assert(!mapIter.hasNext)
          // Populate the vertex array using the vidToIndex map
          val vertexArray = vdTag.newArray(vidToIndex.capacity)
          for ((_, block) <- shippedVertsIter) {
            for (i <- 0 until block.vids.size) {
              val vid = block.vids(i)
              val attr = block.attrs(i)
              val ind = vidToIndex.getPos(vid)
              vertexArray(ind) = attr
            }
          }
          val newVPart = new VertexPartition(
            vidToIndex, vertexArray, vidToIndex.getBitSet)(vdTag)
          Iterator((pid, newVPart))
        }.cache().setName("ReplicatedVertexView %s %s".format(includeSrc, includeDst))
    }
  }
}

private object ReplicatedVertexView {
  protected def buildBuffer[VD: ClassTag](
      pid2vidIter: Iterator[Array[Array[VertexId]]],
      vertexPartIter: Iterator[VertexPartition[VD]]) = {
    val pid2vid: Array[Array[VertexId]] = pid2vidIter.next()
    val vertexPart: VertexPartition[VD] = vertexPartIter.next()

    Iterator.tabulate(pid2vid.size) { pid =>
      val vidsCandidate = pid2vid(pid)
      val size = vidsCandidate.length
      val vids = new PrimitiveVector[VertexId](pid2vid(pid).size)
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

  protected def buildActiveBuffer(
      pid2vidIter: Iterator[Array[Array[VertexId]]],
      activePartIter: Iterator[VertexPartition[_]])
    : Iterator[(Int, Array[VertexId])] = {
    val pid2vid: Array[Array[VertexId]] = pid2vidIter.next()
    val activePart: VertexPartition[_] = activePartIter.next()

    Iterator.tabulate(pid2vid.size) { pid =>
      val vidsCandidate = pid2vid(pid)
      val size = vidsCandidate.length
      val actives = new PrimitiveVector[VertexId](vidsCandidate.size)
      var i = 0
      while (i < size) {
        val vid = vidsCandidate(i)
        if (activePart.isDefined(vid)) {
          actives += vid
        }
        i += 1
      }
      (pid, actives.trim().array)
    }
  }
}

private[graphx]
class VertexAttributeBlock[VD: ClassTag](val vids: Array[VertexId], val attrs: Array[VD])
  extends Serializable {
  def iterator: Iterator[(VertexId, VD)] =
    (0 until vids.size).iterator.map { i => (vids(i), attrs(i)) }
}
