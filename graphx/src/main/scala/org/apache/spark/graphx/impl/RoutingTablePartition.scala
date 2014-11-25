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

import scala.reflect.ClassTag

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.ShuffledRDD
import org.apache.spark.util.collection.{BitSet, PrimitiveVector}

import org.apache.spark.graphx._
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap

import org.apache.spark.graphx.impl.RoutingTablePartition.RoutingTableMessage

private[graphx]
object RoutingTablePartition {
  /**
   * A message from an edge partition to a vertex specifying the position in which the edge
   * partition references the vertex (src, dst, or both). The edge partition is encoded in the lower
   * 30 bytes of the Int, and the position is encoded in the upper 2 bytes of the Int.
   */
  type RoutingTableMessage = (VertexId, Int)

  private def toMessage(vid: VertexId, pid: PartitionID, position: Byte): RoutingTableMessage = {
    val positionUpper2 = position << 30
    val pidLower30 = pid & 0x3FFFFFFF
    (vid, positionUpper2 | pidLower30)
  }

  private def vidFromMessage(msg: RoutingTableMessage): VertexId = msg._1
  private def pidFromMessage(msg: RoutingTableMessage): PartitionID = msg._2 & 0x3FFFFFFF
  private def positionFromMessage(msg: RoutingTableMessage): Byte = (msg._2 >> 30).toByte

  val empty: RoutingTablePartition = new RoutingTablePartition(Array.empty)

  /** Generate a `RoutingTableMessage` for each vertex referenced in `edgePartition`. */
  def edgePartitionToMsgs(pid: PartitionID, edgePartition: EdgePartition[_, _])
    : Iterator[RoutingTableMessage] = {
    // Determine which positions each vertex id appears in using a map where the low 2 bits
    // represent src and dst
    val map = new GraphXPrimitiveKeyOpenHashMap[VertexId, Byte]
    edgePartition.iterator.foreach { e =>
      map.changeValue(e.srcId, 0x1, (b: Byte) => (b | 0x1).toByte)
      map.changeValue(e.dstId, 0x2, (b: Byte) => (b | 0x2).toByte)
    }
    map.iterator.map { vidAndPosition =>
      val vid = vidAndPosition._1
      val position = vidAndPosition._2
      toMessage(vid, pid, position)
    }
  }

  /** Build a `RoutingTablePartition` from `RoutingTableMessage`s. */
  def fromMsgs(numEdgePartitions: Int, iter: Iterator[RoutingTableMessage])
    : RoutingTablePartition = {
    val pid2vid = Array.fill(numEdgePartitions)(new PrimitiveVector[VertexId])
    val srcFlags = Array.fill(numEdgePartitions)(new PrimitiveVector[Boolean])
    val dstFlags = Array.fill(numEdgePartitions)(new PrimitiveVector[Boolean])
    for (msg <- iter) {
      val vid = vidFromMessage(msg)
      val pid = pidFromMessage(msg)
      val position = positionFromMessage(msg)
      pid2vid(pid) += vid
      srcFlags(pid) += (position & 0x1) != 0
      dstFlags(pid) += (position & 0x2) != 0
    }

    new RoutingTablePartition(pid2vid.zipWithIndex.map {
      case (vids, pid) => (vids.trim().array, toBitSet(srcFlags(pid)), toBitSet(dstFlags(pid)))
    })
  }

  /** Compact the given vector of Booleans into a BitSet. */
  private def toBitSet(flags: PrimitiveVector[Boolean]): BitSet = {
    val bitset = new BitSet(flags.size)
    var i = 0
    while (i < flags.size) {
      if (flags(i)) {
        bitset.set(i)
      }
      i += 1
    }
    bitset
  }
}

/**
 * Stores the locations of edge-partition join sites for each vertex attribute in a particular
 * vertex partition. This provides routing information for shipping vertex attributes to edge
 * partitions.
 */
private[graphx]
class RoutingTablePartition(
    private val routingTable: Array[(Array[VertexId], BitSet, BitSet)]) extends Serializable {
  /** The maximum number of edge partitions this `RoutingTablePartition` is built to join with. */
  val numEdgePartitions: Int = routingTable.size

  /** Returns the number of vertices that will be sent to the specified edge partition. */
  def partitionSize(pid: PartitionID): Int = routingTable(pid)._1.size

  /** Returns an iterator over all vertex ids stored in this `RoutingTablePartition`. */
  def iterator: Iterator[VertexId] = routingTable.iterator.flatMap(_._1.iterator)

  /** Returns a new RoutingTablePartition reflecting a reversal of all edge directions. */
  def reverse: RoutingTablePartition = {
    new RoutingTablePartition(routingTable.map {
      case (vids, srcVids, dstVids) => (vids, dstVids, srcVids)
    })
  }

  /**
   * Runs `f` on each vertex id to be sent to the specified edge partition. Vertex ids can be
   * filtered by the position they have in the edge partition.
   */
  def foreachWithinEdgePartition
      (pid: PartitionID, includeSrc: Boolean, includeDst: Boolean)
      (f: VertexId => Unit) {
    val (vidsCandidate, srcVids, dstVids) = routingTable(pid)
    val size = vidsCandidate.length
    if (includeSrc && includeDst) {
      // Avoid checks for performance
      vidsCandidate.iterator.foreach(f)
    } else if (!includeSrc && !includeDst) {
      // Do nothing
    } else {
      val relevantVids = if (includeSrc) srcVids else dstVids
      relevantVids.iterator.foreach { i => f(vidsCandidate(i)) }
    }
  }
}
