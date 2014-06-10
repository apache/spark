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

/**
 * A message from the edge partition `pid` to the vertex partition containing `vid` specifying that
 * the edge partition references `vid` in the specified `position` (src, dst, or both).
*/
private[graphx]
class RoutingTableMessage(
    var vid: VertexId,
    var pid: PartitionID,
    var position: Byte)
  extends Product2[VertexId, (PartitionID, Byte)] with Serializable {
  override def _1 = vid
  override def _2 = (pid, position)
  override def canEqual(that: Any): Boolean = that.isInstanceOf[RoutingTableMessage]
}

private[graphx]
class RoutingTableMessageRDDFunctions(self: RDD[RoutingTableMessage]) {
  /** Copartition an `RDD[RoutingTableMessage]` with the vertex RDD with the given `partitioner`. */
  def copartitionWithVertices(partitioner: Partitioner): RDD[RoutingTableMessage] = {
    new ShuffledRDD[VertexId, (PartitionID, Byte), (PartitionID, Byte), RoutingTableMessage](self,
      partitioner)
      .setSerializer(new RoutingTableMessageSerializer)
  }
}

private[graphx]
object RoutingTableMessageRDDFunctions {
  import scala.language.implicitConversions

  implicit def rdd2RoutingTableMessageRDDFunctions(rdd: RDD[RoutingTableMessage]) = {
    new RoutingTableMessageRDDFunctions(rdd)
  }
}

private[graphx]
object RoutingTablePartition {
  val empty: RoutingTablePartition = new RoutingTablePartition(Array.empty)

  /** Generate a `RoutingTableMessage` for each vertex referenced in `edgePartition`. */
  def edgePartitionToMsgs(pid: PartitionID, edgePartition: EdgePartition[_, _])
    : Iterator[RoutingTableMessage] = {
    // Determine which positions each vertex id appears in using a map where the low 2 bits
    // represent src and dst
    val map = new GraphXPrimitiveKeyOpenHashMap[VertexId, Byte]
    edgePartition.srcIds.iterator.foreach { srcId =>
      map.changeValue(srcId, 0x1, (b: Byte) => (b | 0x1).toByte)
    }
    edgePartition.dstIds.iterator.foreach { dstId =>
      map.changeValue(dstId, 0x2, (b: Byte) => (b | 0x2).toByte)
    }
    map.iterator.map { vidAndPosition =>
      new RoutingTableMessage(vidAndPosition._1, pid, vidAndPosition._2)
    }
  }

  /** Build a `RoutingTablePartition` from `RoutingTableMessage`s. */
  def fromMsgs(numEdgePartitions: Int, iter: Iterator[RoutingTableMessage])
    : RoutingTablePartition = {
    val pid2vid = Array.fill(numEdgePartitions)(new PrimitiveVector[VertexId])
    val srcFlags = Array.fill(numEdgePartitions)(new PrimitiveVector[Boolean])
    val dstFlags = Array.fill(numEdgePartitions)(new PrimitiveVector[Boolean])
    for (msg <- iter) {
      pid2vid(msg.pid) += msg.vid
      srcFlags(msg.pid) += (msg.position & 0x1) != 0
      dstFlags(msg.pid) += (msg.position & 0x2) != 0
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
    private val routingTable: Array[(Array[VertexId], BitSet, BitSet)]) {
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
