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

import org.apache.spark.rdd.IndexedRDDPartition
import org.apache.spark.rdd.IndexedRDDPartition.Index
import org.apache.spark.rdd.IndexedRDDPartitionBase
import org.apache.spark.rdd.IndexedRDDPartitionOps
import org.apache.spark.util.collection.{BitSet, PrimitiveVector}

import org.apache.spark.graphx._

/** Stores vertex attributes to ship to an edge partition. */
private[graphx] class VertexAttributeBlock[@specialized(Long, Int, Double) VD: ClassTag](
    val vids: Array[VertexId],
    val attrs: Array[VD])
  extends Serializable {
  def iterator: Iterator[(VertexId, VD)] =
    (0 until vids.size).iterator.map { i => (vids(i), attrs(i)) }
}

private[graphx] object ShippableVertexPartition {
  /** Construct a `ShippableVertexPartition` from the given vertices without any routing table. */
  def apply[VD: ClassTag](iter: Iterator[(VertexId, VD)]): ShippableVertexPartition[VD] =
    apply(iter, RoutingTablePartition.empty, null.asInstanceOf[VD])

  /**
   * Construct a `ShippableVertexPartition` from the given vertices with the specified routing
   * table, filling in missing vertices mentioned in the routing table using `defaultVal`.
   */
  def apply[VD: ClassTag](
      iter: Iterator[(VertexId, VD)], routingTable: RoutingTablePartition, defaultVal: VD)
    : ShippableVertexPartition[VD] = {
    // The default merge function in IndexedRDDPartition.apply will overwrite earlier values with
    // later ones, so we put the real vertex values last
    val fullIter = routingTable.iterator.map(vid => (vid, defaultVal)) ++ iter
    val p = IndexedRDDPartition(fullIter)
    new ShippableVertexPartition(p.index, p.values, p.mask, routingTable)
  }
}

/**
 * A map from vertex id to vertex attribute that additionally stores edge partition join sites for
 * each vertex attribute, enabling joining with an [[org.apache.spark.graphx.EdgeRDD]].
 */
private[graphx] class ShippableVertexPartition[@specialized(Long, Int, Double) VD](
    val index: Index,
    val values: Array[VD],
    val mask: BitSet,
    val routingTable: RoutingTablePartition)
   (implicit val vTag: ClassTag[VD])
  extends IndexedRDDPartitionBase[VD]
  with IndexedRDDPartitionOps[VD, ShippableVertexPartition] {

  def self: ShippableVertexPartition[VD] = this

  def withIndex(index: Index): ShippableVertexPartition[VD] = {
    new ShippableVertexPartition(index, values, mask, routingTable)
  }

  def withValues[VD2: ClassTag](values: Array[VD2]): ShippableVertexPartition[VD2] = {
    new ShippableVertexPartition(index, values, mask, routingTable)
  }

  def withMask(mask: BitSet): ShippableVertexPartition[VD] = {
    new ShippableVertexPartition(index, values, mask, routingTable)
  }

  /** Return a new ShippableVertexPartition with the specified routing table. */
  def withRoutingTable(routingTable: RoutingTablePartition): ShippableVertexPartition[VD] = {
    new ShippableVertexPartition(index, values, mask, routingTable)
  }

  /**
   * Generate a `VertexAttributeBlock` for each edge partition keyed on the edge partition ID. The
   * `VertexAttributeBlock` contains the vertex attributes from the current partition that are
   * referenced in the specified positions in the edge partition.
   */
  def shipVertexAttributes(
      shipSrc: Boolean, shipDst: Boolean): Iterator[(PartitionID, VertexAttributeBlock[VD])] = {
    Iterator.tabulate(routingTable.numEdgePartitions) { pid =>
      val initialSize = if (shipSrc && shipDst) routingTable.partitionSize(pid) else 64
      val vids = new PrimitiveVector[VertexId](initialSize)
      val attrs = new PrimitiveVector[VD](initialSize)
      var i = 0
      routingTable.foreachWithinEdgePartition(pid, shipSrc, shipDst) { vid =>
        if (isDefined(vid)) {
          vids += vid
          attrs += this(vid)
        }
        i += 1
      }
      (pid, new VertexAttributeBlock(vids.trim().array, attrs.trim().array))
    }
  }

  /**
   * Generate a `VertexId` array for each edge partition keyed on the edge partition ID. The array
   * contains the visible vertex ids from the current partition that are referenced in the edge
   * partition.
   */
  def shipVertexIds(): Iterator[(PartitionID, Array[VertexId])] = {
    Iterator.tabulate(routingTable.numEdgePartitions) { pid =>
      val vids = new PrimitiveVector[VertexId](routingTable.partitionSize(pid))
      var i = 0
      routingTable.foreachWithinEdgePartition(pid, true, true) { vid =>
        if (isDefined(vid)) {
          vids += vid
        }
        i += 1
      }
      (pid, vids.trim().array)
    }
  }
}
