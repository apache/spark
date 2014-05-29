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

package org.apache.spark.graphx

import scala.reflect.{classTag, ClassTag}

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.storage.StorageLevel

import org.apache.spark.graphx.impl.RoutingTablePartition
import org.apache.spark.graphx.impl.ShippableVertexPartition
import org.apache.spark.graphx.impl.VertexAttributeBlock
import org.apache.spark.graphx.impl.RoutingTableMessageRDDFunctions._
import org.apache.spark.graphx.impl.VertexRDDFunctions._

/**
 * Extends `RDD[(VertexId, VD)]` by ensuring that there is only one entry for each vertex and by
 * pre-indexing the entries for fast, efficient joins. Two VertexRDDs with the same index can be
 * joined efficiently. All operations except [[reindex]] preserve the index. To construct a
 * `VertexRDD`, use the [[org.apache.spark.graphx.VertexRDD$ VertexRDD object]].
 *
 * Additionally, stores routing information to enable joining the vertex attributes with an
 * [[EdgeRDD]].
 *
 * @example Construct a `VertexRDD` from a plain RDD:
 * {{{
 * // Construct an initial vertex set
 * val someData: RDD[(VertexId, SomeType)] = loadData(someFile)
 * val vset = VertexRDD(someData)
 * // If there were redundant values in someData we would use a reduceFunc
 * val vset2 = VertexRDD(someData, reduceFunc)
 * // Finally we can use the VertexRDD to index another dataset
 * val otherData: RDD[(VertexId, OtherType)] = loadData(otherFile)
 * val vset3 = vset2.innerJoin(otherData) { (vid, a, b) => b }
 * // Now we can construct very fast joins between the two sets
 * val vset4: VertexRDD[(SomeType, OtherType)] = vset.leftJoin(vset3)
 * }}}
 *
 * @tparam VD the vertex attribute associated with each vertex in the set.
 */
class VertexRDD[@specialized(Long, Int, Double) VD](
    val partitionsRDD: RDD[ShippableVertexPartition[VD]]
    val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
   (implicit val vTag: ClassTag[VD])
  extends RDD[(VertexId, VD)](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD)))
  with IndexedRDDBase[VD, ShippableVertexPartition]
  with IndexedRDDOps[VD, ShippableVertexPartition, VertexRDD] {

  def pTag[VD2]: ClassTag[ShippableVertexPartition[VD2]] =
    classTag[ShippableVertexPartition[VD2]]

  override def setName(_name: String): this.type = {
    if (partitionsRDD.name != null) {
      partitionsRDD.setName(partitionsRDD.name + ", " + _name)
    } else {
      partitionsRDD.setName(_name)
    }
    this
  }
  setName("VertexRDD")

  /** Persists the vertex partitions at `targetStorageLevel`, which defaults to MEMORY_ONLY. */
  override def cache(): this.type = {
    partitionsRDD.persist(targetStorageLevel)
    this
  }

  def withPartitionsRDD[VD2: ClassTag](
      partitionsRDD: RDD[ShippableVertexPartition[VD2]]): VertexRDD[VD2] = {
    new VertexRDD(partitionsRDD, targetStorageLevel)
  }

  /**
   * Changes the target storage level while preserving all other properties of the
   * VertexRDD. Operations on the returned VertexRDD will preserve this storage level.
   *
   * This does not actually trigger a cache; to do this, call
   * [[org.apache.spark.graphx.VertexRDD#cache]] on the returned VertexRDD.
   */
  private[graphx] def withTargetStorageLevel(
      targetStorageLevel: StorageLevel): VertexRDD[VD] = {
    new VertexRDD(this.partitionsRDD, targetStorageLevel)
  }

  def self: VertexRDD[VD] = this

  /**
   * Returns a new `VertexRDD` reflecting a reversal of all edge directions in the corresponding
   * [[EdgeRDD]].
   */
  def reverseRoutingTables(): VertexRDD[VD] =
    this.mapIndexedRDDPartitions(vPart => vPart.withRoutingTable(vPart.routingTable.reverse))

  /** Prepares this VertexRDD for efficient joins with the given EdgeRDD. */
  def withEdges(edges: EdgeRDD[_, _]): VertexRDD[VD] = {
    val routingTables = VertexRDD.createRoutingTables(edges, this.partitioner.get)
    val vertexPartitions = partitionsRDD.zipPartitions(routingTables, true) {
      (partIter, routingTableIter) =>
        val routingTable =
          if (routingTableIter.hasNext) routingTableIter.next() else RoutingTablePartition.empty
        partIter.map(_.withRoutingTable(routingTable))
    }
    this.withPartitionsRDD(vertexPartitions)
  }

  /** Generates an RDD of vertex attributes suitable for shipping to the edge partitions. */
  private[graphx] def shipVertexAttributes(
      shipSrc: Boolean, shipDst: Boolean): RDD[(PartitionID, VertexAttributeBlock[VD])] = {
    partitionsRDD.mapPartitions(_.flatMap(_.shipVertexAttributes(shipSrc, shipDst)))
  }

  /** Generates an RDD of vertex IDs suitable for shipping to the edge partitions. */
  private[graphx] def shipVertexIds(): RDD[(PartitionID, Array[VertexId])] = {
    partitionsRDD.mapPartitions(_.flatMap(_.shipVertexIds()))
  }

} // end of VertexRDD


/**
 * The VertexRDD singleton is used to construct VertexRDDs.
 */
object VertexRDD {

  /**
   * Constructs a standalone `VertexRDD` (one that is not set up for efficient joins with an
   * [[EdgeRDD]]) from an RDD of vertex-attribute pairs. Duplicate entries are removed arbitrarily.
   *
   * @tparam VD the vertex attribute type
   *
   * @param vertices the collection of vertex-attribute pairs
   */
  def apply[VD: ClassTag](vertices: RDD[(VertexId, VD)]): VertexRDD[VD] = {
    val vPartitioned: RDD[(VertexId, VD)] = vertices.partitioner match {
      case Some(p) => vertices
      case None => vertices.copartitionWithVertices(new HashPartitioner(vertices.partitions.size))
    }
    val vertexPartitions = vPartitioned.mapPartitions(
      iter => Iterator(ShippableVertexPartition(iter)),
      preservesPartitioning = true)
    new VertexRDD(vertexPartitions)
  }

  /**
   * Constructs a `VertexRDD` from an RDD of vertex-attribute pairs. Duplicate vertex entries are
   * removed arbitrarily. The resulting `VertexRDD` will be joinable with `edges`, and any missing
   * vertices referred to by `edges` will be created with the attribute `defaultVal`.
   *
   * @tparam VD the vertex attribute type
   *
   * @param vertices the collection of vertex-attribute pairs
   * @param edges the [[EdgeRDD]] that these vertices may be joined with
   * @param defaultVal the vertex attribute to use when creating missing vertices
   */
  def apply[VD: ClassTag](
      vertices: RDD[(VertexId, VD)], edges: EdgeRDD[_, _], defaultVal: VD): VertexRDD[VD] = {
    VertexRDD(vertices, edges, defaultVal, (a, b) => b)
  }

  /**
   * Constructs a `VertexRDD` from an RDD of vertex-attribute pairs. Duplicate vertex entries are
   * merged using `mergeFunc`. The resulting `VertexRDD` will be joinable with `edges`, and any
   * missing vertices referred to by `edges` will be created with the attribute `defaultVal`.
   *
   * @tparam VD the vertex attribute type
   *
   * @param vertices the collection of vertex-attribute pairs
   * @param edges the [[EdgeRDD]] that these vertices may be joined with
   * @param defaultVal the vertex attribute to use when creating missing vertices
   * @param mergeFunc the commutative, associative duplicate vertex attribute merge function
   */
  def apply[VD: ClassTag](
      vertices: RDD[(VertexId, VD)], edges: EdgeRDD[_, _], defaultVal: VD, mergeFunc: (VD, VD) => VD
    ): VertexRDD[VD] = {
    val vPartitioned: RDD[(VertexId, VD)] = vertices.partitioner match {
      case Some(p) => vertices
      case None => vertices.copartitionWithVertices(new HashPartitioner(vertices.partitions.size))
    }
    val routingTables = createRoutingTables(edges, vPartitioned.partitioner.get)
    val vertexPartitions = vPartitioned.zipPartitions(routingTables, preservesPartitioning = true) {
      (vertexIter, routingTableIter) =>
        val routingTable =
          if (routingTableIter.hasNext) routingTableIter.next() else RoutingTablePartition.empty
        Iterator(ShippableVertexPartition(vertexIter, routingTable, defaultVal))
    }
    new VertexRDD(vertexPartitions)
  }

  /**
   * Constructs a `VertexRDD` containing all vertices referred to in `edges`. The vertices will be
   * created with the attribute `defaultVal`. The resulting `VertexRDD` will be joinable with
   * `edges`.
   *
   * @tparam VD the vertex attribute type
   *
   * @param edges the [[EdgeRDD]] referring to the vertices to create
   * @param numPartitions the desired number of partitions for the resulting `VertexRDD`
   * @param defaultVal the vertex attribute to use when creating missing vertices
   */
  def fromEdges[VD: ClassTag](
      edges: EdgeRDD[_, _], numPartitions: Int, defaultVal: VD): VertexRDD[VD] = {
    val routingTables = createRoutingTables(edges, new HashPartitioner(numPartitions))
    val vertexPartitions = routingTables.mapPartitions({ routingTableIter =>
      val routingTable =
        if (routingTableIter.hasNext) routingTableIter.next() else RoutingTablePartition.empty
      Iterator(ShippableVertexPartition(Iterator.empty, routingTable, defaultVal))
    }, preservesPartitioning = true)
    new VertexRDD(vertexPartitions)
  }

  private def createRoutingTables(
      edges: EdgeRDD[_, _], vertexPartitioner: Partitioner): RDD[RoutingTablePartition] = {
    // Determine which vertices each edge partition needs by creating a mapping from vid to pid.
    val vid2pid = edges.partitionsRDD.mapPartitions(_.flatMap(
      Function.tupled(RoutingTablePartition.edgePartitionToMsgs)))
      .setName("VertexRDD.createRoutingTables - vid2pid (aggregation)")

    val numEdgePartitions = edges.partitions.size
    vid2pid.copartitionWithVertices(vertexPartitioner).mapPartitions(
      iter => Iterator(RoutingTablePartition.fromMsgs(numEdgePartitions, iter)),
      preservesPartitioning = true)
  }
}
