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

import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.storage.StorageLevel

import org.apache.spark.graphx.impl.RoutingTablePartition
import org.apache.spark.graphx.impl.ShippableVertexPartition
import org.apache.spark.graphx.impl.VertexAttributeBlock
import org.apache.spark.graphx.impl.VertexRDDImpl

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
abstract class VertexRDD[VD](
    @transient sc: SparkContext,
    @transient deps: Seq[Dependency[_]]) extends RDD[(VertexId, VD)](sc, deps) {

  implicit protected def vdTag: ClassTag[VD]

  private[graphx] def partitionsRDD: RDD[ShippableVertexPartition[VD]]

  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  /**
   * Provides the `RDD[(VertexId, VD)]` equivalent output.
   */
  override def compute(part: Partition, context: TaskContext): Iterator[(VertexId, VD)] = {
    firstParent[ShippableVertexPartition[VD]].iterator(part, context).next.iterator
  }

  /**
   * Construct a new VertexRDD that is indexed by only the visible vertices. The resulting
   * VertexRDD will be based on a different index and can no longer be quickly joined with this
   * RDD.
   */
  def reindex(): VertexRDD[VD]

  /**
   * Applies a function to each `VertexPartition` of this RDD and returns a new VertexRDD.
   */
  private[graphx] def mapVertexPartitions[VD2: ClassTag](
      f: ShippableVertexPartition[VD] => ShippableVertexPartition[VD2])
    : VertexRDD[VD2]

  /**
   * Restricts the vertex set to the set of vertices satisfying the given predicate. This operation
   * preserves the index for efficient joins with the original RDD, and it sets bits in the bitmask
   * rather than allocating new memory.
   *
   * It is declared and defined here to allow refining the return type from `RDD[(VertexId, VD)]` to
   * `VertexRDD[VD]`.
   *
   * @param pred the user defined predicate, which takes a tuple to conform to the
   * `RDD[(VertexId, VD)]` interface
   */
  override def filter(pred: Tuple2[VertexId, VD] => Boolean): VertexRDD[VD] =
    this.mapVertexPartitions(_.filter(Function.untupled(pred)))

  /**
   * Maps each vertex attribute, preserving the index.
   *
   * @tparam VD2 the type returned by the map function
   *
   * @param f the function applied to each value in the RDD
   * @return a new VertexRDD with values obtained by applying `f` to each of the entries in the
   * original VertexRDD
   */
  def mapValues[VD2: ClassTag](f: VD => VD2): VertexRDD[VD2]

  /**
   * Maps each vertex attribute, additionally supplying the vertex ID.
   *
   * @tparam VD2 the type returned by the map function
   *
   * @param f the function applied to each ID-value pair in the RDD
   * @return a new VertexRDD with values obtained by applying `f` to each of the entries in the
   * original VertexRDD.  The resulting VertexRDD retains the same index.
   */
  def mapValues[VD2: ClassTag](f: (VertexId, VD) => VD2): VertexRDD[VD2]

  /**
   * Hides vertices that are the same between `this` and `other`; for vertices that are different,
   * keeps the values from `other`.
   */
  def diff(other: VertexRDD[VD]): VertexRDD[VD]

  /**
   * Left joins this RDD with another VertexRDD with the same index. This function will fail if
   * both VertexRDDs do not share the same index. The resulting vertex set contains an entry for
   * each vertex in `this`.
   * If `other` is missing any vertex in this VertexRDD, `f` is passed `None`.
   *
   * @tparam VD2 the attribute type of the other VertexRDD
   * @tparam VD3 the attribute type of the resulting VertexRDD
   *
   * @param other the other VertexRDD with which to join.
   * @param f the function mapping a vertex id and its attributes in this and the other vertex set
   * to a new vertex attribute.
   * @return a VertexRDD containing the results of `f`
   */
  def leftZipJoin[VD2: ClassTag, VD3: ClassTag]
      (other: VertexRDD[VD2])(f: (VertexId, VD, Option[VD2]) => VD3): VertexRDD[VD3]

  /**
   * Left joins this VertexRDD with an RDD containing vertex attribute pairs. If the other RDD is
   * backed by a VertexRDD with the same index then the efficient [[leftZipJoin]] implementation is
   * used. The resulting VertexRDD contains an entry for each vertex in `this`. If `other` is
   * missing any vertex in this VertexRDD, `f` is passed `None`. If there are duplicates,
   * the vertex is picked arbitrarily.
   *
   * @tparam VD2 the attribute type of the other VertexRDD
   * @tparam VD3 the attribute type of the resulting VertexRDD
   *
   * @param other the other VertexRDD with which to join
   * @param f the function mapping a vertex id and its attributes in this and the other vertex set
   * to a new vertex attribute.
   * @return a VertexRDD containing all the vertices in this VertexRDD with the attributes emitted
   * by `f`.
   */
  def leftJoin[VD2: ClassTag, VD3: ClassTag]
      (other: RDD[(VertexId, VD2)])
      (f: (VertexId, VD, Option[VD2]) => VD3)
    : VertexRDD[VD3]

  /**
   * Efficiently inner joins this VertexRDD with another VertexRDD sharing the same index. See
   * [[innerJoin]] for the behavior of the join.
   */
  def innerZipJoin[U: ClassTag, VD2: ClassTag](other: VertexRDD[U])
      (f: (VertexId, VD, U) => VD2): VertexRDD[VD2]

  /**
   * Inner joins this VertexRDD with an RDD containing vertex attribute pairs. If the other RDD is
   * backed by a VertexRDD with the same index then the efficient [[innerZipJoin]] implementation
   * is used.
   *
   * @param other an RDD containing vertices to join. If there are multiple entries for the same
   * vertex, one is picked arbitrarily. Use [[aggregateUsingIndex]] to merge multiple entries.
   * @param f the join function applied to corresponding values of `this` and `other`
   * @return a VertexRDD co-indexed with `this`, containing only vertices that appear in both
   *         `this` and `other`, with values supplied by `f`
   */
  def innerJoin[U: ClassTag, VD2: ClassTag](other: RDD[(VertexId, U)])
      (f: (VertexId, VD, U) => VD2): VertexRDD[VD2]

  /**
   * Aggregates vertices in `messages` that have the same ids using `reduceFunc`, returning a
   * VertexRDD co-indexed with `this`.
   *
   * @param messages an RDD containing messages to aggregate, where each message is a pair of its
   * target vertex ID and the message data
   * @param reduceFunc the associative aggregation function for merging messages to the same vertex
   * @return a VertexRDD co-indexed with `this`, containing only vertices that received messages.
   * For those vertices, their values are the result of applying `reduceFunc` to all received
   * messages.
   */
  def aggregateUsingIndex[VD2: ClassTag](
      messages: RDD[(VertexId, VD2)], reduceFunc: (VD2, VD2) => VD2): VertexRDD[VD2]

  /**
   * Returns a new `VertexRDD` reflecting a reversal of all edge directions in the corresponding
   * [[EdgeRDD]].
   */
  def reverseRoutingTables(): VertexRDD[VD]

  /** Prepares this VertexRDD for efficient joins with the given EdgeRDD. */
  def withEdges(edges: EdgeRDD[_]): VertexRDD[VD]

  /** Replaces the vertex partitions while preserving all other properties of the VertexRDD. */
  private[graphx] def withPartitionsRDD[VD2: ClassTag](
      partitionsRDD: RDD[ShippableVertexPartition[VD2]]): VertexRDD[VD2]

  /**
   * Changes the target storage level while preserving all other properties of the
   * VertexRDD. Operations on the returned VertexRDD will preserve this storage level.
   *
   * This does not actually trigger a cache; to do this, call
   * [[org.apache.spark.graphx.VertexRDD#cache]] on the returned VertexRDD.
   */
  private[graphx] def withTargetStorageLevel(
      targetStorageLevel: StorageLevel): VertexRDD[VD]

  /** Generates an RDD of vertex attributes suitable for shipping to the edge partitions. */
  private[graphx] def shipVertexAttributes(
      shipSrc: Boolean, shipDst: Boolean): RDD[(PartitionID, VertexAttributeBlock[VD])]

  /** Generates an RDD of vertex IDs suitable for shipping to the edge partitions. */
  private[graphx] def shipVertexIds(): RDD[(PartitionID, Array[VertexId])]

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
      case None => vertices.partitionBy(new HashPartitioner(vertices.partitions.size))
    }
    val vertexPartitions = vPartitioned.mapPartitions(
      iter => Iterator(ShippableVertexPartition(iter)),
      preservesPartitioning = true)
    new VertexRDDImpl(vertexPartitions)
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
      vertices: RDD[(VertexId, VD)], edges: EdgeRDD[_], defaultVal: VD): VertexRDD[VD] = {
    VertexRDD(vertices, edges, defaultVal, (a, b) => a)
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
      vertices: RDD[(VertexId, VD)], edges: EdgeRDD[_], defaultVal: VD, mergeFunc: (VD, VD) => VD
    ): VertexRDD[VD] = {
    val vPartitioned: RDD[(VertexId, VD)] = vertices.partitioner match {
      case Some(p) => vertices
      case None => vertices.partitionBy(new HashPartitioner(vertices.partitions.size))
    }
    val routingTables = createRoutingTables(edges, vPartitioned.partitioner.get)
    val vertexPartitions = vPartitioned.zipPartitions(routingTables, preservesPartitioning = true) {
      (vertexIter, routingTableIter) =>
        val routingTable =
          if (routingTableIter.hasNext) routingTableIter.next() else RoutingTablePartition.empty
        Iterator(ShippableVertexPartition(vertexIter, routingTable, defaultVal, mergeFunc))
    }
    new VertexRDDImpl(vertexPartitions)
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
      edges: EdgeRDD[_], numPartitions: Int, defaultVal: VD): VertexRDD[VD] = {
    val routingTables = createRoutingTables(edges, new HashPartitioner(numPartitions))
    val vertexPartitions = routingTables.mapPartitions({ routingTableIter =>
      val routingTable =
        if (routingTableIter.hasNext) routingTableIter.next() else RoutingTablePartition.empty
      Iterator(ShippableVertexPartition(Iterator.empty, routingTable, defaultVal))
    }, preservesPartitioning = true)
    new VertexRDDImpl(vertexPartitions)
  }

  private[graphx] def createRoutingTables(
      edges: EdgeRDD[_], vertexPartitioner: Partitioner): RDD[RoutingTablePartition] = {
    // Determine which vertices each edge partition needs by creating a mapping from vid to pid.
    val vid2pid = edges.partitionsRDD.mapPartitions(_.flatMap(
      Function.tupled(RoutingTablePartition.edgePartitionToMsgs)))
      .setName("VertexRDD.createRoutingTables - vid2pid (aggregation)")

    val numEdgePartitions = edges.partitions.size
    vid2pid.partitionBy(vertexPartitioner).mapPartitions(
      iter => Iterator(RoutingTablePartition.fromMsgs(numEdgePartitions, iter)),
      preservesPartitioning = true)
  }
}
