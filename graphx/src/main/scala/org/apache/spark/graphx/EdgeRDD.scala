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
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import org.apache.spark.graphx.impl.EdgePartition
import org.apache.spark.graphx.impl.EdgePartitionBuilder

/**
 * `EdgeRDD[ED, VD]` extends `RDD[Edge[ED]]` by storing the edges in columnar format on each
 * partition for performance. It may additionally store the vertex attributes associated with each
 * edge to provide the triplet view. Shipping of the vertex attributes is managed by
 * `impl.ReplicatedVertexView`.
 */
class EdgeRDD[@specialized ED: ClassTag, VD: ClassTag](
    val partitionsRDD: RDD[(PartitionID, EdgePartition[ED, VD])],
    val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
  extends RDD[Edge[ED]](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {

  partitionsRDD.setName("EdgeRDD")

  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  /**
   * If `partitionsRDD` already has a partitioner, use it. Otherwise assume that the
   * [[PartitionID]]s in `partitionsRDD` correspond to the actual partitions and create a new
   * partitioner that allows co-partitioning with `partitionsRDD`.
   */
  override val partitioner =
    partitionsRDD.partitioner.orElse(Some(new HashPartitioner(partitionsRDD.partitions.size)))

  override def compute(part: Partition, context: TaskContext): Iterator[Edge[ED]] = {
    val p = firstParent[(PartitionID, EdgePartition[ED, VD])].iterator(part, context)
    if (p.hasNext) {
      p.next._2.iterator.map(_.copy())
    } else {
      Iterator.empty
    }
  }

  override def collect(): Array[Edge[ED]] = this.map(_.copy()).collect()

  /**
   * Persists the edge partitions at the specified storage level, ignoring any existing target
   * storage level.
   */
  override def persist(newLevel: StorageLevel): this.type = {
    partitionsRDD.persist(newLevel)
    this
  }

  override def unpersist(blocking: Boolean = true): this.type = {
    partitionsRDD.unpersist(blocking)
    this
  }

  /** Persists the vertex partitions using `targetStorageLevel`, which defaults to MEMORY_ONLY. */
  override def cache(): this.type = {
    partitionsRDD.persist(targetStorageLevel)
    this
  }

  private[graphx] def mapEdgePartitions[ED2: ClassTag, VD2: ClassTag](
      f: (PartitionID, EdgePartition[ED, VD]) => EdgePartition[ED2, VD2]): EdgeRDD[ED2, VD2] = {
    this.withPartitionsRDD[ED2, VD2](partitionsRDD.mapPartitions({ iter =>
      if (iter.hasNext) {
        val (pid, ep) = iter.next()
        Iterator(Tuple2(pid, f(pid, ep)))
      } else {
        Iterator.empty
      }
    }, preservesPartitioning = true))
  }

  /**
   * Map the values in an edge partitioning preserving the structure but changing the values.
   *
   * @tparam ED2 the new edge value type
   * @param f the function from an edge to a new edge value
   * @return a new EdgeRDD containing the new edge values
   */
  def mapValues[ED2: ClassTag](f: Edge[ED] => ED2): EdgeRDD[ED2, VD] =
    mapEdgePartitions((pid, part) => part.map(f))

  /**
   * Reverse all the edges in this RDD.
   *
   * @return a new EdgeRDD containing all the edges reversed
   */
  def reverse: EdgeRDD[ED, VD] = mapEdgePartitions((pid, part) => part.reverse)

  /** Removes all edges but those matching `epred` and where both vertices match `vpred`. */
  def filter(
      epred: EdgeTriplet[VD, ED] => Boolean,
      vpred: (VertexId, VD) => Boolean): EdgeRDD[ED, VD] = {
    mapEdgePartitions((pid, part) => part.filter(epred, vpred))
  }

  /**
   * Inner joins this EdgeRDD with another EdgeRDD, assuming both are partitioned using the same
   * [[PartitionStrategy]].
   *
   * @param other the EdgeRDD to join with
   * @param f the join function applied to corresponding values of `this` and `other`
   * @return a new EdgeRDD containing only edges that appear in both `this` and `other`,
   *         with values supplied by `f`
   */
  def innerJoin[ED2: ClassTag, ED3: ClassTag]
      (other: EdgeRDD[ED2, _])
      (f: (VertexId, VertexId, ED, ED2) => ED3): EdgeRDD[ED3, VD] = {
    val ed2Tag = classTag[ED2]
    val ed3Tag = classTag[ED3]
    this.withPartitionsRDD[ED3, VD](partitionsRDD.zipPartitions(other.partitionsRDD, true) {
      (thisIter, otherIter) =>
        val (pid, thisEPart) = thisIter.next()
        val (_, otherEPart) = otherIter.next()
        Iterator(Tuple2(pid, thisEPart.innerJoin(otherEPart)(f)(ed2Tag, ed3Tag)))
    })
  }

  /** Replaces the vertex partitions while preserving all other properties of the VertexRDD. */
  private[graphx] def withPartitionsRDD[ED2: ClassTag, VD2: ClassTag](
      partitionsRDD: RDD[(PartitionID, EdgePartition[ED2, VD2])]): EdgeRDD[ED2, VD2] = {
    new EdgeRDD(partitionsRDD, this.targetStorageLevel)
  }

  /**
   * Changes the target storage level while preserving all other properties of the
   * EdgeRDD. Operations on the returned EdgeRDD will preserve this storage level.
   *
   * This does not actually trigger a cache; to do this, call
   * [[org.apache.spark.graphx.EdgeRDD#cache]] on the returned EdgeRDD.
   */
  private[graphx] def withTargetStorageLevel(
      targetStorageLevel: StorageLevel): EdgeRDD[ED, VD] = {
    new EdgeRDD(this.partitionsRDD, targetStorageLevel)
  }

}

object EdgeRDD {
  /**
   * Creates an EdgeRDD from a set of edges.
   *
   * @tparam ED the edge attribute type
   * @tparam VD the type of the vertex attributes that may be joined with the returned EdgeRDD
   */
  def fromEdges[ED: ClassTag, VD: ClassTag](edges: RDD[Edge[ED]]): EdgeRDD[ED, VD] = {
    val edgePartitions = edges.mapPartitionsWithIndex { (pid, iter) =>
      val builder = new EdgePartitionBuilder[ED, VD]
      iter.foreach { e =>
        builder.add(e.srcId, e.dstId, e.attr)
      }
      Iterator((pid, builder.toEdgePartition))
    }
    EdgeRDD.fromEdgePartitions(edgePartitions)
  }

  /**
   * Creates an EdgeRDD from already-constructed edge partitions.
   *
   * @tparam ED the edge attribute type
   * @tparam VD the type of the vertex attributes that may be joined with the returned EdgeRDD
   */
  def fromEdgePartitions[ED: ClassTag, VD: ClassTag](
      edgePartitions: RDD[(Int, EdgePartition[ED, VD])]): EdgeRDD[ED, VD] = {
    new EdgeRDD(edgePartitions)
  }
}
