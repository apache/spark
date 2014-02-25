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

import org.apache.spark.{OneToOneDependency, Partition, Partitioner, TaskContext}
import org.apache.spark.graphx.impl.EdgePartition
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * `EdgeRDD[ED]` extends `RDD[Edge[ED]]` by storing the edges in columnar format on each partition
 * for performance.
 */
class EdgeRDD[@specialized ED: ClassTag](
    val partitionsRDD: RDD[(PartitionID, EdgePartition[ED])])
  extends RDD[Edge[ED]](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {

  partitionsRDD.setName("EdgeRDD")

  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  /**
   * If `partitionsRDD` already has a partitioner, use it. Otherwise assume that the
   * [[PartitionID]]s in `partitionsRDD` correspond to the actual partitions and create a new
   * partitioner that allows co-partitioning with `partitionsRDD`.
   */
  override val partitioner =
    partitionsRDD.partitioner.orElse(Some(Partitioner.defaultPartitioner(partitionsRDD)))

  override def compute(part: Partition, context: TaskContext): Iterator[Edge[ED]] = {
    firstParent[(PartitionID, EdgePartition[ED])].iterator(part, context).next._2.iterator
  }

  override def collect(): Array[Edge[ED]] = this.map(_.copy()).collect()

  override def persist(newLevel: StorageLevel): EdgeRDD[ED] = {
    partitionsRDD.persist(newLevel)
    this
  }

  /** Persist this RDD with the default storage level (`MEMORY_ONLY`). */
  override def persist(): EdgeRDD[ED] = persist(StorageLevel.MEMORY_ONLY)

  /** Persist this RDD with the default storage level (`MEMORY_ONLY`). */
  override def cache(): EdgeRDD[ED] = persist()

  override def unpersist(blocking: Boolean = true): EdgeRDD[ED] = {
    partitionsRDD.unpersist(blocking)
    this
  }

  private[graphx] def mapEdgePartitions[ED2: ClassTag](
      f: (PartitionID, EdgePartition[ED]) => EdgePartition[ED2]): EdgeRDD[ED2] = {
    new EdgeRDD[ED2](partitionsRDD.mapPartitions({ iter =>
      val (pid, ep) = iter.next()
      Iterator(Tuple2(pid, f(pid, ep)))
    }, preservesPartitioning = true))
  }

  /**
   * Map the values in an edge partitioning preserving the structure but changing the values.
   *
   * @tparam ED2 the new edge value type
   * @param f the function from an edge to a new edge value
   * @return a new EdgeRDD containing the new edge values
   */
  def mapValues[ED2: ClassTag](f: Edge[ED] => ED2): EdgeRDD[ED2] =
    mapEdgePartitions((pid, part) => part.map(f))

  /**
   * Reverse all the edges in this RDD.
   *
   * @return a new EdgeRDD containing all the edges reversed
   */
  def reverse: EdgeRDD[ED] = mapEdgePartitions((pid, part) => part.reverse)

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
      (other: EdgeRDD[ED2])
      (f: (VertexId, VertexId, ED, ED2) => ED3): EdgeRDD[ED3] = {
    val ed2Tag = classTag[ED2]
    val ed3Tag = classTag[ED3]
    new EdgeRDD[ED3](partitionsRDD.zipPartitions(other.partitionsRDD, true) {
      (thisIter, otherIter) =>
        val (pid, thisEPart) = thisIter.next()
        val (_, otherEPart) = otherIter.next()
        Iterator(Tuple2(pid, thisEPart.innerJoin(otherEPart)(f)(ed2Tag, ed3Tag)))
    })
  }

  private[graphx] def collectVertexIds(): RDD[VertexId] = {
    partitionsRDD.flatMap { case (_, p) => Array.concat(p.srcIds, p.dstIds) }
  }
}
