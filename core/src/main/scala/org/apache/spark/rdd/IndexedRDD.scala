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

package org.apache.spark.rdd

import scala.language.higherKinds
import scala.reflect.{classTag, ClassTag}

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel

import IndexedRDD.Id
import IndexedRDDFunctions.rdd2IndexedRDDFunctions

private[spark] trait IndexedRDDBase[
    @specialized(Long, Int, Double) V,
    P[X] <: IndexedRDDPartitionBase[X] with IndexedRDDPartitionOps[X, P]]
  extends RDD[(Id, V)] {

  def partitionsRDD: RDD[P[V]]
  implicit def pTag[V2]: ClassTag[P[V2]]

  require(partitionsRDD.partitioner.isDefined)

  override val partitioner = partitionsRDD.partitioner

  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  override protected def getPreferredLocations(s: Partition): Seq[String] =
    partitionsRDD.preferredLocations(s)

  override def persist(newLevel: StorageLevel): this.type = {
    partitionsRDD.persist(newLevel)
    this
  }

  override def unpersist(blocking: Boolean = true): this.type = {
    partitionsRDD.unpersist(blocking)
    this
  }

  /** The number of entries in the RDD. */
  override def count(): Long = {
    partitionsRDD.map(_.size).reduce(_ + _)
  }

  /**
   * Provides the `RDD[(Id, V)]` equivalent output.
   */
  override def compute(part: Partition, context: TaskContext): Iterator[(Id, V)] = {
    firstParent[P[V]].iterator(part, context).next.iterator
  }
}

/**
 * Extends RDD[(Id, V)] to provide a key-value store by enforcing key uniqueness and pre-indexing
 * the entries for efficient joins. Two IndexedRDDs with the same index can be joined
 * efficiently. All operations except [[reindex]] preserve the index. To construct an `IndexedRDD`,
 * use the [[org.apache.spark.rdd.IndexedRDD$ IndexedRDD object]].
 *
 * @example Construct a `IndexedRDD` from a plain RDD:
 * {{{
 * // Construct an initial dataset
 * val someData: RDD[(Id, SomeType)] = loadData(someFile)
 * val vset = IndexedRDD(someData)
 * // If there were redundant values in someData we would use a reduceFunc
 * val vset2 = IndexedRDD(someData, reduceFunc)
 * // Finally we can use the IndexedRDD to index another dataset
 * val otherData: RDD[(Id, OtherType)] = loadData(otherFile)
 * val vset3 = vset2.innerJoin(otherData) { (vid, a, b) => b }
 * // Now we can construct very fast joins between the two sets
 * val vset4: IndexedRDD[(SomeType, OtherType)] = vset.leftJoin(vset3)
 * }}}
 *
 * @tparam V the value associated with each entry in the set.
 */
class IndexedRDD[@specialized(Long, Int, Double) V: ClassTag]
    (val partitionsRDD: RDD[IndexedRDDPartition[V]])
  extends RDD[(Id, V)](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD)))
  with IndexedRDDBase[V, IndexedRDDPartition]
  with IndexedRDDOps[V, IndexedRDDPartition, IndexedRDD] {

  def vTag: ClassTag[V] = classTag[V]

  def pTag[V2]: ClassTag[IndexedRDDPartition[V2]] = classTag[IndexedRDDPartition[V2]]

  def withPartitionsRDD[V2: ClassTag](
      partitionsRDD: RDD[IndexedRDDPartition[V2]]): IndexedRDD[V2] = {
    new IndexedRDD(partitionsRDD)
  }

  def self: IndexedRDD[V] = this
}

/**
 * The IndexedRDD singleton is used to construct IndexedRDDs.
 */
object IndexedRDD {
  type Id = Long

  def apply[V: ClassTag](elems: RDD[(Id, V)]): IndexedRDD[V] = {
    IndexedRDD(elems, elems.partitioner.getOrElse(new HashPartitioner(elems.partitions.size)))
  }

  def apply[V: ClassTag](elems: RDD[(Id, V)], partitioner: Partitioner): IndexedRDD[V] = {
    IndexedRDD(elems, partitioner, (a, b) => b)
  }

  def apply[V: ClassTag](
      elems: RDD[(Id, V)], partitioner: Partitioner, mergeValues: (V, V) => V): IndexedRDD[V] = {
    val partitioned: RDD[(Id, V)] = elems.partitionWithSerializer(partitioner)
    val partitions = partitioned.mapPartitions(
      iter => Iterator(IndexedRDDPartition(iter, mergeValues)),
      preservesPartitioning = true)
    new IndexedRDD(partitions)
  }
}
