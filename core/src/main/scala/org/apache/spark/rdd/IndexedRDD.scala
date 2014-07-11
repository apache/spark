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
import org.apache.spark.annotation.Experimental
import org.apache.spark.storage.StorageLevel

import IndexedRDD.Id

/**
 * :: Experimental ::
 * An RDD of key-value `(Id, V)` pairs that enforces key uniqueness and pre-indexes the entries for
 * efficient joins and point lookups/updates. Two IndexedRDDs with the same index can be joined
 * efficiently. All operations except [[reindex]] preserve the index. To construct an `IndexedRDD`,
 * use the [[org.apache.spark.rdd.IndexedRDD$ IndexedRDD object]].
 *
 * @tparam V the value associated with each entry in the set.
 */
@Experimental
class IndexedRDD[@specialized(Long, Int, Double) V: ClassTag]
    (val partitionsRDD: RDD[IndexedRDDPartition[V]])
  extends RDD[(Id, V)](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD)))
  with IndexedRDDLike[V, IndexedRDDPartition, IndexedRDD] {

  override protected def vTag: ClassTag[V] = classTag[V]

  override protected def pTag[V2]: ClassTag[IndexedRDDPartition[V2]] =
    classTag[IndexedRDDPartition[V2]]

  override protected def self: IndexedRDD[V] = this

  def withPartitionsRDD[V2: ClassTag](
      partitionsRDD: RDD[IndexedRDDPartition[V2]]): IndexedRDD[V2] = {
    new IndexedRDD(partitionsRDD)
  }
}

object IndexedRDD {
  /** The key type of IndexedRDDs. */
  type Id = Long

  /**
   * Constructs an IndexedRDD from an RDD of pairs, partitioning keys using a hash partitioner,
   * preserving the number of partitions of `elems`, and merging duplicate keys arbitrarily.
   */
  def apply[V: ClassTag](elems: RDD[(Id, V)]): IndexedRDD[V] = {
    IndexedRDD(elems, elems.partitioner.getOrElse(new HashPartitioner(elems.partitions.size)))
  }

  /** Constructs an IndexedRDD from an RDD of pairs, merging duplicate keys arbitrarily. */
  def apply[V: ClassTag](elems: RDD[(Id, V)], partitioner: Partitioner): IndexedRDD[V] = {
    IndexedRDD(elems, partitioner, (a, b) => b)
  }

  /** Constructs an IndexedRDD from an RDD of pairs. */
  def apply[V: ClassTag](
      elems: RDD[(Id, V)], partitioner: Partitioner, mergeValues: (V, V) => V): IndexedRDD[V] = {
    val partitioned: RDD[(Id, V)] = elems.partitionBy(partitioner)
    val partitions = partitioned.mapPartitions(
      iter => Iterator(IndexedRDDPartition(iter, mergeValues)),
      preservesPartitioning = true)
    new IndexedRDD(partitions)
  }
}
