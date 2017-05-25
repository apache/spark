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
import scala.reflect.ClassTag

import org.apache.spark.util.collection.ImmutableBitSet
import org.apache.spark.util.collection.ImmutableLongOpenHashSet
import org.apache.spark.util.collection.ImmutableVector
import org.apache.spark.util.collection.PrimitiveKeyOpenHashMap

import IndexedRDD.Id
import IndexedRDDPartition.Index

/**
 * An immutable map of key-value `(Id, V)` pairs that enforces key uniqueness and pre-indexes the
 * entries for efficient joins and point lookups/updates. Two IndexedRDDPartitions with the same
 * index can be joined efficiently. All operations except [[reindex]] preserve the index. To
 * construct an `IndexedRDDPartition`, use the [[org.apache.spark.rdd.IndexedRDDPartition$
 * IndexedRDDPartition object]].
 *
 * @tparam V the value associated with each entry in the set.
 */
private[spark] class IndexedRDDPartition[@specialized(Long, Int, Double) V](
    val index: Index,
    val values: ImmutableVector[V],
    val mask: ImmutableBitSet)
   (implicit val vTag: ClassTag[V])
  extends IndexedRDDPartitionLike[V, IndexedRDDPartition] {

  def self: IndexedRDDPartition[V] = this

  def withIndex(index: Index): IndexedRDDPartition[V] = {
    new IndexedRDDPartition(index, values, mask)
  }

  def withValues[V2: ClassTag](values: ImmutableVector[V2]): IndexedRDDPartition[V2] = {
    new IndexedRDDPartition(index, values, mask)
  }

  def withMask(mask: ImmutableBitSet): IndexedRDDPartition[V] = {
    new IndexedRDDPartition(index, values, mask)
  }
}

private[spark] object IndexedRDDPartition {
  type Index = ImmutableLongOpenHashSet

  /**
   * Constructs an IndexedRDDPartition from an iterator of pairs, merging duplicate keys
   * arbitrarily.
   */
  def apply[V: ClassTag](iter: Iterator[(Id, V)]): IndexedRDDPartition[V] = {
    val map = new PrimitiveKeyOpenHashMap[Id, V]
    iter.foreach { pair =>
      map(pair._1) = pair._2
    }
    new IndexedRDDPartition(
      ImmutableLongOpenHashSet.fromLongOpenHashSet(map.keySet),
      ImmutableVector.fromArray(map.values),
      map.keySet.getBitSet.toImmutableBitSet)
  }

  /** Constructs an IndexedRDDPartition from an iterator of pairs. */
  def apply[V: ClassTag](iter: Iterator[(Id, V)], mergeFunc: (V, V) => V)
    : IndexedRDDPartition[V] = {
    val map = new PrimitiveKeyOpenHashMap[Id, V]
    iter.foreach { pair =>
      map.setMerge(pair._1, pair._2, mergeFunc)
    }
    new IndexedRDDPartition(
      ImmutableLongOpenHashSet.fromLongOpenHashSet(map.keySet),
      ImmutableVector.fromArray(map.values),
      map.keySet.getBitSet.toImmutableBitSet)
  }
}
