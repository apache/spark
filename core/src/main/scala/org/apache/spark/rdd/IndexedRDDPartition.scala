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

import org.apache.spark.util.collection.BitSet
import org.apache.spark.util.collection.OpenHashSet
import org.apache.spark.util.collection.PrimitiveKeyOpenHashMap

import IndexedRDD.Id
import IndexedRDDPartition.Index

private[spark] object IndexedRDDPartition {
  type Index = OpenHashSet[Id]

  // Same as apply(iter, (a, b) => b)
  def apply[V: ClassTag](iter: Iterator[(Id, V)]): IndexedRDDPartition[V] = {
    val map = new PrimitiveKeyOpenHashMap[Id, V]
    iter.foreach { pair =>
      map(pair._1) = pair._2
    }
    new IndexedRDDPartition(map.keySet, map._values, map.keySet.getBitSet)
  }

  def apply[V: ClassTag](iter: Iterator[(Id, V)], mergeFunc: (V, V) => V)
    : IndexedRDDPartition[V] = {
    val map = new PrimitiveKeyOpenHashMap[Id, V]
    iter.foreach { pair =>
      map.setMerge(pair._1, pair._2, mergeFunc)
    }
    new IndexedRDDPartition(map.keySet, map._values, map.keySet.getBitSet)
  }
}

private[spark] trait IndexedRDDPartitionBase[@specialized(Long, Int, Double) V] {
  def index: Index
  def values: Array[V]
  def mask: BitSet

  val capacity: Int = index.capacity

  def size: Int = mask.cardinality()

  /** Return the value for the given key. */
  def apply(k: Id): V = values(index.getPos(k))

  def isDefined(k: Id): Boolean = {
    val pos = index.getPos(k)
    pos >= 0 && mask.get(pos)
  }

  def iterator: Iterator[(Id, V)] =
    mask.iterator.map(ind => (index.getValue(ind), values(ind)))
}

private[spark] class IndexedRDDPartition[@specialized(Long, Int, Double) V](
    val index: Index,
    val values: Array[V],
    val mask: BitSet)
   (implicit val vTag: ClassTag[V])
  extends IndexedRDDPartitionBase[V]
  with IndexedRDDPartitionOps[V, IndexedRDDPartition] {

  def self: IndexedRDDPartition[V] = this

  def withIndex(index: Index): IndexedRDDPartition[V] = {
    new IndexedRDDPartition(index, values, mask)
  }

  def withValues[V2: ClassTag](values: Array[V2]): IndexedRDDPartition[V2] = {
    new IndexedRDDPartition(index, values, mask)
  }

  def withMask(mask: BitSet): IndexedRDDPartition[V] = {
    new IndexedRDDPartition(index, values, mask)
  }
}
