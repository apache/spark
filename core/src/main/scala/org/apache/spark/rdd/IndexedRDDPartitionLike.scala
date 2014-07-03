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

import org.apache.spark.util.collection.ImmutableBitSet
import org.apache.spark.util.collection.ImmutableLongOpenHashSet
import org.apache.spark.util.collection.ImmutableVector
import org.apache.spark.util.collection.PrimitiveKeyOpenHashMap

import IndexedRDD.Id
import IndexedRDDPartition.Index

/**
 * Contains members that are shared among all variants of IndexedRDDPartition (e.g.,
 * IndexedRDDPartition, ShippableVertexPartition).
 *
 * @tparam V the type of the values stored in the IndexedRDDPartition
 */
private[spark] trait IndexedRDDPartitionLike[@specialized(Long, Int, Double) V]
  extends Serializable {

  def index: Index
  def values: ImmutableVector[V]
  def mask: ImmutableBitSet

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
