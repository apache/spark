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

import org.apache.spark.Logging
import org.apache.spark.util.collection.BitSet
import org.apache.spark.util.collection.PrimitiveKeyOpenHashMap

import IndexedRDD.Id
import IndexedRDDPartition.Index

private[spark] trait IndexedRDDPartitionOps[V, Self[X] <: IndexedRDDPartitionBase[X] with IndexedRDDPartitionOps[X, Self]]
    extends Logging {

  def self: Self[V]

  implicit def vTag: ClassTag[V]

  def withIndex(index: Index): Self[V]
  def withValues[V2: ClassTag](values: Array[V2]): Self[V2]
  def withMask(mask: BitSet): Self[V]

  def map[V2: ClassTag](f: (Id, V) => V2): Self[V2] = {
    // Construct a view of the map transformation
    val newValues = new Array[V2](self.capacity)
    var i = self.mask.nextSetBit(0)
    while (i >= 0) {
      newValues(i) = f(self.index.getValue(i), self.values(i))
      i = self.mask.nextSetBit(i + 1)
    }
    this.withValues(newValues)
  }

  /**
   * Restrict the vertex set to the set of vertices satisfying the given predicate.
   *
   * @param pred the user defined predicate
   *
   * @note The vertex set preserves the original index structure which means that the returned
   *       RDD can be easily joined with the original vertex-set. Furthermore, the filter only
   *       modifies the bitmap index and so no new values are allocated.
   */
  def filter(pred: (Id, V) => Boolean): Self[V] = {
    // Allocate the array to store the results into
    val newMask = new BitSet(self.capacity)
    // Iterate over the active bits in the old mask and evaluate the predicate
    var i = self.mask.nextSetBit(0)
    while (i >= 0) {
      if (pred(self.index.getValue(i), self.values(i))) {
        newMask.set(i)
      }
      i = self.mask.nextSetBit(i + 1)
    }
    this.withMask(newMask)
  }

  /**
   * Hides vertices that are the same between this and other. For vertices that are different, keeps
   * the values from `other`. The indices of `this` and `other` must be the same.
   */
  def diff(other: Self[V]): Self[V] = {
    if (self.index != other.index) {
      logWarning("Diffing two IndexedRDDPartitions with different indexes is slow.")
      diff(createUsingIndex(other.iterator))
    } else {
      val newMask = self.mask & other.mask
      var i = newMask.nextSetBit(0)
      while (i >= 0) {
        if (self.values(i) == other.values(i)) {
          newMask.unset(i)
        }
        i = newMask.nextSetBit(i + 1)
      }
      this.withValues(other.values).withMask(newMask)
    }
  }

  /** Left outer join another IndexedRDDPartition. */
  def leftJoin[V2: ClassTag, V3: ClassTag]
      (other: Self[V2])
      (f: (Id, V, Option[V2]) => V3): Self[V3] = {
    if (self.index != other.index) {
      logWarning("Joining two IndexedRDDPartitions with different indexes is slow.")
      leftJoin(createUsingIndex(other.iterator))(f)
    } else {
      val newValues = new Array[V3](self.capacity)

      var i = self.mask.nextSetBit(0)
      while (i >= 0) {
        val otherV: Option[V2] = if (other.mask.get(i)) Some(other.values(i)) else None
        newValues(i) = f(self.index.getValue(i), self.values(i), otherV)
        i = self.mask.nextSetBit(i + 1)
      }
      this.withValues(newValues)
    }
  }

  /** Left outer join another iterator of messages. */
  def leftJoin[V2: ClassTag, V3: ClassTag]
      (other: Iterator[(Id, V2)])
      (f: (Id, V, Option[V2]) => V3): Self[V3] = {
    leftJoin(createUsingIndex(other))(f)
  }

  def join[U: ClassTag]
      (other: Self[U])
      (f: (Id, V, U) => V): Self[V] = {
    if (self.index != other.index) {
      logWarning("Joining two IndexedRDDPartitions with different indexes is slow.")
      join(createUsingIndex(other.iterator))(f)
    } else {
      val iterMask = self.mask & other.mask
      val newValues = new Array[V](self.capacity)
      System.arraycopy(self.values, 0, newValues, 0, newValues.length)

      var i = iterMask.nextSetBit(0)
      while (i >= 0) {
        newValues(i) = f(self.index.getValue(i), self.values(i), other.values(i))
        i = iterMask.nextSetBit(i + 1)
      }
      this.withValues(newValues)
    }
  }

  /** Join another iterator of messages. */
  def join[U: ClassTag]
      (other: Iterator[(Id, U)])
      (f: (Id, V, U) => V): Self[V] = {
    join(createUsingIndex(other))(f)
  }

  /** Inner join another IndexedRDDPartition. */
  def innerJoin[U: ClassTag, V2: ClassTag]
      (other: Self[U])
      (f: (Id, V, U) => V2): Self[V2] = {
    if (self.index != other.index) {
      logWarning("Joining two IndexedRDDPartitions with different indexes is slow.")
      innerJoin(createUsingIndex(other.iterator))(f)
    } else {
      val newMask = self.mask & other.mask
      val newValues = new Array[V2](self.capacity)
      var i = newMask.nextSetBit(0)
      while (i >= 0) {
        newValues(i) = f(self.index.getValue(i), self.values(i), other.values(i))
        i = newMask.nextSetBit(i + 1)
      }
      this.withValues(newValues).withMask(newMask)
    }
  }

  /**
   * Inner join an iterator of messages.
   */
  def innerJoin[U: ClassTag, V2: ClassTag]
      (iter: Iterator[Product2[Id, U]])
      (f: (Id, V, U) => V2): Self[V2] = {
    innerJoin(createUsingIndex(iter))(f)
  }

  /**
   * Similar effect as aggregateUsingIndex((a, b) => b)
   */
  def createUsingIndex[V2: ClassTag](iter: Iterator[Product2[Id, V2]])
    : Self[V2] = {
    val newMask = new BitSet(self.capacity)
    val newValues = new Array[V2](self.capacity)
    iter.foreach { pair =>
      val pos = self.index.getPos(pair._1)
      if (pos >= 0) {
        newMask.set(pos)
        newValues(pos) = pair._2
      }
    }
    this.withValues(newValues).withMask(newMask)
  }

  /**
   * Similar to innerJoin, but vertices from the left side that don't appear in iter will remain in
   * the partition, hidden by the bitmask.
   */
  def innerJoinKeepLeft(iter: Iterator[Product2[Id, V]]): Self[V] = {
    val newMask = new BitSet(self.capacity)
    val newValues = new Array[V](self.capacity)
    System.arraycopy(self.values, 0, newValues, 0, newValues.length)
    iter.foreach { pair =>
      val pos = self.index.getPos(pair._1)
      if (pos >= 0) {
        newMask.set(pos)
        newValues(pos) = pair._2
      }
    }
    this.withValues(newValues).withMask(newMask)
  }

  def aggregateUsingIndex[V2: ClassTag](
      iter: Iterator[Product2[Id, V2]],
      reduceFunc: (V2, V2) => V2): Self[V2] = {
    val newMask = new BitSet(self.capacity)
    val newValues = new Array[V2](self.capacity)
    iter.foreach { product =>
      val vid = product._1
      val vdata = product._2
      val pos = self.index.getPos(vid)
      if (pos >= 0) {
        if (newMask.get(pos)) {
          newValues(pos) = reduceFunc(newValues(pos), vdata)
        } else { // otherwise just store the new value
          newMask.set(pos)
          newValues(pos) = vdata
        }
      }
    }
    this.withValues(newValues).withMask(newMask)
  }

  /**
   * Construct a new IndexedRDDPartition whose index contains only the vertices in the mask.
   */
  def reindex(): Self[V] = {
    val hashMap = new PrimitiveKeyOpenHashMap[Id, V]
    val arbitraryMerge = (a: V, b: V) => a
    for ((k, v) <- self.iterator) {
      hashMap.setMerge(k, v, arbitraryMerge)
    }
    this.withIndex(hashMap.keySet).withValues(hashMap._values).withMask(hashMap.keySet.getBitSet)
  }
}
