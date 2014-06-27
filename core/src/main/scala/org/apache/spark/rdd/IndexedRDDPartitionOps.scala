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

import scala.collection.immutable.LongMap
import scala.language.higherKinds
import scala.reflect.ClassTag

import org.apache.spark.Logging
import org.apache.spark.util.collection.BitSet
import org.apache.spark.util.collection.ImmutableBitSet
import org.apache.spark.util.collection.ImmutableLongOpenHashSet
import org.apache.spark.util.collection.ImmutableVector
import org.apache.spark.util.collection.OpenHashSet
import org.apache.spark.util.collection.PrimitiveKeyOpenHashMap

import IndexedRDD.Id
import IndexedRDDPartition.Index

private[spark] trait IndexedRDDPartitionOps[
    @specialized(Long, Int, Double) V,
    Self[X] <: IndexedRDDPartitionBase[X] with IndexedRDDPartitionOps[X, Self]]
  extends Logging {

  def self: Self[V]

  implicit def vTag: ClassTag[V]

  def withIndex(index: Index): Self[V]
  def withValues[V2: ClassTag](values: ImmutableVector[V2]): Self[V2]
  def withMask(mask: ImmutableBitSet): Self[V]

  /**
   * Gets the values corresponding to keys ks.
   */
  def multiget(ks: Array[Id]): LongMap[V] = {
    var result = LongMap.empty[V]
    var i = 0
    while (i < ks.length) {
      val k = ks(i)
      if (self.isDefined(k)) {
        result = result.updated(k, self(k))
      }
      i += 1
    }
    result
  }

  /**
   * Updates keys ks to have values vs, running `merge` on old and new values if necessary.
   */
  def multiput(kvs: Seq[(Id, V)], merge: (Id, V, V) => V): Self[V] = {
    if (kvs.forall(kv => self.isDefined(kv._1))) {
      // Pure updates can be implemented by modifying only the values
      join(kvs.iterator)(merge)
    } else {
      var newIndex = self.index
      var newValues = self.values
      var newMask = self.mask

      var preMoveValues: ImmutableVector[V] = null
      var preMoveMask: ImmutableBitSet = null
      def grow(newSize: Int) {
        preMoveValues = newValues
        preMoveMask = newMask

        newValues = ImmutableVector.fill(newSize)(null.asInstanceOf[V])
        newMask = new ImmutableBitSet(newSize)
      }
      def move(oldPos: Int, newPos: Int) {
        newValues = newValues.updated(newPos, preMoveValues(oldPos))
        if (preMoveMask.get(oldPos)) newMask = newMask.set(newPos)
      }

      for (kv <- kvs) {
        val id = kv._1
        val otherValue = kv._2
        newIndex = newIndex.addWithoutResize(id)
        if ((newIndex.focus & OpenHashSet.NONEXISTENCE_MASK) != 0) {
          // This is a new key - need to update index
          val pos = newIndex.focus & OpenHashSet.POSITION_MASK
          newValues = newValues.updated(pos, otherValue)
          newMask = newMask.set(pos)
          newIndex = newIndex.rehashIfNeeded(grow, move)
        } else {
          // Existing key - just need to set value and ensure it appears in newMask
          val pos = newIndex.focus
          newValues = newValues.updated(pos, otherValue)
          newMask = newMask.set(pos)
        }
      }

      this.withIndex(newIndex).withValues(newValues).withMask(newMask)
    }
  }

  def map[V2: ClassTag](f: (Id, V) => V2): Self[V2] = {
    // Construct a view of the map transformation
    val newValues = new Array[V2](self.capacity)
    self.mask.iterator.foreach { i =>
      newValues(i) = f(self.index.getValue(i), self.values(i))
    }
    this.withValues(ImmutableVector.fromArray(newValues))
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
    self.mask.iterator.foreach { i =>
      if (pred(self.index.getValue(i), self.values(i))) {
        newMask.set(i)
      }
    }
    this.withMask(newMask.toImmutableBitSet)
  }

  /**
   * Intersects `this` and `other` and keeps only vertices with differing values. For differing
   * vertices, keeps the values from `this`.
   */
  def diff(other: Self[V]): Self[V] = {
    if (self.index != other.index) {
      logWarning("Diffing two IndexedRDDPartitions with different indexes is slow.")
      val newMask = new BitSet(self.capacity)

      self.mask.iterator.foreach { i =>
        val vid = self.index.getValue(i)
        val keep =
          if (other.index.getValue(i) == vid && other.mask.get(i)) {
            // The indices agree on this entry
            self.values(i) != other.values(i)
          } else if (other.isDefined(vid)) {
            // The indices do not agree, but the corresponding entry exists somewhere
            self.values(i) != other(vid)
          } else {
            // There is no corresponding entry
            false
          }

        if (keep) {
          newMask.set(i)
        }
      }
      this.withMask(newMask.toImmutableBitSet)
    } else {
      val newMask = self.mask & other.mask
      newMask.iterator.foreach { i =>
        if (self.values(i) == other.values(i)) {
          newMask.unset(i)
        }
      }
      this.withMask(newMask)
    }
  }

  /** Left outer join another IndexedRDDPartition. */
  def leftJoin[V2: ClassTag, V3: ClassTag]
      (other: Self[V2])
      (f: (Id, V, Option[V2]) => V3): Self[V3] = {
    if (self.index != other.index) {
      logWarning("Joining two IndexedRDDPartitions with different indexes is slow.")
      val newValues = new Array[V3](self.capacity)

      self.mask.iterator.foreach { i =>
        val vid = self.index.getValue(i)
        val otherI =
          if (other.index.getValue(i) == vid) {
            if (other.mask.get(i)) i else -1
          } else {
            if (other.isDefined(vid)) other.index.getPos(vid) else -1
          }
        val otherV = if (otherI != -1) Some(other.values(otherI)) else None
        newValues(i) = f(vid, self.values(i), otherV)
      }
      this.withValues(ImmutableVector.fromArray(newValues))
    } else {
      val newValues = new Array[V3](self.capacity)

      self.mask.iterator.foreach { i =>
        val otherV: Option[V2] = if (other.mask.get(i)) Some(other.values(i)) else None
        newValues(i) = f(self.index.getValue(i), self.values(i), otherV)
      }
      this.withValues(ImmutableVector.fromArray(newValues))
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
      var newValues = self.values

      other.mask.iterator.foreach { otherI =>
        val vid = other.index.getValue(otherI)
        val selfI =
          if (self.index.getValue(otherI) == vid) {
            if (self.mask.get(otherI)) otherI else -1
          } else {
            if (self.isDefined(vid)) self.index.getPos(vid) else -1
          }
        if (selfI != -1) {
          newValues = newValues.updated(selfI, f(vid, self.values(selfI), other.values(otherI)))
        }
      }
      this.withValues(newValues)
    } else {
      val iterMask = self.mask & other.mask
      var newValues = self.values

      iterMask.iterator.foreach { i =>
        newValues = newValues.updated(i, f(self.index.getValue(i), self.values(i), other.values(i)))
      }
      this.withValues(newValues)
    }
  }

  /** Join another iterator of messages. */
  def join[U: ClassTag]
      (other: Iterator[(Id, U)])
      (f: (Id, V, U) => V): Self[V] = {
    var newValues = self.values
    other.foreach { kv =>
      val id = kv._1
      val otherValue = kv._2
      val i = self.index.getPos(kv._1)
      newValues = newValues.updated(i, f(id, self.values(i), otherValue))
    }
    this.withValues(newValues)
  }

  /** Inner join another IndexedRDDPartition. */
  def innerJoin[U: ClassTag, V2: ClassTag]
      (other: Self[U])
      (f: (Id, V, U) => V2): Self[V2] = {
    if (self.index != other.index) {
      logWarning("Joining two IndexedRDDPartitions with different indexes is slow.")
      val newMask = new BitSet(self.capacity)
      val newValues = new Array[V2](self.capacity)

      self.mask.iterator.foreach { i =>
        val vid = self.index.getValue(i)
        val otherI =
          if (other.index.getValue(i) == vid) {
            if (other.mask.get(i)) i else -1
          } else {
            if (other.isDefined(vid)) other.index.getPos(vid) else -1
          }
        if (otherI != -1) {
          newValues(i) = f(vid, self.values(i), other.values(otherI))
          newMask.set(i)
        }
      }
      this.withValues(ImmutableVector.fromArray(newValues)).withMask(newMask.toImmutableBitSet)
    } else {
      val newMask = self.mask & other.mask
      val newValues = new Array[V2](self.capacity)
      newMask.iterator.foreach { i =>
        newValues(i) = f(self.index.getValue(i), self.values(i), other.values(i))
      }
      this.withValues(ImmutableVector.fromArray(newValues)).withMask(newMask)
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
    this.withValues(ImmutableVector.fromArray(newValues)).withMask(newMask.toImmutableBitSet)
  }

  /**
   * Similar to innerJoin, but vertices from the left side that don't appear in iter will remain in
   * the partition, hidden by the bitmask.
   */
  def innerJoinKeepLeft(iter: Iterator[Product2[Id, V]]): Self[V] = {
    val newMask = new BitSet(self.capacity)
    var newValues = self.values
    iter.foreach { pair =>
      val pos = self.index.getPos(pair._1)
      if (pos >= 0) {
        newMask.set(pos)
        newValues = newValues.updated(pos, pair._2)
      }
    }
    this.withValues(newValues).withMask(newMask.toImmutableBitSet)
  }

  def aggregateUsingIndex[V2: ClassTag](
      iter: Iterator[Product2[Id, V2]],
      reduceFunc: (V2, V2) => V2): Self[V2] = {
    val newMask = new BitSet(self.capacity)
    val newValues = new Array[V2](self.capacity)
    iter.foreach { product =>
      val id = product._1
      val value = product._2
      val pos = self.index.getPos(id)
      if (pos >= 0) {
        if (newMask.get(pos)) {
          newValues(pos) = reduceFunc(newValues(pos), value)
        } else { // otherwise just store the new value
          newMask.set(pos)
          newValues(pos) = value
        }
      }
    }
    this.withValues(ImmutableVector.fromArray(newValues)).withMask(newMask.toImmutableBitSet)
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
    this.withIndex(ImmutableLongOpenHashSet.fromLongOpenHashSet(hashMap.keySet))
      .withValues(ImmutableVector.fromArray(hashMap._values))
      .withMask(hashMap.keySet.getBitSet.toImmutableBitSet)
  }
}
