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
import scala.collection.immutable.Vector
import scala.language.higherKinds
import scala.reflect.ClassTag

import org.apache.spark.Logging
import org.apache.spark.util.collection.BitSet
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
  def withValues[V2: ClassTag](values: Vector[V2]): Self[V2]
  def withMask(mask: BitSet): Self[V]

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
    // TODO: switch to purely functional index to avoid a full reindex on insertions
    if (kvs.forall(kv => self.isDefined(kv._1))) {
      // Pure updates can be implemented more efficiently
      join(kvs.iterator)(merge)
    } else {
      val hashMap = new PrimitiveKeyOpenHashMap[Id, V]
      for ((k, v) <- self.iterator ++ kvs) {
        hashMap.setMerge(k, v, (a, b) => merge(k, a, b))
      }
      this.withIndex(hashMap.keySet).withValues(hashMap._values.toVector)
        .withMask(hashMap.keySet.getBitSet)
    }
  }

  def map[V2: ClassTag](f: (Id, V) => V2): Self[V2] = {
    // Construct a view of the map transformation
    val newValues = new Array[V2](self.capacity)
    var i = self.mask.nextSetBit(0)
    while (i >= 0) {
      newValues(i) = f(self.index.getValue(i), self.values(i))
      i = self.mask.nextSetBit(i + 1)
    }
    this.withValues(newValues.toVector)
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
   * Intersects `this` and `other` and keeps only vertices with differing values. For differing
   * vertices, keeps the values from `this`.
   */
  def diff(other: Self[V]): Self[V] = {
    if (self.index != other.index) {
      logWarning("Diffing two IndexedRDDPartitions with different indexes is slow.")
      val newMask = new BitSet(self.capacity)

      var i = self.mask.nextSetBit(0)
      while (i >= 0) {
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

        i = self.mask.nextSetBit(i + 1)
      }
      this.withMask(newMask)
    } else {
      val newMask = self.mask & other.mask
      var i = newMask.nextSetBit(0)
      while (i >= 0) {
        if (self.values(i) == other.values(i)) {
          newMask.unset(i)
        }
        i = newMask.nextSetBit(i + 1)
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

      var i = self.mask.nextSetBit(0)
      while (i >= 0) {
        val vid = self.index.getValue(i)
        val otherI =
          if (other.index.getValue(i) == vid) {
            if (other.mask.get(i)) i else -1
          } else {
            if (other.isDefined(vid)) other.index.getPos(vid) else -1
          }
        val otherV = if (otherI != -1) Some(other.values(otherI)) else None
        newValues(i) = f(vid, self.values(i), otherV)
        i = self.mask.nextSetBit(i + 1)
      }
      this.withValues(newValues.toVector)
    } else {
      val newValues = new Array[V3](self.capacity)

      var i = self.mask.nextSetBit(0)
      while (i >= 0) {
        val otherV: Option[V2] = if (other.mask.get(i)) Some(other.values(i)) else None
        newValues(i) = f(self.index.getValue(i), self.values(i), otherV)
        i = self.mask.nextSetBit(i + 1)
      }
      this.withValues(newValues.toVector)
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

      var otherI = other.mask.nextSetBit(0)
      while (otherI >= 0) {
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
        otherI = other.mask.nextSetBit(otherI + 1)
      }
      this.withValues(newValues)
    } else {
      val iterMask = self.mask & other.mask
      var newValues = self.values

      var i = iterMask.nextSetBit(0)
      while (i >= 0) {
        newValues = newValues.updated(i, f(self.index.getValue(i), self.values(i), other.values(i)))
        i = iterMask.nextSetBit(i + 1)
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

      var i = self.mask.nextSetBit(0)
      while (i >= 0) {
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
        i = self.mask.nextSetBit(i + 1)
      }
      this.withValues(newValues.toVector).withMask(newMask)
    } else {
      val newMask = self.mask & other.mask
      val newValues = new Array[V2](self.capacity)
      var i = newMask.nextSetBit(0)
      while (i >= 0) {
        newValues(i) = f(self.index.getValue(i), self.values(i), other.values(i))
        i = newMask.nextSetBit(i + 1)
      }
      this.withValues(newValues.toVector).withMask(newMask)
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
    this.withValues(newValues.toVector).withMask(newMask)
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
    this.withValues(newValues).withMask(newMask)
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
    this.withValues(newValues.toVector).withMask(newMask)
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
    this.withIndex(hashMap.keySet).withValues(hashMap._values.toVector)
      .withMask(hashMap.keySet.getBitSet)
  }
}
