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

package org.apache.spark.graphx.impl

import scala.language.higherKinds
import scala.language.implicitConversions
import scala.reflect.ClassTag

import org.apache.spark.Logging
import org.apache.spark.util.collection.BitSet

import org.apache.spark.graphx._
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap

/**
 * An class containing additional operations for subclasses of VertexPartitionBase that provide
 * implicit evidence of membership in the `VertexPartitionBaseOpsConstructor` typeclass (for
 * example, [[VertexPartition.VertexPartitionOpsConstructor]]).
 */
private[graphx] abstract class VertexPartitionBaseOps
    [VD: ClassTag, Self[X] <: VertexPartitionBase[X] : VertexPartitionBaseOpsConstructor]
    (self: Self[VD])
  extends Serializable with Logging {

  def withIndex(index: VertexIdToIndexMap): Self[VD]
  def withValues[VD2: ClassTag](values: Array[VD2]): Self[VD2]
  def withMask(mask: BitSet): Self[VD]

  /**
   * Pass each vertex attribute along with the vertex id through a map
   * function and retain the original RDD's partitioning and index.
   *
   * @tparam VD2 the type returned by the map function
   *
   * @param f the function applied to each vertex id and vertex
   * attribute in the RDD
   *
   * @return a new VertexPartition with values obtained by applying `f` to
   * each of the entries in the original VertexRDD.  The resulting
   * VertexPartition retains the same index.
   */
  def map[VD2: ClassTag](f: (VertexId, VD) => VD2): Self[VD2] = {
    // Construct a view of the map transformation
    val newValues = new Array[VD2](self.capacity)
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
  def filter(pred: (VertexId, VD) => Boolean): Self[VD] = {
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
  def diff(other: Self[VD]): Self[VD] = {
    if (self.index != other.index) {
      logWarning("Diffing two VertexPartitions with different indexes is slow.")
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

  /** Left outer join another VertexPartition. */
  def leftJoin[VD2: ClassTag, VD3: ClassTag]
      (other: Self[VD2])
      (f: (VertexId, VD, Option[VD2]) => VD3): Self[VD3] = {
    if (self.index != other.index) {
      logWarning("Joining two VertexPartitions with different indexes is slow.")
      leftJoin(createUsingIndex(other.iterator))(f)
    } else {
      val newValues = new Array[VD3](self.capacity)

      var i = self.mask.nextSetBit(0)
      while (i >= 0) {
        val otherV: Option[VD2] = if (other.mask.get(i)) Some(other.values(i)) else None
        newValues(i) = f(self.index.getValue(i), self.values(i), otherV)
        i = self.mask.nextSetBit(i + 1)
      }
      this.withValues(newValues)
    }
  }

  /** Left outer join another iterator of messages. */
  def leftJoin[VD2: ClassTag, VD3: ClassTag]
      (other: Iterator[(VertexId, VD2)])
      (f: (VertexId, VD, Option[VD2]) => VD3): Self[VD3] = {
    leftJoin(createUsingIndex(other))(f)
  }

  /** Inner join another VertexPartition. */
  def innerJoin[U: ClassTag, VD2: ClassTag]
      (other: Self[U])
      (f: (VertexId, VD, U) => VD2): Self[VD2] = {
    if (self.index != other.index) {
      logWarning("Joining two VertexPartitions with different indexes is slow.")
      innerJoin(createUsingIndex(other.iterator))(f)
    } else {
      val newMask = self.mask & other.mask
      val newValues = new Array[VD2](self.capacity)
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
  def innerJoin[U: ClassTag, VD2: ClassTag]
      (iter: Iterator[Product2[VertexId, U]])
      (f: (VertexId, VD, U) => VD2): Self[VD2] = {
    innerJoin(createUsingIndex(iter))(f)
  }

  /**
   * Similar effect as aggregateUsingIndex((a, b) => a)
   */
  def createUsingIndex[VD2: ClassTag](iter: Iterator[Product2[VertexId, VD2]])
    : Self[VD2] = {
    val newMask = new BitSet(self.capacity)
    val newValues = new Array[VD2](self.capacity)
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
  def innerJoinKeepLeft(iter: Iterator[Product2[VertexId, VD]]): Self[VD] = {
    val newMask = new BitSet(self.capacity)
    val newValues = new Array[VD](self.capacity)
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

  def aggregateUsingIndex[VD2: ClassTag](
      iter: Iterator[Product2[VertexId, VD2]],
      reduceFunc: (VD2, VD2) => VD2): Self[VD2] = {
    val newMask = new BitSet(self.capacity)
    val newValues = new Array[VD2](self.capacity)
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
   * Construct a new VertexPartition whose index contains only the vertices in the mask.
   */
  def reindex(): Self[VD] = {
    val hashMap = new GraphXPrimitiveKeyOpenHashMap[VertexId, VD]
    val arbitraryMerge = (a: VD, b: VD) => a
    for ((k, v) <- self.iterator) {
      hashMap.setMerge(k, v, arbitraryMerge)
    }
    this.withIndex(hashMap.keySet).withValues(hashMap._values).withMask(hashMap.keySet.getBitSet)
  }

  /**
   * Converts a vertex partition (in particular, one of type `Self`) into a
   * `VertexPartitionBaseOps`. Within this class, this allows chaining the methods defined above,
   * because these methods return a `Self` and this implicit conversion re-wraps that in a
   * `VertexPartitionBaseOps`. This relies on the context bound on `Self`.
   */
  private implicit def toOps[VD2: ClassTag](partition: Self[VD2])
    : VertexPartitionBaseOps[VD2, Self] = {
    implicitly[VertexPartitionBaseOpsConstructor[Self]].toOps(partition)
  }
}
