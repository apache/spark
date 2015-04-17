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

import org.apache.spark.storage.StorageLevel


/**
 * A common interface to be used by all RDD-like objects - RDDs, DStreams, DataFrames, etc. - so 
 * that code written for one can be used by others.
 * 
 * For some background reading on how to do this, check out:
 *  http://adriaanm.github.io/files/higher.pdf
 */
trait RDDLike[T, Col[_]] {

  /** Persist this RDDLike object with the default storage level ('MEMORY_ONLY'). */
  def cache(): this.type

  /** Persist this RDDLike object with the default storage level ('MEMORY_ONLY'). */
  def persist(): this.type

  /**
   * Set this RDDLike object's storage level to persist its values across operations after the 
   * first time it is computed.  This can only be used to assign a new storage level if the RDDLike 
   * object does not have a storage level set already.
   */
  def persist(newLevel: StorageLevel): this.type

  /**
   * Mark this RDDLike object as non-persistent, and remove all blocks for it from memory and disk.
   * 
   * @param blocking Whether to block until all blocks are deleted.
   * @return this RDDLike object
   */
  def unpersist(blocking: Boolean): this.type

  /** Return a nuw RDDLike object by applying a function to all elements of this RDDLike object. */
  def map[R: ClassTag](f: T => R): Col[R]

  /**
   * Return a new RDDLike object by first applying a function to all elements of this RDDLike 
   * object, and then flattening the results.
   */
  def flatMap[R: ClassTag](f: T => Traversable[R]): Col[R]

  /**
   * Returns a new RDDLike object by applying a function to each partition of this RDDLike object.
   * 
   * @param preservesPartitioning indicates whether the input function preserves the partitioner,
   *                              which should be 'false' unless this is a pair RDD and the input
   *                              doesn't modify the keys.
   */
  def mapPartitions[R: ClassTag](f: Iterator[T] => Iterator[R],
                                 preservesPartitioning: Boolean = false): Col[R]

  /** Applies a function f to all elements of this RDDLike object. */
  def foreach(f: T => Unit): Unit

  /** Applies a function f to each partition of this RDDLike object. */
  def foreachPartition(f: Iterator[T] => Unit): Unit

  /**
   * Return a new RDDLike object that has exactly numPartitions partitions.
   *
   * Can increase or decrease the level of parallelism in this RDDLike object. Internally, this uses
   * a shuffle to redistribute data.
   *
   * If you are decreasing the number of partitions in this RDDLike object, and it is an 
   * ImmutableRDDLike object, consider using `coalesce` instead, which can avoid performing a 
   * shuffle.
   */
  def repartition(numPartitions: Int)(implicit ord: Ordering[T] = null): Col[T]
}

/**
 * A common interface to be used by all RDD-like objects with immutable contents
 */
trait ImmutableRDDLike[T, Col[_]] extends RDDLike[T, Col] {

  /**
   * Take the first num elements of this RDDLike objects. It works by first scanning one partition, 
   * and use the results from that partition to estimate the number of additional partitions needed 
   * to satisfy the limit.
   *
   * @note due to complications in the internal implementation, this method will raise
   * an exception if called on an RDD of `Nothing` or `Null`.
   */
  def take(n: Int): Array[T]

  /** Return an array that contains all of the elements of this RDDLike object */
  def collect(): Array[T]

  /** Return the number of elements in this RDDLike object */
  def count(): Long

  /** Return the first element in this RDDLike object. */
  def first(): T

  /** Returns a new RDDLike object containing the distinct elements in this RDDLike object */
  def distinct: Col[T]

  /**
   * Return a new ImmutableRDDLike object that is reduced into `numPartitions` partitions.
   *
   * This results in a narrow dependency, e.g. if you go from 1000 partitions to 100 partitions, 
   * there will not be a shuffle, instead each of the 100 new partitions will claim 10 of the 
   * current partitions.
   *
   * However, if you're doing a drastic coalesce, e.g. to numPartitions = 1, this may result in 
   * your computation taking place on fewer nodes than you like (e.g. one node in the case of 
   * numPartitions = 1). To avoid this, you can pass shuffle = true. This will add a shuffle step, 
   * but means the current upstream partitions will be executed in parallel (per whatever the 
   * current partitioning is).
   *
   * Note: With shuffle = true, you can actually coalesce to a larger number of partitions. This 
   * is useful if you have a small number of partitions, say 100, potentially with a few partitions 
   * being abnormally large. Calling coalesce(1000, shuffle = true) will result in 1000 partitions 
   * with the data distributed using a hash partitioner.
   */
  def coalesce (numPartitions: Int, shuffle: Boolean = false)(implicit ord: Ordering[T] = null)
      : Col[T]
}
