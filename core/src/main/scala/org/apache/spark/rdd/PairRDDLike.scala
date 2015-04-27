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
import scala.collection.Map
import scala.reflect.ClassTag

import org.apache.spark.Partitioner

trait PairRDDLike[K, V, Col[_]] {
  /**
   * Generic function to combine the elements for each key using a custom set of aggregation
   * functions. Turns an RDDLike[(K, V)] into a result of type RDDLike[(K, C)], for a "combined
   * type" C
   * Note that V and C can be different -- for example, one might group an RDDLike object of type
   * (Int, Int) into an RDDLike object of type (Int, Seq[Int]). Users provide three functions:
   *
   * - `createCombiner`, which turns a V into a C (e.g., creates a one-element list)
   * - `mergeValue`, to merge a V into a C (e.g., adds it to the end of a list)
   * - `mergeCombiners`, to combine two C's into a single one.
   *
   * In addition, users can control the partitioning of the output RDDLike object.
   */
  def combineByKey[C: ClassTag] (createCombiner: V => C,
                                 mergeValue: (C, V) => C,
                                 mergeCombiner: (C, C) => C,
                                 partitioner: Partitioner): Col[(K, C)]

  /** A simplified version of combineByKey that hash-partitions the output RDDLike object */
  def combineByKey[C: ClassTag](createCombiner: V => C,
                                mergeValue: (C, V) => C,
                                mergeCombiners: (C, C) => C,
                                numPartitions: Int): Col[(K, C)]
  /**
   * Simplified version of combineByKey that hash-partitions the resulting RDDLike object using the
   * existing partitioner/parallelism level.
   */
  def combineByKey[C: ClassTag](createCombiner: V => C,
                                mergeValue: (C, V) => C,
                                mergeCombiners: (C, C) => C): Col[(K, C)]

  /**
   * Merge the values for each key using an associative reduce function. This will also perform
   * the merging locally on each mapper before sending results to a reducer, similarly to a
   * "combiner" in MapReduce.
   */
  def reduceByKey (reduceFunc: (V, V) => V, partitioner: Partitioner): Col[(K, V)]

  /** A simplified version of reduceByKey that uses the default partitioner. */
  def reduceByKey (reduceFunc: (V, V) => V): Col[(K, V)]

  /**
   * Group the values for each key in the RDDLike object into a single sequence. Allows controlling 
   * the partitioning of the resulting key-value pair RDDLike object by passing a Partitioner.
   * The ordering of elements within each group is not guaranteed, and may even differ
   * each time the resulting RDDLike object is evaluated.
   *
   * Note: This operation may be very expensive. If you are grouping in order to perform an
   * aggregation (such as a sum or average) over each key, using 
   * [[PairRDDLikeFunctions.reduceByKey]] will provide much better performance.
   *
   * Note: As currently implemented, groupByKey must be able to hold all the key-value pairs for any
   * key in memory. If a key has too many values, it can result in an [[OutOfMemoryError]].
   */
  def groupByKey(partitioner: Partitioner): Col[(K, Iterable[V])]

  /** A simplified version of groupByKey that uses a hash partitioner. */
  def groupByKey(numPartitions: Int): Col[(K, Iterable[V])]

  /** A simplified version of groupByKey that uses the default partitioner. */
  def groupByKey(): Col[(K, Iterable[V])]

  /**
   * Return an RDDLike object containing all pairs of elements with matching keys in `this` and 
   * `other`. Each pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is
   * in `this` and (k, v2) is in `other`. Uses the given Partitioner to partition the output 
   * RDDLike object.
   */
  def join[W](other: Col[(K, W)], partitioner: Partitioner): Col[(K, (V, W))]

  /** A simplified version of join that uses a hash partitioner. */
  def join[W](other: Col[(K, W)], numPartitions: Int): Col[(K, (V, W))]

  /** A simplified version of join that uses the default partitioner. */
  def join[W](other: Col[(K, W)]): Col[(K, (V, W))]

  /**
   * Perform a left outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDDLike object will either contain all pairs (k, (v, Some(w))) for w in `other`, 
   * or the pair (k, (v, None)) if no elements in `other` have key k. Uses the given Partitioner 
   * to partition the output RDDLike object.
   */
  def leftOuterJoin[W](other: Col[(K, W)], partitioner: Partitioner): Col[(K, (V, Option[W]))]

  /** A simplified version of leftOuterJoin that uses a hash partitioner. */
  def leftOuterJoin[W](other: Col[(K, W)], numPartitions: Int): Col[(K, (V, Option[W]))]
  /** A simplified version of leftOuterJoin that uses the default partitioner. */
  def leftOuterJoin[W](other: Col[(K, W)]): Col[(K, (V, Option[W]))]

  /**
   * Perform a right outer join of `this` and `other`. For each element (k, w) in `other`, the
   * resulting RDDLike object will either contain all pairs (k, (Some(v), w)) for v in `this`, or 
   * the pair (k, (None, w)) if no elements in `this` have key k. Uses the given Partitioner to
   * partition the output RDDLike object.
   */
  def rightOuterJoin[W](other: Col[(K, W)], partitioner: Partitioner): Col[(K, (Option[V], W))]

  /** A simplified version of rightOuterJoin that uses a hash partitioner. */
  def rightOuterJoin[W](other: Col[(K, W)], numPartitions: Int): Col[(K, (Option[V], W))]

  /** A simplified version of rightOuterJoin that uses the default partitioner. */
  def rightOuterJoin[W](other: Col[(K, W)]): Col[(K, (Option[V], W))]

  /**
   * Perform a full outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDDLike object will either contain all pairs (k, (Some(v), Some(w))) for w in 
   * `other`, or the pair (k, (Some(v), None)) if no elements in `other` have key k. Similarly, 
   * for each element (k, w) in `other`, the resulting RDDLike object will either contain all pairs
   * (k, (Some(v), Some(w))) for v in `this`, or the pair (k, (None, Some(w))) if no elements
   * in `this` have key k. Uses the given Partitioner to partition the output RDDLike object.
   */
  def fullOuterJoin[W](other: Col[(K, W)], partitioner: Partitioner)
      : Col[(K, (Option[V], Option[W]))]

  /** A simplified version of fullOuterJoin that uses a hash partitioner. */
  def fullOuterJoin[W](other: Col[(K, W)], numPartitions: Int)
      : Col[(K, (Option[V], Option[W]))]

  /** A simplified version of fullOuterJoin that uses the default partitioner. */
  def fullOuterJoin[W](other: Col[(K, W)]): Col[(K, (Option[V], Option[W]))]

  /**
   * For each key k in `this` or `other`, return a resulting RDDLike object that contains a tuple 
   * with the list of values for that key in `this` as well as `other`.
   */
  def cogroup [W1](other1: Col[(K, W1)], partitioner: Partitioner)
      : Col[(K, (Iterable[V], Iterable[W1]))]

  /** A simplified version of cogroup that uses a hash partitioner. */
  def cogroup [W1](other1: Col[(K, W1)], numPartitions: Int): Col[(K, (Iterable[V], Iterable[W1]))]

  /** A simplified version of cogroup that uses the default partitioner */
  def cogroup [W1](other1: Col[(K, W1)]): Col[(K, (Iterable[V], Iterable[W1]))]

  /**
   * Pass each value in the key-value pair RDDLike object through a map function without changing 
   * the keys; this also retains the original RDDLike object's partitioning.
   */
  def mapValues[U: ClassTag](f: V => U): Col[(K, U)]

  /**
   * Pass each value in the key-value pair RDDLike object through a flatMap function without 
   * changing the keys; this also retains the original RDDLike object's partitioning.
   */
  def flatMapValues[U: ClassTag](f: V => TraversableOnce[U]): Col[(K, U)]
 }
