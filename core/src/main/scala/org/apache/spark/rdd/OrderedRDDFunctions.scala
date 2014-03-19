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

import scala.reflect.{ClassTag,classTag}

import org.apache.spark.{Logging, RangePartitioner}
import org.apache.spark.SparkContext._
import org.apache.spark.SparkException
import org.apache.spark.HashPartitioner
import org.apache.spark.Partitioner
import org.apache.spark.Partitioner.defaultPartitioner

/**
 * Extra functions available on RDDs of (key, value) pairs where the key is sortable through
 * an implicit conversion. Import `org.apache.spark.SparkContext._` at the top of your program to
 * use these functions. They will work with any key type that has a `scala.math.Ordered`
 * implementation.
 */
class OrderedRDDFunctions[K <% Ordered[K]: ClassTag,
                          V: ClassTag,
                          P <: Product2[K, V] : ClassTag](
    self: RDD[P])
  extends Logging with Serializable {

  /**
   * Sort the RDD by key, so that each partition contains a sorted range of the elements. Calling
   * `collect` or `save` on the resulting RDD will return or output an ordered list of records
   * (in the `save` case, they will be written to multiple `part-X` files in the filesystem, in
   * order of the keys).
   */
  def sortByKey(ascending: Boolean = true, numPartitions: Int = self.partitions.size): RDD[P] = {
    val part = new RangePartitioner(numPartitions, self, ascending)
    val shuffled = new ShuffledRDD[K, V, P](self, part)
    shuffled.mapPartitions(iter => {
      val buf = iter.toArray
      if (ascending) {
        buf.sortWith((x, y) => x._1 < y._1).iterator
      } else {
        buf.sortWith((x, y) => x._1 > y._1).iterator
      }
    }, preservesPartitioning = true)
  }

  /**
   * Return an RDD containing all pairs of elements with matching keys in `this` and `other`. Each
   * pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in `this` and
   * (k, v2) is in `other`. Uses the given Partitioner to partition the output RDD.
   */
  def mergeJoin[W](
      other: RDD[(K, W)],
      partitioner: Partitioner,
      ordered: Boolean = false): RDD[(K, (V, W))] = {
    mergeCogroup(other, partitioner, ordered).flatMapValues {
      case (vs, ws) =>
        for (v <- vs.iterator; w <- ws.iterator) yield (v, w)
    }
  }

  /**
   * Perform a left outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (v, Some(w))) for w in `other`, or the
   * pair (k, (v, None)) if no elements in `other` have key k. Uses the given Partitioner to
   * partition the output RDD.
   */
  def mergeLeftOuterJoin[W](
      other: RDD[(K, W)],
      partitioner: Partitioner,
      ordered: Boolean = false): RDD[(K, (V, Option[W]))] = {
    mergeCogroup(other, partitioner, ordered).flatMapValues {
      case (vs, ws) =>
        if (ws.isEmpty) {
          vs.iterator.map((_, None))
        } else {
          for (v <- vs.iterator; w <- ws.iterator) yield (v, Some(w))
        }
    }
  }

  /**
   * Perform a right outer join of `this` and `other`. For each element (k, w) in `other`, the
   * resulting RDD will either contain all pairs (k, (Some(v), w)) for v in `this`, or the
   * pair (k, (None, w)) if no elements in `this` have key k. Uses the given Partitioner to
   * partition the output RDD.
   */
  def mergeRightOuterJoin[W](
      other: RDD[(K, W)],
      partitioner: Partitioner,
      ordered: Boolean = false): RDD[(K, (Option[V], W))] = {
    mergeCogroup(other, partitioner, ordered).flatMapValues {
      case (vs, ws) =>
        if (vs.isEmpty) {
          ws.iterator.map((None, _))
        } else {
          for (v <- vs.iterator; w <- ws.iterator) yield (Some(v), w)
        }
    }
  }

  /**
   * Return an RDD containing all pairs of elements with matching keys in `this` and `other`. Each
   * pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in `this` and
   * (k, v2) is in `other`. Performs a merge join across the cluster.
   */
  def mergeJoin[W](other: RDD[(K, W)]): RDD[(K, (V, W))] = {
    mergeJoin(other, false)
  }

  /**
   * Return an RDD containing all pairs of elements with matching keys in `this` and `other`. Each
   * pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in `this` and
   * (k, v2) is in `other`. Performs a merge join across the cluster.
   */
  def mergeJoin[W](other: RDD[(K, W)], ordered: Boolean): RDD[(K, (V, W))] = {
    mergeJoin(other, defaultPartitioner(self, other), ordered)
  }

  /**
   * Return an RDD containing all pairs of elements with matching keys in `this` and `other`. Each
   * pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in `this` and
   * (k, v2) is in `other`. Performs a merge join across the cluster.
   */
  def mergeJoin[W](other: RDD[(K, W)], numPartitions: Int): RDD[(K, (V, W))] = {
    mergeJoin(other, numPartitions, false)
  }

  /**
   * Return an RDD containing all pairs of elements with matching keys in `this` and `other`. Each
   * pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in `this` and
   * (k, v2) is in `other`. Performs a merge join across the cluster.
   */
  def mergeJoin[W](other: RDD[(K, W)], numPartitions: Int, ordered: Boolean): RDD[(K, (V, W))] = {
    mergeJoin(other, new HashPartitioner(numPartitions), ordered)
  }

  /**
   * Perform a left outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (v, Some(w))) for w in `other`, or the
   * pair (k, (v, None)) if no elements in `other` have key k. Hash-partitions the output
   * using the existing partitioner/parallelism level.
   */
  def mergeLeftOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (V, Option[W]))] = {
    mergeLeftOuterJoin(other, false)
  }

  /**
   * Perform a left outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (v, Some(w))) for w in `other`, or the
   * pair (k, (v, None)) if no elements in `other` have key k. Hash-partitions the output
   * using the existing partitioner/parallelism level.
   */
  def mergeLeftOuterJoin[W](other: RDD[(K, W)], ordered: Boolean): RDD[(K, (V, Option[W]))] = {
    mergeLeftOuterJoin(other, defaultPartitioner(self, other), ordered)
  }

  /**
   * Perform a left outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (v, Some(w))) for w in `other`, or the
   * pair (k, (v, None)) if no elements in `other` have key k. Hash-partitions the output
   * into `numPartitions` partitions.
   */
  def mergeLeftOuterJoin[W](
      other: RDD[(K, W)],
      numPartitions: Int): RDD[(K, (V, Option[W]))] = {
    mergeLeftOuterJoin(other, numPartitions, false)
  }

  /**
   * Perform a left outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (v, Some(w))) for w in `other`, or the
   * pair (k, (v, None)) if no elements in `other` have key k. Hash-partitions the output
   * into `numPartitions` partitions.
   */
  def mergeLeftOuterJoin[W](
      other: RDD[(K, W)],
      numPartitions: Int,
      ordered: Boolean): RDD[(K, (V, Option[W]))] = {
    mergeLeftOuterJoin(other, new HashPartitioner(numPartitions), ordered)
  }

  /**
   * Perform a right outer join of `this` and `other`. For each element (k, w) in `other`, the
   * resulting RDD will either contain all pairs (k, (Some(v), w)) for v in `this`, or the
   * pair (k, (None, w)) if no elements in `this` have key k. Hash-partitions the resulting
   * RDD using the existing partitioner/parallelism level.
   */
  def mergeRightOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (Option[V], W))] = {
    mergeRightOuterJoin(other, false)
  }

  /**
   * Perform a right outer join of `this` and `other`. For each element (k, w) in `other`, the
   * resulting RDD will either contain all pairs (k, (Some(v), w)) for v in `this`, or the
   * pair (k, (None, w)) if no elements in `this` have key k. Hash-partitions the resulting
   * RDD using the existing partitioner/parallelism level.
   */
  def mergeRightOuterJoin[W](other: RDD[(K, W)], ordered: Boolean): RDD[(K, (Option[V], W))] = {
    mergeRightOuterJoin(other, defaultPartitioner(self, other), ordered)
  }

  /**
   * Perform a right outer join of `this` and `other`. For each element (k, w) in `other`, the
   * resulting RDD will either contain all pairs (k, (Some(v), w)) for v in `this`, or the
   * pair (k, (None, w)) if no elements in `this` have key k. Hash-partitions the resulting
   * RDD into the given number of partitions.
   */
  def mergeRightOuterJoin[W](
      other: RDD[(K, W)],
      numPartitions: Int): RDD[(K, (Option[V], W))] = {
    mergeRightOuterJoin(other, numPartitions, false)
  }

  /**
   * Perform a right outer join of `this` and `other`. For each element (k, w) in `other`, the
   * resulting RDD will either contain all pairs (k, (Some(v), w)) for v in `this`, or the
   * pair (k, (None, w)) if no elements in `this` have key k. Hash-partitions the resulting
   * RDD into the given number of partitions.
   */
  def mergeRightOuterJoin[W](
      other: RDD[(K, W)],
      numPartitions: Int,
      ordered: Boolean): RDD[(K, (Option[V], W))] = {
    mergeRightOuterJoin(other, new HashPartitioner(numPartitions), ordered)
  }

  /**
   * For each key k in `this` or `other`, return a resulting RDD that contains a tuple with the
   * list of values for that key in `this` as well as `other`.
   */
  def mergeCogroup[W](
      other: RDD[(K, W)],
      partitioner: Partitioner,
      ordered: Boolean = false): RDD[(K, (Seq[V], Seq[W]))] = {
    if (partitioner.isInstanceOf[HashPartitioner] && getKeyClass().isArray) {
      throw new SparkException("Default partitioner cannot partition array keys.")
    }
    val cg = new SortMergeCoGroupedRDD(
      Seq(
        if (ordered) {
          self
        } else {
          self.mapPartitions(_.toArray.sortBy(_._1).iterator, preservesPartitioning = true)
        },
        if (ordered) {
          other
        } else {
          other.mapPartitions(_.toArray.sortBy(_._1).iterator, preservesPartitioning = true)
        }),
      partitioner)
    cg.mapValues {
      case Seq(vs, ws) =>
        (vs.asInstanceOf[Seq[V]], ws.asInstanceOf[Seq[W]])
    }
  }

  /**
   * For each key k in `this` or `other1` or `other2`, return a resulting RDD that contains a
   * tuple with the list of values for that key in `this`, `other1` and `other2`.
   */
  def mergeCogroup[W1, W2](
      other1: RDD[(K, W1)],
      other2: RDD[(K, W2)],
      partitioner: Partitioner,
      ordered: Boolean = false): RDD[(K, (Seq[V], Seq[W1], Seq[W2]))] = {
    if (partitioner.isInstanceOf[HashPartitioner] && getKeyClass().isArray) {
      throw new SparkException("Default partitioner cannot partition array keys.")
    }
    val cg = new SortMergeCoGroupedRDD(
      Seq(
        if (ordered) {
          self
        } else {
          self.mapPartitions(_.toArray.sortBy(_._1).iterator, preservesPartitioning = true)
        },
        if (ordered) {
          other1
        } else {
          other1.mapPartitions(_.toArray.sortBy(_._1).iterator, preservesPartitioning = true)
        },
        if (ordered) {
          other2
        } else {
          other2.mapPartitions(_.toArray.sortBy(_._1).iterator, preservesPartitioning = true)
        }),
      partitioner)
    cg.mapValues {
      case Seq(vs, w1s, w2s) =>
        (vs.asInstanceOf[Seq[V]], w1s.asInstanceOf[Seq[W1]], w2s.asInstanceOf[Seq[W2]])
    }
  }

  /**
   * For each key k in `this` or `other`, return a resulting RDD that contains a tuple with the
   * list of values for that key in `this` as well as `other`.
   */
  def mergeCogroup[W](other: RDD[(K, W)]): RDD[(K, (Seq[V], Seq[W]))] = {
    mergeCogroup(other, false)
  }

  /**
   * For each key k in `this` or `other`, return a resulting RDD that contains a tuple with the
   * list of values for that key in `this` as well as `other`.
   */
  def mergeCogroup[W](other: RDD[(K, W)], ordered: Boolean): RDD[(K, (Seq[V], Seq[W]))] = {
    mergeCogroup(other, defaultPartitioner(self, other), ordered)
  }

  /**
   * For each key k in `this` or `other1` or `other2`, return a resulting RDD that contains a
   * tuple with the list of values for that key in `this`, `other1` and `other2`.
   */
  def mergeCogroup[W1, W2](
      other1: RDD[(K, W1)],
      other2: RDD[(K, W2)]): RDD[(K, (Seq[V], Seq[W1], Seq[W2]))] = {
    mergeCogroup(other1, other2, false)
  }

  /**
   * For each key k in `this` or `other1` or `other2`, return a resulting RDD that contains a
   * tuple with the list of values for that key in `this`, `other1` and `other2`.
   */
  def mergeCogroup[W1, W2](
      other1: RDD[(K, W1)],
      other2: RDD[(K, W2)],
      ordered: Boolean): RDD[(K, (Seq[V], Seq[W1], Seq[W2]))] = {
    mergeCogroup(other1, other2, defaultPartitioner(self, other1, other2), ordered)
  }

  /**
   * For each key k in `this` or `other`, return a resulting RDD that contains a tuple with the
   * list of values for that key in `this` as well as `other`.
   */
  def mergeCogroup[W](
      other: RDD[(K, W)],
      numPartitions: Int): RDD[(K, (Seq[V], Seq[W]))] = {
    mergeCogroup(other, numPartitions, false)
  }

  /**
   * For each key k in `this` or `other`, return a resulting RDD that contains a tuple with the
   * list of values for that key in `this` as well as `other`.
   */
  def mergeCogroup[W](
      other: RDD[(K, W)],
      numPartitions: Int,
      ordered: Boolean): RDD[(K, (Seq[V], Seq[W]))] = {
    mergeCogroup(other, new HashPartitioner(numPartitions), ordered)
  }

  /**
   * For each key k in `this` or `other1` or `other2`, return a resulting RDD that contains a
   * tuple with the list of values for that key in `this`, `other1` and `other2`.
   */
  def mergeCogroup[W1, W2](
      other1: RDD[(K, W1)],
      other2: RDD[(K, W2)],
      numPartitions: Int): RDD[(K, (Seq[V], Seq[W1], Seq[W2]))] = {
    mergeCogroup(other1, other2, numPartitions, false)
  }

  /**
   * For each key k in `this` or `other1` or `other2`, return a resulting RDD that contains a
   * tuple with the list of values for that key in `this`, `other1` and `other2`.
   */
  def mergeCogroup[W1, W2](
      other1: RDD[(K, W1)],
      other2: RDD[(K, W2)],
      numPartitions: Int,
      ordered: Boolean): RDD[(K, (Seq[V], Seq[W1], Seq[W2]))] = {
    mergeCogroup(other1, other2, new HashPartitioner(numPartitions), ordered)
  }

  private def getKeyClass() = classTag[K].runtimeClass
}
