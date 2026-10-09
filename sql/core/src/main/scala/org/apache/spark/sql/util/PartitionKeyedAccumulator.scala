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

package org.apache.spark.sql.util

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.util.AccumulatorV2

/**
 * An `AccumulatorV2` that records one value of type `T` per partition, keyed by partition id with
 * LAST-WRITE-WINS merge. When the same partition is recorded more than once -- e.g. duplicate
 * cross-executor computes, or speculative tasks -- the later value replaces the earlier one rather
 * than aggregating, so each partition contributes exactly once. The key set is the set of recorded
 * partitions, and callers fold the values (see [[foldValues]]) to derive de-duplicated aggregates;
 * a plain summing accumulator would instead over-count under duplicate computes.
 *
 * `add` is expected to be called once per task (e.g. from a task completion listener) with that
 * partition's value, so a partition is recorded even when it produced nothing. Updates from
 * failed/interrupted tasks are dropped by the accumulator framework (it is not
 * `countFailedValues`), so only complete per-partition values are ever merged.
 *
 * Backed by a `ConcurrentHashMap`. All access is synchronized so a caller can atomically verify
 * that every partition completed and read its statistics from the same stable snapshot.
 * Framework-facing reads remain safe, weakly consistent views.
 *
 * @tparam T the per-partition value type. Must be non-null (`ConcurrentHashMap` forbids nulls).
 */
class PartitionKeyedAccumulator[T] extends AccumulatorV2[(Int, T), java.util.Map[Int, T]] {

  // partition id -> value. Deliberately a lazily created `var` rather than a `val`: Java
  // deserialization assigns a subclass's fields only AFTER `AccumulatorV2.readObject` has already
  // registered `this` with the `TaskContext`, so another thread can reach this accumulator while
  // the map is still unset -- in production the executor heartbeater, which calls `isZero` on every
  // registered accumulator. A final field would additionally leave that thread no guarantee of ever
  // seeing the post-publication reflective write (JLS 17.5.3). All access goes through
  // `getOrCreate`; see SPARK-20977, which fixed the same hazard in `CollectionAccumulator`.
  private var byPartition: ConcurrentHashMap[Int, T] = _

  private def getOrCreate: ConcurrentHashMap[Int, T] = {
    if (byPartition == null) {
      byPartition = new ConcurrentHashMap[Int, T]()
    }
    byPartition
  }

  override def isZero: Boolean = synchronized {
    getOrCreate.isEmpty
  }

  override def copyAndReset(): PartitionKeyedAccumulator[T] = new PartitionKeyedAccumulator[T]

  override def copy(): PartitionKeyedAccumulator[T] = synchronized {
    val newAcc = new PartitionKeyedAccumulator[T]
    newAcc.getOrCreate.putAll(getOrCreate)
    newAcc
  }

  override def reset(): Unit = synchronized {
    getOrCreate.clear()
  }

  override def add(v: (Int, T)): Unit = synchronized {
    getOrCreate.put(v._1, v._2)
  }

  override def merge(other: AccumulatorV2[(Int, T), java.util.Map[Int, T]]): Unit = other match {
    case o: PartitionKeyedAccumulator[T] =>
      // Resolve the source map before taking our own monitor. Acquiring `o`'s monitor while
      // holding ours would leave the two merge directions with opposite lock orders, so a
      // concurrent `o.merge(this)` could deadlock against this call.
      val source = o.value
      // Last-write-wins per partition id: a partition recorded by more than one task replaces
      // rather than accumulates, keeping any caller-derived aggregate exact.
      synchronized {
        getOrCreate.putAll(source)
      }
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  // A read-only VIEW over the live map -- no copy. Only the accumulator framework calls `value`
  // (event log / `toInfo` / `toString`); our own code reads via `accumulatedNumPartitions` /
  // `foldValues`. The view is thread-safe (ConcurrentHashMap) and weakly consistent, which matches
  // this accumulator's eventual-consistency contract.
  override def value: java.util.Map[Int, T] = synchronized {
    java.util.Collections.unmodifiableMap(getOrCreate)
  }

  /** Number of distinct partitions that have been recorded. */
  def accumulatedNumPartitions: Long = synchronized {
    getOrCreate.size().toLong
  }

  private def foldValuesUnsafe[A](zero: A)(op: (A, T) => A): A = {
    var result = zero
    val it = getOrCreate.values().iterator()
    while (it.hasNext) result = op(result, it.next())
    result
  }

  /** Folds the per-partition values (each partition counted once) into a single aggregate. */
  def foldValues[A](zero: A)(op: (A, T) => A): A = synchronized {
    foldValuesUnsafe(zero)(op)
  }

  /** Atomically checks that all partitions completed and folds their statistics. */
  def foldValuesIfComplete[A](
      expectedNumPartitions: Int,
      zero: A)(
      op: (A, T) => A): Option[A] = synchronized {
    if (getOrCreate.size() == expectedNumPartitions) {
      Some(foldValuesUnsafe(zero)(op))
    } else {
      None
    }
  }
}
