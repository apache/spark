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

package org.apache.spark.sql

import scala.collection.mutable

/**
 * An accumulator whose value is carried through the query plan and aggregated by a
 * `CollectMetrics` (see `Dataset.observe`) node, rather than through the scheduler's accumulator
 * side channel. Because the value is derived from the rows that actually survive at the observe
 * point, it is exactly-once: task retries, speculation, and stage recomputation do not double
 * count.
 *
 * The surface matches a classic accumulator: create it from the session, call `add` inside any
 * UDF, run an action, then read `value` on the driver -- no wrapper API. A plain UDF that
 * references the accumulator is detected (the closure is inspected) and rewritten by the analyzer
 * so its per-row delta is observed. Reads are cumulative across queries. This client-facing API
 * lives in the shared `sql/api` module so it works for both the classic and Spark Connect
 * sessions; the analyzer rule and value harvesting are provided by the runtime (see the
 * `InjectObservedAccumulators` rule in `sql/core` and the session hooks below).
 *
 * Scope: this only backs accumulation performed inside a UDF whose output flows through a plan
 * node. It cannot back `add` inside arbitrary RDD closures (there is no plan node for observe to
 * attach to), nor merge semantics that are not expressible as a SQL aggregate.
 */
class ObservedAccumulator private[sql] (
    @transient private val session: SparkSession,
    val name: String,
    private val zero: Long)
    extends ObservedAccumulatorLike {

  /** Key under which per-row deltas are buffered on the executor for this accumulator. */
  private val key: String = ObservedAccumulator.deltaKey(name)

  /** Add `term` to this accumulator for the current row. Call inside a UDF. */
  def add(term: Long): Unit = ObservedAccumulator.addTo(key, term.toDouble)

  /** Add a fractional `term` (for double-valued accumulators). Call inside a UDF. */
  def add(term: Double): Unit = ObservedAccumulator.addTo(key, term)

  /** Add one to this accumulator for the current row (counter semantics). */
  def add(): Unit = add(1L)

  // Cumulative delta on the driver, from a runtime-specific source (classic JVM registry or Spark
  // Connect client registry) via the SparkSession hook, filled from the injected observe node.
  private def harvested: Double = {
    ObservedAccumulator.checkSession(session, name)
    if (session != null) session.observedAccumulatorValue(name) else 0.0
  }

  /**
   * The accumulated value on the driver as a `Long` (rounded), cumulative across queries.
   *
   * Note: the Scala accumulator buffers its delta as a `Double` (both `add` overloads feed one
   * numeric buffer), so the result is exact only within a `Double`'s 53-bit integer range; a
   * total beyond +/-2^53 loses precision. Use `doubleValue` if you are accumulating fractional
   * values. (The PySpark integer accumulator keeps an exact `Long` because its delta type is
   * fixed by the `zero` value.)
   */
  def value: Long = zero + math.round(harvested)

  /** The accumulated value on the driver as a `Double`, cumulative across queries. */
  def doubleValue: Double = zero.toDouble + harvested
}

/**
 * Common marker for the observe-backed accumulators, so the analyzer rule's closure reflection
 * finds either the numeric [[ObservedAccumulator]] or the typed [[TypedObservedAccumulator]].
 */
private[sql] trait ObservedAccumulatorLike extends Serializable {
  def name: String
}

/**
 * An observe-backed accumulator of an arbitrary type `T` with a user `merge` -- the analog of a
 * classic `AccumulatorV2` for the DataFrame/UDF path. Each task folds its rows into a partial
 * with `merge` (starting from `zero`); the analyzer rule serializes the per-task partial, gathers
 * the partials with `collect_list`, and `value` folds them on the driver, exactly-once like the
 * numeric [[ObservedAccumulator]]. `T` (and `zero`) must be Java-serializable.
 */
class TypedObservedAccumulator[T] private[sql] (
    @transient private val session: SparkSession,
    val name: String,
    private val zero: T,
    private val merge: (T, T) => T)
    extends ObservedAccumulatorLike {

  private val key: String = ObservedAccumulator.objKey(name)

  // Driver-only running fold of the harvested partials (cumulative across queries).
  @transient private var folded: Option[T] = None

  /** Fold `term` into this task's partial for the current row/batch. Call inside a UDF. */
  def add(term: T): Unit = {
    val m = ObservedAccumulator.objBuffers.get()
    val cur = m.getOrElse(key, zero).asInstanceOf[T]
    m.update(key, merge(cur, term))
  }

  /** The accumulated value on the driver, cumulative across queries. */
  def value: T = {
    ObservedAccumulator.checkSession(session, name)
    val partials =
      if (session != null) session.observedAccumulatorPartials(name) else Array.empty[Array[Byte]]
    var acc = folded.getOrElse(zero)
    partials.foreach(bytes => acc = merge(acc, ObservedAccumulator.javaDeserialize[T](bytes)))
    folded = Some(acc)
    acc
  }
}

object ObservedAccumulator {

  // Prefixes shared across modules (rule in sql/core, Connect client capture in connect-common,
  // and the PySpark marker).
  private[sql] val MarkerPrefix = "__oa_udf::"
  private[sql] val NodePrefix = "__oa_node_"
  private[sql] val MetricPrefix = "__oa_metric_"
  private[sql] val StructPrefix = "__oa_struct_"

  private[sql] def deltaKey(name: String): String = "__oa_delta_" + name

  /**
   * Key under which a typed accumulator's per-task object partial is buffered on the executor.
   */
  private[sql] def objKey(name: String): String = "__oa_obj_" + name

  /** Name stamped on a UDF so the analyzer rule can recognize it (used by the PySpark path). */
  private[sql] def markerName(accName: String): String = MarkerPrefix + accName

  private[sql] def accNameFromMarker(n: String): Option[String] =
    if (n != null && n.startsWith(MarkerPrefix)) Some(n.substring(MarkerPrefix.length)) else None

  /**
   * Guard against cross-session use: an accumulator is harvested by the session that created it,
   * so reading it while a different session is active would silently return the zero value. Raise
   * instead; skip when no session is active (so normal single-session use never trips).
   */
  private[sql] def checkSession(session: SparkSession, name: String): Unit = {
    if (session != null) {
      SparkSession.getActiveSession match {
        case Some(active) if !active.eq(session) =>
          throw new IllegalStateException(
            s"ObservedAccumulator '$name' was created by a different SparkSession than the " +
              "active one. Use and read an accumulator only with the session that created it.")
        case _ =>
      }
    }
  }

  // Per-invocation delta buffers on the executor's task thread. Lives in sql/api so it is present
  // wherever the UDF runs (classic executor or Connect server executor). The injected wrapper
  // resets the buffer before evaluating the UDF and reads it back after, so `add` calls in between
  // accumulate into the current row's delta.
  private val buffers: ThreadLocal[mutable.Map[String, Double]] =
    ThreadLocal.withInitial(() => mutable.Map.empty[String, Double])

  private[sql] def reset(key: String): Unit = buffers.get().update(key, 0.0)

  private[sql] def addTo(key: String, term: Double): Unit = {
    val m = buffers.get()
    m.update(key, m.getOrElse(key, 0.0) + term)
  }

  private[sql] def take(key: String): Double = {
    val m = buffers.get()
    val v = m.getOrElse(key, 0.0)
    m.remove(key)
    v
  }

  // Per-invocation object partials for typed (custom-merge) accumulators, parallel to `buffers`.
  // The injected wrapper clears the key before the UDF and reads/serializes the partial after; the
  // typed accumulator's `add` folds into it with the user's `merge`.
  private[sql] val objBuffers: ThreadLocal[mutable.Map[String, Any]] =
    ThreadLocal.withInitial(() => mutable.Map.empty[String, Any])

  private[sql] def objClear(key: String): Unit = objBuffers.get().remove(key)

  private[sql] def objTake(key: String): Option[Any] = objBuffers.get().remove(key)

  /**
   * Take the typed accumulator's per-task partial at `key` and Java-serialize it to a `Binary`,
   * or `null` when the UDF added nothing (so the collect_list metric skips it). Shared by the
   * struct wrapper's interpreted `eval` and its generated code.
   */
  private[sql] def serializeTake(key: String): Array[Byte] = objTake(key) match {
    case Some(partial) => javaSerialize(partial)
    case None => null
  }

  /** Java-serialize a typed accumulator's partial (the analog of the Python path's pickle). */
  private[sql] def javaSerialize(value: Any): Array[Byte] = {
    val bos = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(bos)
    try oos.writeObject(value)
    finally oos.close()
    bos.toByteArray
  }

  private[sql] def javaDeserialize[T](bytes: Array[Byte]): T = {
    val ois = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(bytes))
    try ois.readObject().asInstanceOf[T]
    finally ois.close()
  }
}
