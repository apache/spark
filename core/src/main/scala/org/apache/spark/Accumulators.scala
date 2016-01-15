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

package org.apache.spark

import java.io.{ObjectInputStream, Serializable}
import java.util.concurrent.atomic.AtomicLong
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable
import scala.ref.WeakReference

import org.apache.spark.util.Utils


/**
 * A data type that can be accumulated, ie has an commutative and associative "add" operation,
 * but where the result type, `R`, may be different from the element type being added, `T`.
 *
 * You must define how to add data, and how to merge two of these together.  For some data types,
 * such as a counter, these might be the same operation. In that case, you can use the simpler
 * [[org.apache.spark.Accumulator]]. They won't always be the same, though -- e.g., imagine you are
 * accumulating a set. You will add items to the set, and you will union two sets together.
 *
 * All accumulators created on the driver to be used on the executors must be registered with
 * [[Accumulators]]. This is already done automatically for accumulators created by the user.
 * Internal accumulators must be explicitly registered by the caller.
 *
 * Operations are not thread-safe.
 *
 * @param id ID of this accumulator; for internal use only.
 * @param initialValue initial value of accumulator
 * @param param helper object defining how to add elements of type `R` and `T`
 * @param name human-readable name for use in Spark's web UI
 * @param internal if this [[Accumulable]] is internal. Internal [[Accumulable]]s will be reported
 *                 to the driver via heartbeats. For internal [[Accumulable]]s, `R` must be
 *                 thread safe so that they can be reported correctly.
 * @param countFailedValues whether to accumulate values from failed tasks. This is set to true
 *                          for system and time metrics like serialization time or bytes spilled,
 *                          and false for things with absolute values like number of input rows.
 *                          This should be used for internal metrics only.
 * @tparam R the full accumulated data (result type)
 * @tparam T partial data that can be added in
 */
class Accumulable[R, T] private (
    val id: Long,
    @transient initialValue: R,
    param: AccumulableParam[R, T],
    val name: Option[String],
    internal: Boolean,
    val countFailedValues: Boolean)
  extends Serializable {

  private[spark] def this(
      initialValue: R,
      param: AccumulableParam[R, T],
      name: Option[String],
      internal: Boolean,
      countFailedValues: Boolean) = {
    this(Accumulators.newId(), initialValue, param, name, internal, countFailedValues)
  }

  private[spark] def this(
      initialValue: R,
      param: AccumulableParam[R, T],
      name: Option[String],
      internal: Boolean) = {
    this(initialValue, param, name, internal, false /* countFailedValues */)
  }

  def this(initialValue: R, param: AccumulableParam[R, T], name: Option[String]) =
    this(initialValue, param, name, false)

  def this(initialValue: R, param: AccumulableParam[R, T]) = this(initialValue, param, None)

  @volatile @transient private var value_ : R = initialValue // Current value on driver
  val zero = param.zero(initialValue)  // Zero value to be passed to executors
  private var deserialized = false

  // In many places we create internal accumulators without access to the active context cleaner,
  // so if we register them here then we may never unregister these accumulators. To avoid memory
  // leaks, we require the caller to explicitly register internal accumulators elsewhere.
  if (!internal) {
    Accumulators.register(this)
  }

  /**
   * If this [[Accumulable]] is internal. Internal [[Accumulable]]s will be reported to the driver
   * via heartbeats. For internal [[Accumulable]]s, `R` must be thread safe so that they can be
   * reported correctly.
   */
  private[spark] def isInternal: Boolean = internal

  /**
   * Return a copy of this [[Accumulable]].
   *
   * The copy will have the same ID as the original and will not be registered with
   * [[Accumulators]] again. This method exists so that the caller can avoid passing the
   * same mutable instance around.
   */
  private[spark] def copy(): Accumulable[R, T] = {
    new Accumulable[R, T](id, initialValue, param, name, internal, countFailedValues)
  }

  /**
   * Add more data to this accumulator / accumulable
   * @param term the data to add
   */
  def += (term: T): Unit = { value_ = param.addAccumulator(value_, term) }

  /**
   * Add more data to this accumulator / accumulable
   * @param term the data to add
   */
  def add(term: T): Unit = { value_ = param.addAccumulator(value_, term) }

  /**
   * Merge two accumulable objects together
   *
   * Normally, a user will not want to use this version, but will instead call `+=`.
   * @param term the other `R` that will get merged with this
   */
  def ++= (term: R): Unit = { value_ = param.addInPlace(value_, term) }

  /**
   * Merge two accumulable objects together
   *
   * Normally, a user will not want to use this version, but will instead call `add`.
   * @param term the other `R` that will get merged with this
   */
  def merge(term: R): Unit = { value_ = param.addInPlace(value_, term) }

  /**
   * Access the accumulator's current value; only allowed on driver.
   */
  def value: R = {
    if (!deserialized) {
      value_
    } else {
      throw new UnsupportedOperationException("Can't read accumulator value in task")
    }
  }

  /**
   * Get the current value of this accumulator from within a task.
   *
   * This is NOT the global value of the accumulator.  To get the global value after a
   * completed operation on the dataset, call `value`.
   *
   * The typical use of this method is to directly mutate the local value, eg., to add
   * an element to a Set.
   */
  def localValue: R = value_

  /**
   * Set the accumulator's value; only allowed on driver.
   */
  def value_= (newValue: R) {
    if (!deserialized) {
      value_ = newValue
    } else {
      throw new UnsupportedOperationException("Can't assign accumulator value in task")
    }
  }

  /**
   * Set the accumulator's value. For internal use only.
   */
  private[spark] def setValue(newValue: R): Unit = { value_ = newValue }

  /**
   * Set the accumulator's value.
   * This is used to reconstruct [[org.apache.spark.executor.TaskMetrics]] from accumulator updates.
   */
  private[spark] def setValueAny(newValue: Any): Unit = { setValue(newValue.asInstanceOf[R]) }

  // Called by Java when deserializing an object
  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    in.defaultReadObject()
    value_ = zero
    deserialized = true
    // Automatically register the accumulator when it is deserialized with the task closure.
    // This is for external accumulators and internal ones that do not represent task level
    // metrics, e.g. internal SQL metrics, which are per-operator.
    val taskContext = TaskContext.get()
    if (taskContext != null) {
      taskContext.registerAccumulator(this)
    }
  }

  override def toString: String = if (value_ == null) "null" else value_.toString
}


/**
 * A simpler value of [[Accumulable]] where the result type being accumulated is the same
 * as the types of elements being merged, i.e. variables that are only "added" to through an
 * associative operation and can therefore be efficiently supported in parallel. They can be used
 * to implement counters (as in MapReduce) or sums. Spark natively supports accumulators of numeric
 * value types, and programmers can add support for new types.
 *
 * An accumulator is created from an initial value `v` by calling [[SparkContext#accumulator]].
 * Tasks running on the cluster can then add to it using the [[Accumulable#+=]] operator.
 * However, they cannot read its value. Only the driver program can read the accumulator's value,
 * using its value method.
 *
 * The interpreter session below shows an accumulator being used to add up the elements of an array:
 *
 * {{{
 * scala> val accum = sc.accumulator(0)
 * accum: spark.Accumulator[Int] = 0
 *
 * scala> sc.parallelize(Array(1, 2, 3, 4)).foreach(x => accum += x)
 * ...
 * 10/09/29 18:41:08 INFO SparkContext: Tasks finished in 0.317106 s
 *
 * scala> accum.value
 * res2: Int = 10
 * }}}
 *
 * @param initialValue initial value of accumulator
 * @param param helper object defining how to add elements of type `T`
 * @param name human-readable name associated with this accumulator
 * @param internal whether this accumulator is used internally within Spark only
 * @param countFailedValues whether to accumulate values from failed tasks
 * @tparam T result type
 */
class Accumulator[T] private[spark] (
    @transient private[spark] val initialValue: T,
    param: AccumulatorParam[T],
    name: Option[String],
    internal: Boolean,
    override val countFailedValues: Boolean = false)
  extends Accumulable[T, T](initialValue, param, name, internal, countFailedValues) {

  def this(initialValue: T, param: AccumulatorParam[T], name: Option[String]) = {
    this(initialValue, param, name, false)
  }

  def this(initialValue: T, param: AccumulatorParam[T]) = {
    this(initialValue, param, None, false)
  }
}


// TODO: The multi-thread support in accumulators is kind of lame; check
// if there's a more intuitive way of doing it right
private[spark] object Accumulators extends Logging {
  /**
   * This global map holds the original accumulator objects that are created on the driver.
   * It keeps weak references to these objects so that accumulators can be garbage-collected
   * once the RDDs and user-code that reference them are cleaned up.
   * TODO: Don't use a global map; these should be tied to a SparkContext at the very least.
   */
  @GuardedBy("Accumulators")
  val originals = mutable.Map[Long, WeakReference[Accumulable[_, _]]]()

  private val nextId = new AtomicLong(0L)

  /**
   * Return a globally unique ID for a new [[Accumulable]].
   * Note: Once you copy the [[Accumulable]] the ID is no longer unique.
   */
  def newId(): Long = nextId.getAndIncrement

  /**
   * Register an [[Accumulable]] created on the driver such that it can be used on the executors.
   *
   * All accumulators registered here can later be used as a container for accumulating partial
   * values across multiple tasks. This is what [[org.apache.spark.scheduler.DAGScheduler]] does.
   * Note: if an accumulator is registered here, it should also be registered with the active
   * context cleaner for cleanup so as to avoid memory leaks.
   *
   * If an [[Accumulable]] with the same ID was already registered, do nothing instead of
   * overwriting it. This happens when we copy accumulators, e.g. when we reconstruct
   * [[org.apache.spark.executor.TaskMetrics]] from accumulator updates.
   */
  def register(a: Accumulable[_, _]): Unit = synchronized {
    if (!originals.contains(a.id)) {
      originals(a.id) = new WeakReference[Accumulable[_, _]](a)
    }
  }

  /**
   * Unregister the [[Accumulable]] with the given ID, if any.
   */
  def remove(accId: Long): Unit = synchronized {
    originals.remove(accId)
  }

  /**
   * Return the [[Accumulable]] registered with the given ID, if any.
   */
  def get(id: Long): Option[Accumulable[_, _]] = synchronized {
    originals.get(id).map { weakRef =>
      // Since we are storing weak references, we must check whether the underlying data is valid.
      weakRef.get match {
        case Some(accum) => accum
        case None =>
          throw new IllegalAccessError(s"Attempted to access garbage collected accumulator $id")
      }
    }
  }

  /**
   * Clear all registered [[Accumulable]]s; for testing only.
   */
  def clear(): Unit = synchronized {
    originals.clear()
  }

}
