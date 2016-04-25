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

import java.{lang => jl}
import java.io.{ObjectInputStream, ObjectOutputStream}
import java.util.concurrent.atomic.AtomicLong
import javax.annotation.concurrent.GuardedBy

import org.apache.spark.scheduler.AccumulableInfo
import org.apache.spark.util.Utils


private[spark] case class AccumulatorMetadata(
    id: Long,
    name: Option[String],
    countFailedValues: Boolean) extends Serializable

trait UpdatedValue extends Serializable

private[spark] class UpdatedValueString(s: String) extends UpdatedValue {
  override def toString: String = s
}

private[spark] case class AccumulatorUpdates(
    id: Long,
    name: Option[String],
    countFailedValues: Boolean,
    value: UpdatedValue) extends Serializable

abstract class NewAccumulator[IN, OUT] extends Serializable {
  private[spark] var metadata: AccumulatorMetadata = _
  private[this] var atDriverSide = true

  private[spark] def register(
      sc: SparkContext,
      name: Option[String] = None,
      countFailedValues: Boolean = false): Unit = {
    if (this.metadata != null) {
      throw new IllegalStateException("Cannot register an Accumulator twice.")
    }
    this.metadata = AccumulatorMetadata(AccumulatorContext.newId(), name, countFailedValues)
    AccumulatorContext.register(this)
    sc.cleaner.foreach(_.registerAccumulatorForCleanup(this))
  }

  def id: Long = {
    assert(metadata != null, "Cannot get accumulator id with null metadata")
    metadata.id
  }

  def name: Option[String] = {
    assert(metadata != null, "Cannot get accumulator name with null metadata")
    metadata.name
  }

  def countFailedValues: Boolean = {
    assert(metadata != null, "Cannot get accumulator countFailedValues with null metadata")
    metadata.countFailedValues
  }

  private[spark] def toInfo(update: Option[Any], value: Option[Any]): AccumulableInfo = {
    val isInternal = name.exists(_.startsWith(InternalAccumulator.METRICS_PREFIX))
    new AccumulableInfo(id, name, update, value, isInternal, countFailedValues)
  }

  final private[spark] def isAtDriverSide: Boolean = atDriverSide

  final def isRegistered: Boolean =
    metadata != null && AccumulatorContext.originals.containsKey(metadata.id)

  def initialize(): Unit = {}

  def add(v: IN): Unit

  def +=(v: IN): Unit = add(v)

  def updatedValue: UpdatedValue

  def isNoOp(updates: UpdatedValue): Boolean

  def applyUpdates(updates: UpdatedValue): Unit

  final def value: OUT = {
    if (atDriverSide) {
      localValue
    } else {
      throw new UnsupportedOperationException("Can't read accumulator value in task")
    }
  }

  def localValue: OUT

  private[spark] def getUpdates: AccumulatorUpdates =
    AccumulatorUpdates(id, name, countFailedValues, updatedValue)

  // Called by Java when serializing an object
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    if (atDriverSide && !isRegistered) {
      throw new IllegalStateException(
        "Accumulator must be registered before serialize and send to executor")
    }
    out.defaultWriteObject()
  }

  // Called by Java when deserializing an object
  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    in.defaultReadObject()
    initialize()
    atDriverSide = false

    // Automatically register the accumulator when it is deserialized with the task closure.
    // This is for external accumulators and internal ones that do not represent task level
    // metrics, e.g. internal SQL metrics, which are per-operator.
    val taskContext = TaskContext.get()
    if (taskContext != null) {
      taskContext.registerAccumulator(this)
    }
  }
}


private[spark] object AccumulatorContext {

  /**
   * This global map holds the original accumulator objects that are created on the driver.
   * It keeps weak references to these objects so that accumulators can be garbage-collected
   * once the RDDs and user-code that reference them are cleaned up.
   * TODO: Don't use a global map; these should be tied to a SparkContext (SPARK-13051).
   */
  @GuardedBy("AccumulatorContext")
  val originals = new java.util.HashMap[Long, jl.ref.WeakReference[NewAccumulator[_, _]]]

  private[this] val nextId = new AtomicLong(0L)

  /**
   * Return a globally unique ID for a new [[Accumulator]].
   * Note: Once you copy the [[Accumulator]] the ID is no longer unique.
   */
  def newId(): Long = nextId.getAndIncrement

  /**
   * Register an [[Accumulator]] created on the driver such that it can be used on the executors.
   *
   * All accumulators registered here can later be used as a container for accumulating partial
   * values across multiple tasks. This is what [[org.apache.spark.scheduler.DAGScheduler]] does.
   * Note: if an accumulator is registered here, it should also be registered with the active
   * context cleaner for cleanup so as to avoid memory leaks.
   *
   * If an [[Accumulator]] with the same ID was already registered, this does nothing instead
   * of overwriting it. We will never register same accumulator twice, this is just a sanity check.
   */
  def register(a: NewAccumulator[_, _]): Unit = synchronized {
    if (!originals.containsKey(a.id)) {
      originals.put(a.id, new jl.ref.WeakReference[NewAccumulator[_, _]](a))
    }
  }

  /**
   * Unregister the [[Accumulator]] with the given ID, if any.
   */
  def remove(id: Long): Unit = synchronized {
    originals.remove(id)
  }

  /**
   * Return the [[Accumulator]] registered with the given ID, if any.
   */
  def get(id: Long): Option[NewAccumulator[_, _]] = synchronized {
    Option(originals.get(id)).map { ref =>
      // Since we are storing weak references, we must check whether the underlying data is valid.
      val acc = ref.get
      if (acc eq null) {
        throw new IllegalAccessError(s"Attempted to access garbage collected accumulator $id")
      }
      acc
    }
  }

  /**
   * Clear all registered [[Accumulator]]s. For testing only.
   */
  def clear(): Unit = synchronized {
    originals.clear()
  }
}


case class UpdatedLongValue(l: Long) extends UpdatedValue {
  override def toString: String = l.toString
}

class LongAccumulator extends NewAccumulator[jl.Long, jl.Long] {
  @transient private[this] var _sum = 0L

  override def updatedValue: UpdatedValue = new UpdatedLongValue(_sum)

  override def add(v: jl.Long): Unit = _sum += v

  private[spark] def unboxAdd(v: Long): Unit = _sum += v

  private[spark] def unboxValue: Long = _sum

  private[spark] def setValue(newValue: Long): Unit = _sum = newValue

  override def isNoOp(updates: UpdatedValue): Boolean = {
    val v = updates.asInstanceOf[UpdatedLongValue].l
    v == 0
  }

  override def applyUpdates(updates: UpdatedValue): Unit = {
    val v = updates.asInstanceOf[UpdatedLongValue].l
    _sum += v
  }

  override def localValue: jl.Long = _sum
}


case class UpdatedDoubleValue(d: Double) extends UpdatedValue {
  override def toString: String = d.toString
}

class DoubleAccumulator extends NewAccumulator[jl.Double, jl.Double] {
  @transient private[this] var _sum = 0.0

  override def updatedValue: UpdatedValue = new UpdatedDoubleValue(_sum)

  override def add(v: jl.Double): Unit = _sum += v

  private[spark] def unboxAdd(v: Double): Unit = _sum += v

  private[spark] def unboxValue: Double = _sum

  private[spark] def setValue(newValue: Double): Unit = _sum = newValue

  override def isNoOp(updates: UpdatedValue): Boolean = {
    val v = updates.asInstanceOf[UpdatedDoubleValue].d
    v == 0.0
  }

  override def applyUpdates(updates: UpdatedValue): Unit = {
    val v = updates.asInstanceOf[UpdatedDoubleValue].d
    _sum += v
  }

  override def localValue: jl.Double = _sum
}


case class UpdatedAverageValue(sum: Double, count: Long) extends UpdatedValue {
  override def toString: String = s"sum: $sum, count: $count"
}

class AverageAccumulator extends NewAccumulator[jl.Double, jl.Double] {
  @transient private[this] var _sum = 0.0
  @transient private[this] var _count = 0L

  override def updatedValue: UpdatedValue = new UpdatedAverageValue(_sum, _count)

  override def add(v: jl.Double): Unit = {
    _sum += v
    _count += 1
  }

  override def isNoOp(updates: UpdatedValue): Boolean = {
    val v = updates.asInstanceOf[UpdatedAverageValue]
    v.sum == 0.0 && v.count == 0
  }

  override def applyUpdates(updates: UpdatedValue): Unit = {
    val v = updates.asInstanceOf[UpdatedAverageValue]
    _sum += v.sum
    _count += v.count
  }

  override def localValue: jl.Double = if (_count == 0) {
    Double.NaN
  } else {
    _sum / _count
  }

  def sum: Double = _sum

  def count: Long = _count
}


case class GenericUpdatedValue[T](value: T) extends UpdatedValue {
  override def toString: String = value.toString
}


class CollectionAccumulator[T] extends NewAccumulator[T, java.util.List[T]] {
  @transient private[this] var _list: java.util.List[T] = new java.util.ArrayList[T]

  override def updatedValue: UpdatedValue = new GenericUpdatedValue(_list)

  override def initialize(): Unit = _list = new java.util.ArrayList[T]

  override def add(v: T): Unit = _list.add(v)

  override def isNoOp(updates: UpdatedValue): Boolean = {
    val v = updates.asInstanceOf[GenericUpdatedValue[java.util.List[T]]].value
    v.isEmpty
  }

  override def applyUpdates(updates: UpdatedValue): Unit = {
    val v = updates.asInstanceOf[GenericUpdatedValue[java.util.List[T]]].value
    _list.addAll(v)
  }

  override def localValue: java.util.List[T] = java.util.Collections.unmodifiableList(_list)
}


class LegacyAccumulatorWrapper[R, T](
    initialValue: R,
    param: org.apache.spark.AccumulableParam[R, T]) extends NewAccumulator[T, R] {

  @transient private[spark] var _value = initialValue  // Current value on driver
  val zero = param.zero(initialValue) // Zero value to be passed to executors

  override def updatedValue: UpdatedValue = new GenericUpdatedValue(_value)

  override def initialize(): Unit = _value = param.zero(initialValue)

  override def add(v: T): Unit = _value = param.addAccumulator(_value, v)

  override def isNoOp(updates: UpdatedValue): Boolean = {
    val v = updates.asInstanceOf[GenericUpdatedValue[R]].value
    v == zero
  }

  override def applyUpdates(updates: UpdatedValue): Unit = {
    val v = updates.asInstanceOf[GenericUpdatedValue[R]].value
    _value = param.addInPlace(_value, v)
  }

  override def localValue: R = _value
}
