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
import java.io.ObjectInputStream
import java.util.concurrent.atomic.AtomicLong
import javax.annotation.concurrent.GuardedBy

import org.apache.spark.scheduler.AccumulableInfo
import org.apache.spark.util.Utils


private[spark] case class AccumulatorMetadata(
    id: Long,
    name: Option[String],
    countFailedValues: Boolean) extends Serializable


/**
 * The base class for accumulators, that can accumulate inputs of type `IN`, and produce output of
 * type `OUT`.  Implementations must define following methods:
 *  - isZero:       tell if this accumulator is zero value or not. e.g. for a counter accumulator,
 *                  0 is zero value; for a list accumulator, Nil is zero value.
 *  - copyAndReset: create a new copy of this accumulator, which is zero value. i.e. call `isZero`
 *                  on the copy must return true.
 *  - add:          defines how to accumulate the inputs. e.g. it can be a simple `+=` for counter
 *                  accumulator
 *  - merge:        defines how to merge another accumulator of same type.
 *  - localValue:   defines how to produce the output by the current state of this accumulator.
 *
 * The implementations decide how to store intermediate values, e.g. a long field for a counter
 * accumulator, a double and a long field for a average accumulator(storing the sum and count).
 */
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

  final def isRegistered: Boolean =
    metadata != null && AccumulatorContext.originals.containsKey(metadata.id)

  private def assertMetadataNotNull(): Unit = {
    if (metadata == null) {
      throw new IllegalAccessError("The metadata of this accumulator has not been assigned yet.")
    }
  }

  final def id: Long = {
    assertMetadataNotNull()
    metadata.id
  }

  final def name: Option[String] = {
    assertMetadataNotNull()
    metadata.name
  }

  final def countFailedValues: Boolean = {
    assertMetadataNotNull()
    metadata.countFailedValues
  }

  private[spark] def toInfo(update: Option[Any], value: Option[Any]): AccumulableInfo = {
    val isInternal = name.exists(_.startsWith(InternalAccumulator.METRICS_PREFIX))
    new AccumulableInfo(id, name, update, value, isInternal, countFailedValues)
  }

  final private[spark] def isAtDriverSide: Boolean = atDriverSide

  def isZero(): Boolean

  def copyAndReset(): NewAccumulator[IN, OUT]

  def add(v: IN): Unit

  def +=(v: IN): Unit = add(v)

  def merge(other: NewAccumulator[IN, OUT]): Unit

  final def value: OUT = {
    if (atDriverSide) {
      localValue
    } else {
      throw new UnsupportedOperationException("Can't read accumulator value in task")
    }
  }

  def localValue: OUT

  // Called by Java when serializing an object
  final protected def writeReplace(): Any = {
    if (atDriverSide) {
      if (!isRegistered) {
        throw new UnsupportedOperationException(
          "Accumulator must be registered before send to executor")
      }
      val copy = copyAndReset()
      assert(copy.isZero(), "copyAndReset must return a zero value copy")
      copy.metadata = metadata
      copy
    } else {
      this
    }
  }

  // Called by Java when deserializing an object
  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    in.defaultReadObject()
    if (atDriverSide) {
      atDriverSide = false

      // Automatically register the accumulator when it is deserialized with the task closure.
      // This is for external accumulators and internal ones that do not represent task level
      // metrics, e.g. internal SQL metrics, which are per-operator.
      val taskContext = TaskContext.get()
      if (taskContext != null) {
        taskContext.registerAccumulator(this)
      }
    } else {
      atDriverSide = true
    }
  }

  override def toString: String = {
    if (metadata == null) {
      "Un-registered Accumulator: " + getClass.getSimpleName
    } else {
      getClass.getSimpleName + s"(id: $id, name: $name, value: $localValue)"
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


class LongAccumulator extends NewAccumulator[jl.Long, jl.Long] {
  private[this] var _sum = 0L

  override def isZero(): Boolean = _sum == 0

  override def copyAndReset(): LongAccumulator = new LongAccumulator

  override def add(v: jl.Long): Unit = _sum += v

  def add(v: Long): Unit = _sum += v

  def sum: Long = _sum

  override def merge(other: NewAccumulator[jl.Long, jl.Long]): Unit = other match {
    case o: LongAccumulator => _sum += o.sum
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  private[spark] def setValue(newValue: Long): Unit = _sum = newValue

  override def localValue: jl.Long = _sum
}


class DoubleAccumulator extends NewAccumulator[jl.Double, jl.Double] {
  private[this] var _sum = 0.0

  override def isZero(): Boolean = _sum == 0.0

  override def copyAndReset(): DoubleAccumulator = new DoubleAccumulator

  override def add(v: jl.Double): Unit = _sum += v

  def add(v: Double): Unit = _sum += v

  def sum: Double = _sum

  override def merge(other: NewAccumulator[jl.Double, jl.Double]): Unit = other match {
    case o: DoubleAccumulator => _sum += o.sum
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  private[spark] def setValue(newValue: Double): Unit = _sum = newValue

  override def localValue: jl.Double = _sum
}


class AverageAccumulator extends NewAccumulator[jl.Double, jl.Double] {
  private[this] var _sum = 0.0
  private[this] var _count = 0L

  override def isZero(): Boolean = _sum == 0.0 && _count == 0

  override def copyAndReset(): AverageAccumulator = new AverageAccumulator

  override def add(v: jl.Double): Unit = {
    _sum += v
    _count += 1
  }

  def add(d: Double): Unit = {
    _sum += d
    _count += 1
  }

  override def merge(other: NewAccumulator[jl.Double, jl.Double]): Unit = other match {
    case o: AverageAccumulator =>
      _sum += o.sum
      _count += o.count
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def localValue: jl.Double = if (_count == 0) {
    Double.NaN
  } else {
    _sum / _count
  }

  def sum: Double = _sum

  def count: Long = _count
}


class ListAccumulator[T] extends NewAccumulator[T, java.util.List[T]] {
  private[this] val _list: java.util.List[T] = new java.util.ArrayList[T]

  override def isZero(): Boolean = _list.isEmpty

  override def copyAndReset(): ListAccumulator[T] = new ListAccumulator

  override def add(v: T): Unit = _list.add(v)

  override def merge(other: NewAccumulator[T, java.util.List[T]]): Unit = other match {
    case o: ListAccumulator[T] => _list.addAll(o.localValue)
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def localValue: java.util.List[T] = java.util.Collections.unmodifiableList(_list)
}


class LegacyAccumulatorWrapper[R, T](
    initialValue: R,
    param: org.apache.spark.AccumulableParam[R, T]) extends NewAccumulator[T, R] {
  private[spark] var _value = initialValue  // Current value on driver

  override def isZero(): Boolean = _value == param.zero(initialValue)

  override def copyAndReset(): LegacyAccumulatorWrapper[R, T] = {
    val acc = new LegacyAccumulatorWrapper(initialValue, param)
    acc._value = param.zero(initialValue)
    acc
  }

  override def add(v: T): Unit = _value = param.addAccumulator(_value, v)

  override def merge(other: NewAccumulator[T, R]): Unit = other match {
    case o: LegacyAccumulatorWrapper[R, T] => _value = param.addInPlace(_value, o.localValue)
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def localValue: R = _value
}
