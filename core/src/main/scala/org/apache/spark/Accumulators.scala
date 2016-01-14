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

import scala.collection.generic.Growable
import scala.collection.mutable
import scala.ref.WeakReference
import scala.reflect.ClassTag

import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.storage.{BlockId, BlockStatus}
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
    val taskContext = TaskContext.get()
    if (taskContext != null) {
      taskContext.registerAccumulator(this)
    }
  }

  override def toString: String = if (value_ == null) "null" else value_.toString
}

/**
 * Helper object defining how to accumulate values of a particular type. An implicit
 * AccumulableParam needs to be available when you create [[Accumulable]]s of a specific type.
 *
 * @tparam R the full accumulated data (result type)
 * @tparam T partial data that can be added in
 */
trait AccumulableParam[R, T] extends Serializable {
  /**
   * Add additional data to the accumulator value. Is allowed to modify and return `r`
   * for efficiency (to avoid allocating objects).
   *
   * @param r the current value of the accumulator
   * @param t the data to be added to the accumulator
   * @return the new value of the accumulator
   */
  def addAccumulator(r: R, t: T): R

  /**
   * Merge two accumulated values together. Is allowed to modify and return the first value
   * for efficiency (to avoid allocating objects).
   *
   * @param r1 one set of accumulated data
   * @param r2 another set of accumulated data
   * @return both data sets merged together
   */
  def addInPlace(r1: R, r2: R): R

  /**
   * Return the "zero" (identity) value for an accumulator type, given its initial value. For
   * example, if R was a vector of N dimensions, this would return a vector of N zeroes.
   */
  def zero(initialValue: R): R
}

private[spark] class
GrowableAccumulableParam[R <% Growable[T] with TraversableOnce[T] with Serializable: ClassTag, T]
  extends AccumulableParam[R, T] {

  def addAccumulator(growable: R, elem: T): R = {
    growable += elem
    growable
  }

  def addInPlace(t1: R, t2: R): R = {
    t1 ++= t2
    t1
  }

  def zero(initialValue: R): R = {
    // We need to clone initialValue, but it's hard to specify that R should also be Cloneable.
    // Instead we'll serialize it to a buffer and load it back.
    val ser = new JavaSerializer(new SparkConf(false)).newInstance()
    val copy = ser.deserialize[R](ser.serialize(initialValue))
    copy.clear()   // In case it contained stuff
    copy
  }
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

/**
 * A simpler version of [[org.apache.spark.AccumulableParam]] where the only data type you can add
 * in is the same type as the accumulated value. An implicit AccumulatorParam object needs to be
 * available when you create Accumulators of a specific type.
 *
 * @tparam T type of value to accumulate
 */
trait AccumulatorParam[T] extends AccumulableParam[T, T] {
  def addAccumulator(t1: T, t2: T): T = {
    addInPlace(t1, t2)
  }
}

object AccumulatorParam {

  // The following implicit objects were in SparkContext before 1.2 and users had to
  // `import SparkContext._` to enable them. Now we move them here to make the compiler find
  // them automatically. However, as there are duplicate codes in SparkContext for backward
  // compatibility, please update them accordingly if you modify the following implicit objects.

  implicit object DoubleAccumulatorParam extends AccumulatorParam[Double] {
    def addInPlace(t1: Double, t2: Double): Double = t1 + t2
    def zero(initialValue: Double): Double = 0.0
  }

  implicit object IntAccumulatorParam extends AccumulatorParam[Int] {
    def addInPlace(t1: Int, t2: Int): Int = t1 + t2
    def zero(initialValue: Int): Int = 0
  }

  implicit object LongAccumulatorParam extends AccumulatorParam[Long] {
    def addInPlace(t1: Long, t2: Long): Long = t1 + t2
    def zero(initialValue: Long): Long = 0L
  }

  implicit object FloatAccumulatorParam extends AccumulatorParam[Float] {
    def addInPlace(t1: Float, t2: Float): Float = t1 + t2
    def zero(initialValue: Float): Float = 0f
  }

  // Note: when merging values, this param just adopts the newer value. This is used only
  // internally for things that shouldn't really be accumulated across tasks, like input
  // read method, which should be the same across all tasks in the same stage.
  private[spark] object StringAccumulatorParam extends AccumulatorParam[String] {
    def addInPlace(t1: String, t2: String): String = t2
    def zero(initialValue: String): String = ""
  }

  // Note: this is expensive as it makes a copy of the list every time the caller adds an item.
  // A better way to use this is to first accumulate the values yourself then them all at once.
  private[spark] class ListAccumulatorParam[T] extends AccumulatorParam[Seq[T]] {
    def addInPlace(t1: Seq[T], t2: Seq[T]): Seq[T] = t1 ++ t2
    def zero(initialValue: Seq[T]): Seq[T] = Seq.empty[T]
  }

  // For the internal metric that records what blocks are updated in a particular task
  private[spark] object UpdatedBlockStatusesAccumulatorParam
    extends ListAccumulatorParam[(BlockId, BlockStatus)]

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
   * Return a new globally unique ID for a new [[Accumulable]].
   * Note: Once you copy the [[Accumulable]] the ID is no longer unique.
   */
  def newId(): Long = nextId.getAndIncrement

  /**
   * Register an accumulator created on the driver such that it can be used on the executors.
   *
   * All accumulators registered here can later be used as a container for accumulating partial
   * values across multiple tasks. This is what [[org.apache.spark.scheduler.DAGScheduler]] does.
   * Note: if an accumulator is registered here, it should also be registered with the active
   * context cleaner for cleanup so as to avoid memory leaks.
   *
   * If an [[Accumulable]] with the same ID was already registered, do nothing instead of
   * overwriting it. This can happen when we copy accumulators, e.g. when we reconstruct
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

private[spark] object InternalAccumulator {

  import AccumulatorParam._

  // Prefixes used in names of internal task level metrics
  val METRICS_PREFIX = "internal.metrics."
  val SHUFFLE_READ_METRICS_PREFIX = METRICS_PREFIX + "shuffle.read."
  val SHUFFLE_WRITE_METRICS_PREFIX = METRICS_PREFIX + "shuffle.write."
  val OUTPUT_METRICS_PREFIX = METRICS_PREFIX + "output."
  val INPUT_METRICS_PREFIX = METRICS_PREFIX + "input."

  // Names of internal task level metrics
  val EXECUTOR_DESERIALIZE_TIME = METRICS_PREFIX + "executorDeserializeTime"
  val EXECUTOR_RUN_TIME = METRICS_PREFIX + "executorRunTime"
  val RESULT_SIZE = METRICS_PREFIX + "resultSize"
  val JVM_GC_TIME = METRICS_PREFIX + "jvmGCTime"
  val RESULT_SERIALIZATION_TIME = METRICS_PREFIX + "resultSerializationTime"
  val MEMORY_BYTES_SPILLED = METRICS_PREFIX + "memoryBytesSpilled"
  val DISK_BYTES_SPILLED = METRICS_PREFIX + "diskBytesSpilled"
  val PEAK_EXECUTION_MEMORY = METRICS_PREFIX + "peakExecutionMemory"
  val UPDATED_BLOCK_STATUSES = METRICS_PREFIX + "updatedBlockStatuses"
  val TEST_ACCUM = METRICS_PREFIX + "testAccumulator"

  // scalastyle:off

  // Names of shuffle read metrics
  object shuffleRead {
    val REMOTE_BLOCKS_FETCHED = SHUFFLE_READ_METRICS_PREFIX + "remoteBlocksFetched"
    val LOCAL_BLOCKS_FETCHED = SHUFFLE_READ_METRICS_PREFIX + "localBlocksFetched"
    val REMOTE_BYTES_READ = SHUFFLE_READ_METRICS_PREFIX + "remoteBytesRead"
    val LOCAL_BYTES_READ = SHUFFLE_READ_METRICS_PREFIX + "localBytesRead"
    val FETCH_WAIT_TIME = SHUFFLE_READ_METRICS_PREFIX + "fetchWaitTime"
    val RECORDS_READ = SHUFFLE_READ_METRICS_PREFIX + "recordsRead"
  }

  // Names of shuffle write metrics
  object shuffleWrite {
    val BYTES_WRITTEN = SHUFFLE_WRITE_METRICS_PREFIX + "bytesWritten"
    val RECORDS_WRITTEN = SHUFFLE_WRITE_METRICS_PREFIX + "recordsWritten"
    val WRITE_TIME = SHUFFLE_WRITE_METRICS_PREFIX + "writeTime"
  }

  // Names of output metrics
  object output {
    val WRITE_METHOD = OUTPUT_METRICS_PREFIX + "writeMethod"
    val BYTES_WRITTEN = OUTPUT_METRICS_PREFIX + "bytesWritten"
    val RECORDS_WRITTEN = OUTPUT_METRICS_PREFIX + "recordsWritten"
  }

  // Names of input metrics
  object input {
    val READ_METHOD = INPUT_METRICS_PREFIX + "readMethod"
    val BYTES_READ = INPUT_METRICS_PREFIX + "bytesRead"
    val RECORDS_READ = INPUT_METRICS_PREFIX + "recordsRead"
  }

  // scalastyle:on

  /**
   * Create an internal [[Accumulator]] by name, which must begin with [[METRICS_PREFIX]].
   */
  def create(name: String): Accumulator[_] = {
    assert(name.startsWith(METRICS_PREFIX),
      s"internal accumulator name must start with '$METRICS_PREFIX': $name")
    getParam(name) match {
      case p @ LongAccumulatorParam => newMetric[Long](0L, name, p)
      case p @ IntAccumulatorParam => newMetric[Int](0, name, p)
      case p @ StringAccumulatorParam => newMetric[String]("", name, p)
      case p @ UpdatedBlockStatusesAccumulatorParam =>
        newMetric[Seq[(BlockId, BlockStatus)]](Seq(), name, p)
      case p => throw new IllegalArgumentException(
        s"unsupported accumulator param '${p.getClass.getSimpleName}' for internal metrics.")
    }
  }

  /**
   * Get the [[AccumulatorParam]] associated with the internal metric name,
   * which must begin with [[METRICS_PREFIX]].
   */
  def getParam(name: String): AccumulatorParam[_] = {
    assert(name.startsWith(METRICS_PREFIX),
      s"internal accumulator name must start with '$METRICS_PREFIX': $name")
    name match {
      case UPDATED_BLOCK_STATUSES => UpdatedBlockStatusesAccumulatorParam
      case shuffleRead.LOCAL_BLOCKS_FETCHED => IntAccumulatorParam
      case shuffleRead.REMOTE_BLOCKS_FETCHED => IntAccumulatorParam
      case input.READ_METHOD => StringAccumulatorParam
      case output.WRITE_METHOD => StringAccumulatorParam
      case _ => LongAccumulatorParam
    }
  }

  /**
   * Accumulators for tracking internal metrics.
   */
  def create(): Seq[Accumulator[_]] = {
    Seq[String](
      EXECUTOR_DESERIALIZE_TIME,
      EXECUTOR_RUN_TIME,
      RESULT_SIZE,
      JVM_GC_TIME,
      RESULT_SERIALIZATION_TIME,
      MEMORY_BYTES_SPILLED,
      DISK_BYTES_SPILLED,
      PEAK_EXECUTION_MEMORY,
      UPDATED_BLOCK_STATUSES).map(create) ++
    createShuffleReadAccums() ++
    createShuffleWriteAccums() ++
    createInputAccums() ++
    createOutputAccums() ++
    sys.props.get("spark.testing").map(_ => create(TEST_ACCUM)).toSeq
  }

  /**
   * Accumulators for tracking shuffle read metrics.
   */
  def createShuffleReadAccums(): Seq[Accumulator[_]] = {
    Seq[String](
      shuffleRead.REMOTE_BLOCKS_FETCHED,
      shuffleRead.LOCAL_BLOCKS_FETCHED,
      shuffleRead.REMOTE_BYTES_READ,
      shuffleRead.LOCAL_BYTES_READ,
      shuffleRead.FETCH_WAIT_TIME,
      shuffleRead.RECORDS_READ).map(create)
  }

  /**
   * Accumulators for tracking shuffle write metrics.
   */
  def createShuffleWriteAccums(): Seq[Accumulator[_]] = {
    Seq[String](
      shuffleWrite.BYTES_WRITTEN,
      shuffleWrite.RECORDS_WRITTEN,
      shuffleWrite.WRITE_TIME).map(create)
  }

  /**
   * Accumulators for tracking input metrics.
   */
  def createInputAccums(): Seq[Accumulator[_]] = {
    Seq[String](
      input.READ_METHOD,
      input.BYTES_READ,
      input.RECORDS_READ).map(create)
  }

  /**
   * Accumulators for tracking output metrics.
   */
  private def createOutputAccums(): Seq[Accumulator[_]] = {
    Seq[String](
      output.WRITE_METHOD,
      output.BYTES_WRITTEN,
      output.RECORDS_WRITTEN).map(create)
  }

  /**
   * Accumulators for tracking internal metrics.
   *
   * These accumulators are created with the stage such that all tasks in the stage will
   * add to the same set of accumulators. We do this to report the distribution of accumulator
   * values across all tasks within each stage.
   */
  def create(sc: SparkContext): Seq[Accumulator[_]] = {
    val accums = create()
    accums.foreach { accum =>
      Accumulators.register(accum)
      sc.cleaner.foreach(_.registerAccumulatorForCleanup(accum))
    }
    accums
  }

  /**
   * Create a new accumulator representing an internal task metric.
   */
  private def newMetric[T](
      initialValue: T,
      name: String,
      param: AccumulatorParam[T]): Accumulator[T] = {
    new Accumulator[T](initialValue, param, Some(name), internal = true, countFailedValues = true)
  }

}
