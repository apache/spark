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

package org.apache.spark.executor

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.AccumulableInfo
import org.apache.spark.storage.{BlockId, BlockStatus}


/**
 * :: DeveloperApi ::
 * Metrics tracked during the execution of a task.
 *
 * This class is wrapper around a collection of internal accumulators that represent metrics
 * associated with a task. The local values of these accumulators are sent from the executor
 * to the driver when the task completes. These values are then merged into the corresponding
 * accumulator previously registered on the driver.
 *
 * The accumulator updates are also sent to the driver periodically (on executor heartbeat)
 * and when the task failed with an exception. The [[TaskMetrics]] object itself should never
 * be sent to the driver.
 *
 * @param initialAccums the initial set of accumulators that this [[TaskMetrics]] depends on.
 *                      Each accumulator in this initial set must be uniquely named and marked
 *                      as internal. Additional accumulators registered later need not satisfy
 *                      these requirements.
 */
@DeveloperApi
class TaskMetrics private[spark] (initialAccums: Seq[Accumulator[_]]) extends Serializable {
  import InternalAccumulator._

  // Needed for Java tests
  def this() {
    this(InternalAccumulator.createAll())
  }

  /**
   * All accumulators registered with this task.
   */
  private val accums = new ArrayBuffer[Accumulable[_, _]]
  accums ++= initialAccums

  /**
   * A map for quickly accessing the initial set of accumulators by name.
   */
  private val initialAccumsMap: Map[String, Accumulator[_]] = {
    val map = new mutable.HashMap[String, Accumulator[_]]
    initialAccums.foreach { a =>
      val name = a.name.getOrElse {
        throw new IllegalArgumentException(
          "initial accumulators passed to TaskMetrics must be named")
      }
      require(a.isInternal,
        s"initial accumulator '$name' passed to TaskMetrics must be marked as internal")
      require(!map.contains(name),
        s"detected duplicate accumulator name '$name' when constructing TaskMetrics")
      map(name) = a
    }
    map.toMap
  }

  // Each metric is internally represented as an accumulator
  private val _executorDeserializeTime = getAccum(EXECUTOR_DESERIALIZE_TIME)
  private val _executorRunTime = getAccum(EXECUTOR_RUN_TIME)
  private val _resultSize = getAccum(RESULT_SIZE)
  private val _jvmGCTime = getAccum(JVM_GC_TIME)
  private val _resultSerializationTime = getAccum(RESULT_SERIALIZATION_TIME)
  private val _memoryBytesSpilled = getAccum(MEMORY_BYTES_SPILLED)
  private val _diskBytesSpilled = getAccum(DISK_BYTES_SPILLED)
  private val _peakExecutionMemory = getAccum(PEAK_EXECUTION_MEMORY)
  private val _updatedBlockStatuses =
    TaskMetrics.getAccum[Seq[(BlockId, BlockStatus)]](initialAccumsMap, UPDATED_BLOCK_STATUSES)

  private val _inputMetrics = new InputMetrics(initialAccumsMap)

  private val _outputMetrics = new OutputMetrics(initialAccumsMap)

  private val _shuffleReadMetrics = new ShuffleReadMetrics(initialAccumsMap)

  private val _shuffleWriteMetrics = new ShuffleWriteMetrics(initialAccumsMap)

  /**
   * Time taken on the executor to deserialize this task.
   */
  def executorDeserializeTime: Long = _executorDeserializeTime.localValue

  /**
   * Time the executor spends actually running the task (including fetching shuffle data).
   */
  def executorRunTime: Long = _executorRunTime.localValue

  /**
   * The number of bytes this task transmitted back to the driver as the TaskResult.
   */
  def resultSize: Long = _resultSize.localValue

  /**
   * Amount of time the JVM spent in garbage collection while executing this task.
   */
  def jvmGCTime: Long = _jvmGCTime.localValue

  /**
   * Amount of time spent serializing the task result.
   */
  def resultSerializationTime: Long = _resultSerializationTime.localValue

  /**
   * The number of in-memory bytes spilled by this task.
   */
  def memoryBytesSpilled: Long = _memoryBytesSpilled.localValue

  /**
   * The number of on-disk bytes spilled by this task.
   */
  def diskBytesSpilled: Long = _diskBytesSpilled.localValue

  /**
   * Peak memory used by internal data structures created during shuffles, aggregations and
   * joins. The value of this accumulator should be approximately the sum of the peak sizes
   * across all such data structures created in this task. For SQL jobs, this only tracks all
   * unsafe operators and ExternalSort.
   */
  def peakExecutionMemory: Long = _peakExecutionMemory.localValue

  /**
   * Storage statuses of any blocks that have been updated as a result of this task.
   */
  def updatedBlockStatuses: Seq[(BlockId, BlockStatus)] = _updatedBlockStatuses.localValue

  // Setters and increment-ers
  private[spark] def setExecutorDeserializeTime(v: Long): Unit =
    _executorDeserializeTime.setValue(v)
  private[spark] def setExecutorRunTime(v: Long): Unit = _executorRunTime.setValue(v)
  private[spark] def setResultSize(v: Long): Unit = _resultSize.setValue(v)
  private[spark] def setJvmGCTime(v: Long): Unit = _jvmGCTime.setValue(v)
  private[spark] def setResultSerializationTime(v: Long): Unit =
    _resultSerializationTime.setValue(v)
  private[spark] def incMemoryBytesSpilled(v: Long): Unit = _memoryBytesSpilled.add(v)
  private[spark] def incDiskBytesSpilled(v: Long): Unit = _diskBytesSpilled.add(v)
  private[spark] def incPeakExecutionMemory(v: Long): Unit = _peakExecutionMemory.add(v)
  private[spark] def incUpdatedBlockStatuses(v: Seq[(BlockId, BlockStatus)]): Unit =
    _updatedBlockStatuses.add(v)
  private[spark] def setUpdatedBlockStatuses(v: Seq[(BlockId, BlockStatus)]): Unit =
    _updatedBlockStatuses.setValue(v)

  /**
   * Get a Long accumulator from the given map by name, assuming it exists.
   * Note: this only searches the initial set of accumulators passed into the constructor.
   */
  private[spark] def getAccum(name: String): Accumulator[Long] = {
    TaskMetrics.getAccum[Long](initialAccumsMap, name)
  }

  /**
   * Metrics related to reading data from a [[org.apache.spark.rdd.HadoopRDD]] or from persisted
   * data, defined only in tasks with input.
   */
  def inputMetrics: InputMetrics = _inputMetrics

  /**
   * Metrics related to writing data externally (e.g. to a distributed filesystem),
   * defined only in tasks with output.
   */
  def outputMetrics: OutputMetrics = _outputMetrics

  /**
   * Metrics related to shuffle read aggregated across all shuffle dependencies.
   * This is defined only if there are shuffle dependencies in this task.
   */
  def shuffleReadMetrics: ShuffleReadMetrics = _shuffleReadMetrics

  /**
   * Temporary list of [[ShuffleReadMetrics]], one per shuffle dependency.
   *
   * A task may have multiple shuffle readers for multiple dependencies. To avoid synchronization
   * issues from readers in different threads, in-progress tasks use a [[ShuffleReadMetrics]] for
   * each dependency and merge these metrics before reporting them to the driver.
   */
  @transient private lazy val tempShuffleReadMetrics = new ArrayBuffer[ShuffleReadMetrics]

  /**
   * Create a temporary [[ShuffleReadMetrics]] for a particular shuffle dependency.
   *
   * All usages are expected to be followed by a call to [[mergeShuffleReadMetrics]], which
   * merges the temporary values synchronously. Otherwise, all temporary data collected will
   * be lost.
   */
  private[spark] def createTempShuffleReadMetrics(): ShuffleReadMetrics = synchronized {
    val readMetrics = new ShuffleReadMetrics
    tempShuffleReadMetrics += readMetrics
    readMetrics
  }

  /**
   * Merge values across all temporary [[ShuffleReadMetrics]] into `_shuffleReadMetrics`.
   * This is expected to be called on executor heartbeat and at the end of a task.
   */
  private[spark] def mergeShuffleReadMetrics(): Unit = synchronized {
    if (tempShuffleReadMetrics.nonEmpty) {
      _shuffleReadMetrics.setMergeValues(tempShuffleReadMetrics)
    }
  }

  /**
   * Metrics related to shuffle write, defined only in shuffle map stages.
   */
  def shuffleWriteMetrics: ShuffleWriteMetrics = _shuffleWriteMetrics

  /* ========================== *
   |        OTHER THINGS        |
   * ========================== */

  private[spark] def registerAccumulator(a: Accumulable[_, _]): Unit = {
    accums += a
  }

  /**
   * Return the latest updates of accumulators in this task.
   *
   * The [[AccumulableInfo.update]] field is always defined and the [[AccumulableInfo.value]]
   * field is always empty, since this represents the partial updates recorded in this task,
   * not the aggregated value across multiple tasks.
   */
  def accumulatorUpdates(): Seq[AccumulableInfo] = {
    accums.map { a => a.toInfo(Some(a.localValue), None) }
  }
}

/**
 * Internal subclass of [[TaskMetrics]] which is used only for posting events to listeners.
 * Its purpose is to obviate the need for the driver to reconstruct the original accumulators,
 * which might have been garbage-collected. See SPARK-13407 for more details.
 *
 * Instances of this class should be considered read-only and users should not call `inc*()` or
 * `set*()` methods. While we could override the setter methods to throw
 * UnsupportedOperationException, we choose not to do so because the overrides would quickly become
 * out-of-date when new metrics are added.
 */
private[spark] class ListenerTaskMetrics(
    initialAccums: Seq[Accumulator[_]],
    accumUpdates: Seq[AccumulableInfo]) extends TaskMetrics(initialAccums) {

  override def accumulatorUpdates(): Seq[AccumulableInfo] = accumUpdates

  override private[spark] def registerAccumulator(a: Accumulable[_, _]): Unit = {
    throw new UnsupportedOperationException("This TaskMetrics is read-only")
  }
}

private[spark] object TaskMetrics extends Logging {

  def empty: TaskMetrics = new TaskMetrics

  /**
   * Get an accumulator from the given map by name, assuming it exists.
   */
  def getAccum[T](accumMap: Map[String, Accumulator[_]], name: String): Accumulator[T] = {
    require(accumMap.contains(name), s"metric '$name' is missing")
    val accum = accumMap(name)
    try {
      // Note: we can't do pattern matching here because types are erased by compile time
      accum.asInstanceOf[Accumulator[T]]
    } catch {
      case e: ClassCastException =>
        throw new SparkException(s"accumulator $name was of unexpected type", e)
    }
  }

  /**
   * Construct a [[TaskMetrics]] object from a list of accumulator updates, called on driver only.
   *
   * Executors only send accumulator updates back to the driver, not [[TaskMetrics]]. However, we
   * need the latter to post task end events to listeners, so we need to reconstruct the metrics
   * on the driver.
   *
   * This assumes the provided updates contain the initial set of accumulators representing
   * internal task level metrics.
   */
  def fromAccumulatorUpdates(accumUpdates: Seq[AccumulableInfo]): TaskMetrics = {
    // Initial accumulators are passed into the TaskMetrics constructor first because these
    // are required to be uniquely named. The rest of the accumulators from this task are
    // registered later because they need not satisfy this requirement.
    val definedAccumUpdates = accumUpdates.filter { info => info.update.isDefined }
    val initialAccums = definedAccumUpdates
      .filter { info => info.name.exists(_.startsWith(InternalAccumulator.METRICS_PREFIX)) }
      .map { info =>
        val accum = InternalAccumulator.create(info.name.get)
        accum.setValueAny(info.update.get)
        accum
      }
    new ListenerTaskMetrics(initialAccums, definedAccumUpdates)
  }

}
