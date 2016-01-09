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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{Accumulable, Accumulator, InternalAccumulator, SparkException}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.storage.{BlockId, BlockStatus}


// TODO: make this and related classes private

/**
 * :: DeveloperApi ::
 * Metrics tracked during the execution of a task.
 *
 * This class is wrapper around a collection of internal accumulators that represent metrics
 * associated with a task. The local values of these accumulators are sent from the executor
 * to the driver when the task completes. These values are then merged into the corresponding
 * accumulator previously registered on the driver.
 *
 * These accumulator updates are also sent to the driver periodically (on executor heartbeat)
 * or when the task failed with an exception.
 *
 * @param initialAccums the initial set of accumulators that this [[TaskMetrics]] depends on.
 *                      Each accumulator in this initial set must be named and marked as internal.
 *                      Additional accumulators registered here have no such requirements.
 */
@DeveloperApi
class TaskMetrics private[spark](initialAccums: Seq[Accumulator[_]]) extends Serializable {

  import InternalAccumulator._

  // Needed for Java tests
  def this() {
    this(InternalAccumulator.create())
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
    initialAccums.map { a =>
      assert(a.name.isDefined, "initial accumulators passed to TaskMetrics should be named")
      val name = a.name.get
      assert(a.isInternal,
        s"initial accumulator '$name' passed to TaskMetrics should be marked as internal")
      (name, a)
    }.toMap
  }

  assert(initialAccumsMap.size == initialAccums.size, s"detected duplicate names in initial " +
    s"accumulators passed to TaskMetrics:\n ${initialAccums.map(_.name.get).mkString("\n")}")

  // Each metric is internally represented as an accumulator
  private val _executorDeserializeTime = getAccum(EXECUTOR_DESERIALIZE_TIME)
  private val _executorRunTime = getAccum(EXECUTOR_RUN_TIME)
  private val _resultSize = getAccum(RESULT_SIZE)
  private val _jvmGCTime = getAccum(JVM_GC_TIME)
  private val _resultSerializationTime = getAccum(RESULT_SERIALIZATION_TIME)
  private val _memoryBytesSpilled = getAccum(MEMORY_BYTES_SPILLED)
  private val _diskBytesSpilled = getAccum(DISK_BYTES_SPILLED)
  private val _peakExecutionMemory = getAccum(PEAK_EXECUTION_MEMORY)
  private val _updatedBlocks =
    TaskMetrics.getAccum[Seq[(BlockId, BlockStatus)]](initialAccumsMap, UPDATED_BLOCK_STATUSES)

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
  def updatedBlocks: Seq[(BlockId, BlockStatus)] = _updatedBlocks.localValue

  private[spark] def setExecutorDeserializeTime(v: Long) = _executorDeserializeTime.setValue(v)
  private[spark] def setExecutorRunTime(v: Long) = _executorRunTime.setValue(v)
  private[spark] def setResultSize(v: Long) = _resultSize.setValue(v)
  private[spark] def setJvmGCTime(v: Long) = _jvmGCTime.setValue(v)
  private[spark] def setResultSerializationTime(v: Long) = _resultSerializationTime.setValue(v)
  private[spark] def incMemoryBytesSpilled(v: Long) = _memoryBytesSpilled.add(v)
  private[spark] def incDiskBytesSpilled(v: Long) = _diskBytesSpilled.add(v)
  private[spark] def incPeakExecutionMemory(v: Long) = _peakExecutionMemory.add(v)
  private[spark] def incUpdatedBlocks(v: Seq[(BlockId, BlockStatus)]) = _updatedBlocks.add(v)
  private[spark] def setUpdatedBlocks(v: Seq[(BlockId, BlockStatus)]) = _updatedBlocks.setValue(v)


  /* ============================ *
   |        OUTPUT METRICS        |
   * ============================ */

  private var _outputMetrics: Option[OutputMetrics] = None

  /**
   * Metrics related to writing data externally (e.g. to a distributed filesystem),
   * defined only in tasks with output.
   */
  def outputMetrics: Option[OutputMetrics] = _outputMetrics

  /**
   * Get or create a new [[OutputMetrics]] associated with this task.
   */
  def registerOutputMetrics(writeMethod: DataWriteMethod.Value): OutputMetrics = synchronized {
    _outputMetrics.getOrElse {
      val metrics = new OutputMetrics(initialAccumsMap.toMap)
      metrics.setWriteMethod(writeMethod)
      _outputMetrics = Some(metrics)
      metrics
    }
  }


  /* ========================== *
   |        INPUT METRICS       |
   * ========================== */

  private var _inputMetrics: Option[InputMetrics] = None

  /**
   * Metrics related to reading data from a [[org.apache.spark.rdd.HadoopRDD]] or from persisted
   * data, defined only in tasks with input.
   */
  def inputMetrics: Option[InputMetrics] = _inputMetrics

  /**
   * Get or create a new [[InputMetrics]] associated with this task.
   */
  private[spark] def registerInputMetrics(readMethod: DataReadMethod.Value): InputMetrics = {
    synchronized {
      val metrics = _inputMetrics.getOrElse {
        val metrics = new InputMetrics(initialAccumsMap.toMap)
        metrics.setReadMethod(readMethod)
        _inputMetrics = Some(metrics)
        metrics
      }
      // If there already exists an InputMetric with the same read method, we can just return
      // that one. Otherwise, if the read method is different from the one previously seen by
      // this task, we return a new dummy one to avoid clobbering the values of the old metrics.
      // In the future we should try to store input metrics from all different read methods at
      // the same time (SPARK-5225).
      if (metrics.readMethod == readMethod) {
        metrics
      } else {
        val m = new InputMetrics
        m.setReadMethod(readMethod)
        m
      }
    }
  }


  /* =================================== *
   |        SHUFFLE WRITE METRICS        |
   * =================================== */

  private var _shuffleWriteMetrics: Option[ShuffleWriteMetrics] = None

  /**
   * Metrics related to shuffle write, defined only in shuffle map stages.
   */
  def shuffleWriteMetrics: Option[ShuffleWriteMetrics] = _shuffleWriteMetrics

  /**
   * Get or create a new [[ShuffleWriteMetrics]] associated with this task.
   */
  def registerShuffleWriteMetrics(): ShuffleWriteMetrics = synchronized {
    _shuffleWriteMetrics.getOrElse {
      val metrics = new ShuffleWriteMetrics(initialAccumsMap.toMap)
      _shuffleWriteMetrics = Some(metrics)
      metrics
    }
  }


  /* ================================== *
   |        SHUFFLE READ METRICS        |
   * ================================== */

  private var _shuffleReadMetrics: Option[ShuffleReadMetrics] = None

  /**
   * Metrics related to shuffle read aggregated across all shuffle dependencies.
   * This is defined only if there are shuffle dependencies in this task.
   */
  def shuffleReadMetrics: Option[ShuffleReadMetrics] = _shuffleReadMetrics

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
   * All usages are expected to be followed by a call to [[mergeShuffleReadMetrics]],
   * which merges the temporary values synchronously.
   */
  private[spark] def registerTempShuffleReadMetrics(): ShuffleReadMetrics = synchronized {
    val readMetrics = new ShuffleReadMetrics
    tempShuffleReadMetrics += readMetrics
    readMetrics
  }

  /**
   * Merge values across all temporary [[ShuffleReadMetrics]] into `_shuffleReadMetrics`.
   * This is expected to be called on executor heartbeat and at the end of a task.
   */
  private[spark] def mergeShuffleReadMetrics(): Unit = synchronized {
    val agg = _shuffleReadMetrics.getOrElse {
      val metrics = new ShuffleReadMetrics(initialAccumsMap.toMap)
      _shuffleReadMetrics = Some(metrics)
      metrics
    }
    agg.setRemoteBlocksFetched(tempShuffleReadMetrics.map(_.remoteBlocksFetched).sum)
    agg.setLocalBlocksFetched(tempShuffleReadMetrics.map(_.localBlocksFetched).sum)
    agg.setFetchWaitTime(tempShuffleReadMetrics.map(_.fetchWaitTime).sum)
    agg.setRemoteBytesRead(tempShuffleReadMetrics.map(_.remoteBytesRead).sum)
    agg.setLocalBytesRead(tempShuffleReadMetrics.map(_.localBytesRead).sum)
    agg.setRecordsRead(tempShuffleReadMetrics.map(_.recordsRead).sum)
  }


  /* ========================== *
   |        OTHER THINGS        |
   * ========================== */

  /**
   * Register an accumulator with this task so we can access its value in [[accumulatorUpdates]].
   */
  private[spark] def registerAccumulator(a: Accumulable[_, _]): Unit = {
    accums += a
  }

  /**
   * Return all the accumulators used on this task. Note: This is not a copy.
   */
  private[spark] def accumulators: Seq[Accumulable[_, _]] = accums

  /**
   * Get a Long accumulator from the given map by name, assuming it exists.
   * Note: this only searches the initial set of accumulators passed into the constructor.
   */
  private[spark] def getAccum(name: String): Accumulator[Long] = {
    TaskMetrics.getAccum[Long](initialAccumsMap, name)
  }

  /**
   * Return a map from accumulator ID to the accumulator's latest value in this task.
   */
  def accumulatorUpdates(): Map[Long, Any] = accums.map { a => (a.id, a.localValue) }.toMap

  /**
   * Return whether some accumulators with the given prefix have already been set.
   * This only considers the initial set of accumulators passed into the constructor.
   */
  private def accumsAlreadySet(prefix: String): Boolean = {
    initialAccumsMap.filterKeys(_.startsWith(prefix)).values.exists { a => a.localValue != a.zero }
  }

  // If we are reconstructing this TaskMetrics on the driver, some metrics may already be set.
  // If so, initialize all relevant metrics classes so listeners can access them downstream.
  if (accumsAlreadySet(SHUFFLE_READ_METRICS_PREFIX)) {
    _shuffleReadMetrics = Some(new ShuffleReadMetrics(initialAccumsMap))
  }
  if (accumsAlreadySet(SHUFFLE_WRITE_METRICS_PREFIX)) {
    _shuffleWriteMetrics = Some(new ShuffleWriteMetrics(initialAccumsMap))
  }
  if (accumsAlreadySet(OUTPUT_METRICS_PREFIX)) {
    _outputMetrics = Some(new OutputMetrics(initialAccumsMap))
  }
  if (accumsAlreadySet(INPUT_METRICS_PREFIX)) {
    _inputMetrics = Some(new InputMetrics(initialAccumsMap))
  }

}


private[spark] object TaskMetrics {

  def empty: TaskMetrics = new TaskMetrics

  /**
   * Get an accumulator from the given map by name, assuming it exists.
   */
  def getAccum[T](
      accumMap: Map[String, Accumulator[_]],
      name: String): Accumulator[T] = {
    assert(accumMap.contains(name), s"metric '$name' is missing")
    val accum = accumMap(name)
    try {
      // Note: we can't do pattern matching here because types are erased by compile time
      accum.asInstanceOf[Accumulator[T]]
    } catch {
      case e: ClassCastException =>
        throw new SparkException(s"accumulator $name was of unexpected type", e)
    }
  }

}
