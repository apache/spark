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
import org.apache.spark.storage.{BlockId, BlockStatus}
import org.apache.spark.util.Utils


/**
 * :: DeveloperApi ::
 * Metrics tracked during the execution of a task.
 *
 * This class is used to house metrics both for in-progress and completed tasks. In executors,
 * both the task thread and the heartbeat thread write to the TaskMetrics. The heartbeat thread
 * reads it to send in-progress metrics, and the task thread reads it to send metrics along with
 * the completed task.
 *
 * So, when adding new fields, take into consideration that the whole object can be serialized for
 * shipping off at any time to consumers of the SparkListener interface.
 *
 * @param initialAccums the initial set of accumulators that this [[TaskMetrics]] depends on.
 *                      Each accumulator in this initial set must be uniquely named and marked
 *                      as internal. Additional accumulators registered later need not satisfy
 *                      these requirements.
 */
@DeveloperApi
class TaskMetrics(initialAccums: Seq[Accumulator[_]]) extends Serializable {

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
    val map = new mutable.HashMap[String, Accumulator[_]]
    initialAccums.foreach { a =>
      assert(a.name.isDefined, "initial accumulators passed to TaskMetrics must be named")
      val name = a.name.get
      assert(a.isInternal,
        s"initial accumulator '$name' passed to TaskMetrics must be marked as internal")
      assert(!map.contains(name),
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

  @deprecated("use updatedBlockStatuses instead", "2.0.0")
  def updatedBlocks: Option[Seq[(BlockId, BlockStatus)]] = {
    if (updatedBlockStatuses.nonEmpty) Some(updatedBlockStatuses) else None
  }

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
   * Host's name the task runs on.
   */
  @deprecated("hostname will be removed from TaskMetrics in the future", "2.0.0")
  def hostname: String = Utils.localHostName()

  /**
   * Get a Long accumulator from the given map by name, assuming it exists.
   * Note: this only searches the initial set of accumulators passed into the constructor.
   */
  private[spark] def getAccum(name: String): Accumulator[Long] = {
    TaskMetrics.getAccum[Long](initialAccumsMap, name)
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
  private[spark] def registerOutputMetrics(
      writeMethod: DataWriteMethod.Value): OutputMetrics = synchronized {
    _outputMetrics.getOrElse {
      val metrics = new OutputMetrics(initialAccumsMap.toMap)
      metrics.setWriteMethod(writeMethod)
      _outputMetrics = Some(metrics)
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
   * All usages are expected to be followed by a call to [[mergeShuffleReadMetrics]], which
   * merges the temporary values synchronously. Otherwise, all temporary data collected will
   * be lost.
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
    if (tempShuffleReadMetrics.nonEmpty) {
      val merged = new ShuffleReadMetrics
      for (depMetrics <- tempShuffleReadMetrics) {
        merged.incFetchWaitTime(depMetrics.fetchWaitTime)
        merged.incLocalBlocksFetched(depMetrics.localBlocksFetched)
        merged.incRemoteBlocksFetched(depMetrics.remoteBlocksFetched)
        merged.incRemoteBytesRead(depMetrics.remoteBytesRead)
        merged.incLocalBytesRead(depMetrics.localBytesRead)
        merged.incRecordsRead(depMetrics.recordsRead)
      }
      _shuffleReadMetrics = Some(merged)
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
  private[spark] def registerShuffleWriteMetrics(): ShuffleWriteMetrics = synchronized {
    _shuffleWriteMetrics.getOrElse {
      val metrics = new ShuffleWriteMetrics(initialAccumsMap.toMap)
      _shuffleWriteMetrics = Some(metrics)
      metrics
    }
  }


  /* ========================== *
   |        OTHER THINGS        |
   * ========================== */

  private[spark] def registerAccumulator(a: Accumulable[_, _]): Unit = {
    accums += a
  }
  /**
   * Return the latest updates of accumulators in this task.
   */
  private[spark] def accumulatorUpdates(): Map[Long, Any] =
    accums.map { a => (a.id, a.localValue) }.toMap[Long, Any]

}

private[spark] object TaskMetrics {

  def empty: TaskMetrics = new TaskMetrics

  /**
   * Get an accumulator from the given map by name, assuming it exists.
   */
  def getAccum[T](accumMap: Map[String, Accumulator[_]], name: String): Accumulator[T] = {
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
