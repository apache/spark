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

import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{Accumulable, Accumulator, InternalAccumulator, SparkException}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.executor.DataReadMethod.DataReadMethod
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
 * TODO: update this comment.
 *
 * @param initialAccums the initial set of accumulators that this [[TaskMetrics]] depends on.
 *                      Each accumulator in this initial set must be named and marked as internal.
 *                      Additional accumulators registered here have no such requirements.
 * @param hostname where this task is run.
 */
@DeveloperApi
class TaskMetrics private[spark] (
    initialAccums: Seq[Accumulable[_, _]],
    val hostname: String = TaskMetrics.getCachedHostName)
  extends Serializable {

  import InternalAccumulator._

  def this(host: String) {
    this(InternalAccumulator.create(), host)
  }

  // Needed for Java tests
  def this() {
    this(TaskMetrics.getCachedHostName)
  }

  /**
   * All accumulators registered with this task.
   */
  private val accums = new ArrayBuffer[Accumulable[_, _]]
  accums ++= initialAccums

  /**
   * A map for quickly accessing the initial set of accumulators by name.
   */
  private val initialAccumsMap: Map[String, Accumulable[_, _]] = {
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
  private val _executorDeserializeTime = getLongAccum(EXECUTOR_DESERIALIZE_TIME)
  private val _executorRunTime = getLongAccum(EXECUTOR_RUN_TIME)
  private val _resultSize = getLongAccum(RESULT_SIZE)
  private val _jvmGCTime = getLongAccum(JVM_GC_TIME)
  private val _resultSerializationTime = getLongAccum(RESULT_SERIALIZATION_TIME)
  private val _memoryBytesSpilled = getLongAccum(MEMORY_BYTES_SPILLED)
  private val _diskBytesSpilled = getLongAccum(DISK_BYTES_SPILLED)
  private val _peakExecutionMemory = getLongAccum(PEAK_EXECUTION_MEMORY)

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

  private[spark] def setExecutorDeserializeTime(v: Long) = _executorDeserializeTime.setValue(v)
  private[spark] def setExecutorRunTime(v: Long) = _executorRunTime.setValue(v)
  private[spark] def setResultSize(v: Long) = _resultSize.setValue(v)
  private[spark] def setJvmGCTime(v: Long) = _jvmGCTime.setValue(v)
  private[spark] def setResultSerializationTime(v: Long) = _resultSerializationTime.setValue(v)
  private[spark] def incMemoryBytesSpilled(v: Long) = _memoryBytesSpilled.add(v)
  private[spark] def incDiskBytesSpilled(v: Long) = _diskBytesSpilled.add(v)
  private[spark] def incPeakExecutionMemory(v: Long) = _peakExecutionMemory.add(v)

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
   * Note: this only searches the initial set passed into the constructor.
   */
  private[spark] def getLongAccum(name: String): Accumulator[Long] = {
    TaskMetrics.getLongAccum(initialAccumsMap, name)
  }

  /**
   * Return a map from accumulator ID to the accumulator's latest value in this task.
   */
  def accumulatorUpdates(): Map[Long, Any] = accums.map { a => (a.id, a.localValue) }.toMap


  /**
   * Storage statuses of any blocks that have been updated as a result of this task.
   */
  // TODO: make me an accumulator; right now this doesn't get sent to the driver.
  var updatedBlocks: Option[Seq[(BlockId, BlockStatus)]] = None


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
      val metrics = new OutputMetrics(writeMethod, initialAccumsMap.toMap)
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
  private[spark] def registerInputMetrics(readMethod: DataReadMethod): InputMetrics = {
    synchronized {
      val metrics = _inputMetrics.getOrElse {
        val metrics = new InputMetrics(readMethod, initialAccumsMap.toMap)
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
        new InputMetrics(readMethod)
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

}


private[spark] object TaskMetrics {
  private val hostNameCache = new ConcurrentHashMap[String, String]()

  def empty: TaskMetrics = new TaskMetrics

  /**
   * Get the hostname from cached data, since hostname is the order of number of nodes in cluster,
   * so using cached hostname will decrease the object number and alleviate the GC overhead.
   */
  def getCachedHostName: String = {
    val host = Utils.localHostName()
    val canonicalHost = hostNameCache.putIfAbsent(host, host)
    if (canonicalHost != null) canonicalHost else host
  }

  /**
   * Get a Long accumulator from the given map by name, assuming it exists.
   */
  def getLongAccum(
      accumMap: Map[String, Accumulable[_, _]],
      name: String): Accumulator[Long] = {
    assert(accumMap.contains(name), s"metric '$name' is missing")
    try {
      // Note: we can't do pattern matching here because types are erased by compile time
      accumMap(name).asInstanceOf[Accumulator[Long]]
    } catch {
      case _: ClassCastException =>
        throw new SparkException(s"attempted to access invalid accumulator $name as a long metric")
    }
  }

}
