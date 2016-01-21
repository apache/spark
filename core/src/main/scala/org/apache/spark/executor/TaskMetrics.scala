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

import java.io.{IOException, ObjectInputStream}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable.ArrayBuffer

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
 */
@DeveloperApi
class TaskMetrics extends Serializable {
  /**
   * Host's name the task runs on
   */
  private var _hostname: String = _
  def hostname: String = _hostname
  private[spark] def setHostname(value: String) = _hostname = value

  /**
   * Time taken on the executor to deserialize this task
   */
  private var _executorDeserializeTime: Long = _
  def executorDeserializeTime: Long = _executorDeserializeTime
  private[spark] def setExecutorDeserializeTime(value: Long) = _executorDeserializeTime = value


  /**
   * Time the executor spends actually running the task (including fetching shuffle data)
   */
  private var _executorRunTime: Long = _
  def executorRunTime: Long = _executorRunTime
  private[spark] def setExecutorRunTime(value: Long) = _executorRunTime = value

  /**
   * The number of bytes this task transmitted back to the driver as the TaskResult
   */
  private var _resultSize: Long = _
  def resultSize: Long = _resultSize
  private[spark] def setResultSize(value: Long) = _resultSize = value


  /**
   * Amount of time the JVM spent in garbage collection while executing this task
   */
  private var _jvmGCTime: Long = _
  def jvmGCTime: Long = _jvmGCTime
  private[spark] def setJvmGCTime(value: Long) = _jvmGCTime = value

  /**
   * Amount of time spent serializing the task result
   */
  private var _resultSerializationTime: Long = _
  def resultSerializationTime: Long = _resultSerializationTime
  private[spark] def setResultSerializationTime(value: Long) = _resultSerializationTime = value

  /**
   * The number of in-memory bytes spilled by this task
   */
  private var _memoryBytesSpilled: Long = _
  def memoryBytesSpilled: Long = _memoryBytesSpilled
  private[spark] def incMemoryBytesSpilled(value: Long): Unit = _memoryBytesSpilled += value
  private[spark] def decMemoryBytesSpilled(value: Long): Unit = _memoryBytesSpilled -= value

  /**
   * The number of on-disk bytes spilled by this task
   */
  private var _diskBytesSpilled: Long = _
  def diskBytesSpilled: Long = _diskBytesSpilled
  private[spark] def incDiskBytesSpilled(value: Long): Unit = _diskBytesSpilled += value
  private[spark] def decDiskBytesSpilled(value: Long): Unit = _diskBytesSpilled -= value

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
        val metrics = new InputMetrics(readMethod)
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

  /**
   * This should only be used when recreating TaskMetrics, not when updating input metrics in
   * executors
   */
  private[spark] def setInputMetrics(inputMetrics: Option[InputMetrics]) {
    _inputMetrics = inputMetrics
  }

  private var _outputMetrics: Option[OutputMetrics] = None

  /**
   * Metrics related to writing data externally (e.g. to a distributed filesystem),
   * defined only in tasks with output.
   */
  def outputMetrics: Option[OutputMetrics] = _outputMetrics

  @deprecated("setting OutputMetrics is for internal use only", "2.0.0")
  def outputMetrics_=(om: Option[OutputMetrics]): Unit = {
    _outputMetrics = om
  }

  /**
   * Get or create a new [[OutputMetrics]] associated with this task.
   */
  private[spark] def registerOutputMetrics(
      writeMethod: DataWriteMethod.Value): OutputMetrics = synchronized {
    _outputMetrics.getOrElse {
      val metrics = new OutputMetrics(writeMethod)
      _outputMetrics = Some(metrics)
      metrics
    }
  }

  private var _shuffleReadMetrics: Option[ShuffleReadMetrics] = None

  /**
   * Metrics related to shuffle read aggregated across all shuffle dependencies.
   * This is defined only if there are shuffle dependencies in this task.
   */
  def shuffleReadMetrics: Option[ShuffleReadMetrics] = _shuffleReadMetrics

  /**
   * This should only be used when recreating TaskMetrics, not when updating read metrics in
   * executors.
   */
  private[spark] def setShuffleReadMetrics(shuffleReadMetrics: Option[ShuffleReadMetrics]) {
    _shuffleReadMetrics = shuffleReadMetrics
  }

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

  private var _shuffleWriteMetrics: Option[ShuffleWriteMetrics] = None

  /**
   * Metrics related to shuffle write, defined only in shuffle map stages.
   */
  def shuffleWriteMetrics: Option[ShuffleWriteMetrics] = _shuffleWriteMetrics

  @deprecated("setting ShuffleWriteMetrics is for internal use only", "2.0.0")
  def shuffleWriteMetrics_=(swm: Option[ShuffleWriteMetrics]): Unit = {
    _shuffleWriteMetrics = swm
  }

  /**
   * Get or create a new [[ShuffleWriteMetrics]] associated with this task.
   */
  private[spark] def registerShuffleWriteMetrics(): ShuffleWriteMetrics = synchronized {
    _shuffleWriteMetrics.getOrElse {
      val metrics = new ShuffleWriteMetrics
      _shuffleWriteMetrics = Some(metrics)
      metrics
    }
  }

  private var _updatedBlockStatuses: Seq[(BlockId, BlockStatus)] =
    Seq.empty[(BlockId, BlockStatus)]

  /**
   * Storage statuses of any blocks that have been updated as a result of this task.
   */
  def updatedBlockStatuses: Seq[(BlockId, BlockStatus)] = _updatedBlockStatuses

  @deprecated("setting updated blocks is for internal use only", "2.0.0")
  def updatedBlocks_=(ub: Option[Seq[(BlockId, BlockStatus)]]): Unit = {
    _updatedBlockStatuses = ub.getOrElse(Seq.empty[(BlockId, BlockStatus)])
  }

  private[spark] def incUpdatedBlockStatuses(v: Seq[(BlockId, BlockStatus)]): Unit = {
    _updatedBlockStatuses ++= v
  }

  private[spark] def setUpdatedBlockStatuses(v: Seq[(BlockId, BlockStatus)]): Unit = {
    _updatedBlockStatuses = v
  }

  @deprecated("use updatedBlockStatuses instead", "2.0.0")
  def updatedBlocks: Option[Seq[(BlockId, BlockStatus)]] = {
    if (_updatedBlockStatuses.nonEmpty) Some(_updatedBlockStatuses) else None
  }

  private[spark] def updateInputMetrics(): Unit = synchronized {
    inputMetrics.foreach(_.updateBytesRead())
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    in.defaultReadObject()
    // Get the hostname from cached data, since hostname is the order of number of nodes in
    // cluster, so using cached hostname will decrease the object number and alleviate the GC
    // overhead.
    _hostname = TaskMetrics.getCachedHostName(_hostname)
  }

  private var _accumulatorUpdates: Map[Long, Any] = Map.empty
  @transient private var _accumulatorsUpdater: () => Map[Long, Any] = null

  private[spark] def updateAccumulators(): Unit = synchronized {
    _accumulatorUpdates = _accumulatorsUpdater()
  }

  /**
   * Return the latest updates of accumulators in this task.
   */
  def accumulatorUpdates(): Map[Long, Any] = _accumulatorUpdates

  private[spark] def setAccumulatorsUpdater(accumulatorsUpdater: () => Map[Long, Any]): Unit = {
    _accumulatorsUpdater = accumulatorsUpdater
  }
}


private[spark] object TaskMetrics {
  private val hostNameCache = new ConcurrentHashMap[String, String]()

  def empty: TaskMetrics = new TaskMetrics

  def getCachedHostName(host: String): String = {
    val canonicalHost = hostNameCache.putIfAbsent(host, host)
    if (canonicalHost != null) canonicalHost else host
  }
}
