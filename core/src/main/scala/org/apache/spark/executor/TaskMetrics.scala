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

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.executor.DataReadMethod.DataReadMethod

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.storage.{BlockId, BlockStatus}

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
  def hostname = _hostname
  private[spark] def setHostname(value: String) = _hostname = value
  
  /**
   * Time taken on the executor to deserialize this task
   */
  private var _executorDeserializeTime: Long = _
  def executorDeserializeTime = _executorDeserializeTime
  private[spark] def setExecutorDeserializeTime(value: Long) = _executorDeserializeTime = value
  
  
  /**
   * Time the executor spends actually running the task (including fetching shuffle data)
   */
  private var _executorRunTime: Long = _
  def executorRunTime = _executorRunTime
  private[spark] def setExecutorRunTime(value: Long) = _executorRunTime = value
  
  /**
   * The number of bytes this task transmitted back to the driver as the TaskResult
   */
  private var _resultSize: Long = _
  def resultSize = _resultSize
  private[spark] def setResultSize(value: Long) = _resultSize = value


  /**
   * Amount of time the JVM spent in garbage collection while executing this task
   */
  private var _jvmGCTime: Long = _
  def jvmGCTime = _jvmGCTime
  private[spark] def setJvmGCTime(value: Long) = _jvmGCTime = value

  /**
   * Amount of time spent serializing the task result
   */
  private var _resultSerializationTime: Long = _
  def resultSerializationTime = _resultSerializationTime
  private[spark] def setResultSerializationTime(value: Long) = _resultSerializationTime = value

  /**
   * The number of in-memory bytes spilled by this task
   */
  private var _memoryBytesSpilled: Long = _
  def memoryBytesSpilled = _memoryBytesSpilled
  private[spark] def incMemoryBytesSpilled(value: Long) = _memoryBytesSpilled += value
  private[spark] def decMemoryBytesSpilled(value: Long) = _memoryBytesSpilled -= value

  /**
   * The number of on-disk bytes spilled by this task
   */
  private var _diskBytesSpilled: Long = _
  def diskBytesSpilled = _diskBytesSpilled
  def incDiskBytesSpilled(value: Long) = _diskBytesSpilled += value
  def decDiskBytesSpilled(value: Long) = _diskBytesSpilled -= value

  /**
   * If this task reads from a HadoopRDD or from persisted data, metrics on how much data was read
   * are stored here.
   */
  private var _inputMetrics: Option[InputMetrics] = None

  def inputMetrics = _inputMetrics

  /**
   * This should only be used when recreating TaskMetrics, not when updating input metrics in
   * executors
   */
  private[spark] def setInputMetrics(inputMetrics: Option[InputMetrics]) {
    _inputMetrics = inputMetrics
  }

  /**
   * If this task writes data externally (e.g. to a distributed filesystem), metrics on how much
   * data was written are stored here.
   */
  var outputMetrics: Option[OutputMetrics] = None

  /**
   * If this task reads from shuffle output, metrics on getting shuffle data will be collected here.
   * This includes read metrics aggregated over all the task's shuffle dependencies.
   */
  private var _shuffleReadMetrics: Option[ShuffleReadMetrics] = None

  def shuffleReadMetrics = _shuffleReadMetrics

  /**
   * This should only be used when recreating TaskMetrics, not when updating read metrics in
   * executors.
   */
  private[spark] def setShuffleReadMetrics(shuffleReadMetrics: Option[ShuffleReadMetrics]) {
    _shuffleReadMetrics = shuffleReadMetrics
  }

  /**
   * ShuffleReadMetrics per dependency for collecting independently while task is in progress.
   */
  @transient private lazy val depsShuffleReadMetrics: ArrayBuffer[ShuffleReadMetrics] =
    new ArrayBuffer[ShuffleReadMetrics]()

  /**
   * If this task writes to shuffle output, metrics on the written shuffle data will be collected
   * here
   */
  var shuffleWriteMetrics: Option[ShuffleWriteMetrics] = None

  /**
   * Storage statuses of any blocks that have been updated as a result of this task.
   */
  var updatedBlocks: Option[Seq[(BlockId, BlockStatus)]] = None

  /**
   * A task may have multiple shuffle readers for multiple dependencies. To avoid synchronization
   * issues from readers in different threads, in-progress tasks use a ShuffleReadMetrics for each
   * dependency, and merge these metrics before reporting them to the driver. This method returns
   * a ShuffleReadMetrics for a dependency and registers it for merging later.
   */
  private [spark] def createShuffleReadMetricsForDependency(): ShuffleReadMetrics = synchronized {
    val readMetrics = new ShuffleReadMetrics()
    depsShuffleReadMetrics += readMetrics
    readMetrics
  }

  /**
   * Returns the input metrics object that the task should use. Currently, if
   * there exists an input metric with the same readMethod, we return that one
   * so the caller can accumulate bytes read. If the readMethod is different
   * than previously seen by this task, we return a new InputMetric but don't
   * record it.
   *
   * Once https://issues.apache.org/jira/browse/SPARK-5225 is addressed,
   * we can store all the different inputMetrics (one per readMethod).
   */
  private[spark] def getInputMetricsForReadMethod(readMethod: DataReadMethod):
    InputMetrics =synchronized {
    _inputMetrics match {
      case None =>
        val metrics = new InputMetrics(readMethod)
        _inputMetrics = Some(metrics)
        metrics
      case Some(metrics @ InputMetrics(method)) if method == readMethod =>
        metrics
      case Some(InputMetrics(method)) =>
       new InputMetrics(readMethod)
    }
  }

  /**
   * Aggregates shuffle read metrics for all registered dependencies into shuffleReadMetrics.
   */
  private[spark] def updateShuffleReadMetrics(): Unit = synchronized {
    val merged = new ShuffleReadMetrics()
    for (depMetrics <- depsShuffleReadMetrics) {
      merged.incFetchWaitTime(depMetrics.fetchWaitTime)
      merged.incLocalBlocksFetched(depMetrics.localBlocksFetched)
      merged.incRemoteBlocksFetched(depMetrics.remoteBlocksFetched)
      merged.incRemoteBytesRead(depMetrics.remoteBytesRead)
      merged.incRecordsRead(depMetrics.recordsRead)
    }
    _shuffleReadMetrics = Some(merged)
  }

  private[spark] def updateInputMetrics(): Unit = synchronized {
    inputMetrics.foreach(_.updateBytesRead())
  }
}

private[spark] object TaskMetrics {
  def empty: TaskMetrics = new TaskMetrics
}

/**
 * :: DeveloperApi ::
 * Method by which input data was read.  Network means that the data was read over the network
 * from a remote block manager (which may have stored the data on-disk or in-memory).
 */
@DeveloperApi
object DataReadMethod extends Enumeration with Serializable {
  type DataReadMethod = Value
  val Memory, Disk, Hadoop, Network = Value
}

/**
 * :: DeveloperApi ::
 * Method by which output data was written.
 */
@DeveloperApi
object DataWriteMethod extends Enumeration with Serializable {
  type DataWriteMethod = Value
  val Hadoop = Value
}

/**
 * :: DeveloperApi ::
 * Metrics about reading input data.
 */
@DeveloperApi
case class InputMetrics(readMethod: DataReadMethod.Value) {

  /**
   * This is volatile so that it is visible to the updater thread.
   */
  @volatile @transient var bytesReadCallback: Option[() => Long] = None

  /**
   * Total bytes read.
   */
  private var _bytesRead: Long = _
  def bytesRead: Long = _bytesRead
  def incBytesRead(bytes: Long) = _bytesRead += bytes

  /**
   * Total records read.
   */
  private var _recordsRead: Long = _
  def recordsRead: Long = _recordsRead
  def incRecordsRead(records: Long) =  _recordsRead += records

  /**
   * Invoke the bytesReadCallback and mutate bytesRead.
   */
  def updateBytesRead() {
    bytesReadCallback.foreach { c =>
      _bytesRead = c()
    }
  }

 /**
  * Register a function that can be called to get up-to-date information on how many bytes the task
  * has read from an input source.
  */
  def setBytesReadCallback(f: Option[() => Long]) {
    bytesReadCallback = f
  }
}

/**
 * :: DeveloperApi ::
 * Metrics about writing output data.
 */
@DeveloperApi
case class OutputMetrics(writeMethod: DataWriteMethod.Value) {
  /**
   * Total bytes written
   */
  private var _bytesWritten: Long = _
  def bytesWritten = _bytesWritten
  private[spark] def setBytesWritten(value : Long) = _bytesWritten = value

  /**
   * Total records written
   */
  private var _recordsWritten: Long = 0L
  def recordsWritten = _recordsWritten
  private[spark] def setRecordsWritten(value: Long) = _recordsWritten = value
}

/**
 * :: DeveloperApi ::
 * Metrics pertaining to shuffle data read in a given task.
 */
@DeveloperApi
class ShuffleReadMetrics extends Serializable {
  /**
   * Number of remote blocks fetched in this shuffle by this task
   */
  private var _remoteBlocksFetched: Int = _
  def remoteBlocksFetched = _remoteBlocksFetched
  private[spark] def incRemoteBlocksFetched(value: Int) = _remoteBlocksFetched += value
  private[spark] def decRemoteBlocksFetched(value: Int) = _remoteBlocksFetched -= value
  
  /**
   * Number of local blocks fetched in this shuffle by this task
   */
  private var _localBlocksFetched: Int = _
  def localBlocksFetched = _localBlocksFetched
  private[spark] def incLocalBlocksFetched(value: Int) = _localBlocksFetched += value
  private[spark] def decLocalBlocksFetched(value: Int) = _localBlocksFetched -= value

  /**
   * Time the task spent waiting for remote shuffle blocks. This only includes the time
   * blocking on shuffle input data. For instance if block B is being fetched while the task is
   * still not finished processing block A, it is not considered to be blocking on block B.
   */
  private var _fetchWaitTime: Long = _
  def fetchWaitTime = _fetchWaitTime
  private[spark] def incFetchWaitTime(value: Long) = _fetchWaitTime += value
  private[spark] def decFetchWaitTime(value: Long) = _fetchWaitTime -= value
  
  /**
   * Total number of remote bytes read from the shuffle by this task
   */
  private var _remoteBytesRead: Long = _
  def remoteBytesRead = _remoteBytesRead
  private[spark] def incRemoteBytesRead(value: Long) = _remoteBytesRead += value
  private[spark] def decRemoteBytesRead(value: Long) = _remoteBytesRead -= value

  /**
   * Number of blocks fetched in this shuffle by this task (remote or local)
   */
  def totalBlocksFetched = _remoteBlocksFetched + _localBlocksFetched

  /**
   * Total number of records read from the shuffle by this task
   */
  private var _recordsRead: Long = _
  def recordsRead = _recordsRead
  private[spark] def incRecordsRead(value: Long) = _recordsRead += value
  private[spark] def decRecordsRead(value: Long) = _recordsRead -= value
}

/**
 * :: DeveloperApi ::
 * Metrics pertaining to shuffle data written in a given task.
 */
@DeveloperApi
class ShuffleWriteMetrics extends Serializable {
  /**
   * Number of bytes written for the shuffle by this task
   */
  @volatile private var _shuffleBytesWritten: Long = _
  def shuffleBytesWritten = _shuffleBytesWritten
  private[spark] def incShuffleBytesWritten(value: Long) = _shuffleBytesWritten += value
  private[spark] def decShuffleBytesWritten(value: Long) = _shuffleBytesWritten -= value
  
  /**
   * Time the task spent blocking on writes to disk or buffer cache, in nanoseconds
   */
  @volatile private var _shuffleWriteTime: Long = _
  def shuffleWriteTime= _shuffleWriteTime
  private[spark] def incShuffleWriteTime(value: Long) = _shuffleWriteTime += value
  private[spark] def decShuffleWriteTime(value: Long) = _shuffleWriteTime -= value
  
  /**
   * Total number of records written to the shuffle by this task
   */
  @volatile private var _shuffleRecordsWritten: Long = _
  def shuffleRecordsWritten = _shuffleRecordsWritten
  private[spark] def incShuffleRecordsWritten(value: Long) = _shuffleRecordsWritten += value
  private[spark] def decShuffleRecordsWritten(value: Long) = _shuffleRecordsWritten -= value
  private[spark] def setShuffleRecordsWritten(value: Long) = _shuffleRecordsWritten = value
}
