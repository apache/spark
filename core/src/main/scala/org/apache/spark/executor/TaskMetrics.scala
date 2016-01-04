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

import org.apache.spark.{Accumulator, InternalAccumulator}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.executor.DataReadMethod.DataReadMethod
import org.apache.spark.storage.{BlockId, BlockStatus}
import org.apache.spark.util.Utils

/**
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
private[spark] class TaskMetrics(
    accumMap: Map[String, Accumulator[Long]],
    val hostname: String = TaskMetrics.getCachedHostName)
  extends Serializable {

  import InternalAccumulator._

  def this(host: String) {
    this(TaskMetrics.newAccumMap, host)
  }

  // Needed for Java
  def this() {
    this(TaskMetrics.getCachedHostName)
  }

  /**
   * Return the internal accumulator associated with the specified metric, assuming it exists.
   */
  private def getAccum(baseName: String): Accumulator[Long] = {
    val fullName = METRICS_PREFIX + baseName
    assert(accumMap.contains(fullName), s"metric '$fullName' is missing")
    accumMap(fullName)
  }

  /**
   * Time taken on the executor to deserialize this task.
   */
  private val _executorDeserializeTime: Accumulator[Long] = getAccum(EXECUTOR_DESERIALIZE_TIME)
  def executorDeserializeTime: Long = _executorDeserializeTime.value
  def setExecutorDeserializeTime(v: Long) = _executorDeserializeTime.setValue(v)

  /**
   * Time the executor spends actually running the task (including fetching shuffle data).
   */
  private val _executorRunTime: Accumulator[Long] = getAccum(EXECUTOR_RUN_TIME)
  def executorRunTime: Long = _executorRunTime.value
  def setExecutorRunTime(v: Long) = _executorRunTime.setValue(v)

  /**
   * The number of bytes this task transmitted back to the driver as the TaskResult.
   */
  private val _resultSize: Accumulator[Long] = getAccum(RESULT_SIZE)
  def resultSize: Long = _resultSize.value
  def setResultSize(v: Long) = _resultSize.setValue(v)

  /**
   * Amount of time the JVM spent in garbage collection while executing this task.
   */
  private val _jvmGCTime: Accumulator[Long] = getAccum(JVM_GC_TIME)
  def jvmGCTime: Long = _jvmGCTime.value
  def setJvmGCTime(v: Long) = _jvmGCTime.setValue(v)

  /**
   * Amount of time spent serializing the task result.
   */
  private val _resultSerializationTime: Accumulator[Long] = getAccum(RESULT_SERIALIZATION_TIME)
  def resultSerializationTime: Long = _resultSerializationTime.value
  def setResultSerializationTime(v: Long) = _resultSerializationTime.setValue(v)

  /**
   * The number of in-memory bytes spilled by this task.
   */
  private val _memoryBytesSpilled: Accumulator[Long] = getAccum(MEMORY_BYTES_SPILLED)
  def memoryBytesSpilled: Long = _memoryBytesSpilled.value
  def incMemoryBytesSpilled(v: Long): Unit = _memoryBytesSpilled.add(v)

  /**
   * The number of on-disk bytes spilled by this task.
   */
  private val _diskBytesSpilled: Accumulator[Long] = getAccum(DISK_BYTES_SPILLED)
  def diskBytesSpilled: Long = _diskBytesSpilled.value
  def incDiskBytesSpilled(v: Long): Unit = _diskBytesSpilled.add(v)

  /**
   * ...
   *
   * Execution memory refers to the memory used by internal data structures created
   * during shuffles, aggregations and joins. The value of this accumulator should be
   * approximately the sum of the peak sizes across all such data structures created
   * in this task. For SQL jobs, this only tracks all unsafe operators and ExternalSort.
   */
  private val _peakExecutionMemory: Accumulator[Long] = getAccum(PEAK_EXECUTION_MEMORY)
  def peakExecutionMemory: Long = _peakExecutionMemory.value
  def incPeakExecutionMemory(v: Long): Unit = _peakExecutionMemory.add(v)


  /* ================================== *
   |        SHUFFLE READ METRICS        |
   * ================================== */

  /**
   * Aggregated [[ShuffleReadMetrics]] across all shuffle dependencies.
   */
  private var _shuffleReadMetrics: Option[ShuffleReadMetrics] = None
  def shuffleReadMetrics: Option[ShuffleReadMetrics] = _shuffleReadMetrics
  def setShuffleReadMetrics(metrics: Option[ShuffleReadMetrics]): Unit = {
    _shuffleReadMetrics = metrics
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
   * All values set in this [[ShuffleReadMetrics]] will be merged synchronously later.
   */
  def createShuffleReadMetricsForDependency(): ShuffleReadMetrics = synchronized {
    val readMetrics = new ShuffleReadMetrics
    tempShuffleReadMetrics += readMetrics
    readMetrics
  }

  /**
   * Merge values across all temporary [[ShuffleReadMetrics]] into `_shuffleReadMetrics`.
   */
  def updateShuffleReadMetrics(): Unit = synchronized {
    val agg = _shuffleReadMetrics.getOrElse {
      val metrics = new ShuffleReadMetrics(accumMap)
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








  

  /**
   * If this task reads from a HadoopRDD or from persisted data, metrics on how much data was read
   * are stored here.
   */
  private var _inputMetrics: Option[InputMetrics] = None

  def inputMetrics: Option[InputMetrics] = _inputMetrics

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
   * If this task writes to shuffle output, metrics on the written shuffle data will be collected
   * here
   */
  var shuffleWriteMetrics: Option[ShuffleWriteMetrics] = None

  /**
   * Storage statuses of any blocks that have been updated as a result of this task.
   */
  var updatedBlocks: Option[Seq[(BlockId, BlockStatus)]] = None

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
  private[spark] def getInputMetricsForReadMethod(readMethod: DataReadMethod): InputMetrics = {
    synchronized {
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
  }

  private[spark] def updateInputMetrics(): Unit = synchronized {
    inputMetrics.foreach(_.updateBytesRead())
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
   * Construct a set of new accumulators indexed by metric name.
   */
  private def newAccumMap: Map[String, Accumulator[Long]] = {
    InternalAccumulator.create().map { accum => (accum.name.get, accum) }.toMap
  }
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
  def incBytesRead(bytes: Long): Unit = _bytesRead += bytes

  /**
   * Total records read.
   */
  private var _recordsRead: Long = _
  def recordsRead: Long = _recordsRead
  def incRecordsRead(records: Long): Unit = _recordsRead += records

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
  def bytesWritten: Long = _bytesWritten
  private[spark] def setBytesWritten(value : Long): Unit = _bytesWritten = value

  /**
   * Total records written
   */
  private var _recordsWritten: Long = 0L
  def recordsWritten: Long = _recordsWritten
  private[spark] def setRecordsWritten(value: Long): Unit = _recordsWritten = value
}

/**
 * Metrics pertaining to shuffle data read in a given task.
 */
private[spark] class ShuffleReadMetrics(accumMap: Map[String, Accumulator[Long]])
  extends Serializable {

  import InternalAccumulator.shuffleRead._

  /**
   * Construct a temporary [[ShuffleReadMetrics]], one for each shuffle dependency.
   */
  def this() {
    this(InternalAccumulator.createShuffleReadMetrics().map { a => (a.name.get, a) }.toMap)
  }

  /**
   * Return the internal accumulator associated with the specified metric, assuming it exists.
   * TODO: duplicate code alert!
   */
  private def getAccum(baseName: String): Accumulator[Long] = {
    val fullName = InternalAccumulator.SHUFFLE_READ_METRICS_PREFIX + baseName
    assert(accumMap.contains(fullName), s"metric '$fullName' is missing")
    accumMap(fullName)
  }

  /**
   * Number of remote blocks fetched in this shuffle by this task.
   */
  private val _remoteBlocksFetched: Accumulator[Long] = getAccum(REMOTE_BLOCKS_FETCHED)
  def remoteBlocksFetched: Long = _remoteBlocksFetched.value
  def incRemoteBlocksFetched(v: Long): Unit = _remoteBlocksFetched.add(v)
  def setRemoteBlocksFetched(v: Long): Unit = _remoteBlocksFetched.setValue(v)

  /**
   * Number of local blocks fetched in this shuffle by this task.
   */
  private val _localBlocksFetched: Accumulator[Long] = getAccum(LOCAL_BLOCKS_FETCHED)
  def localBlocksFetched: Long = _localBlocksFetched.value
  def incLocalBlocksFetched(v: Long): Unit = _localBlocksFetched.add(v)
  def setLocalBlocksFetched(v: Long): Unit = _localBlocksFetched.setValue(v)

  /**
   * Time the task spent waiting for remote shuffle blocks. This only includes the time
   * blocking on shuffle input data. For instance if block B is being fetched while the task is
   * still not finished processing block A, it is not considered to be blocking on block B.
   */
  private val _fetchWaitTime: Accumulator[Long] = getAccum(FETCH_WAIT_TIME)
  def fetchWaitTime: Long = _fetchWaitTime.value
  def incFetchWaitTime(v: Long): Unit = _fetchWaitTime.add(v)
  def setFetchWaitTime(v: Long): Unit = _fetchWaitTime.setValue(v)

  /**
   * Total number of remote bytes read from the shuffle by this task.
   */
  private val _remoteBytesRead: Accumulator[Long] = getAccum(REMOTE_BYTES_READ)
  def remoteBytesRead: Long = _remoteBytesRead.value
  def incRemoteBytesRead(v: Long): Unit = _remoteBytesRead.add(v)
  def setRemoteBytesRead(v: Long): Unit = _remoteBytesRead.setValue(v)

  /**
   * Shuffle data that was read from the local disk (as opposed to from a remote executor).
   */
  private val _localBytesRead: Accumulator[Long] = getAccum(LOCAL_BYTES_READ)
  def localBytesRead: Long = _localBytesRead.value
  def incLocalBytesRead(v: Long): Unit = _localBytesRead.add(v)
  def setLocalBytesRead(v: Long): Unit = _localBytesRead.setValue(v)

  /**
   * Total number of records read from the shuffle by this task.
   */
  private val _recordsRead: Accumulator[Long] = getAccum(RECORDS_READ)
  def recordsRead: Long = _recordsRead.value
  def incRecordsRead(v: Long): Unit = _recordsRead.add(v)
  def setRecordsRead(v: Long): Unit = _recordsRead.setValue(v)

  /**
   * Total bytes fetched in the shuffle by this task (both remote and local).
   */
  def totalBytesRead: Long = remoteBytesRead + localBytesRead

  /**
   * Number of blocks fetched in this shuffle by this task (remote or local).
   */
  def totalBlocksFetched: Long = remoteBlocksFetched + localBlocksFetched
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
  def shuffleBytesWritten: Long = _shuffleBytesWritten
  private[spark] def incShuffleBytesWritten(value: Long) = _shuffleBytesWritten += value
  private[spark] def decShuffleBytesWritten(value: Long) = _shuffleBytesWritten -= value

  /**
   * Time the task spent blocking on writes to disk or buffer cache, in nanoseconds
   */
  @volatile private var _shuffleWriteTime: Long = _
  def shuffleWriteTime: Long = _shuffleWriteTime
  private[spark] def incShuffleWriteTime(value: Long) = _shuffleWriteTime += value

  /**
   * Total number of records written to the shuffle by this task
   */
  @volatile private var _shuffleRecordsWritten: Long = _
  def shuffleRecordsWritten: Long = _shuffleRecordsWritten
  private[spark] def incShuffleRecordsWritten(value: Long) = _shuffleRecordsWritten += value
  private[spark] def decShuffleRecordsWritten(value: Long) = _shuffleRecordsWritten -= value
}
