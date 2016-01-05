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
@deprecated("TaskMetrics will be made private in a future version.", "2.0.0")
class TaskMetrics(
    accumMap: Map[String, Accumulator[Long]],
    val hostname: String = TaskMetrics.getCachedHostName)
  extends Serializable {

  import InternalAccumulator._

  def this(host: String) {
    this(InternalAccumulator.create().map { accum => (accum.name.get, accum) }.toMap, host)
  }

  // Needed for Java
  def this() {
    this(TaskMetrics.getCachedHostName)
  }

  // Each metric is internally represented as an accumulator
  private val _executorDeserializeTime: Accumulator[Long] = getAccum(EXECUTOR_DESERIALIZE_TIME)
  private val _executorRunTime: Accumulator[Long] = getAccum(EXECUTOR_RUN_TIME)
  private val _resultSize: Accumulator[Long] = getAccum(RESULT_SIZE)
  private val _jvmGCTime: Accumulator[Long] = getAccum(JVM_GC_TIME)
  private val _resultSerializationTime: Accumulator[Long] = getAccum(RESULT_SERIALIZATION_TIME)
  private val _memoryBytesSpilled: Accumulator[Long] = getAccum(MEMORY_BYTES_SPILLED)
  private val _diskBytesSpilled: Accumulator[Long] = getAccum(DISK_BYTES_SPILLED)
  private val _peakExecutionMemory: Accumulator[Long] = getAccum(PEAK_EXECUTION_MEMORY)

  /**
   * Return the internal accumulator associated with the specified metric, assuming it exists.
   */
  private def getAccum(name: String): Accumulator[Long] = {
    assert(accumMap.contains(name), s"metric '$name' is missing")
    accumMap(name)
  }

  /**
   * Time taken on the executor to deserialize this task.
   */
  def executorDeserializeTime: Long = _executorDeserializeTime.value

  /**
   * Time the executor spends actually running the task (including fetching shuffle data).
   */
  def executorRunTime: Long = _executorRunTime.value

  /**
   * The number of bytes this task transmitted back to the driver as the TaskResult.
   */
  def resultSize: Long = _resultSize.value

  /**
   * Amount of time the JVM spent in garbage collection while executing this task.
   */
  def jvmGCTime: Long = _jvmGCTime.value

  /**
   * Amount of time spent serializing the task result.
   */
  def resultSerializationTime: Long = _resultSerializationTime.value

  /**
   * The number of in-memory bytes spilled by this task.
   */
  def memoryBytesSpilled: Long = _memoryBytesSpilled.value

  /**
   * The number of on-disk bytes spilled by this task.
   */
  def diskBytesSpilled: Long = _diskBytesSpilled.value

  /**
   * Peak memory used by internal data structures created during shuffles, aggregations and
   * joins. The value of this accumulator should be approximately the sum of the peak sizes
   * across all such data structures created in this task. For SQL jobs, this only tracks all
   * unsafe operators and ExternalSort.
   */
  def peakExecutionMemory: Long = _peakExecutionMemory.value

  private[spark] def setExecutorDeserializeTime(v: Long) = _executorDeserializeTime.setValue(v)
  private[spark] def setExecutorRunTime(v: Long) = _executorRunTime.setValue(v)
  private[spark] def setResultSize(v: Long) = _resultSize.setValue(v)
  private[spark] def setJvmGCTime(v: Long) = _jvmGCTime.setValue(v)
  private[spark] def setResultSerializationTime(v: Long) = _resultSerializationTime.setValue(v)
  private[spark] def incMemoryBytesSpilled(v: Long): Unit = _memoryBytesSpilled.add(v)
  private[spark] def incDiskBytesSpilled(v: Long): Unit = _diskBytesSpilled.add(v)
  private[spark] def incPeakExecutionMemory(v: Long): Unit = _peakExecutionMemory.add(v)


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
  def registerShuffleWriteMetrics(): ShuffleWriteMetrics = {
    _shuffleWriteMetrics.getOrElse {
      val metrics = new ShuffleWriteMetrics(accumMap)
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
    val readMetrics = ShuffleReadMetrics.createDummy()
    tempShuffleReadMetrics += readMetrics
    readMetrics
  }

  /**
   * Merge values across all temporary [[ShuffleReadMetrics]] into `_shuffleReadMetrics`.
   * This is expected to be called on executor heartbeat and at the end of a task.
   */
  private[spark] def mergeShuffleReadMetrics(): Unit = synchronized {
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















  /* ================================== *
   |        OTHER THINGS... WIP         |
   * ================================== */

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
@deprecated("ShuffleReadMetrics will be made private in a future version.", "2.0.0")
class ShuffleReadMetrics private (
    _remoteBlocksFetched: Accumulator[Long],
    _localBlocksFetched: Accumulator[Long],
    _remoteBytesRead: Accumulator[Long],
    _localBytesRead: Accumulator[Long],
    _fetchWaitTime: Accumulator[Long],
    _recordsRead: Accumulator[Long])
  extends Serializable {

  private[executor] def this(accumMap: Map[String, Accumulator[Long]]) {
    this(
      accumMap(InternalAccumulator.shuffleRead.REMOTE_BLOCKS_FETCHED),
      accumMap(InternalAccumulator.shuffleRead.LOCAL_BLOCKS_FETCHED),
      accumMap(InternalAccumulator.shuffleRead.REMOTE_BYTES_READ),
      accumMap(InternalAccumulator.shuffleRead.LOCAL_BYTES_READ),
      accumMap(InternalAccumulator.shuffleRead.FETCH_WAIT_TIME),
      accumMap(InternalAccumulator.shuffleRead.RECORDS_READ))
  }

  /**
   * Number of remote blocks fetched in this shuffle by this task.
   */
  def remoteBlocksFetched: Long = _remoteBlocksFetched.value

  /**
   * Number of local blocks fetched in this shuffle by this task.
   */
  def localBlocksFetched: Long = _localBlocksFetched.value

  /**
   * Total number of remote bytes read from the shuffle by this task.
   */
  def remoteBytesRead: Long = _remoteBytesRead.value

  /**
   * Shuffle data that was read from the local disk (as opposed to from a remote executor).
   */
  def localBytesRead: Long = _localBytesRead.value

  /**
   * Time the task spent waiting for remote shuffle blocks. This only includes the time
   * blocking on shuffle input data. For instance if block B is being fetched while the task is
   * still not finished processing block A, it is not considered to be blocking on block B.
   */
  def fetchWaitTime: Long = _fetchWaitTime.value

  /**
   * Total number of records read from the shuffle by this task.
   */
  def recordsRead: Long = _recordsRead.value

  /**
   * Total bytes fetched in the shuffle by this task (both remote and local).
   */
  def totalBytesRead: Long = remoteBytesRead + localBytesRead

  /**
   * Number of blocks fetched in this shuffle by this task (remote or local).
   */
  def totalBlocksFetched: Long = remoteBlocksFetched + localBlocksFetched

  private[spark] def incRemoteBlocksFetched(v: Long): Unit = _remoteBlocksFetched.add(v)
  private[spark] def incLocalBlocksFetched(v: Long): Unit = _localBlocksFetched.add(v)
  private[spark] def incRemoteBytesRead(v: Long): Unit = _remoteBytesRead.add(v)
  private[spark] def incLocalBytesRead(v: Long): Unit = _localBytesRead.add(v)
  private[spark] def incFetchWaitTime(v: Long): Unit = _fetchWaitTime.add(v)
  private[spark] def incRecordsRead(v: Long): Unit = _recordsRead.add(v)

  private[spark] def setRemoteBlocksFetched(v: Long): Unit = _remoteBlocksFetched.setValue(v)
  private[spark] def setLocalBlocksFetched(v: Long): Unit = _localBlocksFetched.setValue(v)
  private[spark] def setRemoteBytesRead(v: Long): Unit = _remoteBytesRead.setValue(v)
  private[spark] def setLocalBytesRead(v: Long): Unit = _localBytesRead.setValue(v)
  private[spark] def setFetchWaitTime(v: Long): Unit = _fetchWaitTime.setValue(v)
  private[spark] def setRecordsRead(v: Long): Unit = _recordsRead.setValue(v)
}

private[spark] object ShuffleReadMetrics {

  /**
   * Create a new [[ShuffleReadMetrics]] that is not associated with any particular task.
   *
   * This mainly exists for legacy reasons, because we use dummy [[ShuffleReadMetrics]] in
   * many places only to merge their values together later. In the future, we should revisit
   * whether this is needed.
   */
  def createDummy(): ShuffleReadMetrics = {
    new ShuffleReadMetrics(
      InternalAccumulator.createShuffleReadAccums().map { acc => (acc.name.get, acc) }.toMap)
  }
}


/**
 * Metrics pertaining to shuffle data written in a given task.
 */
@deprecated("ShuffleWriteMetrics will be made private in a future version.", "2.0.0")
class ShuffleWriteMetrics private (
    _shuffleBytesWritten: Accumulator[Long],
    _shuffleRecordsWritten: Accumulator[Long],
    _shuffleWriteTime: Accumulator[Long])
  extends Serializable {

  private[executor] def this(accumMap: Map[String, Accumulator[Long]]) {
    this(
      accumMap(InternalAccumulator.shuffleWrite.SHUFFLE_BYTES_WRITTEN),
      accumMap(InternalAccumulator.shuffleWrite.SHUFFLE_RECORDS_WRITTEN),
      accumMap(InternalAccumulator.shuffleWrite.SHUFFLE_WRITE_TIME))
  }

  /**
   * Number of bytes written for the shuffle by this task.
   */
  def shuffleBytesWritten: Long = _shuffleBytesWritten.value

  /**
   * Total number of records written to the shuffle by this task.
   */
  def shuffleRecordsWritten: Long = _shuffleRecordsWritten.value

  /**
   * Time the task spent blocking on writes to disk or buffer cache, in nanoseconds.
   */
  def shuffleWriteTime: Long = _shuffleWriteTime.value

  // TODO: these are not thread-safe. Is that OK?

  private[spark] def incShuffleBytesWritten(v: Long): Unit = _shuffleBytesWritten.add(v)
  private[spark] def incShuffleRecordsWritten(v: Long): Unit = _shuffleRecordsWritten.add(v)
  private[spark] def incShuffleWriteTime(v: Long): Unit = _shuffleWriteTime.add(v)
  private[spark] def decShuffleBytesWritten(v: Long): Unit = {
    _shuffleBytesWritten.setValue(shuffleBytesWritten - v)
  }
  private[spark] def decShuffleRecordsWritten(v: Long): Unit = {
    _shuffleRecordsWritten.setValue(shuffleRecordsWritten - v)
  }
}

private[spark] object ShuffleWriteMetrics {

  /**
   * Create a new [[ShuffleWriteMetrics]] that is not associated with any particular task.
   *
   * This mainly exists for legacy reasons, because we use dummy [[ShuffleWriteMetrics]] in
   * many places only to merge their values together later. In the future, we should revisit
   * whether this is needed.
   */
  def createDummy(): ShuffleWriteMetrics = {
    new ShuffleWriteMetrics(
      InternalAccumulator.createShuffleWriteAccums().map { acc => (acc.name.get, acc) }.toMap)
  }
}
