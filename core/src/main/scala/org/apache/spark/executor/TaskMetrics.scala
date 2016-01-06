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
    this(InternalAccumulator.create().map { a => (a.name.get, a) }.toMap, host)
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
      val metrics = new OutputMetrics(writeMethod, accumMap)
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
        val metrics = new InputMetrics(readMethod, accumMap)
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
   * Storage statuses of any blocks that have been updated as a result of this task.
   */
  var updatedBlocks: Option[Seq[(BlockId, BlockStatus)]] = None

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


/**
 * Method by which input data was read.  Network means that the data was read over the network
 * from a remote block manager (which may have stored the data on-disk or in-memory).
 */
@deprecated("DataReadMethod will be made private in a future version.", "2.0.0")
object DataReadMethod extends Enumeration with Serializable {
  type DataReadMethod = Value
  val Memory, Disk, Hadoop, Network = Value
}


/**
 * Method by which output data was written.
 */
@deprecated("DataWriteMethod will be made private in a future version.", "2.0.0")
object DataWriteMethod extends Enumeration with Serializable {
  type DataWriteMethod = Value
  val Hadoop = Value
}


/**
 * Metrics about reading input data.
 */
@deprecated("InputMetrics will be made private in a future version.", "2.0.0")
class InputMetrics private (
    val readMethod: DataReadMethod.Value,
    _bytesRead: Accumulator[Long],
    _recordsRead: Accumulator[Long])
  extends Serializable {

  private[executor] def this(
      readMethod: DataReadMethod.Value,
      accumMap: Map[String, Accumulator[Long]]) {
    this(
      readMethod,
      accumMap(InternalAccumulator.input.BYTES_READ),
      accumMap(InternalAccumulator.input.RECORDS_READ))
  }

  /**
   * Create a new [[InputMetrics]] that is not associated with any particular task.
   *
   * This mainly exists because of SPARK-5225, where we are forced to use a dummy [[InputMetrics]]
   * because we want to ignore metrics from a second read method. In the future, we should revisit
   * whether this is needed.
   *
   * A better alternative to use is [[TaskMetrics.registerInputMetrics]].
   */
  private[spark] def this(readMethod: DataReadMethod.Value) {
    this(
      readMethod,
      InternalAccumulator.createInputAccums().map { a => (a.name.get, a) }.toMap)
  }

  /**
   * Total number of bytes read.
   */
  def bytesRead: Long = _bytesRead.value

  /**
   * Total number of records read.
   */
  def recordsRead: Long = _recordsRead.value

  private[spark] def setBytesRead(v: Long): Unit = _bytesRead.add(v)
  private[spark] def incRecordsRead(v: Long): Unit = _recordsRead.add(v)
}


/**
 * Metrics about writing output data.
 */
@deprecated("OutputMetrics will be made private in a future version.", "2.0.0")
class OutputMetrics private (
    val writeMethod: DataWriteMethod.Value,
    _bytesWritten: Accumulator[Long],
    _recordsWritten: Accumulator[Long])
  extends Serializable {

  private[executor] def this(
      writeMethod: DataWriteMethod.Value,
      accumMap: Map[String, Accumulator[Long]]) {
    this(
      writeMethod,
      accumMap(InternalAccumulator.output.BYTES_WRITTEN),
      accumMap(InternalAccumulator.output.RECORDS_WRITTEN))
  }

  /**
   * Total number of bytes written.
   */
  def bytesWritten: Long = _bytesWritten.value

  /**
   * Total number of records written.
   */
  def recordsWritten: Long = _recordsWritten.value

  private[spark] def setBytesWritten(v: Long): Unit = _bytesWritten.setValue(v)
  private[spark] def setRecordsWritten(v: Long): Unit = _recordsWritten.setValue(v)
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
   * Create a new [[ShuffleReadMetrics]] that is not associated with any particular task.
   *
   * This mainly exists for legacy reasons, because we use dummy [[ShuffleReadMetrics]] in
   * many places only to merge their values together later. In the future, we should revisit
   * whether this is needed.
   *
   * * A better alternative to use is [[TaskMetrics.registerTempShuffleReadMetrics]].
   */
  private[spark] def this() {
    this(InternalAccumulator.createShuffleReadAccums().map { a => (a.name.get, a) }.toMap)
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


/**
 * Metrics pertaining to shuffle data written in a given task.
 */
@deprecated("ShuffleWriteMetrics will be made private in a future version.", "2.0.0")
class ShuffleWriteMetrics private (
    _bytesWritten: Accumulator[Long],
    _recordsWritten: Accumulator[Long],
    _shuffleWriteTime: Accumulator[Long])
  extends Serializable {

  private[executor] def this(accumMap: Map[String, Accumulator[Long]]) {
    this(
      accumMap(InternalAccumulator.shuffleWrite.BYTES_WRITTEN),
      accumMap(InternalAccumulator.shuffleWrite.RECORDS_WRITTEN),
      accumMap(InternalAccumulator.shuffleWrite.WRITE_TIME))
  }

  /**
   * Create a new [[ShuffleWriteMetrics]] that is not associated with any particular task.
   *
   * This mainly exists for legacy reasons, because we use dummy [[ShuffleWriteMetrics]] in
   * many places only to merge their values together later. In the future, we should revisit
   * whether this is needed.
   *
   * A better alternative to use is [[TaskMetrics.registerShuffleWriteMetrics]].
   */
  private[spark] def this() {
    this(InternalAccumulator.createShuffleWriteAccums().map { a => (a.name.get, a) }.toMap)
  }

  /**
   * Number of bytes written for the shuffle by this task.
   */
  def bytesWritten: Long = _bytesWritten.value

  /**
   * Total number of records written to the shuffle by this task.
   */
  def recordsWritten: Long = _recordsWritten.value

  /**
   * Time the task spent blocking on writes to disk or buffer cache, in nanoseconds.
   */
  def shuffleWriteTime: Long = _shuffleWriteTime.value

  // TODO: these are not thread-safe. Is that OK?

  private[spark] def incBytesWritten(v: Long): Unit = _bytesWritten.add(v)
  private[spark] def incRecordsWritten(v: Long): Unit = _recordsWritten.add(v)
  private[spark] def incWriteTime(v: Long): Unit = _shuffleWriteTime.add(v)
  private[spark] def decBytesWritten(v: Long): Unit = {
    _bytesWritten.setValue(bytesWritten - v)
  }
  private[spark] def decRecordsWritten(v: Long): Unit = {
    _recordsWritten.setValue(recordsWritten - v)
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
