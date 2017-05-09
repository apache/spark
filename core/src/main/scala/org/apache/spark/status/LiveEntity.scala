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

package org.apache.spark.status

import java.util.Date
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.HashMap

import com.google.common.collect.Interners

import org.apache.spark.JobExecutionStatus
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.{AccumulableInfo, StageInfo, TaskInfo}
import org.apache.spark.status.api.v1
import org.apache.spark.storage.RDDInfo
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.AccumulatorContext
import org.apache.spark.util.collection.OpenHashSet
import org.apache.spark.util.kvstore.KVStore

/**
 * A mutable representation of a live entity in Spark (jobs, stages, tasks, et al). Every live
 * entity uses one of these instances to keep track of their evolving state, and periodically
 * flush an immutable view of the entity to the app state store.
 */
private[spark] abstract class LiveEntity {

  var lastWriteTime = -1L

  def write(store: ElementTrackingStore, now: Long, checkTriggers: Boolean = false): Unit = {
    // Always check triggers on the first write, since adding an element to the store may
    // cause the maximum count for the element type to be exceeded.
    store.write(doUpdate(), checkTriggers || lastWriteTime == -1L)
    lastWriteTime = now
  }

  /**
   * Returns an updated view of entity data, to be stored in the status store, reflecting the
   * latest information collected by the listener.
   */
  protected def doUpdate(): Any

}

private class LiveJob(
    val jobId: Int,
    name: String,
    submissionTime: Option[Date],
    val stageIds: Seq[Int],
    jobGroup: Option[String],
    numTasks: Int) extends LiveEntity {

  var activeTasks = 0
  var completedTasks = 0
  var failedTasks = 0

  // Holds both the stage ID and the task index, packed into a single long value.
  val completedIndices = new OpenHashSet[Long]()

  var killedTasks = 0
  var killedSummary: Map[String, Int] = Map()

  var skippedTasks = 0
  var skippedStages = Set[Int]()

  var status = JobExecutionStatus.RUNNING
  var completionTime: Option[Date] = None

  var completedStages: Set[Int] = Set()
  var activeStages = 0
  var failedStages = 0

  override protected def doUpdate(): Any = {
    val info = new v1.JobData(
      jobId,
      name,
      None, // description is always None?
      submissionTime,
      completionTime,
      stageIds,
      jobGroup,
      status,
      numTasks,
      activeTasks,
      completedTasks,
      skippedTasks,
      failedTasks,
      killedTasks,
      completedIndices.size,
      activeStages,
      completedStages.size,
      skippedStages.size,
      failedStages,
      killedSummary)
    new JobDataWrapper(info, skippedStages)
  }

}

private class LiveTask(
    var info: TaskInfo,
    stageId: Int,
    stageAttemptId: Int,
    lastUpdateTime: Option[Long]) extends LiveEntity {

  import LiveEntityHelpers._

  private var metrics: MetricsTracker = new MetricsTracker()

  var errorMessage: Option[String] = None

  /**
   * Update the metrics for the task and return the difference between the previous and new
   * values.
   */
  def updateMetrics(metrics: TaskMetrics): MetricsTracker = {
    if (metrics != null) {
      val old = this.metrics
      val newMetrics = new MetricsTracker()
      newMetrics.executorDeserializeTime = metrics.executorDeserializeTime
      newMetrics.executorDeserializeCpuTime = metrics.executorDeserializeCpuTime
      newMetrics.executorRunTime = metrics.executorRunTime
      newMetrics.executorCpuTime = metrics.executorCpuTime
      newMetrics.resultSize = metrics.resultSize
      newMetrics.jvmGcTime = metrics.jvmGCTime
      newMetrics.resultSerializationTime = metrics.resultSerializationTime
      newMetrics.memoryBytesSpilled = metrics.memoryBytesSpilled
      newMetrics.diskBytesSpilled = metrics.diskBytesSpilled
      newMetrics.peakExecutionMemory = metrics.peakExecutionMemory
      newMetrics.inputBytesRead = metrics.inputMetrics.bytesRead
      newMetrics.inputRecordsRead = metrics.inputMetrics.recordsRead
      newMetrics.outputBytesWritten = metrics.outputMetrics.bytesWritten
      newMetrics.outputRecordsWritten = metrics.outputMetrics.recordsWritten
      newMetrics.shuffleRemoteBlocksFetched = metrics.shuffleReadMetrics.remoteBlocksFetched
      newMetrics.shuffleLocalBlocksFetched = metrics.shuffleReadMetrics.localBlocksFetched
      newMetrics.shuffleFetchWaitTime = metrics.shuffleReadMetrics.fetchWaitTime
      newMetrics.shuffleRemoteBytesRead = metrics.shuffleReadMetrics.remoteBytesRead
      newMetrics.shuffleRemoteBytesReadToDisk = metrics.shuffleReadMetrics.remoteBytesReadToDisk
      newMetrics.shuffleLocalBytesRead = metrics.shuffleReadMetrics.localBytesRead
      newMetrics.shuffleRecordsRead = metrics.shuffleReadMetrics.recordsRead
      newMetrics.shuffleBytesWritten = metrics.shuffleWriteMetrics.bytesWritten
      newMetrics.shuffleWriteTime = metrics.shuffleWriteMetrics.writeTime
      newMetrics.shuffleRecordsWritten = metrics.shuffleWriteMetrics.recordsWritten

      this.metrics = newMetrics
      if (old.executorDeserializeTime >= 0L) {
        old.subtract(newMetrics)
        old
      } else {
        newMetrics
      }
    } else {
      null
    }
  }

  private def buildUpdate(): TaskDataWrapper = {
    val duration = if (info.finished) {
      info.duration
    } else {
      info.timeRunning(lastUpdateTime.getOrElse(System.currentTimeMillis()))
    }

    new TaskDataWrapper(
      info.taskId,
      info.index,
      info.attemptNumber,
      info.launchTime,
      if (info.gettingResult) info.gettingResultTime else -1L,
      duration,
      weakIntern(info.executorId),
      weakIntern(info.host),
      weakIntern(info.status),
      weakIntern(info.taskLocality.toString()),
      info.speculative,
      newAccumulatorInfos(info.accumulables),
      errorMessage,

      metrics.executorDeserializeTime,
      metrics.executorDeserializeCpuTime,
      metrics.executorRunTime,
      metrics.executorCpuTime,
      metrics.resultSize,
      metrics.jvmGcTime,
      metrics.resultSerializationTime,
      metrics.memoryBytesSpilled,
      metrics.diskBytesSpilled,
      metrics.peakExecutionMemory,
      metrics.inputBytesRead,
      metrics.inputRecordsRead,
      metrics.outputBytesWritten,
      metrics.outputRecordsWritten,
      metrics.shuffleRemoteBlocksFetched,
      metrics.shuffleLocalBlocksFetched,
      metrics.shuffleFetchWaitTime,
      metrics.shuffleRemoteBytesRead,
      metrics.shuffleRemoteBytesReadToDisk,
      metrics.shuffleLocalBytesRead,
      metrics.shuffleRecordsRead,
      metrics.shuffleBytesWritten,
      metrics.shuffleWriteTime,
      metrics.shuffleRecordsWritten,

      stageId,
      stageAttemptId)
  }

  override protected def doUpdate(): Any = {
    buildUpdate()
  }

  def updateAndGet(kvstore: KVStore, now: Long): TaskDataWrapper = {
    val info = buildUpdate()
    kvstore.write(info)
    lastWriteTime = now
    info
  }

}

private class LiveExecutor(val executorId: String, _addTime: Long) extends LiveEntity {

  var hostPort: String = null
  var host: String = null
  var isActive = true
  var totalCores = 0

  val addTime = new Date(_addTime)
  var removeTime: Date = null
  var removeReason: String = null

  var rddBlocks = 0
  var memoryUsed = 0L
  var diskUsed = 0L
  var maxTasks = 0
  var maxMemory = 0L

  var totalTasks = 0
  var activeTasks = 0
  var completedTasks = 0
  var failedTasks = 0
  var totalDuration = 0L
  var totalGcTime = 0L
  var totalInputBytes = 0L
  var totalShuffleRead = 0L
  var totalShuffleWrite = 0L
  var isBlacklisted = false

  var executorLogs = Map[String, String]()

  // Memory metrics. They may not be recorded (e.g. old event logs) so if totalOnHeap is not
  // initialized, the store will not contain this information.
  var totalOnHeap = -1L
  var totalOffHeap = 0L
  var usedOnHeap = 0L
  var usedOffHeap = 0L

  def hasMemoryInfo: Boolean = totalOnHeap >= 0L

  def hostname: String = if (host != null) host else hostPort.split(":")(0)

  override protected def doUpdate(): Any = {
    val memoryMetrics = if (totalOnHeap >= 0) {
      Some(new v1.MemoryMetrics(usedOnHeap, usedOffHeap, totalOnHeap, totalOffHeap))
    } else {
      None
    }

    val info = new v1.ExecutorSummary(
      executorId,
      if (hostPort != null) hostPort else host,
      isActive,
      rddBlocks,
      memoryUsed,
      diskUsed,
      totalCores,
      maxTasks,
      activeTasks,
      failedTasks,
      completedTasks,
      totalTasks,
      totalDuration,
      totalGcTime,
      totalInputBytes,
      totalShuffleRead,
      totalShuffleWrite,
      isBlacklisted,
      maxMemory,
      addTime,
      Option(removeTime),
      Option(removeReason),
      executorLogs,
      memoryMetrics)
    new ExecutorSummaryWrapper(info)
  }

}

private class MetricsTracker(default: Long = -1L) {
  var executorDeserializeTime = default
  var executorDeserializeCpuTime = default
  var executorRunTime = default
  var executorCpuTime = default
  var resultSize = default
  var jvmGcTime = default
  var resultSerializationTime = default
  var memoryBytesSpilled = default
  var diskBytesSpilled = default
  var peakExecutionMemory = default
  var inputBytesRead = default
  var inputRecordsRead = default
  var outputBytesWritten = default
  var outputRecordsWritten = default
  var shuffleRemoteBlocksFetched = default
  var shuffleLocalBlocksFetched = default
  var shuffleFetchWaitTime = default
  var shuffleRemoteBytesRead = default
  var shuffleRemoteBytesReadToDisk = default
  var shuffleLocalBytesRead = default
  var shuffleRecordsRead = default
  var shuffleBytesWritten = default
  var shuffleWriteTime = default
  var shuffleRecordsWritten = default

  def add(delta: MetricsTracker): Unit = {
    executorDeserializeTime += delta.executorDeserializeTime
    executorDeserializeCpuTime += delta.executorDeserializeCpuTime
    executorRunTime += delta.executorRunTime
    executorCpuTime += delta.executorCpuTime
    resultSize += delta.resultSize
    jvmGcTime += delta.jvmGcTime
    resultSerializationTime += delta.resultSerializationTime
    memoryBytesSpilled += delta.memoryBytesSpilled
    diskBytesSpilled += delta.diskBytesSpilled
    peakExecutionMemory += delta.peakExecutionMemory
    inputBytesRead += delta.inputBytesRead
    inputRecordsRead += delta.inputRecordsRead
    outputBytesWritten += delta.outputBytesWritten
    outputRecordsWritten += delta.outputRecordsWritten
    shuffleRemoteBlocksFetched += delta.shuffleRemoteBlocksFetched
    shuffleLocalBlocksFetched += delta.shuffleLocalBlocksFetched
    shuffleFetchWaitTime += delta.shuffleFetchWaitTime
    shuffleRemoteBytesRead += delta.shuffleRemoteBytesRead
    shuffleRemoteBytesReadToDisk += delta.shuffleRemoteBytesReadToDisk
    shuffleLocalBytesRead += delta.shuffleLocalBytesRead
    shuffleRecordsRead += delta.shuffleRecordsRead
    shuffleBytesWritten += delta.shuffleBytesWritten
    shuffleWriteTime += delta.shuffleWriteTime
    shuffleRecordsWritten += delta.shuffleRecordsWritten
  }

  def subtract(delta: MetricsTracker): Unit = {
    executorDeserializeTime -= delta.executorDeserializeTime
    executorDeserializeCpuTime -= delta.executorDeserializeCpuTime
    executorRunTime -= delta.executorRunTime
    executorCpuTime -= delta.executorCpuTime
    resultSize -= delta.resultSize
    jvmGcTime -= delta.jvmGcTime
    resultSerializationTime -= delta.resultSerializationTime
    memoryBytesSpilled -= delta.memoryBytesSpilled
    diskBytesSpilled -= delta.diskBytesSpilled
    peakExecutionMemory -= delta.peakExecutionMemory
    inputBytesRead -= delta.inputBytesRead
    inputRecordsRead -= delta.inputRecordsRead
    outputBytesWritten -= delta.outputBytesWritten
    outputRecordsWritten -= delta.outputRecordsWritten
    shuffleRemoteBlocksFetched -= delta.shuffleRemoteBlocksFetched
    shuffleLocalBlocksFetched -= delta.shuffleLocalBlocksFetched
    shuffleFetchWaitTime -= delta.shuffleFetchWaitTime
    shuffleRemoteBytesRead -= delta.shuffleRemoteBytesRead
    shuffleRemoteBytesReadToDisk -= delta.shuffleRemoteBytesReadToDisk
    shuffleLocalBytesRead -= delta.shuffleLocalBytesRead
    shuffleRecordsRead -= delta.shuffleRecordsRead
    shuffleBytesWritten -= delta.shuffleBytesWritten
    shuffleWriteTime -= delta.shuffleWriteTime
    shuffleRecordsWritten -= delta.shuffleRecordsWritten
  }

}

private class LiveExecutorStageSummary(
    stageId: Int,
    attemptId: Int,
    executorId: String) extends LiveEntity {

  var taskTime = 0L
  var succeededTasks = 0
  var failedTasks = 0
  var killedTasks = 0

  val metrics = new MetricsTracker(default = 0L)

  override protected def doUpdate(): Any = {
    val info = new v1.ExecutorStageSummary(
      taskTime,
      failedTasks,
      succeededTasks,
      killedTasks,
      metrics.inputBytesRead,
      metrics.inputRecordsRead,
      metrics.outputBytesWritten,
      metrics.outputRecordsWritten,
      metrics.shuffleRemoteBytesRead + metrics.shuffleLocalBytesRead,
      metrics.shuffleRecordsRead,
      metrics.shuffleBytesWritten,
      metrics.shuffleRecordsWritten,
      metrics.memoryBytesSpilled,
      metrics.diskBytesSpilled)
    new ExecutorStageSummaryWrapper(stageId, attemptId, executorId, info)
  }

}

private class LiveStage extends LiveEntity {

  import LiveEntityHelpers._

  var jobs = Seq[LiveJob]()
  var jobIds = Set[Int]()

  var info: StageInfo = null
  var status = v1.StageStatus.PENDING

  var description: Option[String] = None
  var schedulingPool: String = SparkUI.DEFAULT_POOL_NAME

  var activeTasks = 0
  var completedTasks = 0
  var failedTasks = 0
  val completedIndices = new OpenHashSet[Int]()

  var killedTasks = 0
  var killedSummary: Map[String, Int] = Map()

  var firstLaunchTime = Long.MaxValue

  var localitySummary: Map[String, Long] = Map()

  val metrics = new MetricsTracker(default = 0L)

  val executorSummaries = new HashMap[String, LiveExecutorStageSummary]()

  // Used for cleanup of tasks after they reach the configured limit. Not written to the store.
  @volatile var cleaning = false
  var savedTasks = new AtomicInteger(0)

  def executorSummary(executorId: String): LiveExecutorStageSummary = {
    executorSummaries.getOrElseUpdate(executorId,
      new LiveExecutorStageSummary(info.stageId, info.attemptId, executorId))
  }

  def toApi(): v1.StageData = {
    new v1.StageData(
      status,
      info.stageId,
      info.attemptId,

      info.numTasks,
      activeTasks,
      completedTasks,
      failedTasks,
      killedTasks,
      completedIndices.size,

      metrics.executorRunTime,
      metrics.executorCpuTime,
      info.submissionTime.map(new Date(_)),
      if (firstLaunchTime < Long.MaxValue) Some(new Date(firstLaunchTime)) else None,
      info.completionTime.map(new Date(_)),
      info.failureReason,

      metrics.inputBytesRead,
      metrics.inputRecordsRead,
      metrics.outputBytesWritten,
      metrics.outputRecordsWritten,
      metrics.shuffleLocalBytesRead + metrics.shuffleRemoteBytesRead,
      metrics.shuffleRecordsRead,
      metrics.shuffleBytesWritten,
      metrics.shuffleRecordsWritten,
      metrics.memoryBytesSpilled,
      metrics.diskBytesSpilled,

      info.name,
      description,
      info.details,
      schedulingPool,

      info.rddInfos.map(_.id),
      newAccumulatorInfos(info.accumulables.values),
      None,
      None,
      killedSummary)
  }

  override protected def doUpdate(): Any = {
    new StageDataWrapper(toApi(), jobIds, localitySummary)
  }

}

private class LiveRDDPartition(val blockName: String) {

  import LiveEntityHelpers._

  // Pointers used by RDDPartitionSeq.
  @volatile var prev: LiveRDDPartition = null
  @volatile var next: LiveRDDPartition = null

  var value: v1.RDDPartitionInfo = null

  def executors: Seq[String] = value.executors

  def memoryUsed: Long = value.memoryUsed

  def diskUsed: Long = value.diskUsed

  def update(
      executors: Seq[String],
      storageLevel: String,
      memoryUsed: Long,
      diskUsed: Long): Unit = {
    value = new v1.RDDPartitionInfo(
      blockName,
      weakIntern(storageLevel),
      memoryUsed,
      diskUsed,
      executors)
  }

}

private class LiveRDDDistribution(exec: LiveExecutor) {

  import LiveEntityHelpers._

  val executorId = exec.executorId
  var memoryUsed = 0L
  var diskUsed = 0L

  var onHeapUsed = 0L
  var offHeapUsed = 0L

  // Keep the last update handy. This avoids recomputing the API view when not needed.
  var lastUpdate: v1.RDDDataDistribution = null

  def toApi(): v1.RDDDataDistribution = {
    if (lastUpdate == null) {
      lastUpdate = new v1.RDDDataDistribution(
        weakIntern(exec.hostPort),
        memoryUsed,
        exec.maxMemory - exec.memoryUsed,
        diskUsed,
        if (exec.hasMemoryInfo) Some(onHeapUsed) else None,
        if (exec.hasMemoryInfo) Some(offHeapUsed) else None,
        if (exec.hasMemoryInfo) Some(exec.totalOnHeap - exec.usedOnHeap) else None,
        if (exec.hasMemoryInfo) Some(exec.totalOffHeap - exec.usedOffHeap) else None)
    }
    lastUpdate
  }

}

private class LiveRDD(val info: RDDInfo) extends LiveEntity {

  import LiveEntityHelpers._

  var storageLevel: String = weakIntern(info.storageLevel.description)
  var memoryUsed = 0L
  var diskUsed = 0L

  private val partitions = new HashMap[String, LiveRDDPartition]()
  private val partitionSeq = new RDDPartitionSeq()

  private val distributions = new HashMap[String, LiveRDDDistribution]()

  def setStorageLevel(level: String): Unit = {
    this.storageLevel = weakIntern(level)
  }

  def partition(blockName: String): LiveRDDPartition = {
    partitions.getOrElseUpdate(blockName, {
      val part = new LiveRDDPartition(blockName)
      part.update(Nil, storageLevel, 0L, 0L)
      partitionSeq.addPartition(part)
      part
    })
  }

  def removePartition(blockName: String): Unit = {
    partitions.remove(blockName).foreach(partitionSeq.removePartition)
  }

  def distribution(exec: LiveExecutor): LiveRDDDistribution = {
    distributions.getOrElseUpdate(exec.executorId, new LiveRDDDistribution(exec))
  }

  def removeDistribution(exec: LiveExecutor): Boolean = {
    distributions.remove(exec.executorId).isDefined
  }

  def distributionOpt(exec: LiveExecutor): Option[LiveRDDDistribution] = {
    distributions.get(exec.executorId)
  }

  override protected def doUpdate(): Any = {
    val dists = if (distributions.nonEmpty) {
      Some(distributions.values.map(_.toApi()).toSeq)
    } else {
      None
    }

    val rdd = new v1.RDDStorageInfo(
      info.id,
      info.name,
      info.numPartitions,
      partitions.size,
      storageLevel,
      memoryUsed,
      diskUsed,
      dists,
      Some(partitionSeq))

    new RDDStorageInfoWrapper(rdd)
  }

}

private class SchedulerPool(name: String) extends LiveEntity {

  var stageIds = Set[Int]()

  override protected def doUpdate(): Any = {
    new PoolData(name, stageIds)
  }

}

private object LiveEntityHelpers {

  private val stringInterner = Interners.newWeakInterner[String]()


  def newAccumulatorInfos(accums: Iterable[AccumulableInfo]): Seq[v1.AccumulableInfo] = {
    accums
      .filter { acc =>
        // We don't need to store internal or SQL accumulables as their values will be shown in
        // other places, so drop them to reduce the memory usage.
        !acc.internal && (!acc.metadata.isDefined ||
          acc.metadata.get != Some(AccumulatorContext.SQL_ACCUM_IDENTIFIER))
      }
      .map { acc =>
        new v1.AccumulableInfo(
          acc.id,
          acc.name.map(weakIntern).orNull,
          acc.update.map(_.toString()),
          acc.value.map(_.toString()).orNull)
      }
      .toSeq
  }

  /** String interning to reduce the memory usage. */
  def weakIntern(s: String): String = {
    stringInterner.intern(s)
  }

}

/**
 * A custom sequence of partitions based on a mutable linked list.
 *
 * The external interface is an immutable Seq, which is thread-safe for traversal. There are no
 * guarantees about consistency though - iteration might return elements that have been removed
 * or miss added elements.
 *
 * Internally, the sequence is mutable, and elements can modify the data they expose. Additions and
 * removals are O(1). It is not safe to do multiple writes concurrently.
 */
private class RDDPartitionSeq extends Seq[v1.RDDPartitionInfo] {

  @volatile private var _head: LiveRDDPartition = null
  @volatile private var _tail: LiveRDDPartition = null
  @volatile var count = 0

  override def apply(idx: Int): v1.RDDPartitionInfo = {
    var curr = 0
    var e = _head
    while (curr < idx && e != null) {
      curr += 1
      e = e.next
    }
    if (e != null) e.value else throw new IndexOutOfBoundsException(idx.toString)
  }

  override def iterator: Iterator[v1.RDDPartitionInfo] = {
    new Iterator[v1.RDDPartitionInfo] {
      var current = _head

      override def hasNext: Boolean = current != null

      override def next(): v1.RDDPartitionInfo = {
        if (current != null) {
          val tmp = current
          current = tmp.next
          tmp.value
        } else {
          throw new NoSuchElementException()
        }
      }
    }
  }

  override def length: Int = count

  def addPartition(part: LiveRDDPartition): Unit = {
    part.prev = _tail
    if (_tail != null) {
      _tail.next = part
    }
    if (_head == null) {
      _head = part
    }
    _tail = part
    count += 1
  }

  def removePartition(part: LiveRDDPartition): Unit = {
    count -= 1
    // Remove the partition from the list, but leave the pointers unchanged. That ensures a best
    // effort at returning existing elements when iterations still reference the removed partition.
    if (part.prev != null) {
      part.prev.next = part.next
    }
    if (part eq _head) {
      _head = part.next
    }
    if (part.next != null) {
      part.next.prev = part.prev
    }
    if (part eq _tail) {
      _tail = part.prev
    }
  }

}
