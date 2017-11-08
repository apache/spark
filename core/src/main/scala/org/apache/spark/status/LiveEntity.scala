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

import scala.collection.mutable.HashMap

import org.apache.spark.JobExecutionStatus
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.{AccumulableInfo, StageInfo, TaskInfo}
import org.apache.spark.status.api.v1
import org.apache.spark.storage.RDDInfo
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.AccumulatorContext
import org.apache.spark.util.kvstore.KVStore

/**
 * A mutable representation of a live entity in Spark (jobs, stages, tasks, et al). Every live
 * entity uses one of these instances to keep track of their evolving state, and periodically
 * flush an immutable view of the entity to the app state store.
 */
private[spark] abstract class LiveEntity {

  var lastWriteTime = 0L

  def write(store: KVStore, now: Long): Unit = {
    store.write(doUpdate())
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
      activeStages,
      completedStages.size,
      skippedStages.size,
      failedStages)
    new JobDataWrapper(info, skippedStages)
  }

}

private class LiveTask(
    info: TaskInfo,
    stageId: Int,
    stageAttemptId: Int) extends LiveEntity {

  import LiveEntityHelpers._

  private var recordedMetrics: v1.TaskMetrics = null

  var errorMessage: Option[String] = None

  /**
   * Update the metrics for the task and return the difference between the previous and new
   * values.
   */
  def updateMetrics(metrics: TaskMetrics): v1.TaskMetrics = {
    if (metrics != null) {
      val old = recordedMetrics
      recordedMetrics = new v1.TaskMetrics(
        metrics.executorDeserializeTime,
        metrics.executorDeserializeCpuTime,
        metrics.executorRunTime,
        metrics.executorCpuTime,
        metrics.resultSize,
        metrics.jvmGCTime,
        metrics.resultSerializationTime,
        metrics.memoryBytesSpilled,
        metrics.diskBytesSpilled,
        new v1.InputMetrics(
          metrics.inputMetrics.bytesRead,
          metrics.inputMetrics.recordsRead),
        new v1.OutputMetrics(
          metrics.outputMetrics.bytesWritten,
          metrics.outputMetrics.recordsWritten),
        new v1.ShuffleReadMetrics(
          metrics.shuffleReadMetrics.remoteBlocksFetched,
          metrics.shuffleReadMetrics.localBlocksFetched,
          metrics.shuffleReadMetrics.fetchWaitTime,
          metrics.shuffleReadMetrics.remoteBytesRead,
          metrics.shuffleReadMetrics.remoteBytesReadToDisk,
          metrics.shuffleReadMetrics.localBytesRead,
          metrics.shuffleReadMetrics.recordsRead),
        new v1.ShuffleWriteMetrics(
          metrics.shuffleWriteMetrics.bytesWritten,
          metrics.shuffleWriteMetrics.writeTime,
          metrics.shuffleWriteMetrics.recordsWritten))
      if (old != null) calculateMetricsDelta(recordedMetrics, old) else recordedMetrics
    } else {
      null
    }
  }

  /**
   * Return a new TaskMetrics object containing the delta of the various fields of the given
   * metrics objects. This is currently targeted at updating stage data, so it does not
   * necessarily calculate deltas for all the fields.
   */
  private def calculateMetricsDelta(
      metrics: v1.TaskMetrics,
      old: v1.TaskMetrics): v1.TaskMetrics = {
    val shuffleWriteDelta = new v1.ShuffleWriteMetrics(
      metrics.shuffleWriteMetrics.bytesWritten - old.shuffleWriteMetrics.bytesWritten,
      0L,
      metrics.shuffleWriteMetrics.recordsWritten - old.shuffleWriteMetrics.recordsWritten)

    val shuffleReadDelta = new v1.ShuffleReadMetrics(
      0L, 0L, 0L,
      metrics.shuffleReadMetrics.remoteBytesRead - old.shuffleReadMetrics.remoteBytesRead,
      metrics.shuffleReadMetrics.remoteBytesReadToDisk -
        old.shuffleReadMetrics.remoteBytesReadToDisk,
      metrics.shuffleReadMetrics.localBytesRead - old.shuffleReadMetrics.localBytesRead,
      metrics.shuffleReadMetrics.recordsRead - old.shuffleReadMetrics.recordsRead)

    val inputDelta = new v1.InputMetrics(
      metrics.inputMetrics.bytesRead - old.inputMetrics.bytesRead,
      metrics.inputMetrics.recordsRead - old.inputMetrics.recordsRead)

    val outputDelta = new v1.OutputMetrics(
      metrics.outputMetrics.bytesWritten - old.outputMetrics.bytesWritten,
      metrics.outputMetrics.recordsWritten - old.outputMetrics.recordsWritten)

    new v1.TaskMetrics(
      0L, 0L,
      metrics.executorRunTime - old.executorRunTime,
      metrics.executorCpuTime - old.executorCpuTime,
      0L, 0L, 0L,
      metrics.memoryBytesSpilled - old.memoryBytesSpilled,
      metrics.diskBytesSpilled - old.diskBytesSpilled,
      inputDelta,
      outputDelta,
      shuffleReadDelta,
      shuffleWriteDelta)
  }

  override protected def doUpdate(): Any = {
    val task = new v1.TaskData(
      info.taskId,
      info.index,
      info.attemptNumber,
      new Date(info.launchTime),
      if (info.finished) Some(info.duration) else None,
      info.executorId,
      info.host,
      info.status,
      info.taskLocality.toString(),
      info.speculative,
      newAccumulatorInfos(info.accumulables),
      errorMessage,
      Option(recordedMetrics))
    new TaskDataWrapper(task, stageId, stageAttemptId)
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

/** Metrics tracked per stage (both total and per executor). */
private class MetricsTracker {
  var executorRunTime = 0L
  var executorCpuTime = 0L
  var inputBytes = 0L
  var inputRecords = 0L
  var outputBytes = 0L
  var outputRecords = 0L
  var shuffleReadBytes = 0L
  var shuffleReadRecords = 0L
  var shuffleWriteBytes = 0L
  var shuffleWriteRecords = 0L
  var memoryBytesSpilled = 0L
  var diskBytesSpilled = 0L

  def update(delta: v1.TaskMetrics): Unit = {
    executorRunTime += delta.executorRunTime
    executorCpuTime += delta.executorCpuTime
    inputBytes += delta.inputMetrics.bytesRead
    inputRecords += delta.inputMetrics.recordsRead
    outputBytes += delta.outputMetrics.bytesWritten
    outputRecords += delta.outputMetrics.recordsWritten
    shuffleReadBytes += delta.shuffleReadMetrics.localBytesRead +
      delta.shuffleReadMetrics.remoteBytesRead
    shuffleReadRecords += delta.shuffleReadMetrics.recordsRead
    shuffleWriteBytes += delta.shuffleWriteMetrics.bytesWritten
    shuffleWriteRecords += delta.shuffleWriteMetrics.recordsWritten
    memoryBytesSpilled += delta.memoryBytesSpilled
    diskBytesSpilled += delta.diskBytesSpilled
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

  val metrics = new MetricsTracker()

  override protected def doUpdate(): Any = {
    val info = new v1.ExecutorStageSummary(
      taskTime,
      failedTasks,
      succeededTasks,
      metrics.inputBytes,
      metrics.outputBytes,
      metrics.shuffleReadBytes,
      metrics.shuffleWriteBytes,
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

  var schedulingPool: String = SparkUI.DEFAULT_POOL_NAME

  var activeTasks = 0
  var completedTasks = 0
  var failedTasks = 0

  var firstLaunchTime = Long.MaxValue

  val metrics = new MetricsTracker()

  val executorSummaries = new HashMap[String, LiveExecutorStageSummary]()

  def executorSummary(executorId: String): LiveExecutorStageSummary = {
    executorSummaries.getOrElseUpdate(executorId,
      new LiveExecutorStageSummary(info.stageId, info.attemptId, executorId))
  }

  override protected def doUpdate(): Any = {
    val update = new v1.StageData(
      status,
      info.stageId,
      info.attemptId,

      activeTasks,
      completedTasks,
      failedTasks,

      metrics.executorRunTime,
      metrics.executorCpuTime,
      info.submissionTime.map(new Date(_)),
      if (firstLaunchTime < Long.MaxValue) Some(new Date(firstLaunchTime)) else None,
      info.completionTime.map(new Date(_)),

      metrics.inputBytes,
      metrics.inputRecords,
      metrics.outputBytes,
      metrics.outputRecords,
      metrics.shuffleReadBytes,
      metrics.shuffleReadRecords,
      metrics.shuffleWriteBytes,
      metrics.shuffleWriteRecords,
      metrics.memoryBytesSpilled,
      metrics.diskBytesSpilled,

      info.name,
      info.details,
      schedulingPool,

      newAccumulatorInfos(info.accumulables.values),
      None,
      None)

    new StageDataWrapper(update, jobIds)
  }

}

private class LiveRDDPartition(val blockName: String) {

  var executors = Set[String]()
  var storageLevel: String = null
  var memoryUsed = 0L
  var diskUsed = 0L

  def toApi(): v1.RDDPartitionInfo = {
    new v1.RDDPartitionInfo(
      blockName,
      storageLevel,
      memoryUsed,
      diskUsed,
      executors.toSeq.sorted)
  }

}

private class LiveRDDDistribution(val exec: LiveExecutor) {

  var memoryRemaining = exec.maxMemory
  var memoryUsed = 0L
  var diskUsed = 0L

  var onHeapUsed = 0L
  var offHeapUsed = 0L
  var onHeapRemaining = 0L
  var offHeapRemaining = 0L

  def toApi(): v1.RDDDataDistribution = {
    new v1.RDDDataDistribution(
      exec.hostPort,
      memoryUsed,
      memoryRemaining,
      diskUsed,
      if (exec.hasMemoryInfo) Some(onHeapUsed) else None,
      if (exec.hasMemoryInfo) Some(offHeapUsed) else None,
      if (exec.hasMemoryInfo) Some(onHeapRemaining) else None,
      if (exec.hasMemoryInfo) Some(offHeapRemaining) else None)
  }

}

private class LiveRDD(info: RDDInfo) extends LiveEntity {

  var storageLevel: String = info.storageLevel.description
  var memoryUsed = 0L
  var diskUsed = 0L

  private val partitions = new HashMap[String, LiveRDDPartition]()
  private val distributions = new HashMap[String, LiveRDDDistribution]()

  def partition(blockName: String): LiveRDDPartition = {
    partitions.getOrElseUpdate(blockName, new LiveRDDPartition(blockName))
  }

  def removePartition(blockName: String): Unit = partitions.remove(blockName)

  def distribution(exec: LiveExecutor): LiveRDDDistribution = {
    distributions.getOrElseUpdate(exec.hostPort, new LiveRDDDistribution(exec))
  }

  def removeDistribution(exec: LiveExecutor): Unit = {
    distributions.remove(exec.hostPort)
  }

  override protected def doUpdate(): Any = {
    val parts = if (partitions.nonEmpty) {
      Some(partitions.values.toList.sortBy(_.blockName).map(_.toApi()))
    } else {
      None
    }

    val dists = if (distributions.nonEmpty) {
      Some(distributions.values.toList.sortBy(_.exec.executorId).map(_.toApi()))
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
      parts)

    new RDDStorageInfoWrapper(rdd)
  }

}

private object LiveEntityHelpers {

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
          acc.name.map(_.intern()).orNull,
          acc.update.map(_.toString()),
          acc.value.map(_.toString()).orNull)
      }
      .toSeq
  }

}
