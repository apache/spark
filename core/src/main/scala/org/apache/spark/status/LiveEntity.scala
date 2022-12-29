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

import scala.collection.JavaConverters._
import scala.collection.immutable.{HashSet, TreeSet}
import scala.collection.mutable.HashMap

import org.apache.spark.JobExecutionStatus
import org.apache.spark.executor.{ExecutorMetrics, TaskMetrics}
import org.apache.spark.resource.{ExecutorResourceRequest, ResourceInformation, ResourceProfile, TaskResourceRequest}
import org.apache.spark.scheduler.{AccumulableInfo, StageInfo, TaskInfo}
import org.apache.spark.status.api.v1
import org.apache.spark.storage.{RDDInfo, StorageLevel}
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.{AccumulatorContext, Utils}
import org.apache.spark.util.Utils.weakIntern
import org.apache.spark.util.collection.OpenHashSet

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
    description: Option[String],
    val submissionTime: Option[Date],
    val stageIds: Seq[Int],
    jobGroup: Option[String],
    numTasks: Int,
    sqlExecutionId: Option[Long]) extends LiveEntity {

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
      description,
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
    new JobDataWrapper(info, skippedStages, sqlExecutionId)
  }

}

private class LiveTask(
    var info: TaskInfo,
    stageId: Int,
    stageAttemptId: Int,
    lastUpdateTime: Option[Long]) extends LiveEntity {

  import LiveEntityHelpers._

  // The task metrics use a special value when no metrics have been reported. The special value is
  // checked when calculating indexed values when writing to the store (see [[TaskDataWrapper]]).
  private var metrics: v1.TaskMetrics = createMetrics(default = -1L)

  var errorMessage: Option[String] = None

  /**
   * Update the metrics for the task and return the difference between the previous and new
   * values.
   */
  def updateMetrics(metrics: TaskMetrics): v1.TaskMetrics = {
    if (metrics != null) {
      val old = this.metrics
      val newMetrics = createMetrics(
        metrics.executorDeserializeTime,
        metrics.executorDeserializeCpuTime,
        metrics.executorRunTime,
        metrics.executorCpuTime,
        metrics.resultSize,
        metrics.jvmGCTime,
        metrics.resultSerializationTime,
        metrics.memoryBytesSpilled,
        metrics.diskBytesSpilled,
        metrics.peakExecutionMemory,
        metrics.inputMetrics.bytesRead,
        metrics.inputMetrics.recordsRead,
        metrics.outputMetrics.bytesWritten,
        metrics.outputMetrics.recordsWritten,
        metrics.shuffleReadMetrics.remoteBlocksFetched,
        metrics.shuffleReadMetrics.localBlocksFetched,
        metrics.shuffleReadMetrics.fetchWaitTime,
        metrics.shuffleReadMetrics.remoteBytesRead,
        metrics.shuffleReadMetrics.remoteBytesReadToDisk,
        metrics.shuffleReadMetrics.localBytesRead,
        metrics.shuffleReadMetrics.recordsRead,
        metrics.shuffleWriteMetrics.bytesWritten,
        metrics.shuffleWriteMetrics.writeTime,
        metrics.shuffleWriteMetrics.recordsWritten)

      this.metrics = newMetrics

      // Only calculate the delta if the old metrics contain valid information, otherwise
      // the new metrics are the delta.
      if (old.executorDeserializeTime >= 0L) {
        subtractMetrics(newMetrics, old)
      } else {
        newMetrics
      }
    } else {
      null
    }
  }

  override protected def doUpdate(): Any = {
    val duration = if (info.finished) {
      info.duration
    } else {
      info.timeRunning(lastUpdateTime.getOrElse(System.currentTimeMillis()))
    }

    val hasMetrics = metrics.executorDeserializeTime >= 0

    /**
     * SPARK-26260: For non successful tasks, store the metrics as negative to avoid
     * the calculation in the task summary. `toApi` method in the `TaskDataWrapper` will make
     * it actual value.
     */
    val taskMetrics: v1.TaskMetrics = if (hasMetrics && !info.successful) {
      makeNegative(metrics)
    } else {
      metrics
    }

    new TaskDataWrapper(
      info.taskId,
      info.index,
      info.attemptNumber,
      info.partitionId,
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

      hasMetrics,
      taskMetrics.executorDeserializeTime,
      taskMetrics.executorDeserializeCpuTime,
      taskMetrics.executorRunTime,
      taskMetrics.executorCpuTime,
      taskMetrics.resultSize,
      taskMetrics.jvmGcTime,
      taskMetrics.resultSerializationTime,
      taskMetrics.memoryBytesSpilled,
      taskMetrics.diskBytesSpilled,
      taskMetrics.peakExecutionMemory,
      taskMetrics.inputMetrics.bytesRead,
      taskMetrics.inputMetrics.recordsRead,
      taskMetrics.outputMetrics.bytesWritten,
      taskMetrics.outputMetrics.recordsWritten,
      taskMetrics.shuffleReadMetrics.remoteBlocksFetched,
      taskMetrics.shuffleReadMetrics.localBlocksFetched,
      taskMetrics.shuffleReadMetrics.fetchWaitTime,
      taskMetrics.shuffleReadMetrics.remoteBytesRead,
      taskMetrics.shuffleReadMetrics.remoteBytesReadToDisk,
      taskMetrics.shuffleReadMetrics.localBytesRead,
      taskMetrics.shuffleReadMetrics.recordsRead,
      taskMetrics.shuffleWriteMetrics.bytesWritten,
      taskMetrics.shuffleWriteMetrics.writeTime,
      taskMetrics.shuffleWriteMetrics.recordsWritten,

      stageId,
      stageAttemptId)
  }

}

private class LiveResourceProfile(
    val resourceProfileId: Int,
    val executorResources: Map[String, ExecutorResourceRequest],
    val taskResources: Map[String, TaskResourceRequest],
    val maxTasksPerExecutor: Option[Int]) extends LiveEntity {

  def toApi(): v1.ResourceProfileInfo = {
    new v1.ResourceProfileInfo(resourceProfileId, executorResources, taskResources)
  }

  override protected def doUpdate(): Any = {
    new ResourceProfileWrapper(toApi())
  }
}

private[spark] class LiveExecutor(val executorId: String, _addTime: Long) extends LiveEntity {

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
  var isExcluded = false
  var excludedInStages: Set[Int] = TreeSet()

  var executorLogs = Map[String, String]()
  var attributes = Map[String, String]()
  var resources = Map[String, ResourceInformation]()

  // Memory metrics. They may not be recorded (e.g. old event logs) so if totalOnHeap is not
  // initialized, the store will not contain this information.
  var totalOnHeap = -1L
  var totalOffHeap = 0L
  var usedOnHeap = 0L
  var usedOffHeap = 0L

  var resourceProfileId = ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID

  def hasMemoryInfo: Boolean = totalOnHeap >= 0L

  // peak values for executor level metrics
  val peakExecutorMetrics = new ExecutorMetrics()

  def hostname: String = if (host != null) host else Utils.parseHostPort(hostPort)._1

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
      isExcluded,
      maxMemory,
      addTime,
      Option(removeTime),
      Option(removeReason),
      executorLogs,
      memoryMetrics,
      excludedInStages,
      Some(peakExecutorMetrics).filter(_.isSet),
      attributes,
      resources,
      resourceProfileId,
      isExcluded,
      excludedInStages)
    new ExecutorSummaryWrapper(info)
  }
}

private class LiveExecutorStageSummary(
    stageId: Int,
    attemptId: Int,
    executorId: String) extends LiveEntity {

  import LiveEntityHelpers._

  var taskTime = 0L
  var succeededTasks = 0
  var failedTasks = 0
  var killedTasks = 0
  var isExcluded = false

  var metrics = createMetrics(default = 0L)

  val peakExecutorMetrics = new ExecutorMetrics()

  override protected def doUpdate(): Any = {
    val info = new v1.ExecutorStageSummary(
      taskTime,
      failedTasks,
      succeededTasks,
      killedTasks,
      metrics.inputMetrics.bytesRead,
      metrics.inputMetrics.recordsRead,
      metrics.outputMetrics.bytesWritten,
      metrics.outputMetrics.recordsWritten,
      metrics.shuffleReadMetrics.remoteBytesRead + metrics.shuffleReadMetrics.localBytesRead,
      metrics.shuffleReadMetrics.recordsRead,
      metrics.shuffleWriteMetrics.bytesWritten,
      metrics.shuffleWriteMetrics.recordsWritten,
      metrics.memoryBytesSpilled,
      metrics.diskBytesSpilled,
      isExcluded,
      Some(peakExecutorMetrics).filter(_.isSet),
      isExcluded)
    new ExecutorStageSummaryWrapper(stageId, attemptId, executorId, info)
  }

}

private class LiveSpeculationStageSummary(
    stageId: Int,
    attemptId: Int) extends LiveEntity {

  var numTasks = 0
  var numActiveTasks = 0
  var numCompletedTasks = 0
  var numFailedTasks = 0
  var numKilledTasks = 0

  override protected def doUpdate(): Any = {
    val info = new v1.SpeculationStageSummary(
      numTasks,
      numActiveTasks,
      numCompletedTasks,
      numFailedTasks,
      numKilledTasks
    )
    new SpeculationStageSummaryWrapper(stageId, attemptId, info)
  }
}

private class LiveStage(var info: StageInfo) extends LiveEntity {

  import LiveEntityHelpers._

  var jobs = Seq[LiveJob]()
  var jobIds = Set[Int]()

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

  var metrics = createMetrics(default = 0L)

  val executorSummaries = new HashMap[String, LiveExecutorStageSummary]()

  val activeTasksPerExecutor = new HashMap[String, Int]().withDefaultValue(0)

  var excludedExecutors = new HashSet[String]()

  val peakExecutorMetrics = new ExecutorMetrics()

  lazy val speculationStageSummary: LiveSpeculationStageSummary =
    new LiveSpeculationStageSummary(info.stageId, info.attemptNumber)

  // Used for cleanup of tasks after they reach the configured limit. Not written to the store.
  @volatile var cleaning = false
  val savedTasks = new AtomicInteger(0)

  def executorSummary(executorId: String): LiveExecutorStageSummary = {
    executorSummaries.getOrElseUpdate(executorId,
      new LiveExecutorStageSummary(info.stageId, info.attemptNumber, executorId))
  }

  def toApi(): v1.StageData = {
    new v1.StageData(
      status = status,
      stageId = info.stageId,
      attemptId = info.attemptNumber,
      numTasks = info.numTasks,
      numActiveTasks = activeTasks,
      numCompleteTasks = completedTasks,
      numFailedTasks = failedTasks,
      numKilledTasks = killedTasks,
      numCompletedIndices = completedIndices.size,

      submissionTime = info.submissionTime.map(new Date(_)),
      firstTaskLaunchedTime =
        if (firstLaunchTime < Long.MaxValue) Some(new Date(firstLaunchTime)) else None,
      completionTime = info.completionTime.map(new Date(_)),
      failureReason = info.failureReason,

      executorDeserializeTime = metrics.executorDeserializeTime,
      executorDeserializeCpuTime = metrics.executorDeserializeCpuTime,
      executorRunTime = metrics.executorRunTime,
      executorCpuTime = metrics.executorCpuTime,
      resultSize = metrics.resultSize,
      jvmGcTime = metrics.jvmGcTime,
      resultSerializationTime = metrics.resultSerializationTime,
      memoryBytesSpilled = metrics.memoryBytesSpilled,
      diskBytesSpilled = metrics.diskBytesSpilled,
      peakExecutionMemory = metrics.peakExecutionMemory,
      inputBytes = metrics.inputMetrics.bytesRead,
      inputRecords = metrics.inputMetrics.recordsRead,
      outputBytes = metrics.outputMetrics.bytesWritten,
      outputRecords = metrics.outputMetrics.recordsWritten,
      shuffleRemoteBlocksFetched = metrics.shuffleReadMetrics.remoteBlocksFetched,
      shuffleLocalBlocksFetched = metrics.shuffleReadMetrics.localBlocksFetched,
      shuffleFetchWaitTime = metrics.shuffleReadMetrics.fetchWaitTime,
      shuffleRemoteBytesRead = metrics.shuffleReadMetrics.remoteBytesRead,
      shuffleRemoteBytesReadToDisk = metrics.shuffleReadMetrics.remoteBytesReadToDisk,
      shuffleLocalBytesRead = metrics.shuffleReadMetrics.localBytesRead,
      shuffleReadBytes =
        metrics.shuffleReadMetrics.localBytesRead + metrics.shuffleReadMetrics.remoteBytesRead,
      shuffleReadRecords = metrics.shuffleReadMetrics.recordsRead,
      shuffleWriteBytes = metrics.shuffleWriteMetrics.bytesWritten,
      shuffleWriteTime = metrics.shuffleWriteMetrics.writeTime,
      shuffleWriteRecords = metrics.shuffleWriteMetrics.recordsWritten,

      name = info.name,
      description = description,
      details = info.details,
      schedulingPool = schedulingPool,

      rddIds = info.rddInfos.map(_.id),
      accumulatorUpdates = newAccumulatorInfos(info.accumulables.values),
      tasks = None,
      executorSummary = None,
      speculationSummary = None,
      killedTasksSummary = killedSummary,
      resourceProfileId = info.resourceProfileId,
      peakExecutorMetrics = Some(peakExecutorMetrics).filter(_.isSet),
      taskMetricsDistributions = None,
      executorMetricsDistributions = None)
  }

  override protected def doUpdate(): Any = {
    new StageDataWrapper(toApi(), jobIds, localitySummary)
  }

}

/**
 * Data about a single partition of a cached RDD. The RDD storage level is used to compute the
 * effective storage level of the partition, which takes into account the storage actually being
 * used by the partition in the executors, and thus may differ from the storage level requested
 * by the application.
 */
private class LiveRDDPartition(val blockName: String, rddLevel: StorageLevel) {

  // Pointers used by RDDPartitionSeq.
  @volatile var prev: LiveRDDPartition = null
  @volatile var next: LiveRDDPartition = null

  var value: v1.RDDPartitionInfo = null

  def executors: collection.Seq[String] = value.executors

  def memoryUsed: Long = value.memoryUsed

  def diskUsed: Long = value.diskUsed

  def update(
      executors: collection.Seq[String],
      memoryUsed: Long,
      diskUsed: Long): Unit = {
    val level = StorageLevel(diskUsed > 0, memoryUsed > 0, rddLevel.useOffHeap,
      if (memoryUsed > 0) rddLevel.deserialized else false, executors.size)
    value = new v1.RDDPartitionInfo(
      blockName,
      weakIntern(level.description),
      memoryUsed,
      diskUsed,
      executors)
  }

}

private class LiveRDDDistribution(exec: LiveExecutor) {

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
        weakIntern(if (exec.hostPort != null) exec.hostPort else exec.host),
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

/**
 * Tracker for data related to a persisted RDD.
 *
 * The RDD storage level is immutable, following the current behavior of `RDD.persist()`, even
 * though it is mutable in the `RDDInfo` structure. Since the listener does not track unpersisted
 * RDDs, this covers the case where an early stage is run on the unpersisted RDD, and a later stage
 * it started after the RDD is marked for caching.
 */
private class LiveRDD(val info: RDDInfo, storageLevel: StorageLevel) extends LiveEntity {

  var memoryUsed = 0L
  var diskUsed = 0L

  private val levelDescription = weakIntern(storageLevel.description)
  private val partitions = new HashMap[String, LiveRDDPartition]()
  private val partitionSeq = new RDDPartitionSeq()

  private val distributions = new HashMap[String, LiveRDDDistribution]()

  def partition(blockName: String): LiveRDDPartition = {
    partitions.getOrElseUpdate(blockName, {
      val part = new LiveRDDPartition(blockName, storageLevel)
      part.update(Nil, 0L, 0L)
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

  def getPartitions(): scala.collection.Map[String, LiveRDDPartition] = partitions

  def getDistributions(): scala.collection.Map[String, LiveRDDDistribution] = distributions

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
      levelDescription,
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

private[spark] object LiveEntityHelpers {

  private def accuValuetoString(value: Any): String = value match {
    case list: java.util.List[_] =>
      // SPARK-30379: For collection accumulator, string representation might
      // takes much more memory (e.g. long => string of it) and cause OOM.
      // So we only show first few elements.
      if (list.size() > 5) {
        list.asScala.take(5).mkString("[", ",", "," + "... " + (list.size() - 5) + " more items]")
      } else {
        list.toString
      }
    case _ => value.toString
  }

  def newAccumulatorInfos(accums: Iterable[AccumulableInfo]): Seq[v1.AccumulableInfo] = {
    accums
      .filter { acc =>
        // We don't need to store internal or SQL accumulables as their values will be shown in
        // other places, so drop them to reduce the memory usage.
        !acc.internal && acc.metadata != Some(AccumulatorContext.SQL_ACCUM_IDENTIFIER)
      }
      .map { acc =>
        new v1.AccumulableInfo(
          acc.id,
          acc.name.map(weakIntern).orNull,
          acc.update.map(accuValuetoString),
          acc.value.map(accuValuetoString).orNull)
      }
      .toSeq
  }

  // scalastyle:off argcount
  def createMetrics(
      executorDeserializeTime: Long,
      executorDeserializeCpuTime: Long,
      executorRunTime: Long,
      executorCpuTime: Long,
      resultSize: Long,
      jvmGcTime: Long,
      resultSerializationTime: Long,
      memoryBytesSpilled: Long,
      diskBytesSpilled: Long,
      peakExecutionMemory: Long,
      inputBytesRead: Long,
      inputRecordsRead: Long,
      outputBytesWritten: Long,
      outputRecordsWritten: Long,
      shuffleRemoteBlocksFetched: Long,
      shuffleLocalBlocksFetched: Long,
      shuffleFetchWaitTime: Long,
      shuffleRemoteBytesRead: Long,
      shuffleRemoteBytesReadToDisk: Long,
      shuffleLocalBytesRead: Long,
      shuffleRecordsRead: Long,
      shuffleBytesWritten: Long,
      shuffleWriteTime: Long,
      shuffleRecordsWritten: Long): v1.TaskMetrics = {
    new v1.TaskMetrics(
      executorDeserializeTime,
      executorDeserializeCpuTime,
      executorRunTime,
      executorCpuTime,
      resultSize,
      jvmGcTime,
      resultSerializationTime,
      memoryBytesSpilled,
      diskBytesSpilled,
      peakExecutionMemory,
      new v1.InputMetrics(
        inputBytesRead,
        inputRecordsRead),
      new v1.OutputMetrics(
        outputBytesWritten,
        outputRecordsWritten),
      new v1.ShuffleReadMetrics(
        shuffleRemoteBlocksFetched,
        shuffleLocalBlocksFetched,
        shuffleFetchWaitTime,
        shuffleRemoteBytesRead,
        shuffleRemoteBytesReadToDisk,
        shuffleLocalBytesRead,
        shuffleRecordsRead),
      new v1.ShuffleWriteMetrics(
        shuffleBytesWritten,
        shuffleWriteTime,
        shuffleRecordsWritten))
  }
  // scalastyle:on argcount

  def createMetrics(default: Long): v1.TaskMetrics = {
    createMetrics(default, default, default, default, default, default, default, default,
      default, default, default, default, default, default, default, default,
      default, default, default, default, default, default, default, default)
  }

  /** Add m2 values to m1. */
  def addMetrics(m1: v1.TaskMetrics, m2: v1.TaskMetrics): v1.TaskMetrics = addMetrics(m1, m2, 1)

  /** Subtract m2 values from m1. */
  def subtractMetrics(m1: v1.TaskMetrics, m2: v1.TaskMetrics): v1.TaskMetrics = {
    addMetrics(m1, m2, -1)
  }

  /**
   * Convert all the metric values to negative as well as handle zero values.
   * This method assumes that all the metric values are greater than or equal to zero
   */
  def makeNegative(m: v1.TaskMetrics): v1.TaskMetrics = {
    // To handle 0 metric value, add  1 and make the metric negative.
    // To recover actual value do `math.abs(metric + 1)`
    // Eg: if the metric values are (5, 3, 0, 1) => Updated metric values will be (-6, -4, -1, -2)
    // To get actual metric value, do math.abs(metric + 1) => (5, 3, 0, 1)
    def updateMetricValue(metric: Long): Long = {
      metric * -1L - 1L
    }

    createMetrics(
      updateMetricValue(m.executorDeserializeTime),
      updateMetricValue(m.executorDeserializeCpuTime),
      updateMetricValue(m.executorRunTime),
      updateMetricValue(m.executorCpuTime),
      updateMetricValue(m.resultSize),
      updateMetricValue(m.jvmGcTime),
      updateMetricValue(m.resultSerializationTime),
      updateMetricValue(m.memoryBytesSpilled),
      updateMetricValue(m.diskBytesSpilled),
      updateMetricValue(m.peakExecutionMemory),
      updateMetricValue(m.inputMetrics.bytesRead),
      updateMetricValue(m.inputMetrics.recordsRead),
      updateMetricValue(m.outputMetrics.bytesWritten),
      updateMetricValue(m.outputMetrics.recordsWritten),
      updateMetricValue(m.shuffleReadMetrics.remoteBlocksFetched),
      updateMetricValue(m.shuffleReadMetrics.localBlocksFetched),
      updateMetricValue(m.shuffleReadMetrics.fetchWaitTime),
      updateMetricValue(m.shuffleReadMetrics.remoteBytesRead),
      updateMetricValue(m.shuffleReadMetrics.remoteBytesReadToDisk),
      updateMetricValue(m.shuffleReadMetrics.localBytesRead),
      updateMetricValue(m.shuffleReadMetrics.recordsRead),
      updateMetricValue(m.shuffleWriteMetrics.bytesWritten),
      updateMetricValue(m.shuffleWriteMetrics.writeTime),
      updateMetricValue(m.shuffleWriteMetrics.recordsWritten))
  }

  private def addMetrics(m1: v1.TaskMetrics, m2: v1.TaskMetrics, mult: Int): v1.TaskMetrics = {
    createMetrics(
      m1.executorDeserializeTime + m2.executorDeserializeTime * mult,
      m1.executorDeserializeCpuTime + m2.executorDeserializeCpuTime * mult,
      m1.executorRunTime + m2.executorRunTime * mult,
      m1.executorCpuTime + m2.executorCpuTime * mult,
      m1.resultSize + m2.resultSize * mult,
      m1.jvmGcTime + m2.jvmGcTime * mult,
      m1.resultSerializationTime + m2.resultSerializationTime * mult,
      m1.memoryBytesSpilled + m2.memoryBytesSpilled * mult,
      m1.diskBytesSpilled + m2.diskBytesSpilled * mult,
      m1.peakExecutionMemory + m2.peakExecutionMemory * mult,
      m1.inputMetrics.bytesRead + m2.inputMetrics.bytesRead * mult,
      m1.inputMetrics.recordsRead + m2.inputMetrics.recordsRead * mult,
      m1.outputMetrics.bytesWritten + m2.outputMetrics.bytesWritten * mult,
      m1.outputMetrics.recordsWritten + m2.outputMetrics.recordsWritten * mult,
      m1.shuffleReadMetrics.remoteBlocksFetched + m2.shuffleReadMetrics.remoteBlocksFetched * mult,
      m1.shuffleReadMetrics.localBlocksFetched + m2.shuffleReadMetrics.localBlocksFetched * mult,
      m1.shuffleReadMetrics.fetchWaitTime + m2.shuffleReadMetrics.fetchWaitTime * mult,
      m1.shuffleReadMetrics.remoteBytesRead + m2.shuffleReadMetrics.remoteBytesRead * mult,
      m1.shuffleReadMetrics.remoteBytesReadToDisk +
        m2.shuffleReadMetrics.remoteBytesReadToDisk * mult,
      m1.shuffleReadMetrics.localBytesRead + m2.shuffleReadMetrics.localBytesRead * mult,
      m1.shuffleReadMetrics.recordsRead + m2.shuffleReadMetrics.recordsRead * mult,
      m1.shuffleWriteMetrics.bytesWritten + m2.shuffleWriteMetrics.bytesWritten * mult,
      m1.shuffleWriteMetrics.writeTime + m2.shuffleWriteMetrics.writeTime * mult,
      m1.shuffleWriteMetrics.recordsWritten + m2.shuffleWriteMetrics.recordsWritten * mult)
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

private[spark] class LiveMiscellaneousProcess(val processId: String,
    creationTime: Long) extends LiveEntity {

  var hostPort: String = null
  var isActive = true
  var totalCores = 0
  val addTime = new Date(creationTime)
  var processLogs = Map[String, String]()

  override protected def doUpdate(): Any = {

    val info = new v1.ProcessSummary(
      processId,
      hostPort,
      isActive,
      totalCores,
      addTime,
      None,
      processLogs)
    new ProcessSummaryWrapper(info)
  }
}
