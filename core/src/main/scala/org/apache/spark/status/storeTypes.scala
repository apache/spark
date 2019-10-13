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

import java.lang.{Long => JLong}
import java.util.Date

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.databind.annotation.JsonDeserialize

import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.scheduler.{StageInfo, TaskInfo, TaskLocality}
import org.apache.spark.status.KVUtils._
import org.apache.spark.status.api.v1._
import org.apache.spark.storage.{RDDInfo, StorageLevel}
import org.apache.spark.ui.scope._
import org.apache.spark.util.kvstore.KVIndex

private[spark] case class AppStatusStoreMetadata(version: Long)

private[spark] class ApplicationInfoWrapper(val info: ApplicationInfo) {

  @JsonIgnore @KVIndex
  def id: String = info.id

}

private[spark] class ApplicationEnvironmentInfoWrapper(val info: ApplicationEnvironmentInfo) {

  /**
   * There's always a single ApplicationEnvironmentInfo object per application, so this
   * ID doesn't need to be dynamic. But the KVStore API requires an ID.
   */
  @JsonIgnore @KVIndex
  def id: String = classOf[ApplicationEnvironmentInfoWrapper].getName()

}

private[spark] class ExecutorSummaryWrapper(val info: ExecutorSummary) {

  @JsonIgnore @KVIndex
  private def id: String = info.id

  @JsonIgnore @KVIndex("active")
  private def active: Boolean = info.isActive

  @JsonIgnore @KVIndex("host")
  val host: String = info.hostPort.split(":")(0)

  def toLiveExecutor: LiveExecutor = {
    val liveExecutor = new LiveExecutor(info.id, info.addTime.getTime)
    liveExecutor.hostPort = info.hostPort
    liveExecutor.host = info.hostPort.split(":")(0)
    liveExecutor.totalCores = info.totalCores
    liveExecutor.rddBlocks = info.rddBlocks
    liveExecutor.memoryUsed = info.memoryUsed
    liveExecutor.diskUsed = info.diskUsed
    liveExecutor.maxTasks = info.maxTasks
    liveExecutor.maxMemory = info.maxMemory
    liveExecutor.totalTasks = info.totalTasks
    liveExecutor.activeTasks = info.activeTasks
    liveExecutor.completedTasks = info.completedTasks
    liveExecutor.failedTasks = info.failedTasks
    liveExecutor.totalDuration = info.totalDuration
    liveExecutor.totalGcTime = info.totalGCTime
    liveExecutor.totalInputBytes = info.totalInputBytes
    liveExecutor.totalShuffleRead = info.totalShuffleRead
    liveExecutor.totalShuffleWrite = info.totalShuffleWrite
    liveExecutor.isBlacklisted = info.isBlacklisted
    liveExecutor.blacklistedInStages = info.blacklistedInStages
    liveExecutor.executorLogs = info.executorLogs
    liveExecutor.attributes = info.attributes
    liveExecutor.totalOnHeap = info.memoryMetrics.map(_.totalOnHeapStorageMemory).getOrElse(-1)
    liveExecutor.totalOffHeap = info.memoryMetrics.map(_.totalOffHeapStorageMemory).getOrElse(0)
    liveExecutor.usedOnHeap = info.memoryMetrics.map(_.usedOnHeapStorageMemory).getOrElse(0)
    liveExecutor.usedOffHeap = info.memoryMetrics.map(_.usedOffHeapStorageMemory).getOrElse(0)
    liveExecutor.peakExecutorMetrics = info.peakMemoryMetrics.getOrElse(new ExecutorMetrics())
    liveExecutor
  }
}

/**
 * Keep track of the existing stages when the job was submitted, and those that were
 * completed during the job's execution. This allows a more accurate accounting of how
 * many tasks were skipped for the job.
 */
private[spark] class JobDataWrapper(
    val info: JobData,
    val skippedStages: Set[Int],
    val sqlExecutionId: Option[Long]) {

  @JsonIgnore @KVIndex
  private def id: Int = info.jobId

  @JsonIgnore @KVIndex("completionTime")
  private def completionTime: Long = info.completionTime.map(_.getTime).getOrElse(-1L)

  def toLiveJob: LiveJob = {
    val liveJob = new LiveJob(
      info.jobId,
      info.name,
      info.submissionTime,
      info.stageIds,
      info.jobGroup,
      info.numTasks,
      sqlExecutionId)
    liveJob.activeTasks = info.numActiveTasks
    liveJob.completedTasks = info.numCompletedTasks
    liveJob.failedTasks = info.numFailedTasks
    liveJob.numCompletedIndices = info.numCompletedIndices
    liveJob.killedTasks = info.numKilledTasks
    liveJob.killedSummary = info.killedTasksSummary
    liveJob.skippedTasks = info.numSkippedTasks
    liveJob.skippedStages = skippedStages
    liveJob.numCompletedStages = info.numCompletedStages
    liveJob.activeStages = info.numActiveStages
    liveJob.failedStages = info.numFailedStages
    liveJob
  }
}

private[spark] class StageDataWrapper(
    val info: StageData,
    val jobIds: Set[Int],
    @JsonDeserialize(contentAs = classOf[JLong])
    val locality: Map[String, Long]) {

  @JsonIgnore @KVIndex
  private[this] val id: Array[Int] = Array(info.stageId, info.attemptId)

  @JsonIgnore @KVIndex("stageId")
  private def stageId: Int = info.stageId

  @JsonIgnore @KVIndex("active")
  private def active: Boolean = info.status == StageStatus.ACTIVE

  @JsonIgnore @KVIndex("completionTime")
  private def completionTime: Long = info.completionTime.map(_.getTime).getOrElse(-1L)

  private def idAwareRDDInfos(rddIds: Seq[Int]): Seq[RDDInfo] = {
    // It's safe to give arbitrary values except id while recovering RDDInfo,
    // since a running LiveStage only concerns about rddInfo's id.
    rddIds.map { id =>
      new RDDInfo(id, id.toString, 0, null, false, Nil)
    }
  }

  def toLiveStage(jobs: Seq[LiveJob]): LiveStage = {
    val liveStage = new LiveStage
    val firstLaunchTime = if (info.firstTaskLaunchedTime.isEmpty) {
      Long.MaxValue
    } else {
      info.firstTaskLaunchedTime.get.getTime
    }
    val metrics = LiveEntityHelpers.createMetrics(
      info.executorDeserializeTime,
      info.executorDeserializeCpuTime,
      info.executorRunTime,
      info.executorCpuTime,
      info.resultSize,
      info.jvmGcTime,
      info.resultSerializationTime,
      info.memoryBytesSpilled,
      info.diskBytesSpilled,
      info.peakExecutionMemory,
      info.inputBytes,
      info.inputRecords,
      info.outputBytes,
      info.outputRecords,
      info.shuffleRemoteBlocksFetched,
      info.shuffleLocalBlocksFetched,
      info.shuffleFetchWaitTime,
      info.shuffleRemoteBytesRead,
      info.shuffleRemoteBytesReadToDisk,
      info.shuffleLocalBytesRead,
      info.shuffleReadRecords,
      info.shuffleWriteBytes,
      info.shuffleWriteTime,
      info.shuffleWriteRecords
    )

    // parentIds, taskMetrics, taskLocalityPreferences and shuffleDepId aren't assigned here
    // but it's also OK since a running LiveStage don't visit these attributes. And we'll
    // get a complete StageInfo again when we receive SparkListenerStageCompleted event.
    val stageInfo = new StageInfo(
      info.stageId,
      info.attemptId,
      info.name,
      info.numTasks,
      idAwareRDDInfos(info.rddIds),
      Nil, // parentIds
      info.details)

    // Note that attributes for `executorSummaries`, `activeTasksPerExecutor`,
    // `blackListedExecutors`, `savedTasks` are computed later in
    // AppStatusListener.recoverLiveEntities().
    liveStage.jobs = jobs
    liveStage.jobIds = jobs.map(_.jobId).toSet
    liveStage.info = stageInfo
    liveStage.status = info.status
    liveStage.description = info.description
    liveStage.schedulingPool = info.schedulingPool
    liveStage.activeTasks = info.numActiveTasks
    liveStage.completedTasks = info.numCompleteTasks
    liveStage.failedTasks = info.numFailedTasks
    liveStage.numCompletedIndices = info.numCompletedIndices
    liveStage.killedTasks = info.numKilledTasks
    liveStage.killedSummary = info.killedTasksSummary
    liveStage.firstLaunchTime = firstLaunchTime
    liveStage.localitySummary = locality
    liveStage.metrics = metrics
    liveStage
  }
}

/**
 * Tasks have a lot of indices that are used in a few different places. This object keeps logical
 * names for these indices, mapped to short strings to save space when using a disk store.
 */
private[spark] object TaskIndexNames {
  final val ACCUMULATORS = "acc"
  final val ATTEMPT = "att"
  final val DESER_CPU_TIME = "dct"
  final val DESER_TIME = "des"
  final val DISK_SPILL = "dbs"
  final val DURATION = "dur"
  final val ERROR = "err"
  final val EXECUTOR = "exe"
  final val HOST = "hst"
  final val EXEC_CPU_TIME = "ect"
  final val EXEC_RUN_TIME = "ert"
  final val GC_TIME = "gc"
  final val GETTING_RESULT_TIME = "grt"
  final val INPUT_RECORDS = "ir"
  final val INPUT_SIZE = "is"
  final val LAUNCH_TIME = "lt"
  final val LOCALITY = "loc"
  final val MEM_SPILL = "mbs"
  final val OUTPUT_RECORDS = "or"
  final val OUTPUT_SIZE = "os"
  final val PEAK_MEM = "pem"
  final val RESULT_SIZE = "rs"
  final val SCHEDULER_DELAY = "dly"
  final val SER_TIME = "rst"
  final val SHUFFLE_LOCAL_BLOCKS = "slbl"
  final val SHUFFLE_READ_RECORDS = "srr"
  final val SHUFFLE_READ_TIME = "srt"
  final val SHUFFLE_REMOTE_BLOCKS = "srbl"
  final val SHUFFLE_REMOTE_READS = "srby"
  final val SHUFFLE_REMOTE_READS_TO_DISK = "srbd"
  final val SHUFFLE_TOTAL_READS = "stby"
  final val SHUFFLE_TOTAL_BLOCKS = "stbl"
  final val SHUFFLE_WRITE_RECORDS = "swr"
  final val SHUFFLE_WRITE_SIZE = "sws"
  final val SHUFFLE_WRITE_TIME = "swt"
  final val STAGE = "stage"
  final val STATUS = "sta"
  final val TASK_INDEX = "idx"
  final val COMPLETION_TIME = "ct"
}

/**
 * Unlike other data types, the task data wrapper does not keep a reference to the API's TaskData.
 * That is to save memory, since for large applications there can be a large number of these
 * elements (by default up to 100,000 per stage), and every bit of wasted memory adds up.
 *
 * It also contains many secondary indices, which are used to sort data efficiently in the UI at the
 * expense of storage space (and slower write times).
 */
private[spark] class TaskDataWrapper(
    // Storing this as an object actually saves memory; it's also used as the key in the in-memory
    // store, so in that case you'd save the extra copy of the value here.
    @KVIndexParam
    val taskId: JLong,
    @KVIndexParam(value = TaskIndexNames.TASK_INDEX, parent = TaskIndexNames.STAGE)
    val index: Int,
    @KVIndexParam(value = TaskIndexNames.ATTEMPT, parent = TaskIndexNames.STAGE)
    val attempt: Int,
    @KVIndexParam(value = TaskIndexNames.LAUNCH_TIME, parent = TaskIndexNames.STAGE)
    val launchTime: Long,
    val resultFetchStart: Long,
    @KVIndexParam(value = TaskIndexNames.DURATION, parent = TaskIndexNames.STAGE)
    val duration: Long,
    @KVIndexParam(value = TaskIndexNames.EXECUTOR, parent = TaskIndexNames.STAGE)
    val executorId: String,
    @KVIndexParam(value = TaskIndexNames.HOST, parent = TaskIndexNames.STAGE)
    val host: String,
    @KVIndexParam(value = TaskIndexNames.STATUS, parent = TaskIndexNames.STAGE)
    val status: String,
    @KVIndexParam(value = TaskIndexNames.LOCALITY, parent = TaskIndexNames.STAGE)
    val taskLocality: String,
    val speculative: Boolean,
    val accumulatorUpdates: Seq[AccumulableInfo],
    val errorMessage: Option[String],

    // The following is an exploded view of a TaskMetrics API object. This saves 5 objects
    // (= 80 bytes of Java object overhead) per instance of this wrapper. If the first value
    // (executorDeserializeTime) is -1L, it means the metrics for this task have not been
    // recorded.
    @KVIndexParam(value = TaskIndexNames.DESER_TIME, parent = TaskIndexNames.STAGE)
    val executorDeserializeTime: Long,
    @KVIndexParam(value = TaskIndexNames.DESER_CPU_TIME, parent = TaskIndexNames.STAGE)
    val executorDeserializeCpuTime: Long,
    @KVIndexParam(value = TaskIndexNames.EXEC_RUN_TIME, parent = TaskIndexNames.STAGE)
    val executorRunTime: Long,
    @KVIndexParam(value = TaskIndexNames.EXEC_CPU_TIME, parent = TaskIndexNames.STAGE)
    val executorCpuTime: Long,
    @KVIndexParam(value = TaskIndexNames.RESULT_SIZE, parent = TaskIndexNames.STAGE)
    val resultSize: Long,
    @KVIndexParam(value = TaskIndexNames.GC_TIME, parent = TaskIndexNames.STAGE)
    val jvmGcTime: Long,
    @KVIndexParam(value = TaskIndexNames.SER_TIME, parent = TaskIndexNames.STAGE)
    val resultSerializationTime: Long,
    @KVIndexParam(value = TaskIndexNames.MEM_SPILL, parent = TaskIndexNames.STAGE)
    val memoryBytesSpilled: Long,
    @KVIndexParam(value = TaskIndexNames.DISK_SPILL, parent = TaskIndexNames.STAGE)
    val diskBytesSpilled: Long,
    @KVIndexParam(value = TaskIndexNames.PEAK_MEM, parent = TaskIndexNames.STAGE)
    val peakExecutionMemory: Long,
    @KVIndexParam(value = TaskIndexNames.INPUT_SIZE, parent = TaskIndexNames.STAGE)
    val inputBytesRead: Long,
    @KVIndexParam(value = TaskIndexNames.INPUT_RECORDS, parent = TaskIndexNames.STAGE)
    val inputRecordsRead: Long,
    @KVIndexParam(value = TaskIndexNames.OUTPUT_SIZE, parent = TaskIndexNames.STAGE)
    val outputBytesWritten: Long,
    @KVIndexParam(value = TaskIndexNames.OUTPUT_RECORDS, parent = TaskIndexNames.STAGE)
    val outputRecordsWritten: Long,
    @KVIndexParam(value = TaskIndexNames.SHUFFLE_REMOTE_BLOCKS, parent = TaskIndexNames.STAGE)
    val shuffleRemoteBlocksFetched: Long,
    @KVIndexParam(value = TaskIndexNames.SHUFFLE_LOCAL_BLOCKS, parent = TaskIndexNames.STAGE)
    val shuffleLocalBlocksFetched: Long,
    @KVIndexParam(value = TaskIndexNames.SHUFFLE_READ_TIME, parent = TaskIndexNames.STAGE)
    val shuffleFetchWaitTime: Long,
    @KVIndexParam(value = TaskIndexNames.SHUFFLE_REMOTE_READS, parent = TaskIndexNames.STAGE)
    val shuffleRemoteBytesRead: Long,
    @KVIndexParam(value = TaskIndexNames.SHUFFLE_REMOTE_READS_TO_DISK,
      parent = TaskIndexNames.STAGE)
    val shuffleRemoteBytesReadToDisk: Long,
    val shuffleLocalBytesRead: Long,
    @KVIndexParam(value = TaskIndexNames.SHUFFLE_READ_RECORDS, parent = TaskIndexNames.STAGE)
    val shuffleRecordsRead: Long,
    @KVIndexParam(value = TaskIndexNames.SHUFFLE_WRITE_SIZE, parent = TaskIndexNames.STAGE)
    val shuffleBytesWritten: Long,
    @KVIndexParam(value = TaskIndexNames.SHUFFLE_WRITE_TIME, parent = TaskIndexNames.STAGE)
    val shuffleWriteTime: Long,
    @KVIndexParam(value = TaskIndexNames.SHUFFLE_WRITE_RECORDS, parent = TaskIndexNames.STAGE)
    val shuffleRecordsWritten: Long,

    val stageId: Int,
    val stageAttemptId: Int) {

  def hasMetrics: Boolean = executorDeserializeTime >= 0

  def toApi: TaskData = {
    val metrics = if (hasMetrics) {
      Some(new TaskMetrics(
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
        new InputMetrics(
          inputBytesRead,
          inputRecordsRead),
        new OutputMetrics(
          outputBytesWritten,
          outputRecordsWritten),
        new ShuffleReadMetrics(
          shuffleRemoteBlocksFetched,
          shuffleLocalBlocksFetched,
          shuffleFetchWaitTime,
          shuffleRemoteBytesRead,
          shuffleRemoteBytesReadToDisk,
          shuffleLocalBytesRead,
          shuffleRecordsRead),
        new ShuffleWriteMetrics(
          shuffleBytesWritten,
          shuffleWriteTime,
          shuffleRecordsWritten)))
    } else {
      None
    }

    new TaskData(
      taskId,
      index,
      attempt,
      new Date(launchTime),
      if (resultFetchStart > 0L) Some(new Date(resultFetchStart)) else None,
      if (duration > 0L) Some(duration) else None,
      executorId,
      host,
      status,
      taskLocality,
      speculative,
      accumulatorUpdates,
      errorMessage,
      metrics,
      executorLogs = null,
      schedulerDelay = 0L,
      gettingResultTime = 0L)
  }

  def toLiveTask: LiveTask = {
    val taskInfo =
      new TaskInfo(
        taskId,
        index,
        attempt,
        launchTime,
        executorId,
        host,
        TaskLocality.withName(taskLocality),
        speculative)
    taskInfo.gettingResultTime = gettingResultTime
    val lastUpdateTime = duration + launchTime
    val liveTask = new LiveTask(taskInfo, stageId, stageAttemptId, Some(lastUpdateTime))
    liveTask
  }

  @JsonIgnore @KVIndex(TaskIndexNames.STAGE)
  private def stage: Array[Int] = Array(stageId, stageAttemptId)

  @JsonIgnore @KVIndex(value = TaskIndexNames.SCHEDULER_DELAY, parent = TaskIndexNames.STAGE)
  def schedulerDelay: Long = {
    if (hasMetrics) {
      AppStatusUtils.schedulerDelay(launchTime, resultFetchStart, duration, executorDeserializeTime,
        resultSerializationTime, executorRunTime)
    } else {
      -1L
    }
  }

  @JsonIgnore @KVIndex(value = TaskIndexNames.GETTING_RESULT_TIME, parent = TaskIndexNames.STAGE)
  def gettingResultTime: Long = {
    if (hasMetrics) {
      AppStatusUtils.gettingResultTime(launchTime, resultFetchStart, duration)
    } else {
      -1L
    }
  }

  /**
   * Sorting by accumulators is a little weird, and the previous behavior would generate
   * insanely long keys in the index. So this implementation just considers the first
   * accumulator and its String representation.
   */
  @JsonIgnore @KVIndex(value = TaskIndexNames.ACCUMULATORS, parent = TaskIndexNames.STAGE)
  private def accumulators: String = {
    if (accumulatorUpdates.nonEmpty) {
      val acc = accumulatorUpdates.head
      s"${acc.name}:${acc.value}"
    } else {
      ""
    }
  }

  @JsonIgnore @KVIndex(value = TaskIndexNames.SHUFFLE_TOTAL_READS, parent = TaskIndexNames.STAGE)
  private def shuffleTotalReads: Long = {
    if (hasMetrics) {
      shuffleLocalBytesRead + shuffleRemoteBytesRead
    } else {
      -1L
    }
  }

  @JsonIgnore @KVIndex(value = TaskIndexNames.SHUFFLE_TOTAL_BLOCKS, parent = TaskIndexNames.STAGE)
  private def shuffleTotalBlocks: Long = {
    if (hasMetrics) {
      shuffleLocalBlocksFetched + shuffleRemoteBlocksFetched
    } else {
      -1L
    }
  }

  @JsonIgnore @KVIndex(value = TaskIndexNames.ERROR, parent = TaskIndexNames.STAGE)
  private def error: String = if (errorMessage.isDefined) errorMessage.get else ""

  @JsonIgnore @KVIndex(value = TaskIndexNames.COMPLETION_TIME, parent = TaskIndexNames.STAGE)
  private def completionTime: Long = launchTime + duration
}

private[spark] class RDDStorageInfoWrapper(val info: RDDStorageInfo) {

  @JsonIgnore @KVIndex
  def id: Int = info.id

  @JsonIgnore @KVIndex("cached")
  def cached: Boolean = info.numCachedPartitions > 0

  def toLiveRDD(executors: scala.collection.Map[String, LiveExecutor]): LiveRDD = {
    val rddInfo = new RDDInfo(
      info.id,
      info.name,
      info.numPartitions,
      StorageLevel.fromDescription(info.storageLevel),
      false,
      Nil)
    val liveRDD = new LiveRDD(rddInfo)
    liveRDD.memoryUsed = info.memoryUsed
    liveRDD.diskUsed = info.diskUsed
    info.partitions.get.foreach { rddPartition =>
      val liveRDDPartition = rddPartition.toLiveRDDPartition
      liveRDD.partitions.put(rddPartition.blockName, liveRDDPartition)
      liveRDD.partitionSeq.addPartition(liveRDDPartition)
    }
    if (info.dataDistribution.nonEmpty) {
      info.dataDistribution.get.foreach { rddDist =>
        val liveRDDDist = rddDist.toLiveRDDDistribution(executors)
        liveRDD.distributions.put(liveRDDDist.executorId, liveRDDDist)
      }
    }
    liveRDD
  }

}

private[spark] class ExecutorStageSummaryWrapper(
    val stageId: Int,
    val stageAttemptId: Int,
    val executorId: String,
    val info: ExecutorStageSummary) {

  @JsonIgnore @KVIndex
  private val _id: Array[Any] = Array(stageId, stageAttemptId, executorId)

  @JsonIgnore @KVIndex("stage")
  private def stage: Array[Int] = Array(stageId, stageAttemptId)

  @JsonIgnore
  def id: Array[Any] = _id

  def toLiveExecutorStageSummary: LiveExecutorStageSummary = {
    val liveESSummary = new LiveExecutorStageSummary(stageId, stageAttemptId, executorId)
    val metrics = LiveEntityHelpers.createMetrics(
      executorDeserializeTime = 0,
      executorDeserializeCpuTime = 0,
      executorRunTime = 0,
      executorCpuTime = 0,
      resultSize = 0,
      jvmGcTime = 0,
      resultSerializationTime = 0,
      memoryBytesSpilled = info.memoryBytesSpilled,
      diskBytesSpilled = info.diskBytesSpilled,
      peakExecutionMemory = 0,
      inputBytesRead = info.inputBytes,
      inputRecordsRead = info.inputRecords,
      outputBytesWritten = info.outputBytes,
      outputRecordsWritten = info.outputRecords,
      shuffleRemoteBlocksFetched = 0,
      shuffleLocalBlocksFetched = 0,
      shuffleFetchWaitTime = 0,
      shuffleRemoteBytesRead = info.shuffleRead,
      shuffleRemoteBytesReadToDisk = 0,
      shuffleLocalBytesRead = 0,
      shuffleRecordsRead = info.shuffleReadRecords,
      shuffleBytesWritten = info.shuffleWrite,
      shuffleWriteTime = 0,
      shuffleRecordsWritten = info.shuffleWriteRecords)
    liveESSummary.taskTime = info.taskTime
    liveESSummary.succeededTasks = info.succeededTasks
    liveESSummary.failedTasks = info.failedTasks
    liveESSummary.isBlacklisted = info.isBlacklistedForStage
    liveESSummary.metrics = metrics
    liveESSummary
  }

}

private[spark] class StreamBlockData(
  val name: String,
  val executorId: String,
  val hostPort: String,
  val storageLevel: String,
  val useMemory: Boolean,
  val useDisk: Boolean,
  val deserialized: Boolean,
  val memSize: Long,
  val diskSize: Long) {

  @JsonIgnore @KVIndex
  def key: Array[String] = Array(name, executorId)

}

private[spark] class RDDOperationClusterWrapper(
    val id: String,
    val name: String,
    val childNodes: Seq[RDDOperationNode],
    val childClusters: Seq[RDDOperationClusterWrapper]) {

  def toRDDOperationCluster(): RDDOperationCluster = {
    val isBarrier = childNodes.exists(_.barrier)
    val name = if (isBarrier) this.name + "\n(barrier mode)" else this.name
    val cluster = new RDDOperationCluster(id, isBarrier, name)
    childNodes.foreach(cluster.attachChildNode)
    childClusters.foreach { child =>
      cluster.attachChildCluster(child.toRDDOperationCluster())
    }
    cluster
  }

}

private[spark] class RDDOperationGraphWrapper(
    @KVIndexParam val stageId: Int,
    val edges: Seq[RDDOperationEdge],
    val outgoingEdges: Seq[RDDOperationEdge],
    val incomingEdges: Seq[RDDOperationEdge],
    val rootCluster: RDDOperationClusterWrapper) {

  def toRDDOperationGraph(): RDDOperationGraph = {
    new RDDOperationGraph(edges, outgoingEdges, incomingEdges, rootCluster.toRDDOperationCluster())
  }

}

private[spark] class PoolData(
    @KVIndexParam val name: String,
    val stageIds: Set[Int]) {

  def toSchedulerPool: SchedulerPool = {
    val pool = new SchedulerPool(name)
    pool.stageIds = stageIds
    pool
  }
}

/**
 * A class with information about an app, to be used by the UI. There's only one instance of
 * this summary per application, so its ID in the store is the class name.
 */
private[spark] class AppSummary(
    val numCompletedJobs: Int,
    val numCompletedStages: Int) {

  @KVIndex
  def id: String = classOf[AppSummary].getName()

}

/**
 * A cached view of a specific quantile for one stage attempt's metrics.
 */
private[spark] class CachedQuantile(
    val stageId: Int,
    val stageAttemptId: Int,
    val quantile: String,
    val taskCount: Long,

    // The following fields are an exploded view of a single entry for TaskMetricDistributions.
    val executorDeserializeTime: Double,
    val executorDeserializeCpuTime: Double,
    val executorRunTime: Double,
    val executorCpuTime: Double,
    val resultSize: Double,
    val jvmGcTime: Double,
    val resultSerializationTime: Double,
    val gettingResultTime: Double,
    val schedulerDelay: Double,
    val peakExecutionMemory: Double,
    val memoryBytesSpilled: Double,
    val diskBytesSpilled: Double,

    val bytesRead: Double,
    val recordsRead: Double,

    val bytesWritten: Double,
    val recordsWritten: Double,

    val shuffleReadBytes: Double,
    val shuffleRecordsRead: Double,
    val shuffleRemoteBlocksFetched: Double,
    val shuffleLocalBlocksFetched: Double,
    val shuffleFetchWaitTime: Double,
    val shuffleRemoteBytesRead: Double,
    val shuffleRemoteBytesReadToDisk: Double,
    val shuffleTotalBlocksFetched: Double,

    val shuffleWriteBytes: Double,
    val shuffleWriteRecords: Double,
    val shuffleWriteTime: Double) {

  @KVIndex @JsonIgnore
  def id: Array[Any] = Array(stageId, stageAttemptId, quantile)

  @KVIndex("stage") @JsonIgnore
  def stage: Array[Int] = Array(stageId, stageAttemptId)

}
