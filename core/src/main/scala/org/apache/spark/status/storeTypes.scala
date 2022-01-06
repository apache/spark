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

import org.apache.spark.status.KVUtils._
import org.apache.spark.status.api.v1._
import org.apache.spark.ui.scope._
import org.apache.spark.util.Utils
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
  val host: String = Utils.parseHostPort(info.hostPort)._1

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
  def completionTime: Long = info.completionTime.map(_.getTime).getOrElse(-1L)
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
  final val SHUFFLE_READ_FETCH_WAIT_TIME = "srt"
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
    @KVIndexParam(parent = TaskIndexNames.STAGE)
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

    val hasMetrics: Boolean,
    // The following is an exploded view of a TaskMetrics API object. This saves 5 objects
    // (= 80 bytes of Java object overhead) per instance of this wrapper. Non successful
    // tasks' metrics will have negative values in `TaskDataWrapper`. `TaskData` will have
    // actual metric values. To recover the actual metric value from `TaskDataWrapper`,
    // need use `getMetricValue` method. If `hasMetrics` is false, it means the metrics
    // for this task have not been recorded.
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
    @KVIndexParam(value = TaskIndexNames.SHUFFLE_READ_FETCH_WAIT_TIME,
      parent = TaskIndexNames.STAGE)
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

  // SPARK-26260: To handle non successful tasks metrics (Running, Failed, Killed).
  private def getMetricValue(metric: Long): Long = {
    if (status != "SUCCESS") {
      math.abs(metric + 1)
    } else {
      metric
    }
  }

  def toApi: TaskData = {
    val metrics = if (hasMetrics) {
      Some(new TaskMetrics(
        getMetricValue(executorDeserializeTime),
        getMetricValue(executorDeserializeCpuTime),
        getMetricValue(executorRunTime),
        getMetricValue(executorCpuTime),
        getMetricValue(resultSize),
        getMetricValue(jvmGcTime),
        getMetricValue(resultSerializationTime),
        getMetricValue(memoryBytesSpilled),
        getMetricValue(diskBytesSpilled),
        getMetricValue(peakExecutionMemory),
        new InputMetrics(
          getMetricValue(inputBytesRead),
          getMetricValue(inputRecordsRead)),
        new OutputMetrics(
          getMetricValue(outputBytesWritten),
          getMetricValue(outputRecordsWritten)),
        new ShuffleReadMetrics(
          getMetricValue(shuffleRemoteBlocksFetched),
          getMetricValue(shuffleLocalBlocksFetched),
          getMetricValue(shuffleFetchWaitTime),
          getMetricValue(shuffleRemoteBytesRead),
          getMetricValue(shuffleRemoteBytesReadToDisk),
          getMetricValue(shuffleLocalBytesRead),
          getMetricValue(shuffleRecordsRead)),
        new ShuffleWriteMetrics(
          getMetricValue(shuffleBytesWritten),
          getMetricValue(shuffleWriteTime),
          getMetricValue(shuffleRecordsWritten))))
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

  @JsonIgnore @KVIndex(TaskIndexNames.STAGE)
  private def stage: Array[Int] = Array(stageId, stageAttemptId)

  @JsonIgnore @KVIndex(value = TaskIndexNames.SCHEDULER_DELAY, parent = TaskIndexNames.STAGE)
  def schedulerDelay: Long = {
    if (hasMetrics) {
      AppStatusUtils.schedulerDelay(launchTime, resultFetchStart, duration,
        getMetricValue(executorDeserializeTime),
        getMetricValue(resultSerializationTime),
        getMetricValue(executorRunTime))
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
      getMetricValue(shuffleLocalBytesRead) + getMetricValue(shuffleRemoteBytesRead)
    } else {
      -1L
    }
  }

  @JsonIgnore @KVIndex(value = TaskIndexNames.SHUFFLE_TOTAL_BLOCKS, parent = TaskIndexNames.STAGE)
  private def shuffleTotalBlocks: Long = {
    if (hasMetrics) {
      getMetricValue(shuffleLocalBlocksFetched) + getMetricValue(shuffleRemoteBlocksFetched)
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

}

private[spark] class ResourceProfileWrapper(val rpInfo: ResourceProfileInfo) {

  @JsonIgnore @KVIndex
  def id: Int = rpInfo.id

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

}

private[spark] class SpeculationStageSummaryWrapper(
    val stageId: Int,
    val stageAttemptId: Int,
    val info: SpeculationStageSummary) {

  @JsonIgnore @KVIndex("stage")
  private def stage: Array[Int] = Array(stageId, stageAttemptId)

  @KVIndex
  private[this] val id: Array[Int] = Array(stageId, stageAttemptId)
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
    val stageIds: Set[Int])

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
    val duration: Double,
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

private[spark] class ProcessSummaryWrapper(val info: ProcessSummary) {

  @JsonIgnore @KVIndex
  private def id: String = info.id

  @JsonIgnore @KVIndex("active")
  private def active: Boolean = info.isActive

  @JsonIgnore @KVIndex("host")
  val host: String = Utils.parseHostPort(info.hostPort)._1

}
