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

import java.io.File
import java.io.IOException
import java.util.{List => JList}

import scala.collection.mutable.HashMap
import scala.jdk.CollectionConverters._

import org.apache.spark.{JobExecutionStatus, SparkConf, SparkContext}
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.PATH
import org.apache.spark.internal.config.Status.LIVE_UI_LOCAL_STORE_DIR
import org.apache.spark.status.AppStatusUtils.getQuantilesValue
import org.apache.spark.status.api.v1
import org.apache.spark.storage.FallbackStorage.FALLBACK_BLOCK_MANAGER_ID
import org.apache.spark.ui.scope._
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils
import org.apache.spark.util.kvstore.KVStore

/**
 * A wrapper around a KVStore that provides methods for accessing the API data stored within.
 */
private[spark] class AppStatusStore(
    val store: KVStore,
    val listener: Option[AppStatusListener] = None,
    val storePath: Option[File] = None) {

  def applicationInfo(): v1.ApplicationInfo = {
    try {
      // The ApplicationInfo may not be available when Spark is starting up.
      Utils.tryWithResource(
        store.view(classOf[ApplicationInfoWrapper])
          .max(1)
          .closeableIterator()
      ) { it =>
        it.next().info
      }
    } catch {
      case _: NoSuchElementException =>
        throw new NoSuchElementException("Failed to get the application information. " +
          "If you are starting up Spark, please wait a while until it's ready.")
    }
  }

  def environmentInfo(): v1.ApplicationEnvironmentInfo = {
    val klass = classOf[ApplicationEnvironmentInfoWrapper]
    store.read(klass, klass.getName()).info
  }

  def resourceProfileInfo(): Seq[v1.ResourceProfileInfo] = {
    KVUtils.mapToSeq(store.view(classOf[ResourceProfileWrapper]))(_.rpInfo)
  }

  def jobsList(statuses: JList[JobExecutionStatus]): Seq[v1.JobData] = {
    val it = KVUtils.mapToSeq(store.view(classOf[JobDataWrapper]).reverse())(_.info)
    if (statuses != null && !statuses.isEmpty()) {
      it.filter { job => statuses.contains(job.status) }
    } else {
      it
    }
  }

  def job(jobId: Int): v1.JobData = {
    store.read(classOf[JobDataWrapper], jobId).info
  }

  // Returns job data and associated SQL execution ID of certain Job ID.
  // If there is no related SQL execution, the SQL execution ID part will be None.
  def jobWithAssociatedSql(jobId: Int): (v1.JobData, Option[Long]) = {
    val data = store.read(classOf[JobDataWrapper], jobId)
    (data.info, data.sqlExecutionId)
  }

  def executorList(activeOnly: Boolean): Seq[v1.ExecutorSummary] = {
    val base = store.view(classOf[ExecutorSummaryWrapper])
    val filtered = if (activeOnly) {
      base.index("active").reverse().first(true).last(true)
    } else {
      base
    }
    KVUtils.mapToSeq(filtered)(_.info)
      .filter(_.id != FALLBACK_BLOCK_MANAGER_ID.executorId)
      .map(replaceExec)
  }

  private def replaceExec(origin: v1.ExecutorSummary): v1.ExecutorSummary = {
    if (origin.id == SparkContext.DRIVER_IDENTIFIER) {
      replaceDriverGcTime(origin, extractGcTime(origin), extractAppTime)
    } else {
      origin
    }
  }

  private def replaceDriverGcTime(source: v1.ExecutorSummary,
    totalGcTime: Option[Long], totalAppTime: Option[Long]): v1.ExecutorSummary = {
    new v1.ExecutorSummary(source.id, source.hostPort, source.isActive, source.rddBlocks,
      source.memoryUsed, source.diskUsed, source.totalCores, source.maxTasks, source.activeTasks,
      source.failedTasks, source.completedTasks, source.totalTasks,
      totalAppTime.getOrElse(source.totalDuration),
      totalGcTime.getOrElse(source.totalGCTime),
      source.totalInputBytes, source.totalShuffleRead,
      source.totalShuffleWrite, source.isBlacklisted, source.maxMemory, source.addTime,
      source.removeTime, source.removeReason, source.executorLogs, source.memoryMetrics,
      source.blacklistedInStages, source.peakMemoryMetrics, source.attributes, source.resources,
      source.resourceProfileId, source.isExcluded, source.excludedInStages)
  }

  private def extractGcTime(source: v1.ExecutorSummary): Option[Long] = {
    source.peakMemoryMetrics.map(_.getMetricValue("TotalGCTime"))
  }

  private def extractAppTime: Option[Long] = {
    var startTime = 0L
    // -1 when SparkListenerApplicationStart event written to kvStore
    // event time when SparkListenerApplicationStart event written to kvStore
    var endTime = 0L
    try {
      val appInfo = applicationInfo()
      startTime = appInfo.attempts.head.startTime.getTime()
      endTime = appInfo.attempts.head.endTime.getTime()
    } catch {
      //  too early to get appInfo, should wait a while
      case _: NoSuchElementException =>
    }
    if (endTime == 0) {
      None
    } else if (endTime < 0) {
      Option(System.currentTimeMillis() - startTime)
    } else {
      Option(endTime - startTime)
    }
  }

  def miscellaneousProcessList(activeOnly: Boolean): Seq[v1.ProcessSummary] = {
    val base = store.view(classOf[ProcessSummaryWrapper])
    val filtered = if (activeOnly) {
      base.index("active").reverse().first(true).last(true)
    } else {
      base
    }
    KVUtils.mapToSeq(filtered)(_.info)
  }

  def executorSummary(executorId: String): v1.ExecutorSummary = {
    store.read(classOf[ExecutorSummaryWrapper], executorId).info
  }

  /**
   * This is used by ConsoleProgressBar to quickly fetch active stages for drawing the progress
   * bar. It will only return anything useful when called from a live application.
   */
  def activeStages(): Seq[v1.StageData] = {
    listener.map(_.activeStages()).getOrElse(Nil)
  }

  def stageList(
    statuses: JList[v1.StageStatus],
    details: Boolean = false,
    withSummaries: Boolean = false,
    unsortedQuantiles: Array[Double] = Array.empty,
    taskStatus: JList[v1.TaskStatus] = List().asJava): Seq[v1.StageData] = {
    val quantiles = unsortedQuantiles.sorted
    val it = KVUtils.mapToSeq(store.view(classOf[StageDataWrapper]).reverse())(_.info)
    val ret = if (statuses != null && !statuses.isEmpty()) {
      it.filter { s => statuses.contains(s.status) }
    } else {
      it
    }

    ret.map { s =>
      newStageData(s, withDetail = details, taskStatus = taskStatus,
        withSummaries = withSummaries, unsortedQuantiles = quantiles)
    }
  }

  def stageData(
    stageId: Int,
    details: Boolean = false,
    taskStatus: JList[v1.TaskStatus] = List().asJava,
    withSummaries: Boolean = false,
    unsortedQuantiles: Array[Double] = Array.empty[Double]): Seq[v1.StageData] = {
    KVUtils.mapToSeq(store.view(classOf[StageDataWrapper]).index("stageId")
      .first(stageId).last(stageId)) { s =>
      newStageData(s.info, withDetail = details, taskStatus = taskStatus,
        withSummaries = withSummaries, unsortedQuantiles = unsortedQuantiles)
    }
  }

  def lastStageAttempt(stageId: Int): v1.StageData = {
    val it = store.view(classOf[StageDataWrapper])
      .index("stageId")
      .reverse()
      .first(stageId)
      .last(stageId)
      .closeableIterator()
    try {
      if (it.hasNext()) {
        it.next().info
      } else {
        throw new NoSuchElementException(s"No stage with id $stageId")
      }
    } finally {
      it.close()
    }
  }

  def stageAttempt(
      stageId: Int, stageAttemptId: Int,
      details: Boolean = false,
      taskStatus: JList[v1.TaskStatus] = List().asJava,
      withSummaries: Boolean = false,
      unsortedQuantiles: Array[Double] = Array.empty[Double]): (v1.StageData, Seq[Int]) = {
    val stageKey = Array(stageId, stageAttemptId)
    val stageDataWrapper = store.read(classOf[StageDataWrapper], stageKey)
    val stage = newStageData(stageDataWrapper.info, withDetail = details, taskStatus = taskStatus,
      withSummaries = withSummaries, unsortedQuantiles = unsortedQuantiles)
    (stage, stageDataWrapper.jobIds.toSeq)
  }

  def taskCount(stageId: Int, stageAttemptId: Int): Long = {
    store.count(classOf[TaskDataWrapper], "stage", Array(stageId, stageAttemptId))
  }

  def localitySummary(stageId: Int, stageAttemptId: Int): Map[String, Long] = {
    store.read(classOf[StageDataWrapper], Array(stageId, stageAttemptId)).locality
  }

  /**
   * Calculates a summary of the task metrics for the given stage attempt, returning the
   * requested quantiles for the recorded metrics.
   *
   * This method can be expensive if the requested quantiles are not cached; the method
   * will only cache certain quantiles (every 0.05 step), so it's recommended to stick to
   * those to avoid expensive scans of all task data.
   */
  def taskSummary(
      stageId: Int,
      stageAttemptId: Int,
      unsortedQuantiles: Array[Double]): Option[v1.TaskMetricDistributions] = {
    val stageKey = Array(stageId, stageAttemptId)
    val quantiles = unsortedQuantiles.sorted.toImmutableArraySeq

    // We don't know how many tasks remain in the store that actually have metrics. So scan one
    // metric and count how many valid tasks there are. Use skip() instead of next() since it's
    // cheaper for disk stores (avoids deserialization).
    val count = {
      Utils.tryWithResource(
        store.view(classOf[TaskDataWrapper])
          .parent(stageKey)
          .index(TaskIndexNames.EXEC_RUN_TIME)
          .first(0L)
          .closeableIterator()
      ) { it =>
        var _count = 0L
        while (it.hasNext()) {
          _count += 1
          it.skip(1)
        }
        _count
      }
    }

    if (count <= 0) {
      return None
    }

    // Find out which quantiles are already cached. The data in the store must match the expected
    // task count to be considered, otherwise it will be re-scanned and overwritten.
    val cachedQuantiles = quantiles.filter(shouldCacheQuantile).flatMap { q =>
      val qkey = Array(stageId, stageAttemptId, quantileToString(q))
      asOption(store.read(classOf[CachedQuantile], qkey)).filter(_.taskCount == count)
    }

    // If there are no missing quantiles, return the data. Otherwise, just compute everything
    // to make the code simpler.
    if (cachedQuantiles.size == quantiles.size) {
      def toValues(fn: CachedQuantile => Double): IndexedSeq[Double] = cachedQuantiles.map(fn)

      val distributions = new v1.TaskMetricDistributions(
        quantiles = quantiles,
        duration = toValues(_.duration),
        executorDeserializeTime = toValues(_.executorDeserializeTime),
        executorDeserializeCpuTime = toValues(_.executorDeserializeCpuTime),
        executorRunTime = toValues(_.executorRunTime),
        executorCpuTime = toValues(_.executorCpuTime),
        resultSize = toValues(_.resultSize),
        jvmGcTime = toValues(_.jvmGcTime),
        resultSerializationTime = toValues(_.resultSerializationTime),
        gettingResultTime = toValues(_.gettingResultTime),
        schedulerDelay = toValues(_.schedulerDelay),
        peakExecutionMemory = toValues(_.peakExecutionMemory),
        memoryBytesSpilled = toValues(_.memoryBytesSpilled),
        diskBytesSpilled = toValues(_.diskBytesSpilled),
        inputMetrics = new v1.InputMetricDistributions(
          toValues(_.bytesRead),
          toValues(_.recordsRead)),
        outputMetrics = new v1.OutputMetricDistributions(
          toValues(_.bytesWritten),
          toValues(_.recordsWritten)),
        shuffleReadMetrics = new v1.ShuffleReadMetricDistributions(
          toValues(_.shuffleReadBytes),
          toValues(_.shuffleRecordsRead),
          toValues(_.shuffleRemoteBlocksFetched),
          toValues(_.shuffleLocalBlocksFetched),
          toValues(_.shuffleFetchWaitTime),
          toValues(_.shuffleRemoteBytesRead),
          toValues(_.shuffleRemoteBytesReadToDisk),
          toValues(_.shuffleTotalBlocksFetched),
          toValues(_.shuffleRemoteReqsDuration),
          new v1.ShufflePushReadMetricDistributions(
            toValues(_.shuffleCorruptMergedBlockChunks),
            toValues(_.shuffleMergedFetchFallbackCount),
            toValues(_.shuffleMergedRemoteBlocksFetched),
            toValues(_.shuffleMergedLocalBlocksFetched),
            toValues(_.shuffleMergedRemoteChunksFetched),
            toValues(_.shuffleMergedLocalChunksFetched),
            toValues(_.shuffleMergedRemoteBytesRead),
            toValues(_.shuffleMergedLocalBytesRead),
            toValues(_.shuffleMergedRemoteReqsDuration))),
        shuffleWriteMetrics = new v1.ShuffleWriteMetricDistributions(
          toValues(_.shuffleWriteBytes),
          toValues(_.shuffleWriteRecords),
          toValues(_.shuffleWriteTime)))

      return Some(distributions)
    }

    // Compute quantiles by scanning the tasks in the store. This is not really stable for live
    // stages (e.g. the number of recorded tasks may change while this code is running), but should
    // stabilize once the stage finishes. It's also slow, especially with disk stores.
    val indices = quantiles.map { q => math.min((q * count).toLong, count - 1) }

    def scanTasks(index: String)(fn: TaskDataWrapper => Long): IndexedSeq[Double] = {
      Utils.tryWithResource(
        store.view(classOf[TaskDataWrapper])
          .parent(stageKey)
          .index(index)
          .first(0L)
          .closeableIterator()
      ) { it =>
        var last = Double.NaN
        var currentIdx = -1L
        indices.map { idx =>
          if (idx == currentIdx) {
            last
          } else {
            val diff = idx - currentIdx
            currentIdx = idx
            if (it.skip(diff - 1)) {
              last = fn(it.next()).toDouble
              last
            } else {
              Double.NaN
            }
          }
        }
      }
    }

    val computedQuantiles = new v1.TaskMetricDistributions(
      quantiles = quantiles,
      duration = scanTasks(TaskIndexNames.DURATION) { t =>
        t.duration
      },
      executorDeserializeTime = scanTasks(TaskIndexNames.DESER_TIME) { t =>
        t.executorDeserializeTime
      },
      executorDeserializeCpuTime = scanTasks(TaskIndexNames.DESER_CPU_TIME) { t =>
        t.executorDeserializeCpuTime
      },
      executorRunTime = scanTasks(TaskIndexNames.EXEC_RUN_TIME) { t => t.executorRunTime },
      executorCpuTime = scanTasks(TaskIndexNames.EXEC_CPU_TIME) { t => t.executorCpuTime },
      resultSize = scanTasks(TaskIndexNames.RESULT_SIZE) { t => t.resultSize },
      jvmGcTime = scanTasks(TaskIndexNames.GC_TIME) { t => t.jvmGcTime },
      resultSerializationTime = scanTasks(TaskIndexNames.SER_TIME) { t =>
        t.resultSerializationTime
      },
      gettingResultTime = scanTasks(TaskIndexNames.GETTING_RESULT_TIME) { t =>
        t.gettingResultTime
      },
      schedulerDelay = scanTasks(TaskIndexNames.SCHEDULER_DELAY) { t => t.schedulerDelay },
      peakExecutionMemory = scanTasks(TaskIndexNames.PEAK_MEM) { t => t.peakExecutionMemory },
      memoryBytesSpilled = scanTasks(TaskIndexNames.MEM_SPILL) { t => t.memoryBytesSpilled },
      diskBytesSpilled = scanTasks(TaskIndexNames.DISK_SPILL) { t => t.diskBytesSpilled },
      inputMetrics = new v1.InputMetricDistributions(
        scanTasks(TaskIndexNames.INPUT_SIZE) { t => t.inputBytesRead },
        scanTasks(TaskIndexNames.INPUT_RECORDS) { t => t.inputRecordsRead }),
      outputMetrics = new v1.OutputMetricDistributions(
        scanTasks(TaskIndexNames.OUTPUT_SIZE) { t => t.outputBytesWritten },
        scanTasks(TaskIndexNames.OUTPUT_RECORDS) { t => t.outputRecordsWritten }),
      shuffleReadMetrics = new v1.ShuffleReadMetricDistributions(
        scanTasks(TaskIndexNames.SHUFFLE_TOTAL_READS) { m =>
          m.shuffleLocalBytesRead + m.shuffleRemoteBytesRead
        },
        scanTasks(TaskIndexNames.SHUFFLE_READ_RECORDS) { t => t.shuffleRecordsRead },
        scanTasks(TaskIndexNames.SHUFFLE_REMOTE_BLOCKS) { t => t.shuffleRemoteBlocksFetched },
        scanTasks(TaskIndexNames.SHUFFLE_LOCAL_BLOCKS) { t => t.shuffleLocalBlocksFetched },
        scanTasks(TaskIndexNames.SHUFFLE_READ_FETCH_WAIT_TIME) { t => t.shuffleFetchWaitTime },
        scanTasks(TaskIndexNames.SHUFFLE_REMOTE_READS) { t => t.shuffleRemoteBytesRead },
        scanTasks(TaskIndexNames.SHUFFLE_REMOTE_READS_TO_DISK) { t =>
          t.shuffleRemoteBytesReadToDisk
        },
        scanTasks(TaskIndexNames.SHUFFLE_TOTAL_BLOCKS) { m =>
          m.shuffleLocalBlocksFetched + m.shuffleRemoteBlocksFetched
        },
        scanTasks(TaskIndexNames.SHUFFLE_REMOTE_REQS_DURATION) {
          t => t.shuffleRemoteReqsDuration
        },
        new v1.ShufflePushReadMetricDistributions(
          scanTasks(TaskIndexNames.SHUFFLE_PUSH_CORRUPT_MERGED_BLOCK_CHUNKS) { t =>
            t.shuffleCorruptMergedBlockChunks
          },
          scanTasks(TaskIndexNames.SHUFFLE_PUSH_MERGED_FETCH_FALLBACK_COUNT) {
            t => t.shuffleMergedFetchFallbackCount
          },
          scanTasks(TaskIndexNames.SHUFFLE_PUSH_MERGED_REMOTE_BLOCKS) { t =>
            t.shuffleMergedRemoteBlocksFetched
          },
          scanTasks(TaskIndexNames.SHUFFLE_PUSH_MERGED_LOCAL_BLOCKS) { t =>
            t.shuffleMergedLocalBlocksFetched
          },
          scanTasks(TaskIndexNames.SHUFFLE_PUSH_MERGED_REMOTE_CHUNKS) { t =>
            t.shuffleMergedRemoteChunksFetched
          },
          scanTasks(TaskIndexNames.SHUFFLE_PUSH_MERGED_LOCAL_CHUNKS) { t =>
            t.shuffleMergedLocalChunksFetched
          },
          scanTasks(TaskIndexNames.SHUFFLE_PUSH_MERGED_REMOTE_READS) { t =>
            t.shuffleMergedRemoteBytesRead
          },
          scanTasks(TaskIndexNames.SHUFFLE_PUSH_MERGED_LOCAL_READS) { t =>
            t.shuffleMergedLocalBytesRead
          },
          scanTasks(TaskIndexNames.SHUFFLE_PUSH_MERGED_REMOTE_REQS_DURATION) { t =>
            t.shuffleMergedRemoteReqDuration
          })),
        shuffleWriteMetrics = new v1.ShuffleWriteMetricDistributions(
          scanTasks(TaskIndexNames.SHUFFLE_WRITE_SIZE) { t => t.shuffleBytesWritten },
          scanTasks(TaskIndexNames.SHUFFLE_WRITE_RECORDS) { t => t.shuffleRecordsWritten },
          scanTasks(TaskIndexNames.SHUFFLE_WRITE_TIME) { t => t.shuffleWriteTime }))

    // Go through the computed quantiles and cache the values that match the caching criteria.
    computedQuantiles.quantiles.zipWithIndex
      .filter { case (q, _) => quantiles.contains(q) && shouldCacheQuantile(q) }
      .foreach { case (q, idx) =>
        val cached = new CachedQuantile(stageId, stageAttemptId, quantileToString(q), count,
          duration = computedQuantiles.duration(idx),
          executorDeserializeTime = computedQuantiles.executorDeserializeTime(idx),
          executorDeserializeCpuTime = computedQuantiles.executorDeserializeCpuTime(idx),
          executorRunTime = computedQuantiles.executorRunTime(idx),
          executorCpuTime = computedQuantiles.executorCpuTime(idx),
          resultSize = computedQuantiles.resultSize(idx),
          jvmGcTime = computedQuantiles.jvmGcTime(idx),
          resultSerializationTime = computedQuantiles.resultSerializationTime(idx),
          gettingResultTime = computedQuantiles.gettingResultTime(idx),
          schedulerDelay = computedQuantiles.schedulerDelay(idx),
          peakExecutionMemory = computedQuantiles.peakExecutionMemory(idx),
          memoryBytesSpilled = computedQuantiles.memoryBytesSpilled(idx),
          diskBytesSpilled = computedQuantiles.diskBytesSpilled(idx),

          bytesRead = computedQuantiles.inputMetrics.bytesRead(idx),
          recordsRead = computedQuantiles.inputMetrics.recordsRead(idx),

          bytesWritten = computedQuantiles.outputMetrics.bytesWritten(idx),
          recordsWritten = computedQuantiles.outputMetrics.recordsWritten(idx),

          shuffleReadBytes = computedQuantiles.shuffleReadMetrics.readBytes(idx),
          shuffleRecordsRead = computedQuantiles.shuffleReadMetrics.readRecords(idx),
          shuffleRemoteBlocksFetched =
            computedQuantiles.shuffleReadMetrics.remoteBlocksFetched(idx),
          shuffleLocalBlocksFetched = computedQuantiles.shuffleReadMetrics.localBlocksFetched(idx),
          shuffleFetchWaitTime = computedQuantiles.shuffleReadMetrics.fetchWaitTime(idx),
          shuffleRemoteBytesRead = computedQuantiles.shuffleReadMetrics.remoteBytesRead(idx),
          shuffleRemoteBytesReadToDisk =
            computedQuantiles.shuffleReadMetrics.remoteBytesReadToDisk(idx),
          shuffleTotalBlocksFetched = computedQuantiles.shuffleReadMetrics.totalBlocksFetched(idx),
          shuffleCorruptMergedBlockChunks =
            computedQuantiles.shuffleReadMetrics.shufflePushReadMetricsDist
              .corruptMergedBlockChunks(idx),
          shuffleMergedFetchFallbackCount =
            computedQuantiles.shuffleReadMetrics.shufflePushReadMetricsDist
              .mergedFetchFallbackCount(idx),
          shuffleMergedRemoteBlocksFetched =
            computedQuantiles.shuffleReadMetrics.shufflePushReadMetricsDist
              .remoteMergedBlocksFetched(idx),
          shuffleMergedLocalBlocksFetched =
            computedQuantiles.shuffleReadMetrics.shufflePushReadMetricsDist
              .localMergedBlocksFetched(idx),
          shuffleMergedRemoteChunksFetched =
            computedQuantiles.shuffleReadMetrics.shufflePushReadMetricsDist
              .remoteMergedChunksFetched(idx),
          shuffleMergedLocalChunksFetched =
            computedQuantiles.shuffleReadMetrics.shufflePushReadMetricsDist
              .localMergedChunksFetched(idx),
          shuffleMergedRemoteBytesRead =
            computedQuantiles.shuffleReadMetrics.shufflePushReadMetricsDist
              .remoteMergedBytesRead(idx),
          shuffleMergedLocalBytesRead =
            computedQuantiles.shuffleReadMetrics.shufflePushReadMetricsDist
              .localMergedBytesRead(idx),
          shuffleRemoteReqsDuration =
            computedQuantiles.shuffleReadMetrics.remoteReqsDuration(idx),
          shuffleMergedRemoteReqsDuration =
            computedQuantiles.shuffleReadMetrics.shufflePushReadMetricsDist
              .remoteMergedReqsDuration(idx),

          shuffleWriteBytes = computedQuantiles.shuffleWriteMetrics.writeBytes(idx),
          shuffleWriteRecords = computedQuantiles.shuffleWriteMetrics.writeRecords(idx),
          shuffleWriteTime = computedQuantiles.shuffleWriteMetrics.writeTime(idx))
        store.write(cached)
      }

    Some(computedQuantiles)
  }

  /**
   * Whether to cache information about a specific metric quantile. We cache quantiles at every 0.05
   * step, which covers the default values used both in the API and in the stages page.
   */
  private def shouldCacheQuantile(q: Double): Boolean = (math.round(q * 100) % 5) == 0

  private def quantileToString(q: Double): String = math.round(q * 100).toString

  def taskList(stageId: Int, stageAttemptId: Int, maxTasks: Int): Seq[v1.TaskData] = {
    val stageKey = Array(stageId, stageAttemptId)
    val taskDataWrapperSeq = KVUtils.viewToSeq(store.view(classOf[TaskDataWrapper]).index("stage")
      .first(stageKey).last(stageKey).reverse().max(maxTasks))
    constructTaskDataList(taskDataWrapperSeq).reverse
  }

  def taskList(
      stageId: Int,
      stageAttemptId: Int,
      offset: Int,
      length: Int,
      sortBy: v1.TaskSorting,
      statuses: JList[v1.TaskStatus]): Seq[v1.TaskData] = {
    val (indexName, ascending) = sortBy match {
      case v1.TaskSorting.ID =>
        (None, true)
      case v1.TaskSorting.INCREASING_RUNTIME =>
        (Some(TaskIndexNames.EXEC_RUN_TIME), true)
      case v1.TaskSorting.DECREASING_RUNTIME =>
        (Some(TaskIndexNames.EXEC_RUN_TIME), false)
    }
    taskList(stageId, stageAttemptId, offset, length, indexName, ascending, statuses)
  }

  def taskList(
      stageId: Int,
      stageAttemptId: Int,
      offset: Int,
      length: Int,
      sortBy: Option[String],
      ascending: Boolean,
      statuses: JList[v1.TaskStatus] = List().asJava): Seq[v1.TaskData] = {
    val stageKey = Array(stageId, stageAttemptId)
    val base = store.view(classOf[TaskDataWrapper])
    val indexed = sortBy match {
      case Some(index) =>
        base.index(index).parent(stageKey)

      case _ =>
        // Sort by ID, which is the "stage" index.
        base.index("stage").first(stageKey).last(stageKey)
    }

    val ordered = if (ascending) indexed else indexed.reverse()
    val taskDataWrapperSeq = if (statuses != null && !statuses.isEmpty) {
      val statusesStr = statuses.asScala.map(_.toString).toSet
      KVUtils.viewToSeq(ordered, offset, offset + length)(s => statusesStr.contains(s.status))
    } else {
      KVUtils.viewToSeq(ordered.skip(offset).max(length))
    }

    constructTaskDataList(taskDataWrapperSeq)
  }

  def executorSummary(stageId: Int, attemptId: Int): Map[String, v1.ExecutorStageSummary] = {
    val stageKey = Array(stageId, attemptId)
    KVUtils.mapToSeq(store.view(classOf[ExecutorStageSummaryWrapper])
      .index("stage").first(stageKey).last(stageKey)) { exec =>
      (exec.executorId -> exec.info)
    }.toMap
  }

  def speculationSummary(stageId: Int, attemptId: Int): Option[v1.SpeculationStageSummary] = {
    val stageKey = Array(stageId, attemptId)
    asOption(store.read(classOf[SpeculationStageSummaryWrapper], stageKey).info)
  }

  def rddList(cachedOnly: Boolean = true): Seq[v1.RDDStorageInfo] = {
    KVUtils.mapToSeq(store.view(classOf[RDDStorageInfoWrapper]))(_.info)
      .filter { rdd =>
        !cachedOnly || rdd.numCachedPartitions > 0
      }
  }

  /**
   * Calls a closure that may throw a NoSuchElementException and returns `None` when the exception
   * is thrown.
   */
  def asOption[T](fn: => T): Option[T] = {
    try {
      Some(fn)
    } catch {
      case _: NoSuchElementException => None
    }
  }

  def newStageData(
    stage: v1.StageData,
    withDetail: Boolean = false,
    taskStatus: JList[v1.TaskStatus] = List().asJava,
    withSummaries: Boolean = false,
    unsortedQuantiles: Array[Double] = Array.empty[Double]): v1.StageData = {
    if (!withDetail && !withSummaries) {
      stage
    } else {
      val quantiles = unsortedQuantiles.sorted
      val tasks: Option[Map[Long, v1.TaskData]] = if (withDetail) {
        val tasks =
          taskList(stage.stageId, stage.attemptId, 0, Int.MaxValue, None, false, taskStatus)
            .map { t => (t.taskId, t) }
            .toMap
        Some(tasks)
      } else {
        None
      }
      val executorSummaries: Option[Map[String, v1.ExecutorStageSummary]] = if (withDetail) {
        Some(executorSummary(stage.stageId, stage.attemptId))
      } else {
        None
      }
      val taskMetricsDistribution: Option[v1.TaskMetricDistributions] = if (withSummaries) {
        taskSummary(stage.stageId, stage.attemptId, quantiles)
      } else {
        None
      }
      val executorMetricsDistributions: Option[v1.ExecutorMetricsDistributions] =
        if (withSummaries) {
          stageExecutorSummary(stage.stageId, stage.attemptId, quantiles)
        } else {
          None
        }
      val speculationStageSummary: Option[v1.SpeculationStageSummary] = if (withDetail) {
        speculationSummary(stage.stageId, stage.attemptId)
      } else {
        None
      }

      new v1.StageData(
        status = stage.status,
        stageId = stage.stageId,
        attemptId = stage.attemptId,
        numTasks = stage.numTasks,
        numActiveTasks = stage.numActiveTasks,
        numCompleteTasks = stage.numCompleteTasks,
        numFailedTasks = stage.numFailedTasks,
        numKilledTasks = stage.numKilledTasks,
        numCompletedIndices = stage.numCompletedIndices,
        submissionTime = stage.submissionTime,
        firstTaskLaunchedTime = stage.firstTaskLaunchedTime,
        completionTime = stage.completionTime,
        failureReason = stage.failureReason,
        executorDeserializeTime = stage.executorDeserializeTime,
        executorDeserializeCpuTime = stage.executorDeserializeCpuTime,
        executorRunTime = stage.executorRunTime,
        executorCpuTime = stage.executorCpuTime,
        resultSize = stage.resultSize,
        jvmGcTime = stage.jvmGcTime,
        resultSerializationTime = stage.resultSerializationTime,
        memoryBytesSpilled = stage.memoryBytesSpilled,
        diskBytesSpilled = stage.diskBytesSpilled,
        peakExecutionMemory = stage.peakExecutionMemory,
        inputBytes = stage.inputBytes,
        inputRecords = stage.inputRecords,
        outputBytes = stage.outputBytes,
        outputRecords = stage.outputRecords,
        shuffleRemoteBlocksFetched = stage.shuffleRemoteBlocksFetched,
        shuffleLocalBlocksFetched = stage.shuffleLocalBlocksFetched,
        shuffleFetchWaitTime = stage.shuffleFetchWaitTime,
        shuffleRemoteBytesRead = stage.shuffleRemoteBytesRead,
        shuffleRemoteBytesReadToDisk = stage.shuffleRemoteBytesReadToDisk,
        shuffleLocalBytesRead = stage.shuffleLocalBytesRead,
        shuffleReadBytes = stage.shuffleReadBytes,
        shuffleReadRecords = stage.shuffleReadRecords,
        shuffleCorruptMergedBlockChunks = stage.shuffleCorruptMergedBlockChunks,
        shuffleMergedFetchFallbackCount = stage.shuffleMergedFetchFallbackCount,
        shuffleMergedRemoteBlocksFetched = stage.shuffleMergedRemoteBlocksFetched,
        shuffleMergedLocalBlocksFetched = stage.shuffleMergedLocalBlocksFetched,
        shuffleMergedRemoteChunksFetched = stage.shuffleMergedRemoteChunksFetched,
        shuffleMergedLocalChunksFetched = stage.shuffleMergedLocalChunksFetched,
        shuffleMergedRemoteBytesRead = stage.shuffleMergedRemoteBytesRead,
        shuffleMergedLocalBytesRead = stage.shuffleMergedLocalBytesRead,
        shuffleRemoteReqsDuration = stage.shuffleRemoteReqsDuration,
        shuffleMergedRemoteReqsDuration = stage.shuffleMergedRemoteReqsDuration,
        shuffleWriteBytes = stage.shuffleWriteBytes,
        shuffleWriteTime = stage.shuffleWriteTime,
        shuffleWriteRecords = stage.shuffleWriteRecords,
        name = stage.name,
        description = stage.description,
        details = stage.details,
        schedulingPool = stage.schedulingPool,
        rddIds = stage.rddIds,
        accumulatorUpdates = stage.accumulatorUpdates,
        tasks = tasks,
        executorSummary = executorSummaries,
        speculationSummary = speculationStageSummary,
        killedTasksSummary = stage.killedTasksSummary,
        resourceProfileId = stage.resourceProfileId,
        peakExecutorMetrics = stage.peakExecutorMetrics,
        taskMetricsDistributions = taskMetricsDistribution,
        executorMetricsDistributions = executorMetricsDistributions,
        isShufflePushEnabled = stage.isShufflePushEnabled,
        shuffleMergersCount = stage.shuffleMergersCount)
    }
  }

  def stageExecutorSummary(
    stageId: Int,
    stageAttemptId: Int,
    unsortedQuantiles: Array[Double]): Option[v1.ExecutorMetricsDistributions] = {
    val quantiles = unsortedQuantiles.sorted
    val summary = executorSummary(stageId, stageAttemptId)
    if (summary.isEmpty) {
      None
    } else {
      val values = summary.values.toIndexedSeq
      Some(new v1.ExecutorMetricsDistributions(
        quantiles = quantiles.toImmutableArraySeq,
        taskTime = getQuantilesValue(values.map(_.taskTime.toDouble).sorted, quantiles),
        failedTasks = getQuantilesValue(values.map(_.failedTasks.toDouble).sorted, quantiles),
        succeededTasks = getQuantilesValue(values.map(_.succeededTasks.toDouble).sorted, quantiles),
        killedTasks = getQuantilesValue(values.map(_.killedTasks.toDouble).sorted, quantiles),
        inputBytes = getQuantilesValue(values.map(_.inputBytes.toDouble).sorted, quantiles),
        inputRecords = getQuantilesValue(values.map(_.inputRecords.toDouble).sorted, quantiles),
        outputBytes = getQuantilesValue(values.map(_.outputBytes.toDouble).sorted, quantiles),
        outputRecords = getQuantilesValue(values.map(_.outputRecords.toDouble).sorted, quantiles),
        shuffleRead = getQuantilesValue(values.map(_.shuffleRead.toDouble).sorted, quantiles),
        shuffleReadRecords =
          getQuantilesValue(values.map(_.shuffleReadRecords.toDouble).sorted, quantiles),
        shuffleWrite = getQuantilesValue(values.map(_.shuffleWrite.toDouble).sorted, quantiles),
        shuffleWriteRecords =
          getQuantilesValue(values.map(_.shuffleWriteRecords.toDouble).sorted, quantiles),
        memoryBytesSpilled =
          getQuantilesValue(values.map(_.memoryBytesSpilled.toDouble).sorted, quantiles),
        diskBytesSpilled =
          getQuantilesValue(values.map(_.diskBytesSpilled.toDouble).sorted, quantiles),
        peakMemoryMetrics =
          new v1.ExecutorPeakMetricsDistributions(quantiles.toImmutableArraySeq,
            values.flatMap(_.peakMemoryMetrics))
      ))
    }
  }

  def rdd(rddId: Int): v1.RDDStorageInfo = {
    store.read(classOf[RDDStorageInfoWrapper], rddId).info
  }

  def streamBlocksList(): Seq[StreamBlockData] = {
    KVUtils.viewToSeq(store.view(classOf[StreamBlockData]))
  }

  def operationGraphForStage(stageId: Int): RDDOperationGraph = {
    store.read(classOf[RDDOperationGraphWrapper], stageId).toRDDOperationGraph()
  }

  def operationGraphForJob(jobId: Int): collection.Seq[RDDOperationGraph] = {
    val job = store.read(classOf[JobDataWrapper], jobId)
    val stages = job.info.stageIds.sorted

    stages.map { id =>
      val g = store.read(classOf[RDDOperationGraphWrapper], id).toRDDOperationGraph()
      if (job.skippedStages.contains(id) && !g.rootCluster.name.contains("skipped")) {
        g.rootCluster.setName(g.rootCluster.name + " (skipped)")
      }
      g
    }
  }

  def pool(name: String): PoolData = {
    store.read(classOf[PoolData], name)
  }

  def appSummary(): AppSummary = {
    try {
      store.read(classOf[AppSummary], classOf[AppSummary].getName())
    } catch {
      case _: NoSuchElementException =>
        throw new NoSuchElementException("Failed to get the application summary. " +
          "If you are starting up Spark, please wait a while until it's ready.")
    }
  }

  def close(): Unit = {
    store.close()
    cleanUpStorePath()
  }

  private def cleanUpStorePath(): Unit = {
    storePath.foreach(Utils.deleteRecursively)
  }

  def constructTaskDataList(taskDataWrapperIter: Iterable[TaskDataWrapper]): Seq[v1.TaskData] = {
    val executorIdToLogs = new HashMap[String, Map[String, String]]()
    taskDataWrapperIter.map { taskDataWrapper =>
      val taskDataOld: v1.TaskData = taskDataWrapper.toApi
      val executorLogs = executorIdToLogs.getOrElseUpdate(taskDataOld.executorId, {
        try {
          executorSummary(taskDataOld.executorId).executorLogs
        } catch {
          case e: NoSuchElementException =>
            Map.empty
        }
      })

      new v1.TaskData(taskDataOld.taskId, taskDataOld.index,
        taskDataOld.attempt, taskDataOld.partitionId,
        taskDataOld.launchTime, taskDataOld.resultFetchStart,
        taskDataOld.duration, taskDataOld.executorId, taskDataOld.host, taskDataOld.status,
        taskDataOld.taskLocality, taskDataOld.speculative, taskDataOld.accumulatorUpdates,
        taskDataOld.errorMessage, taskDataOld.taskMetrics,
        executorLogs,
        AppStatusUtils.schedulerDelay(taskDataOld),
        AppStatusUtils.gettingResultTime(taskDataOld))
    }.toSeq
  }
}

private[spark] object AppStatusStore extends Logging {

  val CURRENT_VERSION = 2L

  /**
   * Create an in-memory store for a live application.
   */
  def createLiveStore(
      conf: SparkConf,
      appStatusSource: Option[AppStatusSource] = None): AppStatusStore = {

    def createStorePath(rootDir: String): Option[File] = {
      try {
        val localDir = Utils.createDirectory(rootDir, "spark-ui")
        logInfo(log"Created spark ui store directory at ${MDC(PATH, rootDir)}")
        Some(localDir)
      } catch {
        case e: IOException =>
          logError(log"Failed to create spark ui store path in ${MDC(PATH, rootDir)}.", e)
          None
      }
    }

    val storePath =
      conf.get(LIVE_UI_LOCAL_STORE_DIR)
        .orElse(sys.env.get("LIVE_UI_LOCAL_STORE_DIR")) // the ENV variable is for testing purpose
        .flatMap(createStorePath)
    val kvStore = KVUtils.createKVStore(storePath, live = true, conf)
    val store = new ElementTrackingStore(kvStore, conf)
    val listener = new AppStatusListener(store, conf, true, appStatusSource)
    new AppStatusStore(store, listener = Some(listener), storePath)
  }
}
