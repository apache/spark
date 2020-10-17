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

import java.util.{List => JList}

import scala.collection.JavaConverters._

import org.apache.spark.{JobExecutionStatus, SparkConf, SparkException}
import org.apache.spark.status.api.v1
import org.apache.spark.ui.scope._
import org.apache.spark.util.{Distribution, Utils}
import org.apache.spark.util.kvstore.{InMemoryStore, KVStore}

/**
 * A wrapper around a KVStore that provides methods for accessing the API data stored within.
 */
private[spark] class AppStatusStore(
    val store: KVStore,
    val listener: Option[AppStatusListener] = None) {

  def applicationInfo(): v1.ApplicationInfo = {
    try {
      // The ApplicationInfo may not be available when Spark is starting up.
      store.view(classOf[ApplicationInfoWrapper]).max(1).iterator().next().info
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

  def jobsList(statuses: JList[JobExecutionStatus]): Seq[v1.JobData] = {
    val it = store.view(classOf[JobDataWrapper]).reverse().asScala.map(_.info)
    if (statuses != null && !statuses.isEmpty()) {
      it.filter { job => statuses.contains(job.status) }.toSeq
    } else {
      it.toSeq
    }
  }

  def job(jobId: Int): v1.JobData = {
    store.read(classOf[JobDataWrapper], jobId).info
  }

  def executorList(activeOnly: Boolean): Seq[v1.ExecutorSummary] = {
    val base = store.view(classOf[ExecutorSummaryWrapper])
    val filtered = if (activeOnly) {
      base.index("active").reverse().first(true).last(true)
    } else {
      base
    }
    filtered.asScala.map(_.info).toSeq
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

  def stageList(statuses: JList[v1.StageStatus]): Seq[v1.StageData] = {
    val it = store.view(classOf[StageDataWrapper]).reverse().asScala.map(_.info)
    if (statuses != null && !statuses.isEmpty()) {
      it.filter { s => statuses.contains(s.status) }.toSeq
    } else {
      it.toSeq
    }
  }

  def stageData(stageId: Int, details: Boolean = false): Seq[v1.StageData] = {
    store.view(classOf[StageDataWrapper]).index("stageId").first(stageId).last(stageId)
      .asScala.map { s =>
        if (details) stageWithDetails(s.info) else s.info
      }.toSeq
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

  def stageAttempt(stageId: Int, stageAttemptId: Int, details: Boolean = false): v1.StageData = {
    val stageKey = Array(stageId, stageAttemptId)
    val stage = store.read(classOf[StageDataWrapper], stageKey).info
    if (details) stageWithDetails(stage) else stage
  }

  def taskCount(stageId: Int, stageAttemptId: Int): Long = {
    store.count(classOf[TaskDataWrapper], "stage", Array(stageId, stageAttemptId))
  }

  def localitySummary(stageId: Int, stageAttemptId: Int): Map[String, Long] = {
    store.read(classOf[StageDataWrapper], Array(stageId, stageAttemptId)).locality
  }

  // SPARK-26119: we only want to consider successful tasks when calculating the metrics summary,
  // but currently this is very expensive when using a disk store. So we only trigger the slower
  // code path when we know we have all data in memory. The following method checks whether all
  // the data will be in memory.
  private def isInMemoryStore: Boolean = store.isInstanceOf[InMemoryStore] || listener.isDefined

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
    val quantiles = unsortedQuantiles.sorted

    // We don't know how many tasks remain in the store that actually have metrics. So scan one
    // metric and count how many valid tasks there are. Use skip() instead of next() since it's
    // cheaper for disk stores (avoids deserialization).
    val count = {
      Utils.tryWithResource(
        if (isInMemoryStore) {
          // For Live UI, we should count the tasks with status "SUCCESS" only.
          store.view(classOf[TaskDataWrapper])
            .parent(stageKey)
            .index(TaskIndexNames.STATUS)
            .first("SUCCESS")
            .last("SUCCESS")
            .closeableIterator()
        } else {
          store.view(classOf[TaskDataWrapper])
            .parent(stageKey)
            .index(TaskIndexNames.EXEC_RUN_TIME)
            .first(0L)
            .closeableIterator()
        }
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
          toValues(_.shuffleTotalBlocksFetched)),
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

    // TODO: Summary metrics needs to display all the successful tasks' metrics (SPARK-26119).
    // For InMemory case, it is efficient to find using the following code. But for diskStore case
    // we need an efficient solution to avoid deserialization time overhead. For that, we need to
    // rework on the way indexing works, so that we can index by specific metrics for successful
    // and failed tasks differently (would be tricky). Also would require changing the disk store
    // version (to invalidate old stores).
    def scanTasks(index: String)(fn: TaskDataWrapper => Long): IndexedSeq[Double] = {
      if (isInMemoryStore) {
        val quantileTasks = store.view(classOf[TaskDataWrapper])
          .parent(stageKey)
          .index(index)
          .first(0L)
          .asScala
          .filter { _.status == "SUCCESS"} // Filter "SUCCESS" tasks
          .toIndexedSeq

        indices.map { index =>
          fn(quantileTasks(index.toInt)).toDouble
        }.toIndexedSeq
      } else {
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
          }.toIndexedSeq
        }
      }
    }

    val computedQuantiles = new v1.TaskMetricDistributions(
      quantiles = quantiles,
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
        scanTasks(TaskIndexNames.SHUFFLE_READ_TIME) { t => t.shuffleFetchWaitTime },
        scanTasks(TaskIndexNames.SHUFFLE_REMOTE_READS) { t => t.shuffleRemoteBytesRead },
        scanTasks(TaskIndexNames.SHUFFLE_REMOTE_READS_TO_DISK) { t =>
          t.shuffleRemoteBytesReadToDisk
        },
        scanTasks(TaskIndexNames.SHUFFLE_TOTAL_BLOCKS) { m =>
          m.shuffleLocalBlocksFetched + m.shuffleRemoteBlocksFetched
        }),
      shuffleWriteMetrics = new v1.ShuffleWriteMetricDistributions(
        scanTasks(TaskIndexNames.SHUFFLE_WRITE_SIZE) { t => t.shuffleBytesWritten },
        scanTasks(TaskIndexNames.SHUFFLE_WRITE_RECORDS) { t => t.shuffleRecordsWritten },
        scanTasks(TaskIndexNames.SHUFFLE_WRITE_TIME) { t => t.shuffleWriteTime }))

    // Go through the computed quantiles and cache the values that match the caching criteria.
    computedQuantiles.quantiles.zipWithIndex
      .filter { case (q, _) => quantiles.contains(q) && shouldCacheQuantile(q) }
      .foreach { case (q, idx) =>
        val cached = new CachedQuantile(stageId, stageAttemptId, quantileToString(q), count,
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
    store.view(classOf[TaskDataWrapper]).index("stage").first(stageKey).last(stageKey).reverse()
      .max(maxTasks).asScala.map(_.toApi).toSeq.reverse
  }

  def taskList(
      stageId: Int,
      stageAttemptId: Int,
      offset: Int,
      length: Int,
      sortBy: v1.TaskSorting): Seq[v1.TaskData] = {
    val (indexName, ascending) = sortBy match {
      case v1.TaskSorting.ID =>
        (None, true)
      case v1.TaskSorting.INCREASING_RUNTIME =>
        (Some(TaskIndexNames.EXEC_RUN_TIME), true)
      case v1.TaskSorting.DECREASING_RUNTIME =>
        (Some(TaskIndexNames.EXEC_RUN_TIME), false)
    }
    taskList(stageId, stageAttemptId, offset, length, indexName, ascending)
  }

  def taskList(
      stageId: Int,
      stageAttemptId: Int,
      offset: Int,
      length: Int,
      sortBy: Option[String],
      ascending: Boolean): Seq[v1.TaskData] = {
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
    ordered.skip(offset).max(length).asScala.map(_.toApi).toSeq
  }

  def executorSummary(stageId: Int, attemptId: Int): Map[String, v1.ExecutorStageSummary] = {
    val stageKey = Array(stageId, attemptId)
    store.view(classOf[ExecutorStageSummaryWrapper]).index("stage").first(stageKey).last(stageKey)
      .asScala.map { exec => (exec.executorId -> exec.info) }.toMap
  }

  def rddList(cachedOnly: Boolean = true): Seq[v1.RDDStorageInfo] = {
    store.view(classOf[RDDStorageInfoWrapper]).asScala.map(_.info).filter { rdd =>
      !cachedOnly || rdd.numCachedPartitions > 0
    }.toSeq
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

  private def stageWithDetails(stage: v1.StageData): v1.StageData = {
    val tasks = taskList(stage.stageId, stage.attemptId, Int.MaxValue)
      .map { t => (t.taskId, t) }
      .toMap

    new v1.StageData(
      stage.status,
      stage.stageId,
      stage.attemptId,
      stage.numTasks,
      stage.numActiveTasks,
      stage.numCompleteTasks,
      stage.numFailedTasks,
      stage.numKilledTasks,
      stage.numCompletedIndices,
      stage.executorRunTime,
      stage.executorCpuTime,
      stage.submissionTime,
      stage.firstTaskLaunchedTime,
      stage.completionTime,
      stage.failureReason,
      stage.inputBytes,
      stage.inputRecords,
      stage.outputBytes,
      stage.outputRecords,
      stage.shuffleReadBytes,
      stage.shuffleReadRecords,
      stage.shuffleWriteBytes,
      stage.shuffleWriteRecords,
      stage.memoryBytesSpilled,
      stage.diskBytesSpilled,
      stage.name,
      stage.description,
      stage.details,
      stage.schedulingPool,
      stage.rddIds,
      stage.accumulatorUpdates,
      Some(tasks),
      Some(executorSummary(stage.stageId, stage.attemptId)),
      stage.killedTasksSummary)
  }

  def rdd(rddId: Int): v1.RDDStorageInfo = {
    store.read(classOf[RDDStorageInfoWrapper], rddId).info
  }

  def streamBlocksList(): Seq[StreamBlockData] = {
    store.view(classOf[StreamBlockData]).asScala.toSeq
  }

  def operationGraphForStage(stageId: Int): RDDOperationGraph = {
    store.read(classOf[RDDOperationGraphWrapper], stageId).toRDDOperationGraph()
  }

  def operationGraphForJob(jobId: Int): Seq[RDDOperationGraph] = {
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
  }

}

private[spark] object AppStatusStore {

  val CURRENT_VERSION = 1L

  /**
   * Create an in-memory store for a live application.
   */
  def createLiveStore(conf: SparkConf): AppStatusStore = {
    val store = new ElementTrackingStore(new InMemoryStore(), conf)
    val listener = new AppStatusListener(store, conf, true)
    new AppStatusStore(store, listener = Some(listener))
  }

}
