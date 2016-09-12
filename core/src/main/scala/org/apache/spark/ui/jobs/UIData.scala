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

package org.apache.spark.ui.jobs

import scala.collection.mutable
import scala.collection.mutable.{HashMap, LinkedHashMap}

import org.apache.spark.JobExecutionStatus
import org.apache.spark.executor.{ShuffleReadMetrics, ShuffleWriteMetrics, TaskMetrics}
import org.apache.spark.scheduler.{AccumulableInfo, TaskInfo}
import org.apache.spark.util.AccumulatorContext
import org.apache.spark.util.collection.OpenHashSet

private[spark] object UIData {

  class ExecutorSummary {
    var taskTime : Long = 0
    var failedTasks : Int = 0
    var succeededTasks : Int = 0
    var inputBytes : Long = 0
    var inputRecords : Long = 0
    var outputBytes : Long = 0
    var outputRecords : Long = 0
    var shuffleRead : Long = 0
    var shuffleReadRecords : Long = 0
    var shuffleWrite : Long = 0
    var shuffleWriteRecords : Long = 0
    var memoryBytesSpilled : Long = 0
    var diskBytesSpilled : Long = 0
  }

  class JobUIData(
    var jobId: Int = -1,
    var submissionTime: Option[Long] = None,
    var completionTime: Option[Long] = None,
    var stageIds: Seq[Int] = Seq.empty,
    var jobGroup: Option[String] = None,
    var status: JobExecutionStatus = JobExecutionStatus.UNKNOWN,
    /* Tasks */
    // `numTasks` is a potential underestimate of the true number of tasks that this job will run.
    // This may be an underestimate because the job start event references all of the result
    // stages' transitive stage dependencies, but some of these stages might be skipped if their
    // output is available from earlier runs.
    // See https://github.com/apache/spark/pull/3009 for a more extensive discussion.
    var numTasks: Int = 0,
    var numActiveTasks: Int = 0,
    var numCompletedTasks: Int = 0,
    var numSkippedTasks: Int = 0,
    var numFailedTasks: Int = 0,
    /* Stages */
    var numActiveStages: Int = 0,
    // This needs to be a set instead of a simple count to prevent double-counting of rerun stages:
    var completedStageIndices: mutable.HashSet[Int] = new mutable.HashSet[Int](),
    var numSkippedStages: Int = 0,
    var numFailedStages: Int = 0
  )

  class StageUIData {
    var numActiveTasks: Int = _
    var numCompleteTasks: Int = _
    var completedIndices = new OpenHashSet[Int]()
    var numFailedTasks: Int = _

    var executorRunTime: Long = _

    var inputBytes: Long = _
    var inputRecords: Long = _
    var outputBytes: Long = _
    var outputRecords: Long = _
    var shuffleReadTotalBytes: Long = _
    var shuffleReadRecords : Long = _
    var shuffleWriteBytes: Long = _
    var shuffleWriteRecords: Long = _
    var memoryBytesSpilled: Long = _
    var diskBytesSpilled: Long = _

    var schedulingPool: String = ""
    var description: Option[String] = None

    var accumulables = new HashMap[Long, AccumulableInfo]
    var taskData = new LinkedHashMap[Long, TaskUIData]
    var executorSummary = new HashMap[String, ExecutorSummary]

    def hasInput: Boolean = inputBytes > 0
    def hasOutput: Boolean = outputBytes > 0
    def hasShuffleRead: Boolean = shuffleReadTotalBytes > 0
    def hasShuffleWrite: Boolean = shuffleWriteBytes > 0
    def hasBytesSpilled: Boolean = memoryBytesSpilled > 0 && diskBytesSpilled > 0
  }

  /**
   * These are kept mutable and reused throughout a task's lifetime to avoid excessive reallocation.
   */
  class TaskUIData private(
      private var _taskInfo: TaskInfo,
      private var _metrics: Option[TaskMetricsUIData]) {

    var errorMessage: Option[String] = None

    def taskInfo: TaskInfo = _taskInfo

    def metrics: Option[TaskMetricsUIData] = _metrics

    def updateTaskInfo(taskInfo: TaskInfo): Unit = {
      _taskInfo = TaskUIData.dropInternalAndSQLAccumulables(taskInfo)
    }

    def updateTaskMetrics(metrics: Option[TaskMetrics]): Unit = {
      _metrics = TaskUIData.toTaskMetricsUIData(metrics)
    }
  }

  object TaskUIData {
    def apply(taskInfo: TaskInfo, metrics: Option[TaskMetrics]): TaskUIData = {
      new TaskUIData(dropInternalAndSQLAccumulables(taskInfo), toTaskMetricsUIData(metrics))
    }

    private def toTaskMetricsUIData(metrics: Option[TaskMetrics]): Option[TaskMetricsUIData] = {
      metrics.map { m =>
        TaskMetricsUIData(
          executorDeserializeTime = m.executorDeserializeTime,
          executorRunTime = m.executorRunTime,
          resultSize = m.resultSize,
          jvmGCTime = m.jvmGCTime,
          resultSerializationTime = m.resultSerializationTime,
          memoryBytesSpilled = m.memoryBytesSpilled,
          diskBytesSpilled = m.diskBytesSpilled,
          peakExecutionMemory = m.peakExecutionMemory,
          inputMetrics = InputMetricsUIData(m.inputMetrics.bytesRead, m.inputMetrics.recordsRead),
          outputMetrics =
            OutputMetricsUIData(m.outputMetrics.bytesWritten, m.outputMetrics.recordsWritten),
          shuffleReadMetrics = ShuffleReadMetricsUIData(m.shuffleReadMetrics),
          shuffleWriteMetrics = ShuffleWriteMetricsUIData(m.shuffleWriteMetrics))
      }
    }

    /**
     * We don't need to store internal or SQL accumulables as their values will be shown in other
     * places, so drop them to reduce the memory usage.
     */
    private[spark] def dropInternalAndSQLAccumulables(taskInfo: TaskInfo): TaskInfo = {
      val newTaskInfo = new TaskInfo(
        taskId = taskInfo.taskId,
        index = taskInfo.index,
        attemptNumber = taskInfo.attemptNumber,
        launchTime = taskInfo.launchTime,
        executorId = taskInfo.executorId,
        host = taskInfo.host,
        taskLocality = taskInfo.taskLocality,
        speculative = taskInfo.speculative
      )
      newTaskInfo.gettingResultTime = taskInfo.gettingResultTime
      newTaskInfo.accumulables ++= taskInfo.accumulables.filter {
        accum => !accum.internal && accum.metadata != Some(AccumulatorContext.SQL_ACCUM_IDENTIFIER)
      }
      newTaskInfo.finishTime = taskInfo.finishTime
      newTaskInfo.failed = taskInfo.failed
      newTaskInfo
    }
  }

  class ExecutorUIData(
      val startTime: Long,
      var finishTime: Option[Long] = None,
      var finishReason: Option[String] = None)

  case class TaskMetricsUIData(
      executorDeserializeTime: Long,
      executorRunTime: Long,
      resultSize: Long,
      jvmGCTime: Long,
      resultSerializationTime: Long,
      memoryBytesSpilled: Long,
      diskBytesSpilled: Long,
      peakExecutionMemory: Long,
      inputMetrics: InputMetricsUIData,
      outputMetrics: OutputMetricsUIData,
      shuffleReadMetrics: ShuffleReadMetricsUIData,
      shuffleWriteMetrics: ShuffleWriteMetricsUIData)

  case class InputMetricsUIData(bytesRead: Long, recordsRead: Long)

  case class OutputMetricsUIData(bytesWritten: Long, recordsWritten: Long)

  case class ShuffleReadMetricsUIData(
      remoteBlocksFetched: Long,
      localBlocksFetched: Long,
      remoteBytesRead: Long,
      localBytesRead: Long,
      fetchWaitTime: Long,
      recordsRead: Long,
      totalBytesRead: Long,
      totalBlocksFetched: Long)

  object ShuffleReadMetricsUIData {
    def apply(metrics: ShuffleReadMetrics): ShuffleReadMetricsUIData = {
      new ShuffleReadMetricsUIData(
        remoteBlocksFetched = metrics.remoteBlocksFetched,
        localBlocksFetched = metrics.localBlocksFetched,
        remoteBytesRead = metrics.remoteBytesRead,
        localBytesRead = metrics.localBytesRead,
        fetchWaitTime = metrics.fetchWaitTime,
        recordsRead = metrics.recordsRead,
        totalBytesRead = metrics.totalBytesRead,
        totalBlocksFetched = metrics.totalBlocksFetched
      )
    }
  }

  case class ShuffleWriteMetricsUIData(
      bytesWritten: Long,
      recordsWritten: Long,
      writeTime: Long)

  object ShuffleWriteMetricsUIData {
    def apply(metrics: ShuffleWriteMetrics): ShuffleWriteMetricsUIData = {
      new ShuffleWriteMetricsUIData(
        bytesWritten = metrics.bytesWritten,
        recordsWritten = metrics.recordsWritten,
        writeTime = metrics.writeTime
      )
    }
  }

}
