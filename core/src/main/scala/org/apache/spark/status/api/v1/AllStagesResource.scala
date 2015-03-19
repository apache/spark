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
package org.apache.spark.status.api.v1

import java.util.Date
import javax.ws.rs.{GET, PathParam, Produces, QueryParam}
import javax.ws.rs.core.MediaType

import org.apache.spark.executor.{InputMetrics => InternalInputMetrics, OutputMetrics => InternalOutputMetrics, ShuffleReadMetrics => InternalShuffleReadMetrics, ShuffleWriteMetrics => InternalShuffleWriteMetrics, TaskMetrics => InternalTaskMetrics}
import org.apache.spark.scheduler.{AccumulableInfo => InternalAccumulableInfo, StageInfo}
import org.apache.spark.status.api._
import org.apache.spark.ui.SparkUI
import org.apache.spark.ui.jobs.UIData.{StageUIData, TaskUIData}

@Produces(Array(MediaType.APPLICATION_JSON))
class AllStagesResource(uiRoot: UIRoot) {

  @GET
  def stageList(
    @PathParam("appId") appId: String,
    @QueryParam("status") statuses: java.util.List[StageStatus]
  ): Seq[StageData] = {
    uiRoot.withSparkUI(appId) { ui =>
      val listener = ui.stagesTab.listener
      val stageAndStatus = AllStagesResource.stagesAndStatus(ui)
      val adjStatuses = {
        if (statuses.isEmpty()) {
          java.util.Arrays.asList(StageStatus.values(): _*)
        } else {
          statuses
        }
      }
      for {
        (status, stageList) <- stageAndStatus
        stageInfo: StageInfo <- stageList if adjStatuses.contains(status)
        stageUiData: StageUIData <- listener.synchronized {
          listener.stageIdToData.get((stageInfo.stageId, stageInfo.attemptId))
        }
      } yield {
        AllStagesResource.stageUiToStageData(status, stageInfo, stageUiData, includeDetails = false)
      }
    }
  }
}

object AllStagesResource {
  def stageUiToStageData(
    status: StageStatus,
    stageInfo: StageInfo,
    stageUiData: StageUIData,
    includeDetails: Boolean
  ): StageData = {

    val taskData = if(includeDetails) {
      Some(stageUiData.taskData.map { case (k, v) => k -> convertTaskData(v) } )
    } else {
      None
    }
    val executorSummary = if(includeDetails) {
      Some(stageUiData.executorSummary.map { case (k, summary) =>
        k -> new ExecutorStageSummary(
          taskTime = summary.taskTime,
          failedTasks = summary.failedTasks,
          succeededTasks = summary.succeededTasks,
          inputBytes = summary.inputBytes,
          outputBytes = summary.outputBytes,
          shuffleRead = summary.shuffleRead,
          shuffleWrite = summary.shuffleWrite,
          memoryBytesSpilled = summary.memoryBytesSpilled,
          diskBytesSpilled = summary.diskBytesSpilled
        )
      })
    } else {
      None
    }

    val accumulableInfo = stageUiData.accumulables.values.map { convertAccumulableInfo }.toSeq

    new StageData(
      status = status,
      stageId = stageInfo.stageId,
      attemptId = stageInfo.attemptId,
      numActiveTasks = stageUiData.numActiveTasks,
      numCompleteTasks = stageUiData.numCompleteTasks,
      numFailedTasks = stageUiData.numFailedTasks,
      executorRunTime = stageUiData.executorRunTime,
      inputBytes = stageUiData.inputBytes,
      inputRecords = stageUiData.inputRecords,
      outputBytes = stageUiData.outputBytes,
      outputRecords = stageUiData.outputRecords,
      shuffleReadBytes = stageUiData.shuffleReadTotalBytes,
      shuffleReadRecords = stageUiData.shuffleReadRecords,
      shuffleWriteBytes = stageUiData.shuffleWriteBytes,
      shuffleWriteRecords = stageUiData.shuffleWriteRecords,
      memoryBytesSpilled = stageUiData.memoryBytesSpilled,
      diskBytesSpilled = stageUiData.diskBytesSpilled,
      schedulingPool = stageUiData.schedulingPool,
      name = stageInfo.name,
      details = stageInfo.details,
      accumulatorUpdates = accumulableInfo,
      tasks = taskData,
      executorSummary = executorSummary
    )
  }

  def stagesAndStatus(ui: SparkUI): Seq[(StageStatus, Seq[StageInfo])] = {
    val listener = ui.stagesTab.listener
    listener.synchronized {
      Seq(
        StageStatus.Active -> listener.activeStages.values.toSeq,
        StageStatus.Complete -> listener.completedStages.reverse.toSeq,
        StageStatus.Failed -> listener.failedStages.reverse.toSeq,
        StageStatus.Pending -> listener.pendingStages.values.toSeq
      )
    }
  }


  def convertTaskData(uiData: TaskUIData): TaskData = {
    new TaskData(
      taskId = uiData.taskInfo.taskId,
      index = uiData.taskInfo.index,
      attempt = uiData.taskInfo.attempt,
      launchTime = new Date(uiData.taskInfo.launchTime),
      executorId = uiData.taskInfo.executorId,
      host = uiData.taskInfo.host,
      taskLocality = uiData.taskInfo.taskLocality.toString(),
      speculative = uiData.taskInfo.speculative,
      accumulatorUpdates = uiData.taskInfo.accumulables.map { convertAccumulableInfo },
      errorMessage = uiData.errorMessage,
      taskMetrics = uiData.taskMetrics.map { convertUiTaskMetrics }
    )
  }

  def convertAccumulableInfo(acc: InternalAccumulableInfo): AccumulableInfo = {
    new AccumulableInfo(acc.id, acc.name, acc.update, acc.value)
  }


  def convertUiTaskMetrics(internal: InternalTaskMetrics): TaskMetrics = {
    new TaskMetrics(
      executorDeserializeTime = internal.executorDeserializeTime,
      executorRunTime = internal.executorRunTime,
      resultSize = internal.resultSize,
      jvmGcTime = internal.jvmGCTime,
      resultSerializationTime = internal.resultSerializationTime,
      memoryBytesSpilled = internal.memoryBytesSpilled,
      diskBytesSpilled = internal.diskBytesSpilled,
      inputMetrics = internal.inputMetrics.map { convertInputMetrics },
      outputMetrics = Option(internal.outputMetrics).flatten.map { convertOutputMetrics },
      shuffleReadMetrics = internal.shuffleReadMetrics.map { convertShuffleReadMetrics },
      shuffleWriteMetrics = internal.shuffleWriteMetrics.map { convertShuffleWriteMetrics }
    )
  }

  def convertInputMetrics(internal: InternalInputMetrics): InputMetrics = {
    new InputMetrics(
      bytesRead = internal.bytesRead,
      recordsRead = internal.recordsRead
    )
  }

  def convertOutputMetrics(internal: InternalOutputMetrics): OutputMetrics = {
    new OutputMetrics(
      bytesWritten = internal.bytesWritten,
      recordsWritten = internal.recordsWritten
    )
  }

  def convertShuffleReadMetrics(internal: InternalShuffleReadMetrics): ShuffleReadMetrics = {
    new ShuffleReadMetrics(
      remoteBlocksFetched = internal.remoteBlocksFetched,
      localBlocksFetched = internal.localBlocksFetched,
      fetchWaitTime = internal.fetchWaitTime,
      remoteBytesRead = internal.remoteBytesRead,
      totalBlocksFetched = internal.totalBlocksFetched,
      recordsRead = internal.recordsRead
    )
  }

  def convertShuffleWriteMetrics(internal: InternalShuffleWriteMetrics): ShuffleWriteMetrics = {
    new ShuffleWriteMetrics(
      bytesWritten = internal.shuffleBytesWritten,
      writeTime = internal.shuffleWriteTime,
      recordsWritten = internal.shuffleRecordsWritten
    )
  }

}
