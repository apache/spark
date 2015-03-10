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
import javax.ws.rs.{QueryParam, PathParam, GET, Produces}
import javax.ws.rs.core.MediaType

import org.apache.spark.executor.{
TaskMetrics => InternalTaskMetrics,
InputMetrics => InternalInputMetrics,
OutputMetrics => InternalOutputMetrics,
ShuffleReadMetrics => InternalShuffleReadMetrics,
ShuffleWriteMetrics => InternalShuffleWriteMetrics
}
import org.apache.spark.scheduler.StageInfo
import org.apache.spark.status.api._
import org.apache.spark.ui.SparkUI
import org.apache.spark.ui.jobs.UIData.{TaskUIData, StageUIData}

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
      Some(stageUiData.taskData.map{case(k,v) => k -> convertTaskData(v)})
    } else {
      None
    }
    val executorSummary = if(includeDetails) {
      Some(stageUiData.executorSummary.map{case(k,summary) => k ->
        ExecutorStageSummary(
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
    StageData(
      status = status,
      stageId = stageInfo.stageId,
      numActiveTasks = stageUiData.numActiveTasks,
      numCompleteTasks = stageUiData.numCompleteTasks,
      numFailedTasks = stageUiData.numFailedTasks,
      executorRunTime = stageUiData.executorRunTime,
      inputBytes = stageUiData.inputBytes,
      inputRecords = 0, //fix after SPARK-4874
      outputBytes = stageUiData.outputBytes,
      outputRecords = 0, //fix after SPARK-4874
      shuffleReadBytes = stageUiData.shuffleReadTotalBytes,
      shuffleReadRecords = 0, //fix after SPARK-4874
      shuffleWriteBytes = stageUiData.shuffleWriteBytes,
      shuffleWriteRecords = 0, //fix after SPARK-4874
      memoryBytesSpilled = stageUiData.memoryBytesSpilled,
      diskBytesSpilled = stageUiData.diskBytesSpilled,
      schedulingPool = stageUiData.schedulingPool,
      name = stageInfo.name,
      details = stageInfo.details,
      tasks = taskData,
      executorSummary = executorSummary
    )
  }

  def stagesAndStatus(ui: SparkUI): Seq[(StageStatus, Seq[StageInfo])] = {
    val listener = ui.stagesTab.listener
    listener.synchronized{
      Seq(
        StageStatus.Active -> listener.activeStages.values.toSeq,
        StageStatus.Complete -> listener.completedStages.reverse.toSeq,
        StageStatus.Failed -> listener.failedStages.reverse.toSeq,
        StageStatus.Pending -> listener.pendingStages.values.toSeq
      )
    }
  }


  def convertTaskData(uiData: TaskUIData): TaskData = {
    TaskData(
      taskId = uiData.taskInfo.taskId,
      index = uiData.taskInfo.index,
      attempt = uiData.taskInfo.attempt,
      launchTime = new Date(uiData.taskInfo.launchTime),
      executorId = uiData.taskInfo.executorId,
      host = uiData.taskInfo.host,
      taskLocality = uiData.taskInfo.taskLocality.toString(),
      speculative = uiData.taskInfo.speculative,
      errorMessage = uiData.errorMessage,
      taskMetrics = uiData.taskMetrics.map{convertUiTaskMetrics}
    )
  }

  def convertUiTaskMetrics(internal: InternalTaskMetrics): TaskMetrics = {
    TaskMetrics(
      executorDeserializeTime = internal.executorDeserializeTime,
      executorRunTime = internal.executorRunTime,
      resultSize = internal.resultSize,
      jvmGcTime = internal.jvmGCTime,
      resultSerializationTime = internal.resultSerializationTime,
      memoryBytesSpilled = internal.memoryBytesSpilled,
      diskBytesSpilled = internal.diskBytesSpilled,
      inputMetrics = internal.inputMetrics.map{convertInputMetrics},
      outputMetrics = Option(internal.outputMetrics).flatten.map{convertOutputMetrics},
      shuffleReadMetrics = internal.shuffleReadMetrics.map{convertShuffleReadMetrics},
      shuffleWriteMetrics = internal.shuffleWriteMetrics.map{convertShuffleWriteMetrics}
    )
  }

  def convertInputMetrics(internal: InternalInputMetrics): InputMetrics = {
    InputMetrics(
      bytesRead = internal.bytesRead,
      recordsRead = 0 //fix after SPARK-4874
    )
  }

  def convertOutputMetrics(internal: InternalOutputMetrics): OutputMetrics = {
    OutputMetrics(
      bytesWritten = internal.bytesWritten,
      recordsWritten = 0  //fix after SPARK-4874
    )
  }

  def convertShuffleReadMetrics(internal: InternalShuffleReadMetrics): ShuffleReadMetrics = {
    ShuffleReadMetrics(
      remoteBlocksFetched = internal.remoteBlocksFetched,
      localBlocksFetched = internal.localBlocksFetched,
      fetchWaitTime = internal.fetchWaitTime,
      remoteBytesRead = internal.remoteBytesRead,
      totalBlocksFetched = internal.totalBlocksFetched,
      recordsRead = 0 //fix after SPARK-4874
    )
  }

  def convertShuffleWriteMetrics(internal: InternalShuffleWriteMetrics): ShuffleWriteMetrics = {
    ShuffleWriteMetrics(
      bytesWritten = internal.shuffleBytesWritten,
      writeTime = internal.shuffleWriteTime,
      recordsWritten = 0  //fix after SPARK-4874
    )
  }

}