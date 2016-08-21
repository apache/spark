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

import java.util.{Arrays, Date, List => JList}
import javax.ws.rs.{GET, Produces, QueryParam}
import javax.ws.rs.core.MediaType

import org.apache.spark.scheduler.{AccumulableInfo => InternalAccumulableInfo, StageInfo}
import org.apache.spark.ui.SparkUI
import org.apache.spark.ui.jobs.UIData.{StageUIData, TaskUIData}
import org.apache.spark.ui.jobs.UIData.{InputMetricsUIData => InternalInputMetrics, OutputMetricsUIData => InternalOutputMetrics, ShuffleReadMetricsUIData => InternalShuffleReadMetrics, ShuffleWriteMetricsUIData => InternalShuffleWriteMetrics, TaskMetricsUIData => InternalTaskMetrics}
import org.apache.spark.util.Distribution

@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class AllStagesResource(ui: SparkUI) {

  @GET
  def stageList(@QueryParam("status") statuses: JList[StageStatus]): Seq[StageData] = {
    val listener = ui.jobProgressListener
    val stageAndStatus = AllStagesResource.stagesAndStatus(ui)
    val adjStatuses = {
      if (statuses.isEmpty()) {
        Arrays.asList(StageStatus.values(): _*)
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

private[v1] object AllStagesResource {
  def stageUiToStageData(
      status: StageStatus,
      stageInfo: StageInfo,
      stageUiData: StageUIData,
      includeDetails: Boolean): StageData = {

    val taskLaunchTimes = stageUiData.taskData.values.map(_.taskInfo.launchTime).filter(_ > 0)

    val firstTaskLaunchedTime: Option[Date] =
      if (taskLaunchTimes.nonEmpty) {
        Some(new Date(taskLaunchTimes.min))
      } else {
        None
      }

    val taskData = if (includeDetails) {
      Some(stageUiData.taskData.map { case (k, v) => k -> convertTaskData(v) } )
    } else {
      None
    }
    val executorSummary = if (includeDetails) {
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
      submissionTime = stageInfo.submissionTime.map(new Date(_)),
      firstTaskLaunchedTime,
      completionTime = stageInfo.completionTime.map(new Date(_)),
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
    val listener = ui.jobProgressListener
    listener.synchronized {
      Seq(
        StageStatus.ACTIVE -> listener.activeStages.values.toSeq,
        StageStatus.COMPLETE -> listener.completedStages.reverse.toSeq,
        StageStatus.FAILED -> listener.failedStages.reverse.toSeq,
        StageStatus.PENDING -> listener.pendingStages.values.toSeq
      )
    }
  }

  def convertTaskData(uiData: TaskUIData): TaskData = {
    new TaskData(
      taskId = uiData.taskInfo.taskId,
      index = uiData.taskInfo.index,
      attempt = uiData.taskInfo.attemptNumber,
      launchTime = new Date(uiData.taskInfo.launchTime),
      executorId = uiData.taskInfo.executorId,
      host = uiData.taskInfo.host,
      taskLocality = uiData.taskInfo.taskLocality.toString(),
      speculative = uiData.taskInfo.speculative,
      accumulatorUpdates = uiData.taskInfo.accumulables.map { convertAccumulableInfo },
      errorMessage = uiData.errorMessage,
      taskMetrics = uiData.metrics.map { convertUiTaskMetrics }
    )
  }

  def taskMetricDistributions(
      allTaskData: Iterable[TaskUIData],
      quantiles: Array[Double]): TaskMetricDistributions = {

    val rawMetrics = allTaskData.flatMap{_.metrics}.toSeq

    def metricQuantiles(f: InternalTaskMetrics => Double): IndexedSeq[Double] =
      Distribution(rawMetrics.map { d => f(d) }).get.getQuantiles(quantiles)

    // We need to do a lot of similar munging to nested metrics here.  For each one,
    // we want (a) extract the values for nested metrics (b) make a distribution for each metric
    // (c) shove the distribution into the right field in our return type and (d) only return
    // a result if the option is defined for any of the tasks.  MetricHelper is a little util
    // to make it a little easier to deal w/ all of the nested options.  Mostly it lets us just
    // implement one "build" method, which just builds the quantiles for each field.

    val inputMetrics: InputMetricDistributions =
      new MetricHelper[InternalInputMetrics, InputMetricDistributions](rawMetrics, quantiles) {
        def getSubmetrics(raw: InternalTaskMetrics): InternalInputMetrics = raw.inputMetrics

        def build: InputMetricDistributions = new InputMetricDistributions(
          bytesRead = submetricQuantiles(_.bytesRead),
          recordsRead = submetricQuantiles(_.recordsRead)
        )
      }.build

    val outputMetrics: OutputMetricDistributions =
      new MetricHelper[InternalOutputMetrics, OutputMetricDistributions](rawMetrics, quantiles) {
        def getSubmetrics(raw: InternalTaskMetrics): InternalOutputMetrics = raw.outputMetrics

        def build: OutputMetricDistributions = new OutputMetricDistributions(
          bytesWritten = submetricQuantiles(_.bytesWritten),
          recordsWritten = submetricQuantiles(_.recordsWritten)
        )
      }.build

    val shuffleReadMetrics: ShuffleReadMetricDistributions =
      new MetricHelper[InternalShuffleReadMetrics, ShuffleReadMetricDistributions](rawMetrics,
        quantiles) {
        def getSubmetrics(raw: InternalTaskMetrics): InternalShuffleReadMetrics =
          raw.shuffleReadMetrics

        def build: ShuffleReadMetricDistributions = new ShuffleReadMetricDistributions(
          readBytes = submetricQuantiles(_.totalBytesRead),
          readRecords = submetricQuantiles(_.recordsRead),
          remoteBytesRead = submetricQuantiles(_.remoteBytesRead),
          remoteBlocksFetched = submetricQuantiles(_.remoteBlocksFetched),
          localBlocksFetched = submetricQuantiles(_.localBlocksFetched),
          totalBlocksFetched = submetricQuantiles(_.totalBlocksFetched),
          fetchWaitTime = submetricQuantiles(_.fetchWaitTime)
        )
      }.build

    val shuffleWriteMetrics: ShuffleWriteMetricDistributions =
      new MetricHelper[InternalShuffleWriteMetrics, ShuffleWriteMetricDistributions](rawMetrics,
        quantiles) {
        def getSubmetrics(raw: InternalTaskMetrics): InternalShuffleWriteMetrics =
          raw.shuffleWriteMetrics

        def build: ShuffleWriteMetricDistributions = new ShuffleWriteMetricDistributions(
          writeBytes = submetricQuantiles(_.bytesWritten),
          writeRecords = submetricQuantiles(_.recordsWritten),
          writeTime = submetricQuantiles(_.writeTime)
        )
      }.build

    new TaskMetricDistributions(
      quantiles = quantiles,
      executorDeserializeTime = metricQuantiles(_.executorDeserializeTime),
      executorRunTime = metricQuantiles(_.executorRunTime),
      resultSize = metricQuantiles(_.resultSize),
      jvmGcTime = metricQuantiles(_.jvmGCTime),
      resultSerializationTime = metricQuantiles(_.resultSerializationTime),
      memoryBytesSpilled = metricQuantiles(_.memoryBytesSpilled),
      diskBytesSpilled = metricQuantiles(_.diskBytesSpilled),
      inputMetrics = inputMetrics,
      outputMetrics = outputMetrics,
      shuffleReadMetrics = shuffleReadMetrics,
      shuffleWriteMetrics = shuffleWriteMetrics
    )
  }

  def convertAccumulableInfo(acc: InternalAccumulableInfo): AccumulableInfo = {
    new AccumulableInfo(
      acc.id, acc.name.orNull, acc.update.map(_.toString), acc.value.map(_.toString).orNull)
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
      inputMetrics = convertInputMetrics(internal.inputMetrics),
      outputMetrics = convertOutputMetrics(internal.outputMetrics),
      shuffleReadMetrics = convertShuffleReadMetrics(internal.shuffleReadMetrics),
      shuffleWriteMetrics = convertShuffleWriteMetrics(internal.shuffleWriteMetrics)
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
      localBytesRead = internal.localBytesRead,
      recordsRead = internal.recordsRead
    )
  }

  def convertShuffleWriteMetrics(internal: InternalShuffleWriteMetrics): ShuffleWriteMetrics = {
    new ShuffleWriteMetrics(
      bytesWritten = internal.bytesWritten,
      writeTime = internal.writeTime,
      recordsWritten = internal.recordsWritten
    )
  }
}

/**
 * Helper for getting distributions from nested metric types.
 */
private[v1] abstract class MetricHelper[I, O](
    rawMetrics: Seq[InternalTaskMetrics],
    quantiles: Array[Double]) {

  def getSubmetrics(raw: InternalTaskMetrics): I

  def build: O

  val data: Seq[I] = rawMetrics.map(getSubmetrics)

  /** applies the given function to all input metrics, and returns the quantiles */
  def submetricQuantiles(f: I => Double): IndexedSeq[Double] = {
    Distribution(data.map { d => f(d) }).get.getQuantiles(quantiles)
  }
}
