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
import javax.ws.rs.{GET, PathParam, Produces, QueryParam}
import javax.ws.rs.core.MediaType

import org.apache.spark.executor.{InputMetrics => InternalInputMetrics, OutputMetrics => InternalOutputMetrics, ShuffleReadMetrics => InternalShuffleReadMetrics, ShuffleWriteMetrics => InternalShuffleWriteMetrics, TaskMetrics => InternalTaskMetrics}
import org.apache.spark.scheduler.{AccumulableInfo => InternalAccumulableInfo, StageInfo}
import org.apache.spark.ui.SparkUI
import org.apache.spark.ui.jobs.UIData.{StageUIData, TaskUIData}
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

  def taskMetricDistributions(
      allTaskData: Iterable[TaskUIData],
      quantiles: Array[Double]): TaskMetricDistributions = {

    val rawMetrics = allTaskData.flatMap{_.taskMetrics}.toSeq

    def metricQuantiles(f: InternalTaskMetrics => Double): IndexedSeq[Double] =
      Distribution(rawMetrics.map { d => f(d) }).get.getQuantiles(quantiles)

    // We need to do a lot of similar munging to nested metrics here.  For each one,
    // we want (a) extract the values for nested metrics (b) make a distribution for each metric
    // (c) shove the distribution into the right field in our return type and (d) only return
    // a result if the option is defined for any of the tasks.  MetricHelper is a little util
    // to make it a little easier to deal w/ all of the nested options.  Mostly it lets us just
    // implement one "build" method, which just builds the quantiles for each field.

    val inputMetrics: Option[InputMetricDistributions] =
      new MetricHelper[InternalInputMetrics, InputMetricDistributions](rawMetrics, quantiles) {
        def getSubmetrics(raw: InternalTaskMetrics): Option[InternalInputMetrics] = {
          raw.inputMetrics
        }

        def build: InputMetricDistributions = new InputMetricDistributions(
          bytesRead = submetricQuantiles(_.bytesRead),
          recordsRead = submetricQuantiles(_.recordsRead)
        )
      }.metricOption

    val outputMetrics: Option[OutputMetricDistributions] =
      new MetricHelper[InternalOutputMetrics, OutputMetricDistributions](rawMetrics, quantiles) {
        def getSubmetrics(raw:InternalTaskMetrics): Option[InternalOutputMetrics] = {
          raw.outputMetrics
        }
        def build: OutputMetricDistributions = new OutputMetricDistributions(
          bytesWritten = submetricQuantiles(_.bytesWritten),
          recordsWritten = submetricQuantiles(_.recordsWritten)
        )
      }.metricOption

    val shuffleReadMetrics: Option[ShuffleReadMetricDistributions] =
      new MetricHelper[InternalShuffleReadMetrics, ShuffleReadMetricDistributions](rawMetrics,
        quantiles) {
        def getSubmetrics(raw: InternalTaskMetrics): Option[InternalShuffleReadMetrics] = {
          raw.shuffleReadMetrics
        }
        def build: ShuffleReadMetricDistributions = new ShuffleReadMetricDistributions(
          readBytes = submetricQuantiles(_.totalBytesRead),
          readRecords = submetricQuantiles(_.recordsRead),
          remoteBytesRead = submetricQuantiles(_.remoteBytesRead),
          remoteBlocksFetched = submetricQuantiles(_.remoteBlocksFetched),
          localBlocksFetched = submetricQuantiles(_.localBlocksFetched),
          totalBlocksFetched = submetricQuantiles(_.totalBlocksFetched),
          fetchWaitTime = submetricQuantiles(_.fetchWaitTime)
        )
      }.metricOption

    val shuffleWriteMetrics: Option[ShuffleWriteMetricDistributions] =
      new MetricHelper[InternalShuffleWriteMetrics, ShuffleWriteMetricDistributions](rawMetrics,
        quantiles) {
        def getSubmetrics(raw: InternalTaskMetrics): Option[InternalShuffleWriteMetrics] = {
          raw.shuffleWriteMetrics
        }
        def build: ShuffleWriteMetricDistributions = new ShuffleWriteMetricDistributions(
          writeBytes = submetricQuantiles(_.shuffleBytesWritten),
          writeRecords = submetricQuantiles(_.shuffleRecordsWritten),
          writeTime = submetricQuantiles(_.shuffleWriteTime)
        )
      }.metricOption

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

/**
 * Helper for getting distributions from nested metric types.  Many of the metrics we want are
 * contained in options inside TaskMetrics (eg., ShuffleWriteMetrics). This makes it easy to handle
 * the options (returning None if the metrics are all empty), and extract the quantiles for each
 * metric.  After creating an instance, call metricOption to get the result type.
 */
private[v1] abstract class MetricHelper[I,O](
    rawMetrics: Seq[InternalTaskMetrics],
    quantiles: Array[Double]) {

  def getSubmetrics(raw: InternalTaskMetrics): Option[I]

  def build: O

  val data: Seq[I] = rawMetrics.flatMap(getSubmetrics)

  /** applies the given function to all input metrics, and returns the quantiles */
  def submetricQuantiles(f: I => Double): IndexedSeq[Double] = {
    Distribution(data.map { d => f(d) }).get.getQuantiles(quantiles)
  }

  def metricOption: Option[O] = {
    if (data.isEmpty) {
      None
    } else {
      Some(build)
    }
  }
}
