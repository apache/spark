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

package org.apache.spark.streaming.ui

import javax.servlet.http.HttpServletRequest

import scala.xml._

import org.apache.commons.lang3.StringEscapeUtils

import org.apache.spark.status.api.v1.{JobData, StageData}
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.ui.StreamingJobProgressListener.SparkJobId
import org.apache.spark.ui.{UIUtils => SparkUIUtils, WebUIPage}

private[ui] case class SparkJobIdWithUIData(sparkJobId: SparkJobId, jobData: Option[JobData])

private[ui] class BatchPage(parent: StreamingTab) extends WebUIPage("batch") {
  private val streamingListener = parent.listener
  private val store = parent.parent.store

  private def columns: Seq[Node] = {
    <th>Output Op Id</th>
      <th>Description</th>
      <th>Output Op Duration</th>
      <th>Status</th>
      <th>Job Id</th>
      <th>Job Duration</th>
      <th class="sorttable_nosort">Stages: Succeeded/Total</th>
      <th class="sorttable_nosort">Tasks (for all stages): Succeeded/Total</th>
      <th>Error</th>
  }

  private def generateJobRow(
      request: HttpServletRequest,
      outputOpData: OutputOperationUIData,
      outputOpDescription: Seq[Node],
      formattedOutputOpDuration: String,
      numSparkJobRowsInOutputOp: Int,
      isFirstRow: Boolean,
      jobIdWithData: SparkJobIdWithUIData): Seq[Node] = {
    if (jobIdWithData.jobData.isDefined) {
      generateNormalJobRow(request, outputOpData, outputOpDescription, formattedOutputOpDuration,
        numSparkJobRowsInOutputOp, isFirstRow, jobIdWithData.jobData.get)
    } else {
      generateDroppedJobRow(outputOpData, outputOpDescription, formattedOutputOpDuration,
        numSparkJobRowsInOutputOp, isFirstRow, jobIdWithData.sparkJobId)
    }
  }

  private def generateOutputOpRowWithoutSparkJobs(
    outputOpData: OutputOperationUIData,
    outputOpDescription: Seq[Node],
    formattedOutputOpDuration: String): Seq[Node] = {
    <tr>
      <td class="output-op-id-cell" >{outputOpData.id.toString}</td>
      <td>{outputOpDescription}</td>
      <td>{formattedOutputOpDuration}</td>
      {outputOpStatusCell(outputOpData, rowspan = 1)}
      <!-- Job Id -->
      <td>-</td>
      <!-- Duration -->
      <td>-</td>
      <!-- Stages: Succeeded/Total -->
      <td>-</td>
      <!-- Tasks (for all stages): Succeeded/Total -->
      <td>-</td>
      <!-- Error -->
      <td>-</td>
    </tr>
  }

  /**
   * Generate a row for a Spark Job. Because duplicated output op infos needs to be collapsed into
   * one cell, we use "rowspan" for the first row of an output op.
   */
  private def generateNormalJobRow(
      request: HttpServletRequest,
      outputOpData: OutputOperationUIData,
      outputOpDescription: Seq[Node],
      formattedOutputOpDuration: String,
      numSparkJobRowsInOutputOp: Int,
      isFirstRow: Boolean,
      sparkJob: JobData): Seq[Node] = {
    val duration: Option[Long] = {
      sparkJob.submissionTime.map { start =>
        val end = sparkJob.completionTime.map(_.getTime()).getOrElse(System.currentTimeMillis())
        end - start.getTime()
      }
    }
    val lastFailureReason =
      sparkJob.stageIds.sorted.reverse.flatMap(getStageData).
      dropWhile(_.failureReason == None).take(1). // get the first info that contains failure
      flatMap(info => info.failureReason).headOption.getOrElse("")
    val formattedDuration = duration.map(d => SparkUIUtils.formatDuration(d)).getOrElse("-")
    val detailUrl = s"${SparkUIUtils.prependBaseUri(
      request, parent.basePath)}/jobs/job/?id=${sparkJob.jobId}"

    // In the first row, output op id and its information needs to be shown. In other rows, these
    // cells will be taken up due to "rowspan".
    // scalastyle:off
    val prefixCells =
      if (isFirstRow) {
        <td class="output-op-id-cell" rowspan={numSparkJobRowsInOutputOp.toString}>{outputOpData.id.toString}</td>
        <td rowspan={numSparkJobRowsInOutputOp.toString}>
          {outputOpDescription}
        </td>
        <td rowspan={numSparkJobRowsInOutputOp.toString}>{formattedOutputOpDuration}</td> ++
        {outputOpStatusCell(outputOpData, numSparkJobRowsInOutputOp)}
      } else {
        Nil
      }
    // scalastyle:on

    <tr>
      {prefixCells}
      <td sorttable_customkey={sparkJob.jobId.toString}>
        <a href={detailUrl}>
          {sparkJob.jobId}{sparkJob.jobGroup.map(id => s"($id)").getOrElse("")}
        </a>
      </td>
      <td sorttable_customkey={duration.getOrElse(Long.MaxValue).toString}>
        {formattedDuration}
      </td>
      <td class="stage-progress-cell">
        {sparkJob.numCompletedStages}/{sparkJob.stageIds.size - sparkJob.numSkippedStages}
        {if (sparkJob.numFailedStages > 0) s"(${sparkJob.numFailedStages} failed)"}
        {if (sparkJob.numSkippedStages > 0) s"(${sparkJob.numSkippedStages} skipped)"}
      </td>
      <td class="progress-cell">
        {
          SparkUIUtils.makeProgressBar(
            started = sparkJob.numActiveTasks,
            completed = sparkJob.numCompletedTasks,
            failed = sparkJob.numFailedTasks,
            skipped = sparkJob.numSkippedTasks,
            reasonToNumKilled = sparkJob.killedTasksSummary,
            total = sparkJob.numTasks - sparkJob.numSkippedTasks)
        }
      </td>
      {UIUtils.failureReasonCell(lastFailureReason)}
    </tr>
  }

  /**
   * If a job is dropped by sparkListener due to exceeding the limitation, we only show the job id
   * with "-" cells.
   */
  private def generateDroppedJobRow(
      outputOpData: OutputOperationUIData,
      outputOpDescription: Seq[Node],
      formattedOutputOpDuration: String,
      numSparkJobRowsInOutputOp: Int,
      isFirstRow: Boolean,
      jobId: Int): Seq[Node] = {
    // In the first row, output op id and its information needs to be shown. In other rows, these
    // cells will be taken up due to "rowspan".
    // scalastyle:off
    val prefixCells =
      if (isFirstRow) {
        <td class="output-op-id-cell" rowspan={numSparkJobRowsInOutputOp.toString}>{outputOpData.id.toString}</td>
          <td rowspan={numSparkJobRowsInOutputOp.toString}>{outputOpDescription}</td>
          <td rowspan={numSparkJobRowsInOutputOp.toString}>{formattedOutputOpDuration}</td> ++
          {outputOpStatusCell(outputOpData, numSparkJobRowsInOutputOp)}
      } else {
        Nil
      }
    // scalastyle:on

    <tr>
      {prefixCells}
      <td sorttable_customkey={jobId.toString}>
        {if (jobId >= 0) jobId.toString else "-"}
      </td>
      <!-- Duration -->
      <td>-</td>
      <!-- Stages: Succeeded/Total -->
      <td>-</td>
      <!-- Tasks (for all stages): Succeeded/Total -->
      <td>-</td>
      <!-- Error -->
      <td>-</td>
    </tr>
  }

  private def generateOutputOpIdRow(
      request: HttpServletRequest,
      outputOpData: OutputOperationUIData,
      sparkJobs: Seq[SparkJobIdWithUIData]): Seq[Node] = {
    val formattedOutputOpDuration =
      if (outputOpData.duration.isEmpty) {
        "-"
      } else {
        SparkUIUtils.formatDuration(outputOpData.duration.get)
      }

    val description = generateOutputOpDescription(outputOpData)

    if (sparkJobs.isEmpty) {
      generateOutputOpRowWithoutSparkJobs(outputOpData, description, formattedOutputOpDuration)
    } else {
      val firstRow =
        generateJobRow(
          request,
          outputOpData,
          description,
          formattedOutputOpDuration,
          sparkJobs.size,
          true,
          sparkJobs.head)
      val tailRows =
        sparkJobs.tail.map { sparkJob =>
          generateJobRow(
            request,
            outputOpData,
            description,
            formattedOutputOpDuration,
            sparkJobs.size,
            false,
            sparkJob)
        }
      (firstRow ++ tailRows).flatten
    }
  }

  private def generateOutputOpDescription(outputOp: OutputOperationUIData): Seq[Node] = {
    <div>
      {outputOp.name}
      <span
        onclick="this.parentNode.querySelector('.stage-details').classList.toggle('collapsed')"
        class="expand-details">
          +details
      </span>
      <div class="stage-details collapsed">
        <pre>{outputOp.description}</pre>
      </div>
    </div>
  }

  private def getJobData(sparkJobId: SparkJobId): Option[JobData] = {
    try {
      Some(store.job(sparkJobId))
    } catch {
      case _: NoSuchElementException => None
    }
  }

  private def getStageData(stageId: Int): Option[StageData] = {
    try {
      Some(store.lastStageAttempt(stageId))
    } catch {
      case _: NoSuchElementException => None
    }
  }

  private def generateOutputOperationStatusForUI(failure: String): String = {
    if (failure.startsWith("org.apache.spark.SparkException")) {
      "Failed due to Spark job error\n" + failure
    } else {
      var nextLineIndex = failure.indexOf("\n")
      if (nextLineIndex < 0) {
        nextLineIndex = failure.length
      }
      val firstLine = failure.substring(0, nextLineIndex)
      s"Failed due to error: $firstLine\n$failure"
    }
  }

  /**
   * Generate the job table for the batch.
   */
  private def generateJobTable(
      request: HttpServletRequest,
      batchUIData: BatchUIData): Seq[Node] = {
    val outputOpIdToSparkJobIds = batchUIData.outputOpIdSparkJobIdPairs.groupBy(_.outputOpId).
      map { case (outputOpId, outputOpIdAndSparkJobIds) =>
        // sort SparkJobIds for each OutputOpId
        (outputOpId, outputOpIdAndSparkJobIds.map(_.sparkJobId).toSeq.sorted)
      }

    val outputOps: Seq[(OutputOperationUIData, Seq[SparkJobId])] =
      batchUIData.outputOperations.map { case (outputOpId, outputOperation) =>
        val sparkJobIds = outputOpIdToSparkJobIds.getOrElse(outputOpId, Seq.empty)
        (outputOperation, sparkJobIds)
      }.toSeq.sortBy(_._1.id)
    val outputOpWithJobs = outputOps.map { case (outputOpData, sparkJobIds) =>
        (outputOpData, sparkJobIds.map { jobId => SparkJobIdWithUIData(jobId, getJobData(jobId)) })
      }

    <table id="batch-job-table" class="table table-bordered table-striped table-condensed">
      <thead>
        {columns}
      </thead>
      <tbody>
        {
          outputOpWithJobs.map { case (outputOpData, sparkJobs) =>
            generateOutputOpIdRow(request, outputOpData, sparkJobs)
          }
        }
      </tbody>
    </table>
  }

  def render(request: HttpServletRequest): Seq[Node] = streamingListener.synchronized {
    val batchTime = Option(request.getParameter("id")).map(id => Time(id.toLong))
      .getOrElse {
        throw new IllegalArgumentException(s"Missing id parameter")
      }
    val formattedBatchTime =
      UIUtils.formatBatchTime(batchTime.milliseconds, streamingListener.batchDuration)

    val batchUIData = streamingListener.getBatchUIData(batchTime).getOrElse {
      throw new IllegalArgumentException(s"Batch $formattedBatchTime does not exist")
    }

    val formattedSchedulingDelay =
      batchUIData.schedulingDelay.map(SparkUIUtils.formatDuration).getOrElse("-")
    val formattedProcessingTime =
      batchUIData.processingDelay.map(SparkUIUtils.formatDuration).getOrElse("-")
    val formattedTotalDelay = batchUIData.totalDelay.map(SparkUIUtils.formatDuration).getOrElse("-")

    val inputMetadatas = batchUIData.streamIdToInputInfo.values.flatMap { inputInfo =>
      inputInfo.metadataDescription.map(desc => inputInfo.inputStreamId -> desc)
    }.toSeq
    val summary: NodeSeq =
      <div>
        <ul class="unstyled">
          <li>
            <strong>Batch Duration: </strong>
            {SparkUIUtils.formatDuration(streamingListener.batchDuration)}
          </li>
          <li>
            <strong>Input data size: </strong>
            {batchUIData.numRecords} records
          </li>
          <li>
            <strong>Scheduling delay: </strong>
            {formattedSchedulingDelay}
          </li>
          <li>
            <strong>Processing time: </strong>
            {formattedProcessingTime}
          </li>
          <li>
            <strong>Total delay: </strong>
            {formattedTotalDelay}
          </li>
          {
            if (inputMetadatas.nonEmpty) {
              <li>
                <strong>Input Metadata:</strong>{generateInputMetadataTable(inputMetadatas)}
              </li>
            }
          }
        </ul>
      </div>

    val content = summary ++ generateJobTable(request, batchUIData)

    SparkUIUtils.headerSparkPage(
      request, s"Details of batch at $formattedBatchTime", content, parent)
  }

  def generateInputMetadataTable(inputMetadatas: Seq[(Int, String)]): Seq[Node] = {
    <table class={SparkUIUtils.TABLE_CLASS_STRIPED_SORTABLE}>
      <thead>
        <tr>
          <th>Input</th>
          <th>Metadata</th>
        </tr>
      </thead>
      <tbody>
        {inputMetadatas.flatMap(generateInputMetadataRow)}
      </tbody>
    </table>
  }

  def generateInputMetadataRow(inputMetadata: (Int, String)): Seq[Node] = {
    val streamId = inputMetadata._1

    <tr>
      <td>{streamingListener.streamName(streamId).getOrElse(s"Stream-$streamId")}</td>
      <td>{metadataDescriptionToHTML(inputMetadata._2)}</td>
    </tr>
  }

  private def metadataDescriptionToHTML(metadataDescription: String): Seq[Node] = {
    // tab to 4 spaces and "\n" to "<br/>"
    Unparsed(StringEscapeUtils.escapeHtml4(metadataDescription).
      replaceAllLiterally("\t", "&nbsp;&nbsp;&nbsp;&nbsp;").replaceAllLiterally("\n", "<br/>"))
  }

  private def outputOpStatusCell(outputOp: OutputOperationUIData, rowspan: Int): Seq[Node] = {
    outputOp.failureReason match {
      case Some(failureReason) =>
        val failureReasonForUI = UIUtils.createOutputOperationFailureForUI(failureReason)
        UIUtils.failureReasonCell(
          failureReasonForUI, rowspan, includeFirstLineInExpandDetails = false)
      case None =>
        if (outputOp.endTime.isEmpty) {
          <td rowspan={rowspan.toString}>-</td>
        } else {
          <td rowspan={rowspan.toString}>Succeeded</td>
        }
    }
  }
}
