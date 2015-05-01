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

import scala.xml.{NodeSeq, Node}

import org.apache.commons.lang3.StringEscapeUtils

import org.apache.spark.streaming.Time
import org.apache.spark.ui.{UIUtils, WebUIPage}
import org.apache.spark.streaming.ui.StreamingJobProgressListener.{SparkJobId, OutputOpId}
import org.apache.spark.ui.jobs.UIData.JobUIData


private[ui] class BatchPage(parent: StreamingTab) extends WebUIPage("batch") {
  private val streamingListener = parent.listener
  private val sparkListener = parent.ssc.sc.jobProgressListener

  private def columns: Seq[Node] = {
    <th>Output Op Id</th>
      <th>Description</th>
      <th>Duration</th>
      <th>Job Id</th>
      <th>Duration</th>
      <th class="sorttable_nosort">Stages: Succeeded/Total</th>
      <th class="sorttable_nosort">Tasks (for all stages): Succeeded/Total</th>
      <th>Error</th>
  }

  /**
   * Generate a row for a Spark Job. Because duplicated output op infos needs to be collapsed into
   * one cell, we use "rowspan" for the first row of a output op.
   */
  def generateJobRow(
      outputOpId: OutputOpId,
      formattedOutputOpDuration: String,
      numSparkJobRowsInOutputOp: Int,
      isFirstRow: Boolean,
      sparkJob: JobUIData): Seq[Node] = {
    val lastStageInfo = Option(sparkJob.stageIds)
      .filter(_.nonEmpty)
      .flatMap { ids => sparkListener.stageIdToInfo.get(ids.max) }
    val lastStageData = lastStageInfo.flatMap { s =>
      sparkListener.stageIdToData.get((s.stageId, s.attemptId))
    }

    val lastStageName = lastStageInfo.map(_.name).getOrElse("(Unknown Stage Name)")
    val lastStageDescription = lastStageData.flatMap(_.description).getOrElse("")
    val duration: Option[Long] = {
      sparkJob.submissionTime.map { start =>
        val end = sparkJob.completionTime.getOrElse(System.currentTimeMillis())
        end - start
      }
    }
    val lastFailureReason =
      sparkJob.stageIds.sorted.reverse.flatMap(sparkListener.stageIdToInfo.get).
      dropWhile(_.failureReason == None).take(1). // get the first info that contains failure
      flatMap(info => info.failureReason).headOption.getOrElse("")
    val formattedDuration = duration.map(d => UIUtils.formatDuration(d)).getOrElse("-")
    val detailUrl = s"${UIUtils.prependBaseUri(parent.basePath)}/jobs/job?id=${sparkJob.jobId}"

    // In the first row, output op id and its information needs to be shown. In other rows, these
    // cells will be taken up due to "rowspan".
    // scalastyle:off
    val prefixCells =
      if (isFirstRow) {
        <td class="output-op-id-cell" rowspan={numSparkJobRowsInOutputOp.toString}>{outputOpId.toString}</td>
        <td rowspan={numSparkJobRowsInOutputOp.toString}>
          <span class="description-input" title={lastStageDescription}>
            {lastStageDescription}
          </span>{lastStageName}
        </td>
        <td rowspan={numSparkJobRowsInOutputOp.toString}>{formattedOutputOpDuration}</td>
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
        {sparkJob.completedStageIndices.size}/{sparkJob.stageIds.size - sparkJob.numSkippedStages}
        {if (sparkJob.numFailedStages > 0) s"(${sparkJob.numFailedStages} failed)"}
        {if (sparkJob.numSkippedStages > 0) s"(${sparkJob.numSkippedStages} skipped)"}
      </td>
      <td class="progress-cell">
        {
          UIUtils.makeProgressBar(
            started = sparkJob.numActiveTasks,
            completed = sparkJob.numCompletedTasks,
            failed = sparkJob.numFailedTasks,
            skipped = sparkJob.numSkippedTasks,
            total = sparkJob.numTasks - sparkJob.numSkippedTasks)
        }
      </td>
      {failureReasonCell(lastFailureReason)}
    </tr>
  }

  private def generateOutputOpIdRow(
      outputOpId: OutputOpId, sparkJobs: Seq[JobUIData]): Seq[Node] = {
    val sparkjobDurations = sparkJobs.map(sparkJob => {
      sparkJob.submissionTime.map { start =>
        val end = sparkJob.completionTime.getOrElse(System.currentTimeMillis())
        end - start
      }
    })
    val formattedOutputOpDuration =
      if (sparkjobDurations.exists(_ == None)) {
        // If any job does not finish, set "formattedOutputOpDuration" to "-"
        "-"
      } else {
        UIUtils.formatDuration(sparkjobDurations.flatMap(x => x).sum)
      }
    generateJobRow(outputOpId, formattedOutputOpDuration, sparkJobs.size, true, sparkJobs.head) ++
      sparkJobs.tail.map { sparkJob =>
        generateJobRow(outputOpId, formattedOutputOpDuration, sparkJobs.size, false, sparkJob)
      }.flatMap(x => x)
  }

  private def failureReasonCell(failureReason: String): Seq[Node] = {
    val isMultiline = failureReason.indexOf('\n') >= 0
    // Display the first line by default
    val failureReasonSummary = StringEscapeUtils.escapeHtml4(
      if (isMultiline) {
        failureReason.substring(0, failureReason.indexOf('\n'))
      } else {
        failureReason
      })
    val details = if (isMultiline) {
      // scalastyle:off
      <span onclick="this.parentNode.querySelector('.stacktrace-details').classList.toggle('collapsed')"
            class="expand-details">
        +details
      </span> ++
        <div class="stacktrace-details collapsed">
          <pre>{failureReason}</pre>
        </div>
      // scalastyle:on
    } else {
      ""
    }
    <td valign="middle" style="max-width: 300px">{failureReasonSummary}{details}</td>
  }

  private def getJobData(sparkJobId: SparkJobId): Option[JobUIData] = {
    sparkListener.activeJobs.get(sparkJobId).orElse {
      sparkListener.completedJobs.find(_.jobId == sparkJobId).orElse {
        sparkListener.failedJobs.find(_.jobId == sparkJobId)
      }
    }
  }

  /**
   * Generate the job table for the batch.
   */
  private def generateJobTable(batchUIData: BatchUIData): Seq[Node] = {
    val outputOpIdToSparkJobIds = batchUIData.outputOpIdSparkJobIdPairs.groupBy(_.outputOpId).toSeq.
      sortBy(_._1). // sorted by OutputOpId
      map { case (outputOpId, outputOpIdAndSparkJobIds) =>
        // sort SparkJobIds for each OutputOpId
        (outputOpId, outputOpIdAndSparkJobIds.map(_.sparkJobId).sorted)
      }
    sparkListener.synchronized {
      val outputOpIdWithJobs: Seq[(OutputOpId, Seq[JobUIData])] =
        outputOpIdToSparkJobIds.map { case (outputOpId, sparkJobIds) =>
          // Filter out spark Job ids that don't exist in sparkListener
          (outputOpId, sparkJobIds.flatMap(getJobData))
        }

      <table id="batch-job-table" class="table table-bordered table-striped table-condensed">
        <thead>
          {columns}
        </thead>
        <tbody>
          {
            outputOpIdWithJobs.map {
              case (outputOpId, jobs) => generateOutputOpIdRow(outputOpId, jobs)
            }
          }
        </tbody>
      </table>
    }
  }

  def render(request: HttpServletRequest): Seq[Node] = {
    val batchTime = Option(request.getParameter("id")).map(id => Time(id.toLong)).getOrElse {
      throw new IllegalArgumentException(s"Missing id parameter")
    }
    val formattedBatchTime = UIUtils.formatDate(batchTime.milliseconds)

    val batchUIData = streamingListener.getBatchUIData(batchTime).getOrElse {
      throw new IllegalArgumentException(s"Batch $formattedBatchTime does not exist")
    }

    val formattedSchedulingDelay =
      batchUIData.schedulingDelay.map(UIUtils.formatDuration).getOrElse("-")
    val formattedProcessingTime =
      batchUIData.processingDelay.map(UIUtils.formatDuration).getOrElse("-")
    val formattedTotalDelay = batchUIData.totalDelay.map(UIUtils.formatDuration).getOrElse("-")

    val summary: NodeSeq =
      <div>
        <ul class="unstyled">
          <li>
            <strong>Batch Duration: </strong>
            {UIUtils.formatDuration(streamingListener.batchDuration)}
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
        </ul>
      </div>

    val jobTable =
      if (batchUIData.outputOpIdSparkJobIdPairs.isEmpty) {
        <div>Cannot find any job for Batch {formattedBatchTime}.</div>
      } else {
        generateJobTable(batchUIData)
      }

    val content = summary ++ jobTable

    UIUtils.headerSparkPage(s"Details of batch at $formattedBatchTime", content, parent)
  }
}
