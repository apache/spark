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

import org.apache.commons.lang3.StringEscapeUtils
import org.apache.spark.streaming.Time
import org.apache.spark.ui.{UIUtils, WebUIPage}
import org.apache.spark.streaming.ui.StreamingJobProgressListener.{JobId, OutputOpId}
import org.apache.spark.ui.jobs.UIData.JobUIData

import scala.xml.{NodeSeq, Node}

class BatchPage(parent: StreamingTab) extends WebUIPage("batch") {
  private val streaminglistener = parent.listener
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

  private def makeOutputOpIdRow(outputOpId: OutputOpId, jobs: Seq[JobUIData]): Seq[Node] = {
    val jobDurations = jobs.map(job => {
      job.submissionTime.map { start =>
        val end = job.completionTime.getOrElse(System.currentTimeMillis())
        end - start
      }
    })
    val formattedOutputOpDuration =
      if (jobDurations.exists(_ == None)) {
        // If any job does not finish, set "formattedOutputOpDuration" to "-"
        "-"
      } else {
        UIUtils.formatDuration(jobDurations.flatMap(x => x).sum)
      }

    def makeJobRow(job: JobUIData, isFirstRow: Boolean): Seq[Node] = {
      val lastStageInfo = Option(job.stageIds)
        .filter(_.nonEmpty)
        .flatMap { ids => sparkListener.stageIdToInfo.get(ids.max) }
      val lastStageData = lastStageInfo.flatMap { s =>
        sparkListener.stageIdToData.get((s.stageId, s.attemptId))
      }

      val lastStageName = lastStageInfo.map(_.name).getOrElse("(Unknown Stage Name)")
      val lastStageDescription = lastStageData.flatMap(_.description).getOrElse("")
      val duration: Option[Long] = {
        job.submissionTime.map { start =>
          val end = job.completionTime.getOrElse(System.currentTimeMillis())
          end - start
        }
      }
      val lastFailureReason = job.stageIds.sorted.reverse.flatMap(sparkListener.stageIdToInfo.get).
        dropWhile(_.failureReason == None).take(1). // get the first info that contains failure
        flatMap(info => info.failureReason).headOption.getOrElse("")
      val formattedDuration = duration.map(d => UIUtils.formatDuration(d)).getOrElse("-")
      val detailUrl = s"${UIUtils.prependBaseUri(parent.basePath)}/jobs/job?id=${job.jobId}"
      <tr>
        {if(isFirstRow) {
        <td rowspan={jobs.size.toString}>{outputOpId}</td>
        <td rowspan={jobs.size.toString}>
          <span class="description-input" title={lastStageDescription}>
            {lastStageDescription}
          </span>{lastStageName}
        </td>
        <td rowspan={jobs.size.toString}>{formattedOutputOpDuration}</td>}
        }
        <td sorttable_customkey={job.jobId.toString}>
          <a href={detailUrl}>
            {job.jobId}{job.jobGroup.map(id => s"($id)").getOrElse("")}
          </a>
        </td>
        <td sorttable_customkey={duration.getOrElse(Long.MaxValue).toString}>
          {formattedDuration}
        </td>
        <td class="stage-progress-cell">
          {job.completedStageIndices.size}/{job.stageIds.size - job.numSkippedStages}
          {if (job.numFailedStages > 0) s"(${job.numFailedStages} failed)"}
          {if (job.numSkippedStages > 0) s"(${job.numSkippedStages} skipped)"}
        </td>
        <td class="progress-cell">
          {UIUtils.makeProgressBar(started = job.numActiveTasks, completed = job.numCompletedTasks,
          failed = job.numFailedTasks, skipped = job.numSkippedTasks,
          total = job.numTasks - job.numSkippedTasks)}
        </td>
        {failureReasonCell(lastFailureReason)}
      </tr>
    }

    makeJobRow(jobs.head, true) ++ (jobs.tail.map(job => makeJobRow(job, false)).flatMap(x => x))
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
    <td valign="middle">{failureReasonSummary}{details}</td>
  }

  private def jobsTable(jobInfos: Seq[(OutputOpId, JobId)]): Seq[Node] = {
    def getJobData(jobId: JobId): Option[JobUIData] = {
      sparkListener.activeJobs.get(jobId).orElse {
        sparkListener.completedJobs.find(_.jobId == jobId).orElse {
          sparkListener.failedJobs.find(_.jobId == jobId)
        }
      }
    }

    // Group jobInfos by OutputOpId firstly, then sort them.
    // E.g., [(0, 1), (1, 3), (0, 2), (1, 4)] => [(0, [1, 2]), (1, [3, 4])]
    val outputOpIdWithJobIds: Seq[(OutputOpId, Seq[JobId])] =
      jobInfos.groupBy(_._1).toSeq.sortBy(_._1). // sorted by OutputOpId
        map { case (outputOpId, jobs) =>
        (outputOpId, jobs.map(_._2).sortBy(x => x).toSeq)} // sort JobIds for each OutputOpId
    sparkListener.synchronized {
      val outputOpIdWithJobs: Seq[(OutputOpId, Seq[JobUIData])] = outputOpIdWithJobIds.map {
        case (outputOpId, jobIds) =>
          // Filter out JobIds that don't exist in sparkListener
          (outputOpId, jobIds.flatMap(getJobData))
      }

      <table id="batch-job-table" class="table table-bordered table-striped table-condensed">
        <thead>
          {columns}
        </thead>
        <tbody>
          {outputOpIdWithJobs.map { case (outputOpId, jobs) => makeOutputOpIdRow(outputOpId, jobs)}}
        </tbody>
      </table>
    }
  }

  def render(request: HttpServletRequest): Seq[Node] = {
    val batchTime = Option(request.getParameter("id")).map(id => Time(id.toLong)).getOrElse {
      throw new IllegalArgumentException(s"Missing id parameter")
    }
    val formattedBatchTime = UIUtils.formatDate(batchTime.milliseconds)
    val (batchInfo, jobInfos) = streaminglistener.synchronized {
      val _batchInfo = streaminglistener.getBatchInfo(batchTime).getOrElse {
        throw new IllegalArgumentException(s"Batch $formattedBatchTime does not exist")
      }
      val _jobInfos = streaminglistener.getJobInfos(batchTime)
      (_batchInfo, _jobInfos)
    }

    val formattedSchedulingDelay =
      batchInfo.schedulingDelay.map(UIUtils.formatDuration).getOrElse("-")
    val formattedProcessingTime =
      batchInfo.processingDelay.map(UIUtils.formatDuration).getOrElse("-")
    val formattedTotalDelay = batchInfo.totalDelay.map(UIUtils.formatDuration).getOrElse("-")

    val summary: NodeSeq =
      <div>
        <ul class="unstyled">
          <li>
            <strong>Batch Duration: </strong>
            {UIUtils.formatDuration(streaminglistener.batchDuration)}
          </li>
          <li>
            <strong>Input data size: </strong>
            {batchInfo.numRecords} records
          </li>
          <li>
            <strong>Scheduling delay: </strong>
            {formattedSchedulingDelay} records
          </li>
          <li>
            <strong>Processing time: </strong>
            {formattedProcessingTime}
          </li>
          <li>
            <strong>Total delay: </strong>
            {formattedTotalDelay} records
          </li>
        </ul>
      </div>

    val content = summary ++ jobInfos.map(jobsTable).getOrElse {
      <div>Cannot find any job for Batch {formattedBatchTime}</div>
    }
    UIUtils.headerSparkPage(s"Details of batch at $formattedBatchTime", content, parent)
  }
}
