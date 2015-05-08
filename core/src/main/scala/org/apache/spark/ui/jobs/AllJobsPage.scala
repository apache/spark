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

import scala.collection.mutable.{HashMap, ListBuffer}
import scala.xml.{Node, NodeSeq, Unparsed}

import java.util.Date
import javax.servlet.http.HttpServletRequest

import org.apache.spark.ui.{UIUtils, WebUIPage}
import org.apache.spark.ui.jobs.UIData.{ExecutorUIData, JobUIData}
import org.apache.spark.JobExecutionStatus

/** Page showing list of all ongoing and recently finished jobs */
private[ui] class AllJobsPage(parent: JobsTab) extends WebUIPage("") {
  private val JOBS_LEGEND =
    <div class="legend-area"><svg width="150px" height="85px">
      <rect class="succeeded-job-legend"
        x="5px" y="5px" width="20px" height="15px" rx="2px" ry="2px"></rect>
      <text x="35px" y="17px">Succeeded</text>
      <rect class="failed-job-legend"
        x="5px" y="30px" width="20px" height="15px" rx="2px" ry="2px"></rect>
      <text x="35px" y="42px">Failed</text>
      <rect class="running-job-legend"
        x="5px" y="55px" width="20px" height="15px" rx="2px" ry="2px"></rect>
      <text x="35px" y="67px">Running</text>
    </svg></div>.toString.filter(_ != '\n')

  private val EXECUTORS_LEGEND =
    <div class="legend-area"><svg width="150px" height="55px">
      <rect class="executor-added-legend"
        x="5px" y="5px" width="20px" height="15px" rx="2px" ry="2px"></rect>
      <text x="35px" y="17px">Added</text>
      <rect class="executor-removed-legend"
        x="5px" y="30px" width="20px" height="15px" rx="2px" ry="2px"></rect>
      <text x="35px" y="42px">Removed</text>
    </svg></div>.toString.filter(_ != '\n')

  private def getLastStageNameAndDescription(job: JobUIData): (String, String) = {
    val lastStageInfo = Option(job.stageIds)
      .filter(_.nonEmpty)
      .flatMap { ids => parent.jobProgresslistener.stageIdToInfo.get(ids.max)}
    val lastStageData = lastStageInfo.flatMap { s =>
      parent.jobProgresslistener.stageIdToData.get((s.stageId, s.attemptId))
    }
    val name = lastStageInfo.map(_.name).getOrElse("(Unknown Stage Name)")
    val description = lastStageData.flatMap(_.description).getOrElse("")
    (name, description)
  }

  private def makeJobEvent(jobUIDatas: Seq[JobUIData]): Seq[String] = {
    jobUIDatas.filter { jobUIData =>
      jobUIData.status != JobExecutionStatus.UNKNOWN && jobUIData.submissionTime.isDefined
    }.map { jobUIData =>
      val jobId = jobUIData.jobId
      val status = jobUIData.status
      val (jobName, jobDescription) = getLastStageNameAndDescription(jobUIData)
      val displayJobDescription = if (jobDescription.isEmpty) jobName else jobDescription
      val submissionTime = jobUIData.submissionTime.get
      val completionTimeOpt = jobUIData.completionTime
      val completionTime = completionTimeOpt.getOrElse(System.currentTimeMillis())
      val classNameByStatus = status match {
        case JobExecutionStatus.SUCCEEDED => "succeeded"
        case JobExecutionStatus.FAILED => "failed"
        case JobExecutionStatus.RUNNING => "running"
      }

      val jobEventJsonAsStr =
        s"""
           |{
           |  'className': 'job application-timeline-object ${classNameByStatus}',
           |  'group': 'jobs',
           |  'start': new Date(${submissionTime}),
           |  'end': new Date(${completionTime}),
           |  'content': '<div class="application-timeline-content"' +
           |     'data-html="true" data-placement="top" data-toggle="tooltip"' +
           |     'data-title="${displayJobDescription} (Job ${jobId})<br>Status: ${status}<br>' +
           |     'Submission Time: ${UIUtils.formatDate(new Date(submissionTime))}' +
           |     '${
                     if (status != JobExecutionStatus.RUNNING) {
                       s"""<br>Completion Time: ${UIUtils.formatDate(new Date(completionTime))}"""
                     } else {
                       ""
                     }
                  }">' +
           |    '${displayJobDescription} (Job ${jobId})</div>'
           |}
         """.stripMargin
      jobEventJsonAsStr
    }
  }

  private def makeExecutorEvent(executorUIDatas: HashMap[String, ExecutorUIData]): Seq[String] = {
    val events = ListBuffer[String]()
    executorUIDatas.foreach {
      case (executorId, event) =>
        val addedEvent =
          s"""
             |{
             |  'className': 'executor added',
             |  'group': 'executors',
             |  'start': new Date(${event.startTime}),
             |  'content': '<div class="executor-event-content"' +
             |    'data-toggle="tooltip" data-placement="bottom"' +
             |    'data-title="Executor ${executorId}<br>' +
             |    'Added at ${UIUtils.formatDate(new Date(event.startTime))}"' +
             |    'data-html="true">Executor ${executorId} added</div>'
             |}
           """.stripMargin
        events += addedEvent

        if (event.finishTime.isDefined) {
          val removedEvent =
            s"""
               |{
               |  'className': 'executor removed',
               |  'group': 'executors',
               |  'start': new Date(${event.finishTime.get}),
               |  'content': '<div class="executor-event-content"' +
               |    'data-toggle="tooltip" data-placement="bottom"' +
               |    'data-title="Executor ${executorId}<br>' +
               |    'Removed at ${UIUtils.formatDate(new Date(event.finishTime.get))}' +
               |    '${
                        if (event.finishReason.isDefined) {
                          s"""<br>Reason: ${event.finishReason.get}"""
                        } else {
                          ""
                        }
                     }"' +
               |    'data-html="true">Executor ${executorId} removed</div>'
               |}
             """.stripMargin
          events += removedEvent
        }
    }
    events.toSeq
  }

  private def makeTimeline(
      jobs: Seq[JobUIData],
      executors: HashMap[String, ExecutorUIData],
      startTime: Long): Seq[Node] = {

    val jobEventJsonAsStrSeq = makeJobEvent(jobs)
    val executorEventJsonAsStrSeq = makeExecutorEvent(executors)

    val groupJsonArrayAsStr =
      s"""
          |[
          |  {
          |    'id': 'executors',
          |    'content': '<div>Executors</div>${EXECUTORS_LEGEND}',
          |  },
          |  {
          |    'id': 'jobs',
          |    'content': '<div>Jobs</div>${JOBS_LEGEND}',
          |  }
          |]
        """.stripMargin

    val eventArrayAsStr =
      (jobEventJsonAsStrSeq ++ executorEventJsonAsStrSeq).mkString("[", ",", "]")

    <span class="expand-application-timeline">
      <span class="expand-application-timeline-arrow arrow-closed"></span>
      <strong>Event timeline</strong>
    </span> ++
    <div id="application-timeline" class="collapsed">
      <div class="control-panel">
        <div id="application-timeline-zoom-lock">
          <input type="checkbox" checked="checked"></input>
          <span>Zoom Lock</span>
        </div>
      </div>
    </div> ++
    <script type="text/javascript">
      {Unparsed(s"drawApplicationTimeline(${groupJsonArrayAsStr}," +
      s"${eventArrayAsStr}, ${startTime});")}
    </script>
  }

  private def jobsTable(jobs: Seq[JobUIData]): Seq[Node] = {
    val someJobHasJobGroup = jobs.exists(_.jobGroup.isDefined)

    val columns: Seq[Node] = {
      <th>{if (someJobHasJobGroup) "Job Id (Job Group)" else "Job Id"}</th>
      <th>Description</th>
      <th>Submitted</th>
      <th>Duration</th>
      <th class="sorttable_nosort">Stages: Succeeded/Total</th>
      <th class="sorttable_nosort">Tasks (for all stages): Succeeded/Total</th>
    }

    def makeRow(job: JobUIData): Seq[Node] = {
      val (lastStageName, lastStageDescription) = getLastStageNameAndDescription(job)
      val duration: Option[Long] = {
        job.submissionTime.map { start =>
          val end = job.completionTime.getOrElse(System.currentTimeMillis())
          end - start
        }
      }
      val formattedDuration = duration.map(d => UIUtils.formatDuration(d)).getOrElse("Unknown")
      val formattedSubmissionTime = job.submissionTime.map(UIUtils.formatDate).getOrElse("Unknown")
      val detailUrl =
        "%s/jobs/job?id=%s".format(UIUtils.prependBaseUri(parent.basePath), job.jobId)
      <tr id={"job-" + job.jobId}>
        <td sorttable_customkey={job.jobId.toString}>
          {job.jobId} {job.jobGroup.map(id => s"($id)").getOrElse("")}
        </td>
        <td>
          <span class="description-input" title={lastStageDescription}>{lastStageDescription}</span>
          <a href={detailUrl}>{lastStageName}</a>
        </td>
        <td sorttable_customkey={job.submissionTime.getOrElse(-1).toString}>
          {formattedSubmissionTime}
        </td>
        <td sorttable_customkey={duration.getOrElse(-1).toString}>{formattedDuration}</td>
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
      </tr>
    }

    <table class="table table-bordered table-striped table-condensed sortable">
      <thead>{columns}</thead>
      <tbody>
        {jobs.map(makeRow)}
      </tbody>
    </table>
  }

  def render(request: HttpServletRequest): Seq[Node] = {
    val listener = parent.jobProgresslistener
    listener.synchronized {
      val startTime = listener.startTime
      val activeJobs = listener.activeJobs.values.toSeq
      val completedJobs = listener.completedJobs.reverse.toSeq
      val failedJobs = listener.failedJobs.reverse.toSeq

      val activeJobsTable =
        jobsTable(activeJobs.sortBy(_.submissionTime.getOrElse(-1L)).reverse)
      val completedJobsTable =
        jobsTable(completedJobs.sortBy(_.completionTime.getOrElse(-1L)).reverse)
      val failedJobsTable =
        jobsTable(failedJobs.sortBy(_.completionTime.getOrElse(-1L)).reverse)

      val shouldShowActiveJobs = activeJobs.nonEmpty
      val shouldShowCompletedJobs = completedJobs.nonEmpty
      val shouldShowFailedJobs = failedJobs.nonEmpty

      val completedJobNumStr = if (completedJobs.size == listener.numCompletedJobs) {
        s"${completedJobs.size}"
      } else {
        s"${listener.numCompletedJobs}, only showing ${completedJobs.size}"
      }

      val summary: NodeSeq =
        <div>
          <ul class="unstyled">
            {if (parent.sc.isDefined) {
              // Total duration is not meaningful unless the UI is live
              <li>
                <strong>Total Duration: </strong>
                {UIUtils.formatDuration(System.currentTimeMillis() - startTime)}
              </li>
            }}
            <li>
              <strong>Scheduling Mode: </strong>
              {listener.schedulingMode.map(_.toString).getOrElse("Unknown")}
            </li>
            {
              if (shouldShowActiveJobs) {
                <li>
                  <a href="#active"><strong>Active Jobs:</strong></a>
                  {activeJobs.size}
                </li>
              }
            }
            {
              if (shouldShowCompletedJobs) {
                <li id="completed-summary">
                  <a href="#completed"><strong>Completed Jobs:</strong></a>
                  {completedJobNumStr}
                </li>
              }
            }
            {
              if (shouldShowFailedJobs) {
                <li>
                  <a href="#failed"><strong>Failed Jobs:</strong></a>
                  {listener.numFailedJobs}
                </li>
              }
            }
          </ul>
        </div>

      var content = summary
      val executorListener = parent.executorListener
      content ++= makeTimeline(activeJobs ++ completedJobs ++ failedJobs,
          executorListener.executorIdToData, startTime)

      if (shouldShowActiveJobs) {
        content ++= <h4 id="active">Active Jobs ({activeJobs.size})</h4> ++
          activeJobsTable
      }
      if (shouldShowCompletedJobs) {
        content ++= <h4 id="completed">Completed Jobs ({completedJobNumStr})</h4> ++
          completedJobsTable
      }
      if (shouldShowFailedJobs) {
        content ++= <h4 id ="failed">Failed Jobs ({failedJobs.size})</h4> ++
          failedJobsTable
      }

      val helpText = """A job is triggered by an action, like "count()" or "saveAsTextFile()".""" +
        " Click on a job's title to see information about the stages of tasks associated with" +
        " the job."

      UIUtils.headerSparkPage("Spark Jobs", content, parent, helpText = Some(helpText))
    }
  }
}
