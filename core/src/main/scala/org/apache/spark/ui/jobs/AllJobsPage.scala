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

import java.net.URLEncoder
import java.util.Date
import javax.servlet.http.HttpServletRequest

import scala.collection.JavaConverters._
import scala.collection.mutable.{HashMap, ListBuffer}
import scala.xml._

import org.apache.commons.lang3.StringEscapeUtils

import org.apache.spark.JobExecutionStatus
import org.apache.spark.scheduler._
import org.apache.spark.ui._
import org.apache.spark.ui.jobs.UIData.{JobUIData, StageUIData}
import org.apache.spark.util.Utils

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
      val displayJobDescription =
        if (jobDescription.isEmpty) {
          jobName
        } else {
          UIUtils.makeDescription(jobDescription, "", plainText = true).text
        }
      val submissionTime = jobUIData.submissionTime.get
      val completionTimeOpt = jobUIData.completionTime
      val completionTime = completionTimeOpt.getOrElse(System.currentTimeMillis())
      val classNameByStatus = status match {
        case JobExecutionStatus.SUCCEEDED => "succeeded"
        case JobExecutionStatus.FAILED => "failed"
        case JobExecutionStatus.RUNNING => "running"
        case JobExecutionStatus.UNKNOWN => "unknown"
      }

      // The timeline library treats contents as HTML, so we have to escape them. We need to add
      // extra layers of escaping in order to embed this in a Javascript string literal.
      val escapedDesc = Utility.escape(displayJobDescription)
      val jsEscapedDesc = StringEscapeUtils.escapeEcmaScript(escapedDesc)
      val jobEventJsonAsStr =
        s"""
           |{
           |  'className': 'job application-timeline-object ${classNameByStatus}',
           |  'group': 'jobs',
           |  'start': new Date(${submissionTime}),
           |  'end': new Date(${completionTime}),
           |  'content': '<div class="application-timeline-content"' +
           |     'data-html="true" data-placement="top" data-toggle="tooltip"' +
           |     'data-title="${jsEscapedDesc} (Job ${jobId})<br>' +
           |     'Status: ${status}<br>' +
           |     'Submitted: ${UIUtils.formatDate(new Date(submissionTime))}' +
           |     '${
                     if (status != JobExecutionStatus.RUNNING) {
                       s"""<br>Completed: ${UIUtils.formatDate(new Date(completionTime))}"""
                     } else {
                       ""
                     }
                  }">' +
           |    '${jsEscapedDesc} (Job ${jobId})</div>'
           |}
         """.stripMargin
      jobEventJsonAsStr
    }
  }

  private def makeExecutorEvent(executorUIDatas: Seq[SparkListenerEvent]):
      Seq[String] = {
    val events = ListBuffer[String]()
    executorUIDatas.foreach {
      case a: SparkListenerExecutorAdded =>
        val addedEvent =
          s"""
             |{
             |  'className': 'executor added',
             |  'group': 'executors',
             |  'start': new Date(${a.time}),
             |  'content': '<div class="executor-event-content"' +
             |    'data-toggle="tooltip" data-placement="bottom"' +
             |    'data-title="Executor ${a.executorId}<br>' +
             |    'Added at ${UIUtils.formatDate(new Date(a.time))}"' +
             |    'data-html="true">Executor ${a.executorId} added</div>'
             |}
           """.stripMargin
        events += addedEvent
      case e: SparkListenerExecutorRemoved =>
        val removedEvent =
          s"""
             |{
             |  'className': 'executor removed',
             |  'group': 'executors',
             |  'start': new Date(${e.time}),
             |  'content': '<div class="executor-event-content"' +
             |    'data-toggle="tooltip" data-placement="bottom"' +
             |    'data-title="Executor ${e.executorId}<br>' +
             |    'Removed at ${UIUtils.formatDate(new Date(e.time))}' +
             |    '${
                      if (e.reason != null) {
                        s"""<br>Reason: ${e.reason.replace("\n", " ")}"""
                      } else {
                        ""
                      }
                   }"' +
             |    'data-html="true">Executor ${e.executorId} removed</div>'
             |}
           """.stripMargin
        events += removedEvent

    }
    events.toSeq
  }

  private def makeTimeline(
      jobs: Seq[JobUIData],
      executors: Seq[SparkListenerEvent],
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
      <a data-toggle="tooltip" title={ToolTips.JOB_TIMELINE} data-placement="right">
        Event Timeline
      </a>
    </span> ++
    <div id="application-timeline" class="collapsed">
      <div class="control-panel">
        <div id="application-timeline-zoom-lock">
          <input type="checkbox"></input>
          <span>Enable zooming</span>
        </div>
      </div>
    </div> ++
    <script type="text/javascript">
      {Unparsed(s"drawApplicationTimeline(${groupJsonArrayAsStr}," +
      s"${eventArrayAsStr}, ${startTime}, ${UIUtils.getTimeZoneOffset()});")}
    </script>
  }

  private def jobsTable(
      request: HttpServletRequest,
      jobTag: String,
      jobs: Seq[JobUIData]): Seq[Node] = {
    val allParameters = request.getParameterMap.asScala.toMap
    val parameterOtherTable = allParameters.filterNot(_._1.startsWith(jobTag))
      .map(para => para._1 + "=" + para._2(0))

    val someJobHasJobGroup = jobs.exists(_.jobGroup.isDefined)
    val jobIdTitle = if (someJobHasJobGroup) "Job Id (Job Group)" else "Job Id"

    val parameterJobPage = request.getParameter(jobTag + ".page")
    val parameterJobSortColumn = request.getParameter(jobTag + ".sort")
    val parameterJobSortDesc = request.getParameter(jobTag + ".desc")
    val parameterJobPageSize = request.getParameter(jobTag + ".pageSize")
    val parameterJobPrevPageSize = request.getParameter(jobTag + ".prevPageSize")

    val jobPage = Option(parameterJobPage).map(_.toInt).getOrElse(1)
    val jobSortColumn = Option(parameterJobSortColumn).map { sortColumn =>
      UIUtils.decodeURLParameter(sortColumn)
    }.getOrElse(jobIdTitle)
    val jobSortDesc = Option(parameterJobSortDesc).map(_.toBoolean).getOrElse(
      // New jobs should be shown above old jobs by default.
      if (jobSortColumn == jobIdTitle) true else false
    )
    val jobPageSize = Option(parameterJobPageSize).map(_.toInt).getOrElse(100)
    val jobPrevPageSize = Option(parameterJobPrevPageSize).map(_.toInt).getOrElse(jobPageSize)

    val page: Int = {
      // If the user has changed to a larger page size, then go to page 1 in order to avoid
      // IndexOutOfBoundsException.
      if (jobPageSize <= jobPrevPageSize) {
        jobPage
      } else {
        1
      }
    }
    val currentTime = System.currentTimeMillis()

    try {
      new JobPagedTable(
        jobs,
        jobTag,
        UIUtils.prependBaseUri(parent.basePath),
        "jobs", // subPath
        parameterOtherTable,
        parent.jobProgresslistener.stageIdToInfo,
        parent.jobProgresslistener.stageIdToData,
        currentTime,
        jobIdTitle,
        pageSize = jobPageSize,
        sortColumn = jobSortColumn,
        desc = jobSortDesc
      ).table(page)
    } catch {
      case e @ (_ : IllegalArgumentException | _ : IndexOutOfBoundsException) =>
        <div class="alert alert-error">
          <p>Error while rendering job table:</p>
          <pre>
            {Utils.exceptionString(e)}
          </pre>
        </div>
    }
  }

  def render(request: HttpServletRequest): Seq[Node] = {
    val listener = parent.jobProgresslistener
    listener.synchronized {
      val startTime = listener.startTime
      val endTime = listener.endTime
      val activeJobs = listener.activeJobs.values.toSeq
      val completedJobs = listener.completedJobs.reverse.toSeq
      val failedJobs = listener.failedJobs.reverse.toSeq

      val activeJobsTable = jobsTable(request, "activeJob", activeJobs)
      val completedJobsTable = jobsTable(request, "completedJob", completedJobs)
      val failedJobsTable = jobsTable(request, "failedJob", failedJobs)

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
            <li>
              <strong>User:</strong>
              {parent.getSparkUser}
            </li>
            <li>
              <strong>Total Uptime:</strong>
              {
                if (endTime < 0 && parent.sc.isDefined) {
                  UIUtils.formatDuration(System.currentTimeMillis() - startTime)
                } else if (endTime > 0) {
                  UIUtils.formatDuration(endTime - startTime)
                }
              }
            </li>
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
          executorListener.executorEvents, startTime)

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

      val helpText = """A job is triggered by an action, like count() or saveAsTextFile().""" +
        " Click on a job to see information about the stages of tasks inside it."

      UIUtils.headerSparkPage("Spark Jobs", content, parent, helpText = Some(helpText))
    }
  }
}

private[ui] class JobTableRowData(
    val jobData: JobUIData,
    val lastStageName: String,
    val lastStageDescription: String,
    val duration: Long,
    val formattedDuration: String,
    val submissionTime: Long,
    val formattedSubmissionTime: String,
    val jobDescription: NodeSeq,
    val detailUrl: String)

private[ui] class JobDataSource(
    jobs: Seq[JobUIData],
    stageIdToInfo: HashMap[Int, StageInfo],
    stageIdToData: HashMap[(Int, Int), StageUIData],
    basePath: String,
    currentTime: Long,
    pageSize: Int,
    sortColumn: String,
    desc: Boolean) extends PagedDataSource[JobTableRowData](pageSize) {

  // Convert JobUIData to JobTableRowData which contains the final contents to show in the table
  // so that we can avoid creating duplicate contents during sorting the data
  private val data = jobs.map(jobRow).sorted(ordering(sortColumn, desc))

  private var _slicedJobIds: Set[Int] = null

  override def dataSize: Int = data.size

  override def sliceData(from: Int, to: Int): Seq[JobTableRowData] = {
    val r = data.slice(from, to)
    _slicedJobIds = r.map(_.jobData.jobId).toSet
    r
  }

  private def getLastStageNameAndDescription(job: JobUIData): (String, String) = {
    val lastStageInfo = Option(job.stageIds)
      .filter(_.nonEmpty)
      .flatMap { ids => stageIdToInfo.get(ids.max)}
    val lastStageData = lastStageInfo.flatMap { s =>
      stageIdToData.get((s.stageId, s.attemptId))
    }
    val name = lastStageInfo.map(_.name).getOrElse("(Unknown Stage Name)")
    val description = lastStageData.flatMap(_.description).getOrElse("")
    (name, description)
  }

  private def jobRow(jobData: JobUIData): JobTableRowData = {
    val (lastStageName, lastStageDescription) = getLastStageNameAndDescription(jobData)
    val duration: Option[Long] = {
      jobData.submissionTime.map { start =>
        val end = jobData.completionTime.getOrElse(System.currentTimeMillis())
        end - start
      }
    }
    val formattedDuration = duration.map(d => UIUtils.formatDuration(d)).getOrElse("Unknown")
    val submissionTime = jobData.submissionTime
    val formattedSubmissionTime = submissionTime.map(UIUtils.formatDate).getOrElse("Unknown")
    val jobDescription = UIUtils.makeDescription(lastStageDescription, basePath, plainText = false)

    val detailUrl = "%s/jobs/job?id=%s".format(basePath, jobData.jobId)

    new JobTableRowData (
      jobData,
      lastStageName,
      lastStageDescription,
      duration.getOrElse(-1),
      formattedDuration,
      submissionTime.getOrElse(-1),
      formattedSubmissionTime,
      jobDescription,
      detailUrl
    )
  }

  /**
   * Return Ordering according to sortColumn and desc
   */
  private def ordering(sortColumn: String, desc: Boolean): Ordering[JobTableRowData] = {
    val ordering: Ordering[JobTableRowData] = sortColumn match {
      case "Job Id" | "Job Id (Job Group)" => Ordering.by(_.jobData.jobId)
      case "Description" => Ordering.by(x => (x.lastStageDescription, x.lastStageName))
      case "Submitted" => Ordering.by(_.submissionTime)
      case "Duration" => Ordering.by(_.duration)
      case "Stages: Succeeded/Total" | "Tasks (for all stages): Succeeded/Total" =>
        throw new IllegalArgumentException(s"Unsortable column: $sortColumn")
      case unknownColumn => throw new IllegalArgumentException(s"Unknown column: $unknownColumn")
    }
    if (desc) {
      ordering.reverse
    } else {
      ordering
    }
  }

}
private[ui] class JobPagedTable(
    data: Seq[JobUIData],
    jobTag: String,
    basePath: String,
    subPath: String,
    parameterOtherTable: Iterable[String],
    stageIdToInfo: HashMap[Int, StageInfo],
    stageIdToData: HashMap[(Int, Int), StageUIData],
    currentTime: Long,
    jobIdTitle: String,
    pageSize: Int,
    sortColumn: String,
    desc: Boolean
  ) extends PagedTable[JobTableRowData] {
  val parameterPath = basePath + s"/$subPath/?" + parameterOtherTable.mkString("&")

  override def tableId: String = jobTag + "-table"

  override def tableCssClass: String =
    "table table-bordered table-condensed table-striped table-head-clickable"

  override def pageSizeFormField: String = jobTag + ".pageSize"

  override def prevPageSizeFormField: String = jobTag + ".prevPageSize"

  override def pageNumberFormField: String = jobTag + ".page"

  override val dataSource = new JobDataSource(
    data,
    stageIdToInfo,
    stageIdToData,
    basePath,
    currentTime,
    pageSize,
    sortColumn,
    desc)

  override def pageLink(page: Int): String = {
    val encodedSortColumn = URLEncoder.encode(sortColumn, "UTF-8")
    parameterPath +
      s"&$pageNumberFormField=$page" +
      s"&$jobTag.sort=$encodedSortColumn" +
      s"&$jobTag.desc=$desc" +
      s"&$pageSizeFormField=$pageSize"
  }

  override def goButtonFormPath: String = {
    val encodedSortColumn = URLEncoder.encode(sortColumn, "UTF-8")
    s"$parameterPath&$jobTag.sort=$encodedSortColumn&$jobTag.desc=$desc"
  }

  override def headers: Seq[Node] = {
    // Information for each header: title, cssClass, and sortable
    val jobHeadersAndCssClasses: Seq[(String, String, Boolean)] =
      Seq(
        (jobIdTitle, "", true),
        ("Description", "", true), ("Submitted", "", true), ("Duration", "", true),
        ("Stages: Succeeded/Total", "", false),
        ("Tasks (for all stages): Succeeded/Total", "", false)
      )

    if (!jobHeadersAndCssClasses.filter(_._3).map(_._1).contains(sortColumn)) {
      throw new IllegalArgumentException(s"Unknown column: $sortColumn")
    }

    val headerRow: Seq[Node] = {
      jobHeadersAndCssClasses.map { case (header, cssClass, sortable) =>
        if (header == sortColumn) {
          val headerLink = Unparsed(
            parameterPath +
              s"&$jobTag.sort=${URLEncoder.encode(header, "UTF-8")}" +
              s"&$jobTag.desc=${!desc}" +
              s"&$jobTag.pageSize=$pageSize")
          val arrow = if (desc) "&#x25BE;" else "&#x25B4;" // UP or DOWN

          <th class={cssClass}>
            <a href={headerLink}>
              {header}<span>
              &nbsp;{Unparsed(arrow)}
            </span>
            </a>
          </th>
        } else {
          if (sortable) {
            val headerLink = Unparsed(
              parameterPath +
                s"&$jobTag.sort=${URLEncoder.encode(header, "UTF-8")}" +
                s"&$jobTag.pageSize=$pageSize")

            <th class={cssClass}>
              <a href={headerLink}>
                {header}
              </a>
            </th>
          } else {
            <th class={cssClass}>
              {header}
            </th>
          }
        }
      }
    }
    <thead>{headerRow}</thead>
  }

  override def row(jobTableRow: JobTableRowData): Seq[Node] = {
    val job = jobTableRow.jobData

    <tr id={"job-" + job.jobId}>
      <td>
        {job.jobId} {job.jobGroup.map(id => s"($id)").getOrElse("")}
      </td>
      <td>
        {jobTableRow.jobDescription}
        <a href={jobTableRow.detailUrl} class="name-link">{jobTableRow.lastStageName}</a>
      </td>
      <td>
        {jobTableRow.formattedSubmissionTime}
      </td>
      <td>{jobTableRow.formattedDuration}</td>
      <td class="stage-progress-cell">
        {job.completedStageIndices.size}/{job.stageIds.size - job.numSkippedStages}
        {if (job.numFailedStages > 0) s"(${job.numFailedStages} failed)"}
        {if (job.numSkippedStages > 0) s"(${job.numSkippedStages} skipped)"}
      </td>
      <td class="progress-cell">
        {UIUtils.makeProgressBar(started = job.numActiveTasks, completed = job.numCompletedTasks,
        failed = job.numFailedTasks, skipped = job.numSkippedTasks, killed = job.numKilledTasks,
        total = job.numTasks - job.numSkippedTasks)}
      </td>
    </tr>
  }
}
