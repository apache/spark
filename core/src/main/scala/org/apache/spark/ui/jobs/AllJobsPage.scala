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
import java.nio.charset.StandardCharsets.UTF_8
import java.util.{Date, Locale}
import javax.servlet.http.HttpServletRequest

import scala.collection.mutable.ListBuffer
import scala.xml._

import org.apache.commons.text.StringEscapeUtils

import org.apache.spark.JobExecutionStatus
import org.apache.spark.internal.config.SCHEDULER_MODE
import org.apache.spark.internal.config.UI._
import org.apache.spark.scheduler._
import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1
import org.apache.spark.ui._
import org.apache.spark.util.Utils

/** Page showing list of all ongoing and recently finished jobs */
private[ui] class AllJobsPage(parent: JobsTab, store: AppStatusStore) extends WebUIPage("") {

  import ApiHelper._

  private val TIMELINE_ENABLED = parent.conf.get(UI_TIMELINE_ENABLED)
  private val MAX_TIMELINE_JOBS = parent.conf.get(UI_TIMELINE_JOBS_MAXIMUM)
  private val MAX_TIMELINE_EXECUTORS = parent.conf.get(UI_TIMELINE_EXECUTORS_MAXIMUM)

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

  private def makeJobEvent(jobs: Seq[v1.JobData]): Seq[String] = {
    val now = System.currentTimeMillis()
    jobs.filter { job =>
      job.status != JobExecutionStatus.UNKNOWN && job.submissionTime.isDefined
    }.sortBy { j =>
      (j.completionTime.map(_.getTime).getOrElse(now), j.submissionTime.get.getTime)
    }.takeRight(MAX_TIMELINE_JOBS).map { job =>
      val jobId = job.jobId
      val status = job.status
      val (_, lastStageDescription) = lastStageNameAndDescription(store, job)
      val jobDescription = UIUtils.makeDescription(
        job.description.getOrElse(lastStageDescription),
        "",
        plainText = true).text

      val submissionTime = job.submissionTime.get.getTime()
      val completionTime = job.completionTime.map(_.getTime()).getOrElse(now)
      val classNameByStatus = status match {
        case JobExecutionStatus.SUCCEEDED => "succeeded"
        case JobExecutionStatus.FAILED => "failed"
        case JobExecutionStatus.RUNNING => "running"
        case JobExecutionStatus.UNKNOWN => "unknown"
      }

      // The timeline library treats contents as HTML, so we have to escape them. We need to add
      // extra layers of escaping in order to embed this in a JavaScript string literal.
      val escapedDesc = Utility.escape(jobDescription)
      val jsEscapedDescForTooltip = StringEscapeUtils.escapeEcmaScript(Utility.escape(escapedDesc))
      val jsEscapedDescForLabel = StringEscapeUtils.escapeEcmaScript(escapedDesc)
      val jobEventJsonAsStr =
        s"""
           |{
           |  'className': 'job application-timeline-object ${classNameByStatus}',
           |  'group': 'jobs',
           |  'start': new Date(${submissionTime}),
           |  'end': new Date(${completionTime}),
           |  'content': '<div class="application-timeline-content"' +
           |     'data-html="true" data-placement="top" data-toggle="tooltip"' +
           |     'data-title="${jsEscapedDescForTooltip} (Job ${jobId})<br>' +
           |     'Status: ${status}<br>' +
           |     'Submitted: ${UIUtils.formatDate(new Date(submissionTime))}' +
           |     '${
                     if (status != JobExecutionStatus.RUNNING) {
                       s"""<br>Completed: ${UIUtils.formatDate(new Date(completionTime))}"""
                     } else {
                       ""
                     }
                  }">' +
           |    '${jsEscapedDescForLabel} (Job ${jobId})</div>'
           |}
         """.stripMargin
      jobEventJsonAsStr
    }
  }

  private def makeExecutorEvent(executors: Seq[v1.ExecutorSummary]):
      Seq[String] = {
    val events = ListBuffer[String]()
    executors.sortBy { e =>
      e.removeTime.map(_.getTime).getOrElse(e.addTime.getTime)
    }.takeRight(MAX_TIMELINE_EXECUTORS).foreach { e =>
      val addedEvent =
        s"""
           |{
           |  'className': 'executor added',
           |  'group': 'executors',
           |  'start': new Date(${e.addTime.getTime()}),
           |  'content': '<div class="executor-event-content"' +
           |    'data-toggle="tooltip" data-placement="top"' +
           |    'data-title="Executor ${e.id}<br>' +
           |    'Added at ${UIUtils.formatDate(e.addTime)}"' +
           |    'data-html="true">Executor ${e.id} added</div>'
           |}
         """.stripMargin
      events += addedEvent

      e.removeTime.foreach { removeTime =>
        val removedEvent =
          s"""
             |{
             |  'className': 'executor removed',
             |  'group': 'executors',
             |  'start': new Date(${removeTime.getTime()}),
             |  'content': '<div class="executor-event-content"' +
             |    'data-toggle="tooltip" data-placement="top"' +
             |    'data-title="Executor ${e.id}<br>' +
             |    'Removed at ${UIUtils.formatDate(removeTime)}' +
             |    '${
                      e.removeReason.map { reason =>
                        s"""<br>Reason: ${StringEscapeUtils.escapeEcmaScript(
                          reason.replace("\n", " "))}"""
                      }.getOrElse("")
                   }"' +
             |    'data-html="true">Executor ${e.id} removed</div>'
             |}
           """.stripMargin
        events += removedEvent
      }
    }
    events.toSeq
  }

  private def makeTimeline(
      jobs: Seq[v1.JobData],
      executors: Seq[v1.ExecutorSummary],
      startTime: Long): Seq[Node] = {

    if (!TIMELINE_ENABLED) return Seq.empty[Node]

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
      <a data-toggle="tooltip" title={ToolTips.JOB_TIMELINE} data-placement="top">
        Event Timeline
      </a>
    </span> ++
    <div id="application-timeline" class="collapsed">
      {
        if (MAX_TIMELINE_JOBS < jobs.size) {
          <div>
            <strong>
              Only the most recent {MAX_TIMELINE_JOBS} submitted/completed jobs
              (of {jobs.size} total) are shown.
            </strong>
          </div>
        } else {
          Seq.empty
        }
      }
      {
        if (MAX_TIMELINE_EXECUTORS < executors.size) {
          <div>
            <strong>
              Only the most recent {MAX_TIMELINE_EXECUTORS} added/removed executors
              (of {executors.size} total) are shown.
            </strong>
          </div>
        } else {
          Seq.empty
        }
      }
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
      tableHeaderId: String,
      jobTag: String,
      jobs: Seq[v1.JobData],
      killEnabled: Boolean): Seq[Node] = {

    val someJobHasJobGroup = jobs.exists(_.jobGroup.isDefined)
    val jobIdTitle = if (someJobHasJobGroup) "Job Id (Job Group)" else "Job Id"
    val jobPage = Option(request.getParameter(jobTag + ".page")).map(_.toInt).getOrElse(1)

    try {
      new JobPagedTable(
        request,
        store,
        jobs,
        tableHeaderId,
        jobTag,
        UIUtils.prependBaseUri(request, parent.basePath),
        "jobs", // subPath
        killEnabled,
        jobIdTitle
      ).table(jobPage)
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
    val appInfo = store.applicationInfo()
    val startTime = appInfo.attempts.head.startTime.getTime()
    val endTime = appInfo.attempts.head.endTime.getTime()

    val activeJobs = new ListBuffer[v1.JobData]()
    val completedJobs = new ListBuffer[v1.JobData]()
    val failedJobs = new ListBuffer[v1.JobData]()

    store.jobsList(null).foreach { job =>
      job.status match {
        case JobExecutionStatus.SUCCEEDED =>
          completedJobs += job
        case JobExecutionStatus.FAILED =>
          failedJobs += job
        case _ =>
          activeJobs += job
      }
    }

    val activeJobsTable =
      jobsTable(request, "active", "activeJob", activeJobs.toSeq, killEnabled = parent.killEnabled)
    val completedJobsTable =
      jobsTable(request, "completed", "completedJob", completedJobs.toSeq, killEnabled = false)
    val failedJobsTable =
      jobsTable(request, "failed", "failedJob", failedJobs.toSeq, killEnabled = false)

    val shouldShowActiveJobs = activeJobs.nonEmpty
    val shouldShowCompletedJobs = completedJobs.nonEmpty
    val shouldShowFailedJobs = failedJobs.nonEmpty

    val appSummary = store.appSummary()
    val completedJobNumStr = if (completedJobs.size == appSummary.numCompletedJobs) {
      s"${completedJobs.size}"
    } else {
      s"${appSummary.numCompletedJobs}, only showing ${completedJobs.size}"
    }

    // SPARK-33991 Avoid enumeration conversion error.
    val schedulingMode = store.environmentInfo().sparkProperties.toMap
      .get(SCHEDULER_MODE.key)
      .map { mode => SchedulingMode.withName(mode.toUpperCase(Locale.ROOT)).toString }
      .getOrElse("Unknown")

    val summary: NodeSeq =
      <div>
        <ul class="list-unstyled">
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
            {schedulingMode}
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
                {failedJobs.size}
              </li>
            }
          }
        </ul>
      </div>

    var content = summary
    content ++= makeTimeline((activeJobs ++ completedJobs ++ failedJobs).toSeq,
      store.executorList(false), startTime)

    if (shouldShowActiveJobs) {
      content ++=
        <span id="active" class="collapse-aggregated-activeJobs collapse-table"
            onClick="collapseTable('collapse-aggregated-activeJobs','aggregated-activeJobs')">
          <h4>
            <span class="collapse-table-arrow arrow-open"></span>
            <a>Active Jobs ({activeJobs.size})</a>
          </h4>
        </span> ++
        <div class="aggregated-activeJobs collapsible-table">
          {activeJobsTable}
        </div>
    }
    if (shouldShowCompletedJobs) {
      content ++=
        <span id="completed" class="collapse-aggregated-completedJobs collapse-table"
            onClick="collapseTable('collapse-aggregated-completedJobs','aggregated-completedJobs')">
          <h4>
            <span class="collapse-table-arrow arrow-open"></span>
            <a>Completed Jobs ({completedJobNumStr})</a>
          </h4>
        </span> ++
        <div class="aggregated-completedJobs collapsible-table">
          {completedJobsTable}
        </div>
    }
    if (shouldShowFailedJobs) {
      content ++=
        <span id ="failed" class="collapse-aggregated-failedJobs collapse-table"
            onClick="collapseTable('collapse-aggregated-failedJobs','aggregated-failedJobs')">
          <h4>
            <span class="collapse-table-arrow arrow-open"></span>
            <a>Failed Jobs ({failedJobs.size})</a>
          </h4>
        </span> ++
      <div class="aggregated-failedJobs collapsible-table">
        {failedJobsTable}
      </div>
    }

    val helpText = """A job is triggered by an action, like count() or saveAsTextFile().""" +
      " Click on a job to see information about the stages of tasks inside it."

    UIUtils.headerSparkPage(request, "Spark Jobs", content, parent,
      helpText = Some(helpText))
  }

}

private[ui] class JobTableRowData(
    val jobData: v1.JobData,
    val lastStageName: String,
    val lastStageDescription: String,
    val duration: Long,
    val formattedDuration: String,
    val submissionTime: Long,
    val formattedSubmissionTime: String,
    val jobDescription: NodeSeq,
    val detailUrl: String)

private[ui] class JobDataSource(
    store: AppStatusStore,
    jobs: Seq[v1.JobData],
    basePath: String,
    pageSize: Int,
    sortColumn: String,
    desc: Boolean) extends PagedDataSource[JobTableRowData](pageSize) {

  import ApiHelper._

  // Convert JobUIData to JobTableRowData which contains the final contents to show in the table
  // so that we can avoid creating duplicate contents during sorting the data
  private val data = jobs.map(jobRow).sorted(ordering(sortColumn, desc))

  override def dataSize: Int = data.size

  override def sliceData(from: Int, to: Int): Seq[JobTableRowData] = data.slice(from, to)

  private def jobRow(jobData: v1.JobData): JobTableRowData = {
    val duration: Option[Long] = JobDataUtil.getDuration(jobData)
    val formattedDuration = JobDataUtil.getFormattedDuration(jobData)
    val submissionTime = jobData.submissionTime
    val formattedSubmissionTime = JobDataUtil.getFormattedSubmissionTime(jobData)
    val (lastStageName, lastStageDescription) = lastStageNameAndDescription(store, jobData)

    val jobDescription =
      UIUtils.makeDescription(
        jobData.description.getOrElse(lastStageDescription),
        basePath,
        plainText = false)

    val detailUrl = "%s/jobs/job/?id=%s".format(basePath, jobData.jobId)

    new JobTableRowData(
      jobData,
      lastStageName,
      lastStageDescription,
      duration.getOrElse(-1),
      formattedDuration,
      submissionTime.map(_.getTime()).getOrElse(-1L),
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
    request: HttpServletRequest,
    store: AppStatusStore,
    data: Seq[v1.JobData],
    tableHeaderId: String,
    jobTag: String,
    basePath: String,
    subPath: String,
    killEnabled: Boolean,
    jobIdTitle: String
  ) extends PagedTable[JobTableRowData] {

  private val (sortColumn, desc, pageSize) = getTableParameters(request, jobTag, jobIdTitle)
  private val parameterPath = basePath + s"/$subPath/?" + getParameterOtherTable(request, jobTag)
  private val encodedSortColumn = URLEncoder.encode(sortColumn, UTF_8.name())

  override def tableId: String = jobTag + "-table"

  override def tableCssClass: String =
    "table table-bordered table-sm table-striped table-head-clickable table-cell-width-limited"

  override def pageSizeFormField: String = jobTag + ".pageSize"

  override def pageNumberFormField: String = jobTag + ".page"

  override val dataSource = new JobDataSource(
    store,
    data,
    basePath,
    pageSize,
    sortColumn,
    desc)

  override def pageLink(page: Int): String = {
    parameterPath +
      s"&$pageNumberFormField=$page" +
      s"&$jobTag.sort=$encodedSortColumn" +
      s"&$jobTag.desc=$desc" +
      s"&$pageSizeFormField=$pageSize" +
      s"#$tableHeaderId"
  }

  override def goButtonFormPath: String =
    s"$parameterPath&$jobTag.sort=$encodedSortColumn&$jobTag.desc=$desc#$tableHeaderId"

  override def headers: Seq[Node] = {
    // Information for each header: title, sortable, tooltip
    val jobHeadersAndCssClasses: Seq[(String, Boolean, Option[String])] =
      Seq(
        (jobIdTitle, true, None),
        ("Description", true, None),
        ("Submitted", true, None),
        ("Duration", true, Some("Elapsed time since the job was submitted " +
          "until execution completion of all its stages.")),
        ("Stages: Succeeded/Total", false, None),
        ("Tasks (for all stages): Succeeded/Total", false, None)
      )

    isSortColumnValid(jobHeadersAndCssClasses, sortColumn)

    headerRow(jobHeadersAndCssClasses, desc, pageSize, sortColumn, parameterPath,
      jobTag, tableHeaderId)
  }

  override def row(jobTableRow: JobTableRowData): Seq[Node] = {
    val job = jobTableRow.jobData

    val killLink = if (killEnabled) {
      val confirm =
        s"if (window.confirm('Are you sure you want to kill job ${job.jobId} ?')) " +
          "{ this.parentNode.submit(); return true; } else { return false; }"
      // SPARK-6846 this should be POST-only but YARN AM won't proxy POST
      /*
      val killLinkUri = s"$basePathUri/jobs/job/kill/"
      <form action={killLinkUri} method="POST" style="display:inline">
        <input type="hidden" name="id" value={job.jobId.toString}/>
        <a href="#" onclick={confirm} class="kill-link">(kill)</a>
      </form>
       */
      val killLinkUri = s"$basePath/jobs/job/kill/?id=${job.jobId}"
      <a href={killLinkUri} onclick={confirm} class="kill-link">(kill)</a>
    } else {
      Seq.empty
    }

    <tr id={"job-" + job.jobId}>
      <td>
        {job.jobId} {job.jobGroup.map(id => s"($id)").getOrElse("")}
      </td>
      <td>
        {jobTableRow.jobDescription} {killLink}
        <a href={jobTableRow.detailUrl} class="name-link">{jobTableRow.lastStageName}</a>
      </td>
      <td>
        {jobTableRow.formattedSubmissionTime}
      </td>
      <td>{jobTableRow.formattedDuration}</td>
      <td class="stage-progress-cell">
        {job.numCompletedStages}/{job.stageIds.size - job.numSkippedStages}
        {if (job.numFailedStages > 0) s"(${job.numFailedStages} failed)"}
        {if (job.numSkippedStages > 0) s"(${job.numSkippedStages} skipped)"}
      </td>
      <td class="progress-cell">
        {UIUtils.makeProgressBar(started = job.numActiveTasks,
        completed = job.numCompletedIndices,
        failed = job.numFailedTasks, skipped = job.numSkippedTasks,
        reasonToNumKilled = job.killedTasksSummary, total = job.numTasks - job.numSkippedTasks)}
      </td>
    </tr>
  }
}
