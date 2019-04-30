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

package org.apache.spark.sql.execution.ui

import java.net.URLEncoder
import javax.servlet.http.HttpServletRequest

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.xml.{Node, NodeSeq, Unparsed}

import org.apache.spark.JobExecutionStatus
import org.apache.spark.internal.Logging
import org.apache.spark.ui.{PagedDataSource, PagedTable, UIUtils, WebUIPage}
import org.apache.spark.util.Utils

private[ui] class AllExecutionsPage(parent: SQLTab) extends WebUIPage("") with Logging {

  private val sqlStore = parent.sqlStore

  override def render(request: HttpServletRequest): Seq[Node] = {
    val currentTime = System.currentTimeMillis()
    val running = new mutable.ArrayBuffer[SQLExecutionUIData]()
    val completed = new mutable.ArrayBuffer[SQLExecutionUIData]()
    val failed = new mutable.ArrayBuffer[SQLExecutionUIData]()

    sqlStore.executionsList().foreach { e =>
      val isRunning = e.completionTime.isEmpty ||
        e.jobs.exists { case (_, status) => status == JobExecutionStatus.RUNNING }
      val isFailed = e.jobs.exists { case (_, status) => status == JobExecutionStatus.FAILED }
      if (isRunning) {
        running += e
      } else if (isFailed) {
        failed += e
      } else {
        completed += e
      }
    }

    val content = {
      val _content = mutable.ListBuffer[Node]()

      if (running.nonEmpty) {
        val runningPageTable =
          executionsTable(request, "running", running, currentTime, true, true, true)

        _content ++=
          <span id="running" class="collapse-aggregated-runningExecutions collapse-table"
                onClick="collapseTable('collapse-aggregated-runningExecutions',
                'aggregated-runningExecutions')">
            <h4>
              <span class="collapse-table-arrow arrow-open"></span>
              <a>Running Queries ({running.size})</a>
            </h4>
          </span> ++
            <div class="aggregated-runningExecutions collapsible-table">
              {runningPageTable}
            </div>
      }

      if (completed.nonEmpty) {
        val completedPageTable =
          executionsTable(request, "completed", completed, currentTime, false, true, false)

        _content ++=
          <span id="completed" class="collapse-aggregated-completedExecutions collapse-table"
                onClick="collapseTable('collapse-aggregated-completedExecutions',
                'aggregated-completedExecutions')">
            <h4>
              <span class="collapse-table-arrow arrow-open"></span>
              <a>Completed Queries ({completed.size})</a>
            </h4>
          </span> ++
            <div class="aggregated-completedExecutions collapsible-table">
              {completedPageTable}
            </div>
      }

      if (failed.nonEmpty) {
        val failedPageTable =
          executionsTable(request, "failed", failed, currentTime, false, true, true)

        _content ++=
          <span id="failed" class="collapse-aggregated-failedExecutions collapse-table"
                onClick="collapseTable('collapse-aggregated-failedExecutions',
                'aggregated-failedExecutions')">
            <h4>
              <span class="collapse-table-arrow arrow-open"></span>
              <a>Failed Queries ({failed.size})</a>
            </h4>
          </span> ++
            <div class="aggregated-failedExecutions collapsible-table">
              {failedPageTable}
            </div>
      }
      _content
    }
    content ++=
      <script>
        function clickDetail(details) {{
          details.parentNode.querySelector('.stage-details').classList.toggle('collapsed')
        }}
      </script>
    val summary: NodeSeq =
      <div>
        <ul class="unstyled">
          {
            if (running.nonEmpty) {
              <li>
                <a href="#running"><strong>Running Queries:</strong></a>
                {running.size}
              </li>
            }
          }
          {
            if (completed.nonEmpty) {
              <li>
                <a href="#completed"><strong>Completed Queries:</strong></a>
                {completed.size}
              </li>
            }
          }
          {
            if (failed.nonEmpty) {
              <li>
                <a href="#failed"><strong>Failed Queries:</strong></a>
                {failed.size}
              </li>
            }
          }
        </ul>
      </div>

    UIUtils.headerSparkPage(request, "SQL", summary ++ content, parent)
  }

  private def executionsTable(
    request: HttpServletRequest,
    executionTag: String,
    executionData: Seq[SQLExecutionUIData],
    currentTime: Long,
    showRunningJobs: Boolean,
    showSucceededJobs: Boolean,
    showFailedJobs: Boolean): Seq[Node] = {

    val parameterOtherTable = request.getParameterMap().asScala
      .filterNot(_._1.startsWith(executionTag))
      .map { case (name, vals) =>
        name + "=" + vals(0)
      }

    val parameterExecutionPage = request.getParameter(s"$executionTag.page")
    val parameterExecutionSortColumn = request.getParameter(s"$executionTag.sort")
    val parameterExecutionSortDesc = request.getParameter(s"$executionTag.desc")
    val parameterExecutionPageSize = request.getParameter(s"$executionTag.pageSize")

    val executionPage = Option(parameterExecutionPage).map(_.toInt).getOrElse(1)
    val executionSortColumn = Option(parameterExecutionSortColumn).map { sortColumn =>
      UIUtils.decodeURLParameter(sortColumn)
    }.getOrElse("ID")
    val executionSortDesc = Option(parameterExecutionSortDesc).map(_.toBoolean).getOrElse(
      // New executions should be shown above old executions by default.
      executionSortColumn == "ID"
    )
    val executionPageSize = Option(parameterExecutionPageSize).map(_.toInt).getOrElse(100)

    val tableHeaderId = executionTag // "running", "completed" or "failed"

    try {
      new ExecutionPagedTable(
        request,
        parent,
        executionData,
        tableHeaderId,
        executionTag,
        UIUtils.prependBaseUri(request, parent.basePath),
        "SQL", // subPath
        parameterOtherTable,
        currentTime,
        pageSize = executionPageSize,
        sortColumn = executionSortColumn,
        desc = executionSortDesc,
        showRunningJobs,
        showSucceededJobs,
        showFailedJobs).table(executionPage)
    } catch {
      case e@(_: IllegalArgumentException | _: IndexOutOfBoundsException) =>
        <div class="alert alert-error">
          <p>Error while rendering execution table:</p>
          <pre>
            {Utils.exceptionString(e)}
          </pre>
        </div>
    }
  }
}

private[ui] class ExecutionPagedTable(
    request: HttpServletRequest,
    parent: SQLTab,
    data: Seq[SQLExecutionUIData],
    tableHeaderId: String,
    executionTag: String,
    basePath: String,
    subPath: String,
    parameterOtherTable: Iterable[String],
    currentTime: Long,
    pageSize: Int,
    sortColumn: String,
    desc: Boolean,
    showRunningJobs: Boolean,
    showSucceededJobs: Boolean,
    showFailedJobs: Boolean) extends PagedTable[ExecutionTableRowData] {

  override val dataSource = new ExecutionDataSource(
    request,
    parent,
    data,
    basePath,
    currentTime,
    pageSize,
    sortColumn,
    desc,
    showRunningJobs,
    showSucceededJobs,
    showFailedJobs)

  private val parameterPath = s"$basePath/$subPath/?${parameterOtherTable.mkString("&")}"

  override def tableId: String = s"$executionTag-table"

  override def tableCssClass: String =
    "table table-bordered table-condensed table-striped " +
      "table-head-clickable table-cell-width-limited"

  override def pageLink(page: Int): String = {
    val encodedSortColumn = URLEncoder.encode(sortColumn, "UTF-8")
    parameterPath +
      s"&$pageNumberFormField=$page" +
      s"&$executionTag.sort=$encodedSortColumn" +
      s"&$executionTag.desc=$desc" +
      s"&$pageSizeFormField=$pageSize" +
      s"#$tableHeaderId"
  }

  override def pageSizeFormField: String = s"$executionTag.pageSize"

  override def pageNumberFormField: String = s"$executionTag.page"

  override def goButtonFormPath: String = {
    val encodedSortColumn = URLEncoder.encode(sortColumn, "UTF-8")
    s"$parameterPath&$executionTag.sort=$encodedSortColumn&$executionTag.desc=$desc#$tableHeaderId"
  }

  override def headers: Seq[Node] = {
    // Information for each header: title, sortable
    val executionHeadersAndCssClasses: Seq[(String, Boolean)] =
      Seq(
        ("ID", true),
        ("Description", true),
        ("Submitted", true),
        ("Duration", true)) ++ {
        if (showRunningJobs && showSucceededJobs && showFailedJobs) {
          Seq(
            ("Running Job IDs", true),
            ("Succeeded Job IDs", true),
            ("Failed Job IDs", true))
        } else if (showSucceededJobs && showFailedJobs) {
          Seq(
            ("Succeeded Job IDs", true),
            ("Failed Job IDs", true))
        } else {
          Seq(("Job IDs", true))
        }
      }

    val sortableColumnHeaders = executionHeadersAndCssClasses.filter {
      case (_, sortable) => sortable
    }.map { case (title, _) => title }

    require(sortableColumnHeaders.contains(sortColumn), s"Unknown column: $sortColumn")

    val headerRow: Seq[Node] = {
      executionHeadersAndCssClasses.map { case (header, sortable) =>
        if (header == sortColumn) {
          val headerLink = Unparsed(
            parameterPath +
              s"&$executionTag.sort=${URLEncoder.encode(header, "UTF-8")}" +
              s"&$executionTag.desc=${!desc}" +
              s"&$executionTag.pageSize=$pageSize" +
              s"#$tableHeaderId")
          val arrow = if (desc) "&#x25BE;" else "&#x25B4;" // UP or DOWN

          <th>
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
                s"&$executionTag.sort=${URLEncoder.encode(header, "UTF-8")}" +
                s"&$executionTag.pageSize=$pageSize" +
                s"#$tableHeaderId")

            <th>
              <a href={headerLink}>
                {header}
              </a>
            </th>
          } else {
            <th>
              {header}
            </th>
          }
        }
      }
    }
    <thead>
      {headerRow}
    </thead>
  }

  override def row(executionTableRow: ExecutionTableRowData): Seq[Node] = {
    val executionUIData = executionTableRow.executionUIData
    val submissionTime = executionUIData.submissionTime
    val duration = executionTableRow.duration

    def jobLinks(jobData: Seq[Int]): Seq[Node] = {
      jobData.map { jobId =>
        <a href={jobURL(request, jobId)}>[{jobId.toString}]</a>
      }
    }

    <tr>
      <td>
        {executionUIData.executionId.toString}
      </td>
      <td>
        {descriptionCell(executionUIData)}
      </td>
      <td sorttable_customkey={submissionTime.toString}>
        {UIUtils.formatDate(submissionTime)}
      </td>
      <td sorttable_customkey={duration.toString}>
        {UIUtils.formatDuration(duration)}
      </td>
      {if (showRunningJobs) {
        <td>
          {jobLinks(executionTableRow.runningJobData)}
        </td>
      }}
      {if (showSucceededJobs) {
        <td>
          {jobLinks(executionTableRow.completedJobData)}
        </td>
      }}
      {if (showFailedJobs) {
        <td>
          {jobLinks(executionTableRow.failedJobData)}
        </td>
      }}
    </tr>
  }

  private def descriptionCell(execution: SQLExecutionUIData): Seq[Node] = {
    val details = if (execution.details != null && execution.details.nonEmpty) {
      <span onclick="this.parentNode.querySelector('.stage-details').classList.toggle('collapsed')"
            class="expand-details">
        +details
      </span> ++
      <div class="stage-details collapsed">
        <pre>{execution.description}<br></br>{execution.details}</pre>
      </div>
    } else {
      Nil
    }

    val desc = if (execution.description != null && execution.description.nonEmpty) {
      <a href={executionURL(execution.executionId)} class="description-input">
        {execution.description}</a>
    } else {
      <a href={executionURL(execution.executionId)}>{execution.executionId}</a>
    }

    <div>{desc}{details}</div>
  }

  private def jobURL(request: HttpServletRequest, jobId: Long): String =
    "%s/jobs/job/?id=%s".format(UIUtils.prependBaseUri(request, parent.basePath), jobId)

  private def executionURL(executionID: Long): String =
    s"${UIUtils.prependBaseUri(
      request, parent.basePath)}/${parent.prefix}/execution/?id=$executionID"
}


private[ui] class ExecutionTableRowData(
    val submissionTime: Long,
    val duration: Long,
    val executionUIData: SQLExecutionUIData,
    val runningJobData: Seq[Int],
    val completedJobData: Seq[Int],
    val failedJobData: Seq[Int])


private[ui] class ExecutionDataSource(
    request: HttpServletRequest,
    parent: SQLTab,
    executionData: Seq[SQLExecutionUIData],
    basePath: String,
    currentTime: Long,
    pageSize: Int,
    sortColumn: String,
    desc: Boolean,
    showRunningJobs: Boolean,
    showSucceededJobs: Boolean,
    showFailedJobs: Boolean) extends PagedDataSource[ExecutionTableRowData](pageSize) {

  // Convert ExecutionData to ExecutionTableRowData which contains the final contents to show
  // in the table so that we can avoid creating duplicate contents during sorting the data
  private val data = executionData.map(executionRow).sorted(ordering(sortColumn, desc))

  private var _sliceExecutionIds: Set[Int] = _

  override def dataSize: Int = data.size

  override def sliceData(from: Int, to: Int): Seq[ExecutionTableRowData] = {
    val r = data.slice(from, to)
    _sliceExecutionIds = r.map(_.executionUIData.executionId.toInt).toSet
    r
  }

  private def executionRow(executionUIData: SQLExecutionUIData): ExecutionTableRowData = {
    val submissionTime = executionUIData.submissionTime
    val duration = executionUIData.completionTime.map(_.getTime())
      .getOrElse(currentTime) - submissionTime

    val runningJobData = if (showRunningJobs) {
      executionUIData.jobs.filter {
        case (_, jobStatus) => jobStatus == JobExecutionStatus.RUNNING
      }.map { case (jobId, _) => jobId }.toSeq.sorted
    } else Seq.empty

    val completedJobData = if (showSucceededJobs) {
      executionUIData.jobs.filter {
        case (_, jobStatus) => jobStatus == JobExecutionStatus.SUCCEEDED
      }.map { case (jobId, _) => jobId }.toSeq.sorted
    } else Seq.empty

    val failedJobData = if (showFailedJobs) {
      executionUIData.jobs.filter {
        case (_, jobStatus) => jobStatus == JobExecutionStatus.FAILED
      }.map { case (jobId, _) => jobId }.toSeq.sorted
    } else Seq.empty

    new ExecutionTableRowData(
      submissionTime,
      duration,
      executionUIData,
      runningJobData,
      completedJobData,
      failedJobData)
  }

  /** Return Ordering according to sortColumn and desc. */
  private def ordering(sortColumn: String, desc: Boolean): Ordering[ExecutionTableRowData] = {
    val ordering: Ordering[ExecutionTableRowData] = sortColumn match {
      case "ID" => Ordering.by(_.executionUIData.executionId)
      case "Description" => Ordering.by(_.executionUIData.description)
      case "Submitted" => Ordering.by(_.executionUIData.submissionTime)
      case "Duration" => Ordering.by(_.duration)
      case "Job IDs" | "Succeeded Job IDs" => Ordering by (_.completedJobData.headOption)
      case "Running Job IDs" => Ordering.by(_.runningJobData.headOption)
      case "Failed Job IDs" => Ordering.by(_.failedJobData.headOption)
      case unknownColumn => throw new IllegalArgumentException(s"Unknown column: $unknownColumn")
    }
    if (desc) {
      ordering.reverse
    } else {
      ordering
    }
  }
}
