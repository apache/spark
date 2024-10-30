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
import java.nio.charset.StandardCharsets.UTF_8

import scala.collection.mutable
import scala.xml.{Node, NodeSeq}

import jakarta.servlet.http.HttpServletRequest

import org.apache.spark.JobExecutionStatus
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.UI.UI_SQL_GROUP_SUB_EXECUTION_ENABLED
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.ui.{PagedDataSource, PagedTable, UIUtils, WebUIPage}
import org.apache.spark.util.Utils

private[ui] class AllExecutionsPage(parent: SQLTab) extends WebUIPage("") with Logging {

  private val sqlStore = parent.sqlStore
  private val groupSubExecutionEnabled = parent.conf.get(UI_SQL_GROUP_SUB_EXECUTION_ENABLED)

  override def render(request: HttpServletRequest): Seq[Node] = {
    val currentTime = System.currentTimeMillis()
    val running = new mutable.ArrayBuffer[SQLExecutionUIData]()
    val completed = new mutable.ArrayBuffer[SQLExecutionUIData]()
    val failed = new mutable.ArrayBuffer[SQLExecutionUIData]()
    val executionIdToSubExecutions =
      new mutable.HashMap[Long, mutable.ArrayBuffer[SQLExecutionUIData]]()

    sqlStore.executionsList().foreach { e =>
      def processExecution(e: SQLExecutionUIData): Unit = e.executionStatus match {
        case "RUNNING" => running += e
        case "COMPLETED" => completed += e
        case "FAILED" => failed += e
      }

      // group the sub execution only if the root execution will be displayed (i.e. not missing)
      if (groupSubExecutionEnabled &&
          e.executionId != e.rootExecutionId &&
          executionIdToSubExecutions.contains(e.rootExecutionId)) {
        executionIdToSubExecutions(e.rootExecutionId) += e
      } else {
        if (groupSubExecutionEnabled) {
          // add the execution id to indicate it'll be displayed as root, so the executions with
          // the same root execution id will be added here and displayed as sub execution.
          // If the root execution is not found (e.g. event loss), then the sub executions will
          // be displayed in the root list instead.
          // NOTE: this code assumes the root execution id always comes first. which is guaranteed
          // by the `sqlStore.executionsList()`
          executionIdToSubExecutions(e.executionId) = new mutable.ArrayBuffer[SQLExecutionUIData]()
        }
        processExecution(e)
      }
    }

    val content = {
      val _content = mutable.ListBuffer[Node]()

      if (running.nonEmpty) {
        val runningPageTable =
          executionsTable(
            request,
            "running",
            running.toSeq,
            executionIdToSubExecutions.toMap.transform((_, v) => v.toSeq),
            currentTime,
            showErrorMessage = false,
            showRunningJobs = true,
            showSucceededJobs = true,
            showFailedJobs = true)

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
        val completedPageTable = executionsTable(
          request,
          "completed",
          completed.toSeq,
          executionIdToSubExecutions.toMap.transform((_, v) => v.toSeq),
          currentTime,
          showErrorMessage = false,
          showRunningJobs = false,
          showSucceededJobs = true,
          showFailedJobs = false)

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
          executionsTable(
            request,
            "failed",
            failed.toSeq,
            executionIdToSubExecutions.toMap.transform((_, v) => v.toSeq),
            currentTime,
            showErrorMessage = true,
            showRunningJobs = false,
            showSucceededJobs = true,
            showFailedJobs = true)

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
        <ul class="list-unstyled">
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

    UIUtils.headerSparkPage(request, "SQL / DataFrame", summary ++ content, parent)
  }

  private def executionsTable(
    request: HttpServletRequest,
    executionTag: String,
    executionData: Seq[SQLExecutionUIData],
    executionIdToSubExecutions: Map[Long, Seq[SQLExecutionUIData]],
    currentTime: Long,
    showErrorMessage: Boolean,
    showRunningJobs: Boolean,
    showSucceededJobs: Boolean,
    showFailedJobs: Boolean): Seq[Node] = {

    val executionPage =
      Option(request.getParameter(s"$executionTag.page")).map(_.toInt).getOrElse(1)

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
        currentTime,
        showErrorMessage,
        showRunningJobs,
        showSucceededJobs,
        showFailedJobs,
        executionIdToSubExecutions).table(executionPage)
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
    currentTime: Long,
    showErrorMessage: Boolean,
    showRunningJobs: Boolean,
    showSucceededJobs: Boolean,
    showFailedJobs: Boolean,
    subExecutions: Map[Long, Seq[SQLExecutionUIData]] = Map.empty)
  extends PagedTable[ExecutionTableRowData] {

  private val (sortColumn, desc, pageSize) = getTableParameters(request, executionTag, "ID")

  private val encodedSortColumn = URLEncoder.encode(sortColumn, UTF_8.name())

  override val dataSource = new ExecutionDataSource(
    data,
    currentTime,
    pageSize,
    sortColumn,
    desc,
    showRunningJobs,
    showSucceededJobs,
    showFailedJobs,
    subExecutions)

  private val parameterPath =
    s"$basePath/$subPath/?${getParameterOtherTable(request, executionTag)}"

  private val showSubExecutions = subExecutions.exists(_._2.nonEmpty)

  override def tableId: String = s"$executionTag-table"

  override def tableCssClass: String =
    "table table-bordered table-sm table-striped table-head-clickable table-cell-width-limited"

  override def pageLink(page: Int): String = {
    parameterPath +
      s"&$pageNumberFormField=$page" +
      s"&$executionTag.sort=$encodedSortColumn" +
      s"&$executionTag.desc=$desc" +
      s"&$pageSizeFormField=$pageSize" +
      s"#$tableHeaderId"
  }

  override def pageSizeFormField: String = s"$executionTag.pageSize"

  override def pageNumberFormField: String = s"$executionTag.page"

  override def goButtonFormPath: String =
    s"$parameterPath&$executionTag.sort=$encodedSortColumn&$executionTag.desc=$desc#$tableHeaderId"

  // Information for each header: title, sortable, tooltip
  private val headerInfo: Seq[(String, Boolean, Option[String])] = {
    Seq(
      ("ID", true, None),
      ("Description", true, None),
      ("Submitted", true, None),
      ("Duration", true, Some("Time from query submission to completion (or if still executing," +
        " time since submission)"))) ++ {
      if (showRunningJobs && showSucceededJobs && showFailedJobs) {
        Seq(
          ("Running Job IDs", true, None),
          ("Succeeded Job IDs", true, None),
          ("Failed Job IDs", true, None))
      } else if (showSucceededJobs && showFailedJobs) {
        Seq(
          ("Succeeded Job IDs", true, None),
          ("Failed Job IDs", true, None))
      } else {
        Seq(("Job IDs", true, None))
      }
    } ++ {
      if (showErrorMessage) {
        Seq(("Error Message", true, None))
      } else {
        Nil
      }
    } ++ {
      if (showSubExecutions) {
        Seq(("Sub Execution IDs", true, None))
      } else {
        Nil
      }
    }
  }

  override def headers: Seq[Node] = {
    isSortColumnValid(headerInfo, sortColumn)

    headerRow(headerInfo, desc, pageSize, sortColumn, parameterPath,
      executionTag, tableHeaderId)
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

    def executionLinks(executionData: Seq[Long]): Seq[Node] = {
      val details = if (executionData.nonEmpty) {
        val onClickScript = "this.parentNode.parentNode.nextElementSibling.nextElementSibling" +
          ".classList.toggle('collapsed')"
        <span onclick={onClickScript} class="expand-details">
          +details
        </span>
      } else {
        Nil
      }

      <div>
        {
          executionData.map { executionId =>
            <a href={executionURL(executionId)}>[{executionId.toString}]</a>
          }
        }
      </div> ++ details
    }

    val baseRow: Seq[Node] = {
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
        {if (showErrorMessage) {
          UIUtils.errorMessageCell(executionUIData.errorMessage.getOrElse(""))
        }}
        {if (showSubExecutions) {
          <td>
            {executionLinks(executionTableRow.subExecutionData.map(_.executionUIData.executionId))}
          </td>
        }}
      </tr>
    }

    val subRow: Seq[Node] = if (executionTableRow.subExecutionData.nonEmpty) {
      <tr></tr>
      <tr class="sub-execution-list collapsed">
        <td></td>
        <td colspan={s"${headerInfo.length - 1}"}>
          <table class="table table-bordered table-sm table-cell-width-limited">
            <thead>
              <tr>
                {headerInfo.dropRight(1).map(info => <th>{info._1}</th>)}
              </tr>
            </thead>
            <tbody>
              {
                executionTableRow.subExecutionData.map { rowData =>
                  val executionUIData = rowData.executionUIData
                  val submissionTime = executionUIData.submissionTime
                  val duration = rowData.duration
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
                        {jobLinks(rowData.runningJobData)}
                      </td>
                    }}
                    {if (showSucceededJobs) {
                      <td>
                        {jobLinks(rowData.completedJobData)}
                      </td>
                    }}
                    {if (showFailedJobs) {
                      <td>
                        {jobLinks(rowData.failedJobData)}
                      </td>
                    }}
                  </tr>
                }
              }
            </tbody>
          </table>
        </td>
      </tr>
    } else {
      Nil
    }
    baseRow ++ subRow
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
    val duration: Long,
    val executionUIData: SQLExecutionUIData,
    val runningJobData: Seq[Int],
    val completedJobData: Seq[Int],
    val failedJobData: Seq[Int],
    val subExecutionData: Seq[ExecutionTableRowData])


private[ui] class ExecutionDataSource(
    executionData: Seq[SQLExecutionUIData],
    currentTime: Long,
    pageSize: Int,
    sortColumn: String,
    desc: Boolean,
    showRunningJobs: Boolean,
    showSucceededJobs: Boolean,
    showFailedJobs: Boolean,
    subExecutions: Map[Long, Seq[SQLExecutionUIData]])
  extends PagedDataSource[ExecutionTableRowData](pageSize) {

  // Convert ExecutionData to ExecutionTableRowData which contains the final contents to show
  // in the table so that we can avoid creating duplicate contents during sorting the data
  private val data = executionData.map(executionRow).sorted(ordering(sortColumn, desc))

  override def dataSize: Int = data.size

  override def sliceData(from: Int, to: Int): Seq[ExecutionTableRowData] = data.slice(from, to)

  private def executionRow(executionUIData: SQLExecutionUIData): ExecutionTableRowData = {
    val duration = executionUIData.completionTime.map(_.getTime())
      .getOrElse(currentTime) - executionUIData.submissionTime

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

    val executions = subExecutions.get(executionUIData.executionId) match {
      case Some(executions) => executions.map(executionRow)
      case _ => Seq.empty
    }

    new ExecutionTableRowData(
      duration,
      executionUIData,
      runningJobData,
      completedJobData,
      failedJobData,
      executions)
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
      case "Error Message" => Ordering.by(_.executionUIData.errorMessage)
      case unknownColumn => throw QueryExecutionErrors.unknownColumnError(unknownColumn)
    }
    if (desc) {
      ordering.reverse
    } else {
      ordering
    }
  }
}
