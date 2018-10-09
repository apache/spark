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

import javax.servlet.http.HttpServletRequest

import scala.collection.mutable
import scala.xml.{Node, NodeSeq}

import org.apache.commons.lang3.StringEscapeUtils

import org.apache.spark.JobExecutionStatus
import org.apache.spark.internal.Logging
import org.apache.spark.ui.{UIUtils, WebUIPage}

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
        val runningPageTable = new RunningExecutionTable(
          parent, currentTime, running.sortBy(_.submissionTime).reverse).toNodeSeq(request)

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
        val completedPageTable = new CompletedExecutionTable(
          parent, currentTime, completed.sortBy(_.submissionTime).reverse).toNodeSeq(request)

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
        val failedPageTable = new FailedExecutionTable(
          parent, currentTime, failed.sortBy(_.submissionTime).reverse).toNodeSeq(request)

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
                <a href="#running-execution-table"><strong>Running Queries:</strong></a>
                {running.size}
              </li>
            }
          }
          {
            if (completed.nonEmpty) {
              <li>
                <a href="#completed-execution-table"><strong>Completed Queries:</strong></a>
                {completed.size}
              </li>
            }
          }
          {
            if (failed.nonEmpty) {
              <li>
                <a href="#failed-execution-table"><strong>Failed Queries:</strong></a>
                {failed.size}
              </li>
            }
          }
        </ul>
      </div>
    UIUtils.headerSparkPage(request, "SQL", summary ++ content, parent, Some(5000))
  }
}

private[ui] abstract class ExecutionTable(
    parent: SQLTab,
    tableId: String,
    currentTime: Long,
    executionUIDatas: Seq[SQLExecutionUIData],
    showRunningJobs: Boolean,
    showSucceededJobs: Boolean,
    showFailedJobs: Boolean) {

  protected def baseHeader: Seq[String] = Seq(
    "ID",
    "Description",
    "Submitted",
    "Duration")

  protected def header: Seq[String]

  protected def row(
      request: HttpServletRequest,
      currentTime: Long,
      executionUIData: SQLExecutionUIData): Seq[Node] = {
    val submissionTime = executionUIData.submissionTime
    val duration = executionUIData.completionTime.map(_.getTime()).getOrElse(currentTime) -
      submissionTime

    def jobLinks(status: JobExecutionStatus): Seq[Node] = {
      executionUIData.jobs.flatMap { case (jobId, jobStatus) =>
        if (jobStatus == status) {
          <a href={jobURL(request, jobId)}>[{jobId.toString}]</a>
        } else {
          None
        }
      }.toSeq
    }

    <tr>
      <td>
        {executionUIData.executionId.toString}
      </td>
      <td>
        {descriptionCell(request, executionUIData)}
      </td>
      <td sorttable_customkey={submissionTime.toString}>
        {UIUtils.formatDate(submissionTime)}
      </td>
      <td sorttable_customkey={duration.toString}>
        {UIUtils.formatDuration(duration)}
      </td>
      {if (showRunningJobs) {
        <td>
          {jobLinks(JobExecutionStatus.RUNNING)}
        </td>
      }}
      {if (showSucceededJobs) {
        <td>
          {jobLinks(JobExecutionStatus.SUCCEEDED)}
        </td>
      }}
      {if (showFailedJobs) {
        <td>
          {jobLinks(JobExecutionStatus.FAILED)}
        </td>
      }}
    </tr>
  }

  private def descriptionCell(
      request: HttpServletRequest,
      execution: SQLExecutionUIData): Seq[Node] = {
    val details = if (execution.details != null && execution.details.nonEmpty) {
      <span onclick="clickDetail(this)" class="expand-details">
        +details
      </span> ++
      <div class="stage-details collapsed">
        <pre>{execution.details}</pre>
      </div>
    } else {
      Nil
    }

    val desc = if (execution.description != null && execution.description.nonEmpty) {
      <a href={executionURL(request, execution.executionId)}>{execution.description}</a>
    } else {
      <a href={executionURL(request, execution.executionId)}>{execution.executionId}</a>
    }

    <div>{desc} {details}</div>
  }

  def toNodeSeq(request: HttpServletRequest): Seq[Node] = {
    UIUtils.listingTable[SQLExecutionUIData](
      header, row(request, currentTime, _), executionUIDatas, id = Some(tableId))
  }

  private def jobURL(request: HttpServletRequest, jobId: Long): String =
    "%s/jobs/job/?id=%s".format(UIUtils.prependBaseUri(request, parent.basePath), jobId)

  private def executionURL(request: HttpServletRequest, executionID: Long): String =
    s"${UIUtils.prependBaseUri(
      request, parent.basePath)}/${parent.prefix}/execution/?id=$executionID"
}

private[ui] class RunningExecutionTable(
    parent: SQLTab,
    currentTime: Long,
    executionUIDatas: Seq[SQLExecutionUIData])
  extends ExecutionTable(
    parent,
    "running-execution-table",
    currentTime,
    executionUIDatas,
    showRunningJobs = true,
    showSucceededJobs = true,
    showFailedJobs = true) {

  override protected def header: Seq[String] =
    baseHeader ++ Seq("Running Job IDs", "Succeeded Job IDs", "Failed Job IDs")
}

private[ui] class CompletedExecutionTable(
    parent: SQLTab,
    currentTime: Long,
    executionUIDatas: Seq[SQLExecutionUIData])
  extends ExecutionTable(
    parent,
    "completed-execution-table",
    currentTime,
    executionUIDatas,
    showRunningJobs = false,
    showSucceededJobs = true,
    showFailedJobs = false) {

  override protected def header: Seq[String] = baseHeader ++ Seq("Job IDs")
}

private[ui] class FailedExecutionTable(
    parent: SQLTab,
    currentTime: Long,
    executionUIDatas: Seq[SQLExecutionUIData])
  extends ExecutionTable(
    parent,
    "failed-execution-table",
    currentTime,
    executionUIDatas,
    showRunningJobs = false,
    showSucceededJobs = true,
    showFailedJobs = true) {

  override protected def header: Seq[String] =
    baseHeader ++ Seq("Succeeded Job IDs", "Failed Job IDs")
}
