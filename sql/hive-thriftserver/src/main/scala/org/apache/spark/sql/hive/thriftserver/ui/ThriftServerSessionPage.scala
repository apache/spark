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

package org.apache.spark.sql.hive.thriftserver.ui

import java.util.Calendar
import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.apache.commons.lang3.StringEscapeUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2.{ExecutionInfo, ExecutionState}
import org.apache.spark.ui._
import org.apache.spark.ui.UIUtils._

/** Page for Spark Web UI that shows statistics of jobs running in the thrift server */
private[ui] class ThriftServerSessionPage(parent: ThriftServerTab)
  extends WebUIPage("session") with Logging {

  private val listener = parent.listener
  private val startTime = Calendar.getInstance().getTime()
  private val emptyCell = "-"

  /** Render the page */
  def render(request: HttpServletRequest): Seq[Node] = {
    val parameterId = request.getParameter("id")
    require(parameterId != null && parameterId.nonEmpty, "Missing id parameter")

    val content =
      listener.synchronized { // make sure all parts in this page are consistent
        val sessionStat = listener.getSession(parameterId).getOrElse(null)
        require(sessionStat != null, "Invalid sessionID[" + parameterId + "]")

        generateBasicStats() ++
        <br/> ++
        <h4>
        User {sessionStat.userName},
        IP {sessionStat.ip},
        Session created at {formatDate(sessionStat.startTimestamp)},
        Total run {sessionStat.totalExecution} SQL
        </h4> ++
        generateSQLStatsTable(request, sessionStat.sessionId)
      }
    UIUtils.headerSparkPage(request, "JDBC/ODBC Session", content, parent)
  }

  /** Generate basic stats of the thrift server program */
  private def generateBasicStats(): Seq[Node] = {
    val timeSinceStart = System.currentTimeMillis() - startTime.getTime
    <ul class ="unstyled">
      <li>
        <strong>Started at: </strong> {formatDate(startTime)}
      </li>
      <li>
        <strong>Time since start: </strong>{formatDurationVerbose(timeSinceStart)}
      </li>
    </ul>
  }

  /** Generate stats of batch statements of the thrift server program */
  private def generateSQLStatsTable(request: HttpServletRequest, sessionID: String): Seq[Node] = {
    val executionList = listener.getExecutionList
      .filter(_.sessionId == sessionID)
    val numStatement = executionList.size
    val table = if (numStatement > 0) {
      val headerRow = Seq("User", "JobID", "GroupID", "Start Time", "Finish Time", "Duration",
        "Statement", "State", "Detail")
      val dataRows = executionList.sortBy(_.startTimestamp).reverse

      def generateDataRow(info: ExecutionInfo): Seq[Node] = {
        val jobLink = info.jobId.map { id: String =>
          <a href={"%s/jobs/job/?id=%s".format(
              UIUtils.prependBaseUri(request, parent.basePath), id)}>
            [{id}]
          </a>
        }
        val detail = if (info.state == ExecutionState.FAILED) info.detail else info.executePlan
        <tr>
          <td>{info.userName}</td>
          <td>
            {jobLink}
          </td>
          <td>{info.groupId}</td>
          <td>{formatDate(info.startTimestamp)}</td>
          <td>{formatDate(info.finishTimestamp)}</td>
          <td>{formatDurationOption(Some(info.totalTime))}</td>
          <td>{info.statement}</td>
          <td>{info.state}</td>
          {errorMessageCell(detail)}
        </tr>
      }

      Some(UIUtils.listingTable(headerRow, generateDataRow,
        dataRows, false, None, Seq(null), false))
    } else {
      None
    }

    val content =
      <h5>SQL Statistics</h5> ++
        <div>
          <ul class="unstyled">
            {table.getOrElse("No statistics have been generated yet.")}
          </ul>
        </div>

    content
  }

  private def errorMessageCell(errorMessage: String): Seq[Node] = {
    val isMultiline = errorMessage.indexOf('\n') >= 0
    val errorSummary = StringEscapeUtils.escapeHtml4(
      if (isMultiline) {
        errorMessage.substring(0, errorMessage.indexOf('\n'))
      } else {
        errorMessage
      })
    val details = if (isMultiline) {
      // scalastyle:off
      <span onclick="this.parentNode.querySelector('.stacktrace-details').classList.toggle('collapsed')"
            class="expand-details">
        + details
      </span> ++
      <div class="stacktrace-details collapsed">
        <pre>{errorMessage}</pre>
      </div>
      // scalastyle:on
    } else {
      ""
    }
    <td>{errorSummary}{details}</td>
  }

  /**
   * Returns a human-readable string representing a duration such as "5 second 35 ms"
   */
  private def formatDurationOption(msOption: Option[Long]): String = {
    msOption.map(formatDurationVerbose).getOrElse(emptyCell)
  }

  /** Generate HTML table from string data */
  private def listingTable(headers: Seq[String], data: Seq[Seq[String]]) = {
    def generateDataRow(data: Seq[String]): Seq[Node] = {
      <tr> {data.map(d => <td>{d}</td>)} </tr>
    }
    UIUtils.listingTable(headers, generateDataRow, data, fixedWidth = true)
  }
}
