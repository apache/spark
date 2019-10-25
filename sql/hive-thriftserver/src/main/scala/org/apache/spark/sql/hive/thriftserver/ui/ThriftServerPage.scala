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

import java.net.URLEncoder
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Calendar
import javax.servlet.http.HttpServletRequest

import scala.collection.JavaConverters._
import scala.xml.{Node, Unparsed}

import org.apache.commons.text.StringEscapeUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2.{ExecutionInfo, ExecutionState, SessionInfo}
import org.apache.spark.sql.hive.thriftserver.ui.ToolTips._
import org.apache.spark.ui._
import org.apache.spark.ui.UIUtils._
import org.apache.spark.util.Utils

/** Page for Spark Web UI that shows statistics of the thrift server */
private[ui] class ThriftServerPage(parent: ThriftServerTab) extends WebUIPage("") with Logging {

  private val listener = parent.listener
  private val startTime = Calendar.getInstance().getTime()
  private val emptyCell = "-"

  /** Render the page */
  def render(request: HttpServletRequest): Seq[Node] = {
    val content =
      listener.synchronized { // make sure all parts in this page are consistent
        generateBasicStats() ++
        <br/> ++
        <h4>
        {listener.getOnlineSessionNum} session(s) are online,
        running {listener.getTotalRunning} SQL statement(s)
        </h4> ++
        generateSessionStatsTable(request) ++
        generateSQLStatsTable(request)
      }
    UIUtils.headerSparkPage(request, "JDBC/ODBC Server", content, parent)
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
  private def generateSQLStatsTable(request: HttpServletRequest): Seq[Node] = {

    val numStatement = listener.getExecutionList.size

    val table = if (numStatement > 0) {

      val sqlTableTag = "sqlstat"

      val parameterOtherTable = request.getParameterMap().asScala
        .filterNot(_._1.startsWith(sqlTableTag))
        .map { case (name, vals) =>
          name + "=" + vals(0)
        }

      val parameterSqlTablePage = request.getParameter(s"$sqlTableTag.page")
      val parameterSqlTableSortColumn = request.getParameter(s"$sqlTableTag.sort")
      val parameterSqlTableSortDesc = request.getParameter(s"$sqlTableTag.desc")
      val parameterSqlPageSize = request.getParameter(s"$sqlTableTag.pageSize")

      val sqlTablePage = Option(parameterSqlTablePage).map(_.toInt).getOrElse(1)
      val sqlTableSortColumn = Option(parameterSqlTableSortColumn).map { sortColumn =>
        UIUtils.decodeURLParameter(sortColumn)
      }.getOrElse("Start Time")
      val sqlTableSortDesc = Option(parameterSqlTableSortDesc).map(_.toBoolean).getOrElse(
        // New executions should be shown above old executions by default.
        sqlTableSortColumn == "Start Time"
      )
      val sqlTablePageSize = Option(parameterSqlPageSize).map(_.toInt).getOrElse(100)

      try {
        Some(new SqlStatsPagedTable(
          request,
          parent,
          listener.getExecutionList,
          "sqlserver",
          UIUtils.prependBaseUri(request, parent.basePath),
          parameterOtherTable,
          sqlTableTag,
          pageSize = sqlTablePageSize,
          sortColumn = sqlTableSortColumn,
          desc = sqlTableSortDesc
        ).table(sqlTablePage))
      } catch {
        case e@(_: IllegalArgumentException | _: IndexOutOfBoundsException) =>
          Some(<div class="alert alert-error">
            <p>Error while rendering job table:</p>
            <pre>
              {Utils.exceptionString(e)}
            </pre>
          </div>)
      }
    } else {
      None
    }

    val content =
      <h5 id="sqlstat">SQL Statistics ({numStatement})</h5> ++
        <div>
          <ul class="unstyled">
            {table.getOrElse("No statistics have been generated yet.")}
          </ul>
        </div>

    content
  }

  /** Generate stats of batch sessions of the thrift server program */
  private def generateSessionStatsTable(request: HttpServletRequest): Seq[Node] = {
    val sessionList = listener.getSessionList
    val numBatches = sessionList.size
    val table = if (numBatches > 0) {
      val dataRows = sessionList.sortBy(_.startTimestamp).reverse
      val headerRow = Seq("User", "IP", "Session ID", "Start Time", "Finish Time", "Duration",
        "Total Execute")
      def generateDataRow(session: SessionInfo): Seq[Node] = {
        val sessionLink = "%s/%s/session/?id=%s".format(
          UIUtils.prependBaseUri(request, parent.basePath), parent.prefix, session.sessionId)
        <tr>
          <td> {session.userName} </td>
          <td> {session.ip} </td>
          <td> <a href={sessionLink}> {session.sessionId} </a> </td>
          <td> {formatDate(session.startTimestamp)} </td>
          <td> {if (session.finishTimestamp > 0) formatDate(session.finishTimestamp)} </td>
          <td sorttable_customkey={session.totalTime.toString}>
            {formatDurationOption(Some(session.totalTime))} </td>
          <td> {session.totalExecution.toString} </td>
        </tr>
      }
      Some(UIUtils.listingTable(headerRow, generateDataRow, dataRows, true, None, Seq(null), false))
    } else {
      None
    }

    val content =
      <h5 id="sessionstat">Session Statistics ({numBatches})</h5> ++
      <div>
        <ul class="unstyled">
          {table.getOrElse("No statistics have been generated yet.")}
        </ul>
      </div>

    content
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

private[ui] class SqlStatsPagedTable(
    request: HttpServletRequest,
    parent: ThriftServerTab,
    data: Seq[ExecutionInfo],
    subPath: String,
    basePath: String,
    parameterOtherTable: Iterable[String],
    sqlStatsTableTag: String,
    pageSize: Int,
    sortColumn: String,
    desc: Boolean) extends PagedTable[SqlStatsTableRow] {

  override val dataSource = new SqlStatsTableDataSource(data, pageSize, sortColumn, desc)

  private val parameterPath = s"$basePath/$subPath/?${parameterOtherTable.mkString("&")}"

  override def tableId: String = sqlStatsTableTag

  override def tableCssClass: String =
    "table table-bordered table-condensed table-striped " +
      "table-head-clickable table-cell-width-limited"

  override def pageLink(page: Int): String = {
    val encodedSortColumn = URLEncoder.encode(sortColumn, UTF_8.name())
    parameterPath +
      s"&$pageNumberFormField=$page" +
      s"&$sqlStatsTableTag.sort=$encodedSortColumn" +
      s"&$sqlStatsTableTag.desc=$desc" +
      s"&$pageSizeFormField=$pageSize"
  }

  override def pageSizeFormField: String = s"$sqlStatsTableTag.pageSize"

  override def pageNumberFormField: String = s"$sqlStatsTableTag.page"

  override def goButtonFormPath: String = {
    val encodedSortColumn = URLEncoder.encode(sortColumn, UTF_8.name())
    s"$parameterPath&$sqlStatsTableTag.sort=$encodedSortColumn&$sqlStatsTableTag.desc=$desc"
  }

  override def headers: Seq[Node] = {
    val sqlTableHeaders = Seq("User", "JobID", "GroupID", "Start Time", "Finish Time",
      "Close Time", "Execution Time", "Duration", "Statement", "State", "Detail")

    val tooltips = Seq(None, None, None, None, Some(THRIFT_SERVER_FINISH_TIME),
      Some(THRIFT_SERVER_CLOSE_TIME), Some(THRIFT_SERVER_EXECUTION),
      Some(THRIFT_SERVER_DURATION), None, None, None)

    assert(sqlTableHeaders.length == tooltips.length)

    val headerRow: Seq[Node] = {
      sqlTableHeaders.zip(tooltips).map { case (header, tooltip) =>
        if (header == sortColumn) {
          val headerLink = Unparsed(
            parameterPath +
              s"&$sqlStatsTableTag.sort=${URLEncoder.encode(header, UTF_8.name())}" +
              s"&$sqlStatsTableTag.desc=${!desc}" +
              s"&$sqlStatsTableTag.pageSize=$pageSize" +
              s"#$sqlStatsTableTag")
          val arrow = if (desc) "&#x25BE;" else "&#x25B4;" // UP or DOWN

          if (tooltip.nonEmpty) {
            <th>
              <a href={headerLink}>
                <span data-toggle="tooltip" title={tooltip.get}>
                  {header}&nbsp;{Unparsed(arrow)}
                </span>
              </a>
            </th>
          } else {
            <th>
              <a href={headerLink}>
                {header}&nbsp;{Unparsed(arrow)}
              </a>
            </th>
          }
        } else {
          val headerLink = Unparsed(
            parameterPath +
              s"&$sqlStatsTableTag.sort=${URLEncoder.encode(header, UTF_8.name())}" +
              s"&$sqlStatsTableTag.pageSize=$pageSize" +
              s"#$sqlStatsTableTag")

          if(tooltip.nonEmpty) {
            <th>
              <a href={headerLink}>
                <span data-toggle="tooltip" title={tooltip.get}>
                  {header}
                </span>
              </a>
            </th>
          } else {
            <th>
              <a href={headerLink}>
                {header}
              </a>
            </th>
          }
        }
      }
    }
    <thead>
      {headerRow}
    </thead>
  }

  override def row(sqlStatsTableRow: SqlStatsTableRow): Seq[Node] = {
    val info = sqlStatsTableRow.executionInfo
    val startTime = info.startTimestamp
    val executionTime = sqlStatsTableRow.executionTime
    val duration = sqlStatsTableRow.duration

    def jobLinks(jobData: Seq[String]): Seq[Node] = {
      jobData.map { jobId =>
        <a href={jobURL(request, jobId)}>[{jobId.toString}]</a>
      }
    }

    <tr>
      <td>
        {info.userName}
      </td>
      <td>
        {jobLinks(sqlStatsTableRow.jobId)}
      </td>
      <td>
        {info.groupId}
      </td>
      <td >
        {UIUtils.formatDate(startTime)}
      </td>
      <td>
        {if (info.finishTimestamp > 0) formatDate(info.finishTimestamp)}
      </td>
      <td>
        {if (info.closeTimestamp > 0) formatDate(info.closeTimestamp)}
      </td>
      <td >
        {UIUtils.formatDuration(executionTime)}
      </td>
      <td >
        {UIUtils.formatDuration(duration)}
      </td>
      <td>
        {info.statement}
      </td>
      <td>
        {info.state}
      </td>
      {errorMessageCell(sqlStatsTableRow.detail)}
    </tr>
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
          <pre>
            {errorMessage}
          </pre>
        </div>
      // scalastyle:on
    } else {
      ""
    }
    <td>
      {errorSummary}{details}
    </td>
  }

  private def jobURL(request: HttpServletRequest, jobId: String): String =
    "%s/jobs/job/?id=%s".format(UIUtils.prependBaseUri(request, parent.basePath), jobId)
}

  private[ui] class SqlStatsTableRow(
    val jobId: Seq[String],
    val duration: Long,
    val executionTime: Long,
    val executionInfo: ExecutionInfo,
    val detail: String)

  private[ui] class SqlStatsTableDataSource(
    info: Seq[ExecutionInfo],
    pageSize: Int,
    sortColumn: String,
    desc: Boolean) extends PagedDataSource[SqlStatsTableRow](pageSize) {

    // Convert ExecutionInfo to SqlStatsTableRow which contains the final contents to show in
    // the table so that we can avoid creating duplicate contents during sorting the data
    private val data = info.map(sqlStatsTableRow).sorted(ordering(sortColumn, desc))

    private var _slicedStartTime: Set[Long] = null

    override def dataSize: Int = data.size

    override def sliceData(from: Int, to: Int): Seq[SqlStatsTableRow] = {
      val r = data.slice(from, to)
      r.map(x => x)
      _slicedStartTime = r.map(_.executionInfo.startTimestamp).toSet
      r
    }

    private def sqlStatsTableRow(executionInfo: ExecutionInfo): SqlStatsTableRow = {
      val duration = executionInfo.totalTime(executionInfo.closeTimestamp)
      val executionTime = executionInfo.totalTime(executionInfo.finishTimestamp)
      val detail = Option(executionInfo.detail).filter(!_.isEmpty)
        .getOrElse(executionInfo.executePlan)
      val jobId = executionInfo.jobId.toSeq.sorted

      new SqlStatsTableRow(jobId, duration, executionTime, executionInfo, detail)

    }

    /**
     * Return Ordering according to sortColumn and desc.
     */
    private def ordering(sortColumn: String, desc: Boolean): Ordering[SqlStatsTableRow] = {
      val ordering: Ordering[SqlStatsTableRow] = sortColumn match {
        case "User" => Ordering.by(_.executionInfo.userName)
        case "JobID" => Ordering by (_.jobId.headOption)
        case "GroupID" => Ordering.by(_.executionInfo.groupId)
        case "Start Time" => Ordering.by(_.executionInfo.startTimestamp)
        case "Finish Time" => Ordering.by(_.executionInfo.finishTimestamp)
        case "Close Time" => Ordering.by(_.executionInfo.closeTimestamp)
        case "Execution Time" => Ordering.by(_.executionTime)
        case "Duration" => Ordering.by(_.duration)
        case "Statement" => Ordering.by(_.executionInfo.statement)
        case "State" => Ordering.by(_.executionInfo.state)
        case "Detail" => Ordering.by(_.detail)
        case unknownColumn => throw new IllegalArgumentException(s"Unknown column: $unknownColumn")
      }
      if (desc) {
        ordering.reverse
      } else {
        ordering
      }
    }

  }
