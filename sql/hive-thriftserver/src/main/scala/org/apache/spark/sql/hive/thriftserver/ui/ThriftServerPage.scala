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
import org.apache.spark.sql.hive.thriftserver.ui.ToolTips._
import org.apache.spark.ui._
import org.apache.spark.ui.UIUtils._
import org.apache.spark.util.Utils

/** Page for Spark Web UI that shows statistics of the thrift server */
private[ui] class ThriftServerPage(parent: ThriftServerTab) extends WebUIPage("") with Logging {
  private val store = parent.store
  private val startTime = parent.startTime

  /** Render the page */
  def render(request: HttpServletRequest): Seq[Node] = {
    val content = store.synchronized { // make sure all parts in this page are consistent
      generateBasicStats() ++
          <br/> ++
        <h4>
          {store.getOnlineSessionNum}
          session(s) are online,
          running
          {store.getTotalRunning}
          SQL statement(s)
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

    val numStatement = store.getExecutionList.size

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
          store.getExecutionList,
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
      <span id="sqlstat" class="collapse-aggregated-sqlstat collapse-table"
            onClick="collapseTable('collapse-aggregated-sqlstat',
                'aggregated-sqlstat')">
        <h4>
          <span class="collapse-table-arrow arrow-open"></span>
          <a>SQL Statistics ({numStatement})</a>
        </h4>
      </span> ++
        <div class="aggregated-sqlstat collapsible-table">
          {table.getOrElse("No statistics have been generated yet.")}
        </div>
    content
  }

  /** Generate stats of batch sessions of the thrift server program */
  private def generateSessionStatsTable(request: HttpServletRequest): Seq[Node] = {
    val numSessions = store.getSessionList.size
    val table = if (numSessions > 0) {

      val sessionTableTag = "sessionstat"

      val parameterOtherTable = request.getParameterMap().asScala
        .filterNot(_._1.startsWith(sessionTableTag))
        .map { case (name, vals) =>
          name + "=" + vals(0)
        }

      val parameterSessionTablePage = request.getParameter(s"$sessionTableTag.page")
      val parameterSessionTableSortColumn = request.getParameter(s"$sessionTableTag.sort")
      val parameterSessionTableSortDesc = request.getParameter(s"$sessionTableTag.desc")
      val parameterSessionPageSize = request.getParameter(s"$sessionTableTag.pageSize")

      val sessionTablePage = Option(parameterSessionTablePage).map(_.toInt).getOrElse(1)
      val sessionTableSortColumn = Option(parameterSessionTableSortColumn).map { sortColumn =>
        UIUtils.decodeURLParameter(sortColumn)
      }.getOrElse("Start Time")
      val sessionTableSortDesc = Option(parameterSessionTableSortDesc).map(_.toBoolean).getOrElse(
        // New session should be shown above old session by default.
        (sessionTableSortColumn == "Start Time")
      )
      val sessionTablePageSize = Option(parameterSessionPageSize).map(_.toInt).getOrElse(100)

      try {
        Some(new SessionStatsPagedTable(
          request,
          parent,
          store.getSessionList,
          "sqlserver",
          UIUtils.prependBaseUri(request, parent.basePath),
          parameterOtherTable,
          sessionTableTag,
          pageSize = sessionTablePageSize,
          sortColumn = sessionTableSortColumn,
          desc = sessionTableSortDesc
        ).table(sessionTablePage))
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
    <span id="sessionstat" class="collapse-aggregated-sessionstat collapse-table"
          onClick="collapseTable('collapse-aggregated-sessionstat',
                'aggregated-sessionstat')">
      <h4>
        <span class="collapse-table-arrow arrow-open"></span>
        <a>Session Statistics ({numSessions})</a>
      </h4>
    </span> ++
      <div class="aggregated-sessionstat collapsible-table">
        {table.getOrElse("No statistics have been generated yet.")}
      </div>

    content
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
      <!-- Returns a human-readable string representing a duration such as "5 second 35 ms"-->
      <td >
        {formatDurationVerbose(executionTime)}
      </td>
      <td >
        {formatDurationVerbose(duration)}
      </td>
      <td>
        <span class="description-input">
          {info.statement}
        </span>
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
    val details = detailsUINode(isMultiline, errorMessage)
    <td>
      {errorSummary}{details}
    </td>
  }

  private def jobURL(request: HttpServletRequest, jobId: String): String =
    "%s/jobs/job/?id=%s".format(UIUtils.prependBaseUri(request, parent.basePath), jobId)
}

private[ui] class SessionStatsPagedTable(
    request: HttpServletRequest,
    parent: ThriftServerTab,
    data: Seq[SessionInfo],
    subPath: String,
    basePath: String,
    parameterOtherTable: Iterable[String],
    sessionStatsTableTag: String,
    pageSize: Int,
    sortColumn: String,
    desc: Boolean) extends PagedTable[SessionInfo] {

  override val dataSource = new SessionStatsTableDataSource(data, pageSize, sortColumn, desc)

  private val parameterPath = s"$basePath/$subPath/?${parameterOtherTable.mkString("&")}"

  override def tableId: String = sessionStatsTableTag

  override def tableCssClass: String =
    "table table-bordered table-condensed table-striped " +
      "table-head-clickable table-cell-width-limited"

  override def pageLink(page: Int): String = {
    val encodedSortColumn = URLEncoder.encode(sortColumn, UTF_8.name())
    parameterPath +
      s"&$pageNumberFormField=$page" +
      s"&$sessionStatsTableTag.sort=$encodedSortColumn" +
      s"&$sessionStatsTableTag.desc=$desc" +
      s"&$pageSizeFormField=$pageSize"
  }

  override def pageSizeFormField: String = s"$sessionStatsTableTag.pageSize"

  override def pageNumberFormField: String = s"$sessionStatsTableTag.page"

  override def goButtonFormPath: String = {
    val encodedSortColumn = URLEncoder.encode(sortColumn, UTF_8.name())
    s"$parameterPath&$sessionStatsTableTag.sort=$encodedSortColumn&$sessionStatsTableTag.desc=$desc"
  }

  override def headers: Seq[Node] = {
    val sessionTableHeaders =
      Seq("User", "IP", "Session ID", "Start Time", "Finish Time", "Duration", "Total Execute")

    val tooltips = Seq(None, None, None, None, None, Some(THRIFT_SESSION_DURATION),
      Some(THRIFT_SESSION_TOTAL_EXECUTE))
    assert(sessionTableHeaders.length == tooltips.length)
    val colWidthAttr = s"${100.toDouble / sessionTableHeaders.size}%"

    val headerRow: Seq[Node] = {
      sessionTableHeaders.zip(tooltips).map { case (header, tooltip) =>
        if (header == sortColumn) {
          val headerLink = Unparsed(
            parameterPath +
              s"&$sessionStatsTableTag.sort=${URLEncoder.encode(header, UTF_8.name())}" +
              s"&$sessionStatsTableTag.desc=${!desc}" +
              s"&$sessionStatsTableTag.pageSize=$pageSize" +
              s"#$sessionStatsTableTag")
          val arrow = if (desc) "&#x25BE;" else "&#x25B4;" // UP or DOWN
            <th width={colWidthAttr}>
              <a href={headerLink}>
                {
                  if (tooltip.nonEmpty) {
                    <span data-toggle="tooltip" data-placement="top" title={tooltip.get}>
                      {header}&nbsp;{Unparsed(arrow)}
                    </span>
                  } else {
                    <span>
                      {header}&nbsp;{Unparsed(arrow)}
                    </span>
                  }
                }
              </a>
            </th>

        } else {
          val headerLink = Unparsed(
            parameterPath +
              s"&$sessionStatsTableTag.sort=${URLEncoder.encode(header, UTF_8.name())}" +
              s"&$sessionStatsTableTag.pageSize=$pageSize" +
              s"#$sessionStatsTableTag")

            <th width={colWidthAttr}>
              <a href={headerLink}>
                {
                  if (tooltip.nonEmpty) {
                    <span data-toggle="tooltip" data-placement="top" title={tooltip.get}>
                      {header}
                    </span>
                  } else {
                    {header}
                  }
                }
              </a>
            </th>
        }
      }
    }
    <thead>
      {headerRow}
    </thead>
  }

  override def row(session: SessionInfo): Seq[Node] = {
    val sessionLink = "%s/%s/session/?id=%s".format(
      UIUtils.prependBaseUri(request, parent.basePath), parent.prefix, session.sessionId)
    <tr>
      <td> {session.userName} </td>
      <td> {session.ip} </td>
      <td> <a href={sessionLink}> {session.sessionId} </a> </td>
      <td> {formatDate(session.startTimestamp)} </td>
      <td> {if (session.finishTimestamp > 0) formatDate(session.finishTimestamp)} </td>
      <td> {formatDurationVerbose(session.totalTime)} </td>
      <td> {session.totalExecution.toString} </td>
    </tr>
  }
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

  private[ui] class SessionStatsTableDataSource(
    info: Seq[SessionInfo],
    pageSize: Int,
    sortColumn: String,
    desc: Boolean) extends PagedDataSource[SessionInfo](pageSize) {

    // Sorting SessionInfo data
    private val data = info.sorted(ordering(sortColumn, desc))

    private var _slicedStartTime: Set[Long] = null

    override def dataSize: Int = data.size

    override def sliceData(from: Int, to: Int): Seq[SessionInfo] = {
      val r = data.slice(from, to)
      _slicedStartTime = r.map(_.startTimestamp).toSet
      r
    }

    /**
     * Return Ordering according to sortColumn and desc.
     */
    private def ordering(sortColumn: String, desc: Boolean): Ordering[SessionInfo] = {
      val ordering: Ordering[SessionInfo] = sortColumn match {
        case "User" => Ordering.by(_.userName)
        case "IP" => Ordering.by(_.ip)
        case "Session ID" => Ordering.by(_.sessionId)
        case "Start Time" => Ordering by (_.startTimestamp)
        case "Finish Time" => Ordering.by(_.finishTimestamp)
        case "Duration" => Ordering.by(_.totalTime)
        case "Total Execute" => Ordering.by(_.totalExecution)
        case unknownColumn => throw new IllegalArgumentException(s"Unknown column: $unknownColumn")
      }
      if (desc) {
        ordering.reverse
      } else {
        ordering
      }
    }
  }
