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

package org.apache.spark.sql.connect.ui

import java.net.URLEncoder
import java.nio.charset.StandardCharsets.UTF_8

import scala.xml.Node

import jakarta.servlet.http.HttpServletRequest

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connect.ui.ToolTips._
import org.apache.spark.ui._
import org.apache.spark.ui.UIUtils._
import org.apache.spark.util.Utils

/** Page for Spark UI that shows statistics for a Spark Connect Server. */
private[ui] class SparkConnectServerPage(parent: SparkConnectServerTab)
    extends WebUIPage("")
    with Logging {

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
          Request(s)
        </h4> ++
        generateSessionStatsTable(request) ++
        generateSQLStatsTable(request)
    }
    UIUtils.headerSparkPage(request, "Spark Connect", content, parent)
  }

  /** Generate basic stats of the Spark Connect server */
  private def generateBasicStats(): Seq[Node] = {
    val timeSinceStart = System.currentTimeMillis() - startTime.getTime
    <ul class ="list-unstyled">
      <li>
        <strong>Started at: </strong> {formatDate(startTime)}
      </li>
      <li>
        <strong>Time since start: </strong>{formatDurationVerbose(timeSinceStart)}
      </li>
    </ul>
  }

  /** Generate stats of batch statements of the Spark Connect program */
  private def generateSQLStatsTable(request: HttpServletRequest): Seq[Node] = {

    val numStatement = store.getExecutionList.size

    val table = if (numStatement > 0) {

      val sqlTableTag = "sqlstat"

      val sqlTablePage =
        Option(request.getParameter(s"$sqlTableTag.page")).map(_.toInt).getOrElse(1)

      try {
        Some(
          new SqlStatsPagedTable(
            request,
            parent,
            store.getExecutionList,
            "connect",
            UIUtils.prependBaseUri(request, parent.basePath),
            sqlTableTag,
            showSessionLink = true).table(sqlTablePage))
      } catch {
        case e @ (_: IllegalArgumentException | _: IndexOutOfBoundsException) =>
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
          <a>Request Statistics ({numStatement})</a>
        </h4>
      </span> ++
        <div class="aggregated-sqlstat collapsible-table">
          {table.getOrElse("No statistics have been generated yet.")}
        </div>
    content
  }

  /** Generate stats of batch sessions of the Spark Connect server */
  private def generateSessionStatsTable(request: HttpServletRequest): Seq[Node] = {
    val numSessions = store.getSessionList.size
    val table = if (numSessions > 0) {

      val sessionTableTag = "sessionstat"

      val sessionTablePage =
        Option(request.getParameter(s"$sessionTableTag.page")).map(_.toInt).getOrElse(1)

      try {
        Some(
          new SessionStatsPagedTable(
            request,
            parent,
            store.getSessionList,
            "connect",
            UIUtils.prependBaseUri(request, parent.basePath),
            sessionTableTag).table(sessionTablePage))
      } catch {
        case e @ (_: IllegalArgumentException | _: IndexOutOfBoundsException) =>
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
    parent: SparkConnectServerTab,
    data: Seq[ExecutionInfo],
    subPath: String,
    basePath: String,
    sqlStatsTableTag: String,
    showSessionLink: Boolean)
    extends PagedTable[SqlStatsTableRow] {

  private val (sortColumn, desc, pageSize) =
    getTableParameters(request, sqlStatsTableTag, "Start Time")

  private val encodedSortColumn = URLEncoder.encode(sortColumn, UTF_8.name())

  private val parameterPath =
    s"$basePath/$subPath/?${getParameterOtherTable(request, sqlStatsTableTag)}"

  override val dataSource = new SqlStatsTableDataSource(data, pageSize, sortColumn, desc)

  override def tableId: String = sqlStatsTableTag

  override def tableCssClass: String =
    "table table-bordered table-sm table-striped table-head-clickable table-cell-width-limited"

  override def pageLink(page: Int): String = {
    parameterPath +
      s"&$pageNumberFormField=$page" +
      s"&$sqlStatsTableTag.sort=$encodedSortColumn" +
      s"&$sqlStatsTableTag.desc=$desc" +
      s"&$pageSizeFormField=$pageSize" +
      s"#$sqlStatsTableTag"
  }

  override def pageSizeFormField: String = s"$sqlStatsTableTag.pageSize"

  override def pageNumberFormField: String = s"$sqlStatsTableTag.page"

  override def goButtonFormPath: String =
    s"$parameterPath&$sqlStatsTableTag.sort=$encodedSortColumn" +
      s"&$sqlStatsTableTag.desc=$desc#$sqlStatsTableTag"

  override def headers: Seq[Node] = {
    val sqlTableHeadersAndTooltips: Seq[(String, Boolean, Option[String])] =
      if (showSessionLink) {
        Seq(
          ("User", true, None),
          ("Job ID", true, None),
          ("SQL Query ID", true, None),
          ("Session ID", true, None),
          ("Start Time", true, None),
          ("Finish Time", true, Some(SPARK_CONNECT_SERVER_FINISH_TIME)),
          ("Close Time", true, Some(SPARK_CONNECT_SERVER_CLOSE_TIME)),
          ("Execution Time", true, Some(SPARK_CONNECT_SERVER_EXECUTION)),
          ("Duration", true, Some(SPARK_CONNECT_SERVER_DURATION)),
          ("Statement", true, None),
          ("State", true, None),
          ("Operation ID", true, None),
          ("Job Tag", true, None),
          ("Spark Session Tags", true, None),
          ("Detail", true, None))
      } else {
        Seq(
          ("User", true, None),
          ("Job ID", true, None),
          ("SQL Query ID", true, None),
          ("Start Time", true, None),
          ("Finish Time", true, Some(SPARK_CONNECT_SERVER_FINISH_TIME)),
          ("Close Time", true, Some(SPARK_CONNECT_SERVER_CLOSE_TIME)),
          ("Execution Time", true, Some(SPARK_CONNECT_SERVER_EXECUTION)),
          ("Duration", true, Some(SPARK_CONNECT_SERVER_DURATION)),
          ("Statement", true, None),
          ("State", true, None),
          ("Operation ID", true, None),
          ("Job Tag", true, None),
          ("Spark Session Tags", true, None),
          ("Detail", true, None))
      }

    isSortColumnValid(sqlTableHeadersAndTooltips, sortColumn)

    headerRow(
      sqlTableHeadersAndTooltips,
      desc,
      pageSize,
      sortColumn,
      parameterPath,
      sqlStatsTableTag,
      sqlStatsTableTag)
  }

  override def row(sqlStatsTableRow: SqlStatsTableRow): Seq[Node] = {
    val info = sqlStatsTableRow.executionInfo
    val startTime = info.startTimestamp
    val executionTime = sqlStatsTableRow.executionTime
    val duration = sqlStatsTableRow.duration

    def jobLinks(jobData: Seq[String]): Seq[Node] = {
      jobData.map { jobId =>
        <a href={jobURL(request, jobId)}>[{jobId}]</a>
      }
    }
    def sqlLinks(sqlData: Seq[String]): Seq[Node] = {
      sqlData.map { sqlExecId =>
        <a href={sqlURL(request, sqlExecId)}>[{sqlExecId}]</a>
      }
    }
    val sessionLink = "%s/%s/session/?id=%s".format(
      UIUtils.prependBaseUri(request, parent.basePath),
      parent.prefix,
      info.sessionId)

    <tr>
      <td>
        {info.userId}
      </td>
      <td>
        {jobLinks(sqlStatsTableRow.jobId)}
      </td>
      <td>
        {sqlLinks(sqlStatsTableRow.sqlExecId)}
      </td>
      {
      if (showSessionLink) {
        <td>
          <a href={sessionLink}>{info.sessionId}</a>
        </td>
      }
    }
      <td>
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
        {if (info.isExecutionActive) "RUNNING" else info.state}
      </td>
      <td>
        {info.operationId}
      </td>
      <td>
        {info.jobTag}
      </td>
      <td>
        {sqlStatsTableRow.sparkSessionTags.mkString(", ")}
      </td>
      {UIUtils.errorMessageCell(Option(info.detail).getOrElse(""))}
    </tr>
  }

  private def jobURL(request: HttpServletRequest, jobId: String): String =
    "%s/jobs/job/?id=%s".format(UIUtils.prependBaseUri(request, parent.basePath), jobId)

  private def sqlURL(request: HttpServletRequest, sqlExecId: String): String =
    "%s/SQL/execution/?id=%s".format(UIUtils.prependBaseUri(request, parent.basePath), sqlExecId)
}

private[ui] class SessionStatsPagedTable(
    request: HttpServletRequest,
    parent: SparkConnectServerTab,
    data: Seq[SessionInfo],
    subPath: String,
    basePath: String,
    sessionStatsTableTag: String)
    extends PagedTable[SessionInfo] {

  private val (sortColumn, desc, pageSize) =
    getTableParameters(request, sessionStatsTableTag, "Start Time")

  private val encodedSortColumn = URLEncoder.encode(sortColumn, UTF_8.name())

  private val parameterPath =
    s"$basePath/$subPath/?${getParameterOtherTable(request, sessionStatsTableTag)}"

  override val dataSource = new SessionStatsTableDataSource(data, pageSize, sortColumn, desc)

  override def tableId: String = sessionStatsTableTag

  override def tableCssClass: String =
    "table table-bordered table-sm table-striped table-head-clickable table-cell-width-limited"

  override def pageLink(page: Int): String = {
    parameterPath +
      s"&$pageNumberFormField=$page" +
      s"&$sessionStatsTableTag.sort=$encodedSortColumn" +
      s"&$sessionStatsTableTag.desc=$desc" +
      s"&$pageSizeFormField=$pageSize" +
      s"#$sessionStatsTableTag"
  }

  override def pageSizeFormField: String = s"$sessionStatsTableTag.pageSize"

  override def pageNumberFormField: String = s"$sessionStatsTableTag.page"

  override def goButtonFormPath: String =
    s"$parameterPath&$sessionStatsTableTag.sort=$encodedSortColumn" +
      s"&$sessionStatsTableTag.desc=$desc#$sessionStatsTableTag"

  override def headers: Seq[Node] = {
    val sessionTableHeadersAndTooltips: Seq[(String, Boolean, Option[String])] =
      Seq(
        ("User", true, None),
        ("Session ID", true, None),
        ("Start Time", true, None),
        ("Finish Time", true, None),
        ("Duration", true, Some(SPARK_CONNECT_SESSION_DURATION)),
        ("Total Execute", true, Some(SPARK_CONNECT_SESSION_TOTAL_EXECUTE)))

    isSortColumnValid(sessionTableHeadersAndTooltips, sortColumn)

    headerRow(
      sessionTableHeadersAndTooltips,
      desc,
      pageSize,
      sortColumn,
      parameterPath,
      sessionStatsTableTag,
      sessionStatsTableTag)
  }

  override def row(session: SessionInfo): Seq[Node] = {
    val sessionLink = "%s/%s/session/?id=%s".format(
      UIUtils.prependBaseUri(request, parent.basePath),
      parent.prefix,
      session.sessionId)
    <tr>
      <td> {session.userId} </td>
      <td> <a href={sessionLink}> {session.sessionId} </a> </td>
      <td> {formatDate(session.startTimestamp)} </td>
      <td> {if (session.finishTimestamp > 0) formatDate(session.finishTimestamp)} </td>
      <td> {formatDurationVerbose(session.totalTime)} </td>
      <td> {session.totalExecution.toString} </td>
    </tr>
  }
}

private[ui] class SqlStatsTableRow(
    val jobTag: String,
    val jobId: Seq[String],
    val sqlExecId: Seq[String],
    val duration: Long,
    val executionTime: Long,
    val sparkSessionTags: Seq[String],
    val executionInfo: ExecutionInfo)

private[ui] class SqlStatsTableDataSource(
    info: Seq[ExecutionInfo],
    pageSize: Int,
    sortColumn: String,
    desc: Boolean)
    extends PagedDataSource[SqlStatsTableRow](pageSize) {

  // Convert ExecutionInfo to SqlStatsTableRow which contains the final contents to show in
  // the table so that we can avoid creating duplicate contents during sorting the data
  private val data = info.map(sqlStatsTableRow).sorted(ordering(sortColumn, desc))

  override def dataSize: Int = data.size

  override def sliceData(from: Int, to: Int): Seq[SqlStatsTableRow] = data.slice(from, to)

  private def sqlStatsTableRow(executionInfo: ExecutionInfo): SqlStatsTableRow = {
    val duration = executionInfo.totalTime(executionInfo.closeTimestamp)
    val executionTime = executionInfo.totalTime(executionInfo.finishTimestamp)
    val jobId = executionInfo.jobId.toSeq.sorted
    val sqlExecId = executionInfo.sqlExecId.toSeq.sorted
    val sparkSessionTags = executionInfo.sparkSessionTags.toSeq.sorted

    new SqlStatsTableRow(
      executionInfo.jobTag,
      jobId,
      sqlExecId,
      duration,
      executionTime,
      sparkSessionTags,
      executionInfo)
  }

  /**
   * Return Ordering according to sortColumn and desc.
   */
  private def ordering(sortColumn: String, desc: Boolean): Ordering[SqlStatsTableRow] = {
    val ordering: Ordering[SqlStatsTableRow] = sortColumn match {
      case "User" => Ordering.by(_.executionInfo.userId)
      case "Operation ID" => Ordering.by(_.executionInfo.operationId)
      case "Job ID" => Ordering.by(_.jobId.headOption)
      case "SQL Query ID" => Ordering.by(_.sqlExecId.headOption)
      case "Session ID" => Ordering.by(_.executionInfo.sessionId)
      case "Start Time" => Ordering.by(_.executionInfo.startTimestamp)
      case "Finish Time" => Ordering.by(_.executionInfo.finishTimestamp)
      case "Close Time" => Ordering.by(_.executionInfo.closeTimestamp)
      case "Execution Time" => Ordering.by(_.executionTime)
      case "Duration" => Ordering.by(_.duration)
      case "Statement" => Ordering.by(_.executionInfo.statement)
      case "State" => Ordering.by(_.executionInfo.state)
      case "Detail" => Ordering.by(_.executionInfo.detail)
      case "Job Tag" => Ordering.by(_.executionInfo.jobTag)
      case "Spark Session Tags" => Ordering.by(_.sparkSessionTags.headOption)
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
    desc: Boolean)
    extends PagedDataSource[SessionInfo](pageSize) {

  // Sorting SessionInfo data
  private val data = info.sorted(ordering(sortColumn, desc))

  override def dataSize: Int = data.size

  override def sliceData(from: Int, to: Int): Seq[SessionInfo] = data.slice(from, to)

  /**
   * Return Ordering according to sortColumn and desc.
   */
  private def ordering(sortColumn: String, desc: Boolean): Ordering[SessionInfo] = {
    val ordering: Ordering[SessionInfo] = sortColumn match {
      case "User" => Ordering.by(_.userId)
      case "Session ID" => Ordering.by(_.sessionId)
      case "Start Time" => Ordering.by(_.startTimestamp)
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
