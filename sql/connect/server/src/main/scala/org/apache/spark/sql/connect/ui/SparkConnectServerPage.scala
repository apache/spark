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
import java.util.Base64

import scala.xml.Node

import jakarta.servlet.http.HttpServletRequest

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connect.ml.{MLCacheModelInfo, MLCacheStatus}
import org.apache.spark.sql.connect.service.SessionKey
import org.apache.spark.sql.connect.ui.ToolTips._
import org.apache.spark.ui._
import org.apache.spark.ui.UIUtils._
import org.apache.spark.util.Utils

// userId is part of a session's natural key but is opaque and arbitrary. It is carried through UI
// links as unpadded base64url, whose alphabet (A-Za-z0-9-_) survives XssSafeRequest sanitization
// and the PagedTable parameter re-echo unchanged.
private[spark] object ConnectUiUtils {
  def encodeUserId(userId: String): String =
    Base64.getUrlEncoder.withoutPadding.encodeToString(userId.getBytes(UTF_8))

  def decodeUserId(token: String): String =
    new String(Base64.getUrlDecoder.decode(token), UTF_8)
}

/** Page for Spark UI that shows statistics for a Spark Connect Server. */
private[ui] class SparkConnectServerPage(parent: SparkConnectServerTab)
    extends WebUIPage("")
    with Logging {

  private val store = parent.store
  private val startTime = parent.startTime

  /** Render the page */
  def render(request: HttpServletRequest): Seq[Node] = {
    // Do not hold the status store lock while waiting for live ML cache snapshots. ML cache
    // operations can perform disk I/O while holding their own locks.
    val mlCacheStatuses = parent.getMLCacheStatuses
    val initializedMLCacheStatuses = mlCacheStatuses.toSeq.flatMap(_.iterator.collect {
      case (key, Some(status)) => key -> status
    })
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
        generateSessionStatsTable(request, mlCacheStatuses) ++
        generateSQLStatsTable(request) ++
        generateMLCacheStatsTable(request, initializedMLCacheStatuses)
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
          Some(<div class="alert alert-danger">
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
      <span id="sqlstat" class="collapse-table" data-bs-toggle="collapse"
            data-bs-target="#aggregated-sqlstat"
            aria-expanded="true" aria-controls="aggregated-sqlstat"
            data-collapse-name="collapse-aggregated-sqlstat">
        <h4>
          <span class="collapse-table-arrow arrow-open"></span>
          <a>Request Statistics ({numStatement})</a>
        </h4>
      </span> ++
        <div class="collapsible-table collapse show" id="aggregated-sqlstat">
          {table.getOrElse("No statistics have been generated yet.")}
        </div>
    content
  }

  /** Generate stats of batch sessions of the Spark Connect server */
  private def generateSessionStatsTable(
      request: HttpServletRequest,
      mlCacheStatuses: Option[Map[SessionKey, Option[MLCacheStatus]]]): Seq[Node] = {
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
            sessionTableTag,
            mlCacheStatuses).table(sessionTablePage))
      } catch {
        case e @ (_: IllegalArgumentException | _: IndexOutOfBoundsException) =>
          Some(<div class="alert alert-danger">
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
      <span id="sessionstat" class="collapse-table" data-bs-toggle="collapse"
            data-bs-target="#aggregated-sessionstat"
            aria-expanded="true" aria-controls="aggregated-sessionstat"
            data-collapse-name="collapse-aggregated-sessionstat">
        <h4>
          <span class="collapse-table-arrow arrow-open"></span>
          <a>Session Statistics ({numSessions})</a>
        </h4>
      </span> ++
        <div class="collapsible-table collapse show" id="aggregated-sessionstat">
          {table.getOrElse("No statistics have been generated yet.")}
        </div>

    content
  }

  /** Generate live ML cache statistics for active Spark Connect sessions. */
  private def generateMLCacheStatsTable(
      request: HttpServletRequest,
      mlCacheStatuses: Seq[(SessionKey, MLCacheStatus)]): Seq[Node] = {
    val populatedCacheStatuses = mlCacheStatuses.filter(_._2.models.nonEmpty)
    val models = populatedCacheStatuses.flatMap { case (key, status) =>
      status.models.map(MLCacheModelTableRow(key.userId, key.sessionId, _))
    }
    if (models.isEmpty) {
      return Seq.empty
    }

    val tableTag = "mlcachemodels"
    val tablePage = Option(request.getParameter(s"$tableTag.page")).map(_.toInt).getOrElse(1)
    val table =
      try {
        new MLCacheModelStatsPagedTable(
          request,
          parent,
          models,
          "connect",
          UIUtils.prependBaseUri(request, parent.basePath),
          tableTag).table(tablePage)
      } catch {
        case e @ (_: IllegalArgumentException | _: IndexOutOfBoundsException) =>
          <div class="alert alert-danger">
          <p>Error while rendering ML cache table:</p>
          <pre>
            {Utils.exceptionString(e)}
          </pre>
        </div>
      }

    val inMemoryModels = models.count(_.model.inMemory)
    val memoryControlledStatuses =
      populatedCacheStatuses.map(_._2).filter(_.memoryControlEnabled)
    val inMemorySize = memoryControlledStatuses.map(s => BigInt(s.inMemorySizeBytes)).sum
    val maxInMemorySize = memoryControlledStatuses.map(s => BigInt(s.maxInMemorySizeBytes)).sum
    val totalSize = memoryControlledStatuses.map(s => BigInt(s.totalSizeBytes)).sum
    val maxTotalSize = memoryControlledStatuses.map(s => BigInt(s.maxTotalSizeBytes)).sum
    val sizeStats = if (memoryControlledStatuses.nonEmpty) {
      Seq(
        <li>
          <strong>Estimated size (In-memory): </strong>
          {Utils.bytesToString(inMemorySize)} / {Utils.bytesToString(maxInMemorySize)}
        </li>,
        <li>
          <strong>Estimated size (In-memory and Offloaded data): </strong>
          {Utils.bytesToString(totalSize)} / {Utils.bytesToString(maxTotalSize)}
        </li>)
    } else {
      Seq.empty
    }

    <span id="mlcachestat" class="collapse-table" data-bs-toggle="collapse"
          data-bs-target="#aggregated-mlcachestat"
          aria-expanded="true" aria-controls="aggregated-mlcachestat"
          data-collapse-name="collapse-aggregated-mlcachestat">
      <h4>
        <span class="collapse-table-arrow arrow-open"></span>
        <a>ML Cache Statistics ({models.size})</a>
      </h4>
    </span> ++
      <div class="collapsible-table collapse show" id="aggregated-mlcachestat">
        <ul class="list-unstyled">
          <li><strong>Sessions with cached models: </strong>{populatedCacheStatuses.size}</li>
          <li>
            <strong>Cached models: </strong>
            {models.size} ({inMemoryModels} in memory, {models.size - inMemoryModels} offloaded)
          </li>
          {sizeStats}
        </ul>
        <h5>Cached Models</h5>
        {table}
      </div>
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
    val sessionLink = "%s/%s/session/?id=%s&userId=%s".format(
      UIUtils.prependBaseUri(request, parent.basePath),
      parent.prefix,
      URLEncoder.encode(info.sessionId, UTF_8.name()),
      ConnectUiUtils.encodeUserId(info.userId))

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
    sessionStatsTableTag: String,
    mlCacheStatuses: Option[Map[SessionKey, Option[MLCacheStatus]]])
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
        ("Total Execute", true, Some(SPARK_CONNECT_SESSION_TOTAL_EXECUTE)),
        ("ML Cache", false, Some(SPARK_CONNECT_SESSION_ML_CACHE)))

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
    val sessionLink = "%s/%s/session/?id=%s&userId=%s".format(
      UIUtils.prependBaseUri(request, parent.basePath),
      parent.prefix,
      URLEncoder.encode(session.sessionId, UTF_8.name()),
      ConnectUiUtils.encodeUserId(session.userId))
    <tr>
      <td> {session.userId} </td>
      <td> <a href={sessionLink}> {session.sessionId} </a> </td>
      <td> {formatDate(session.startTimestamp)} </td>
      <td> {if (session.finishTimestamp > 0) formatDate(session.finishTimestamp)} </td>
      <td> {formatDurationVerbose(session.totalTime)} </td>
      <td> {session.totalExecution.toString} </td>
      <td> {renderMLCacheStatus(session)} </td>
    </tr>
  }

  private def renderMLCacheStatus(session: SessionInfo): Seq[Node] = {
    if (session.finishTimestamp > 0) {
      <span>N/A</span>
    } else {
      mlCacheStatuses
        .flatMap(_.get(SessionKey(session.userId, session.sessionId)))
        .map {
          case Some(status) if status.models.nonEmpty =>
            if (status.memoryControlEnabled) {
              val inMemoryModels = status.models.count(_.inMemory)
              val modelLabel = if (inMemoryModels == 1) "model" else "models"
              <span>
                {s"$inMemoryModels $modelLabel in memory"}<br/>
                {
                s"${Utils.bytesToString(status.inMemorySizeBytes)} / " +
                  s"${Utils.bytesToString(status.maxInMemorySizeBytes)} memory"
              }<br/>
                {
                s"${Utils.bytesToString(status.totalSizeBytes)} / " +
                  s"${Utils.bytesToString(status.maxTotalSizeBytes)} total"
              }
              </span>
            } else {
              val cachedModels = status.models.size
              val modelLabel = if (cachedModels == 1) "model" else "models"
              <span>
                Memory control disabled<br/>
                {s"$cachedModels cached $modelLabel"}
              </span>
            }
          case Some(_) | None =>
            <span>Not used</span>
        }
        .getOrElse(<span>N/A</span>)
    }
  }
}

private[ui] case class MLCacheModelTableRow(
    userId: String,
    sessionId: String,
    model: MLCacheModelInfo)

private[ui] class MLCacheModelStatsPagedTable(
    request: HttpServletRequest,
    parent: SparkConnectServerTab,
    data: Seq[MLCacheModelTableRow],
    subPath: String,
    basePath: String,
    tableTag: String)
    extends PagedTable[MLCacheModelTableRow] {

  private val (sortColumn, desc, pageSize) =
    getTableParameters(request, tableTag, "Estimated Size")

  private val encodedSortColumn = URLEncoder.encode(sortColumn, UTF_8.name())
  private val parameterPath = s"$basePath/$subPath/?${getParameterOtherTable(request, tableTag)}"

  override val dataSource =
    new MLCacheModelTableDataSource(data, pageSize, sortColumn, desc)

  override def tableId: String = tableTag

  override def tableCssClass: String =
    "table table-bordered table-sm table-striped table-head-clickable table-cell-width-limited"

  override def pageLink(page: Int): String = {
    parameterPath +
      s"&$pageNumberFormField=$page" +
      s"&$tableTag.sort=$encodedSortColumn" +
      s"&$tableTag.desc=$desc" +
      s"&$pageSizeFormField=$pageSize" +
      s"#$tableTag"
  }

  override def pageSizeFormField: String = s"$tableTag.pageSize"

  override def pageNumberFormField: String = s"$tableTag.page"

  override def goButtonFormPath: String =
    s"$parameterPath&$tableTag.sort=$encodedSortColumn" +
      s"&$tableTag.desc=$desc#$tableTag"

  override def headers: Seq[Node] = {
    val headersAndTooltips: Seq[(String, Boolean, Option[String])] = Seq(
      ("User", true, None),
      ("Session ID", true, None),
      ("Model ID", true, None),
      ("Model Class", true, None),
      ("Model Details", true, Some(SPARK_CONNECT_ML_CACHE_MODEL_DETAILS)),
      ("Estimated Size", true, Some(SPARK_CONNECT_ML_CACHE_ESTIMATED_SIZE)),
      ("Storage", true, Some(SPARK_CONNECT_ML_CACHE_STORAGE)))

    isSortColumnValid(headersAndTooltips, sortColumn)
    headerRow(headersAndTooltips, desc, pageSize, sortColumn, parameterPath, tableTag, tableTag)
  }

  override def row(row: MLCacheModelTableRow): Seq[Node] = {
    val model = row.model
    val sessionLink = "%s/%s/session/?id=%s&userId=%s".format(
      UIUtils.prependBaseUri(request, parent.basePath),
      parent.prefix,
      URLEncoder.encode(row.sessionId, UTF_8.name()),
      ConnectUiUtils.encodeUserId(row.userId))
    <tr>
      <td>{row.userId}</td>
      <td><a href={sessionLink}>{row.sessionId}</a></td>
      <td>{model.id}</td>
      <td>{model.className}</td>
      <td>{model.modelString}</td>
      <td>{model.estimatedSizeBytes.map(Utils.bytesToString).getOrElse("N/A")}</td>
      <td>{if (model.inMemory) "In memory" else "Offloaded"}</td>
    </tr>
  }
}

private[ui] class MLCacheModelTableDataSource(
    info: Seq[MLCacheModelTableRow],
    pageSize: Int,
    sortColumn: String,
    desc: Boolean)
    extends PagedDataSource[MLCacheModelTableRow](pageSize) {

  private val data = info.sorted(ordering(sortColumn, desc))

  override def dataSize: Int = data.size

  override def sliceData(from: Int, to: Int): Seq[MLCacheModelTableRow] = data.slice(from, to)

  private def ordering(sortColumn: String, desc: Boolean): Ordering[MLCacheModelTableRow] = {
    val ordering: Ordering[MLCacheModelTableRow] = sortColumn match {
      case "User" => Ordering.by(_.userId)
      case "Session ID" => Ordering.by(_.sessionId)
      case "Model ID" => Ordering.by(_.model.id)
      case "Model Class" => Ordering.by(_.model.className)
      case "Model Details" => Ordering.by(_.model.modelString)
      case "Estimated Size" =>
        Ordering.by((row: MLCacheModelTableRow) => row.model.estimatedSizeBytes)
      case "Storage" => Ordering.by(_.model.inMemory)
      case unknownColumn => throw new IllegalArgumentException(s"Unknown column: $unknownColumn")
    }
    if (desc) ordering.reverse else ordering
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
