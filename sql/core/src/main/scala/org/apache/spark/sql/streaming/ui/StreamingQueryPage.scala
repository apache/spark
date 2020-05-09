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

package org.apache.spark.sql.streaming.ui

import java.net.URLEncoder
import java.nio.charset.StandardCharsets.UTF_8
import javax.servlet.http.HttpServletRequest

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.xml.{Node, Unparsed}

import org.apache.commons.text.StringEscapeUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.ui.UIUtils._
import org.apache.spark.ui.{PagedDataSource, PagedTable, UIUtils => SparkUIUtils, WebUIPage}
import org.apache.spark.util.Utils

private[ui] class StreamingQueryPage(parent: StreamingQueryTab)
    extends WebUIPage("") with Logging {

  override def render(request: HttpServletRequest): Seq[Node] = {
    val content = generateStreamingQueryTable(request)
    SparkUIUtils.headerSparkPage(request, "Streaming Query", content, parent)
  }

  private def generateStreamingQueryTable(request: HttpServletRequest): Seq[Node] = {
    val (activeQueries, inactiveQueries) = parent.statusListener.allQueryStatus
      .partition(_.isActive)

    val content = mutable.ListBuffer[Node]()
    // show active queries table only if there is at least one active query
    if (activeQueries.nonEmpty) {
      // scalastyle:off
      content ++=
        <span id="active" class="collapse-aggregated-activeQueries collapse-table"
            onClick="collapseTable('collapse-aggregated-activeQueries','aggregated-activeQueries')">
          <h5 id="activequeries">
            <span class="collapse-table-arrow arrow-open"></span>
            <a>Active Streaming Queries ({activeQueries.length})</a>
          </h5>
        </span> ++
          <div>
            <ul class="aggregated-activeQueries collapsible-table">
              {queryTable(activeQueries, request, "active")}
            </ul>
          </div>
      // scalastyle:on
    }
    // show active queries table only if there is at least one completed query
    if (inactiveQueries.nonEmpty) {
      // scalastyle:off
      content ++=
        <span id="completed" class="collapse-aggregated-completedQueries collapse-table"
            onClick="collapseTable('collapse-aggregated-completedQueries','aggregated-completedQueries')">
          <h5 id="completedqueries">
            <span class="collapse-table-arrow arrow-open"></span>
            <a>Completed Streaming Queries ({inactiveQueries.length})</a>
          </h5>
        </span> ++
          <div>
            <ul class="aggregated-completedQueries collapsible-table">
              {queryTable(inactiveQueries, request, "completed")}
            </ul>
          </div>
      // scalastyle:on
    }
    content
  }

  private def queryTable(data: Seq[StreamingQueryUIData], request: HttpServletRequest,
      tableTag: String): Seq[Node] = {

    val isActive = if (tableTag.contains("active")) true else false
    val parameterOtherTable = request.getParameterMap.asScala
      .filterNot(_._1.startsWith(tableTag))
      .map { case (name, vals) =>
        name + "=" + vals(0)
      }

    val parameterPage = request.getParameter(s"$tableTag.page")
    val parameterSortColumn = request.getParameter(s"$tableTag.sort")
    val parameterSortDesc = request.getParameter(s"$tableTag.desc")
    val parameterPageSize = request.getParameter(s"$tableTag.pageSize")

    val page = Option(parameterPage).map(_.toInt).getOrElse(1)
    val sortColumn = Option(parameterSortColumn).map { sortColumn =>
      SparkUIUtils.decodeURLParameter(sortColumn)
    }.getOrElse("Start Time")
    val sortDesc = Option(parameterSortDesc).map(_.toBoolean).getOrElse(sortColumn == "Start Time")
    val pageSize = Option(parameterPageSize).map(_.toInt).getOrElse(100)

    try {
      new StreamingQueryPagedTable(
        request,
        parent,
        data,
        tableTag,
        pageSize,
        sortColumn,
        sortDesc,
        isActive,
        parameterOtherTable,
        SparkUIUtils.prependBaseUri(request, parent.basePath),
        "StreamingQuery"
      ).table(page)
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

class StreamingQueryPagedTable(
    request: HttpServletRequest,
    parent: StreamingQueryTab,
    data: Seq[StreamingQueryUIData],
    tableTag: String,
    pageSize: Int,
    sortColumn: String,
    sortDesc: Boolean,
    isActive: Boolean,
    parameterOtherTable: Iterable[String],
    basePath: String,
    subPath: String) extends PagedTable[StructuredStreamingRow] {

  private val parameterPath = s"$basePath/$subPath/?${parameterOtherTable.mkString("&")}"
  private val encodedSortColumn = URLEncoder.encode(sortColumn, UTF_8.name())

  override def tableId: String = s"$tableTag-table"

  override def tableCssClass: String =
    "table table-bordered table-sm table-striped table-head-clickable table-cell-width-limited"

  override def pageSizeFormField: String = s"$tableTag.pageSize"

  override def pageNumberFormField: String = s"$tableTag.page"

  override def pageLink(page: Int): String = {
    parameterPath +
    s"&$pageNumberFormField=$page" +
    s"&$tableTag.sort=$encodedSortColumn" +
    s"&$tableTag.desc=$sortDesc" +
    s"&$pageSizeFormField=$pageSize" +
    s"#$tableTag"
  }

  override def goButtonFormPath: String =
    s"$parameterPath&$tableTag.sort=$encodedSortColumn&$tableTag.desc=$sortDesc#$tableTag"

  override def dataSource: PagedDataSource[StructuredStreamingRow] =
    new StreamingQueryDataSource(data, sortColumn, sortDesc, pageSize, isActive)

  override def headers: Seq[Node] = {
    val headerAndCss: Seq[(String, Boolean)] = {
      Seq(
        ("Name", true),
        ("Status", false),
        ("ID", true),
        ("Run ID", true),
        ("Start Time", true),
        ("Duration", false),
        ("Avg Input /sec", false),
        ("Avg Process /sec", false),
        ("Lastest Batch", true)) ++ {
        if (!isActive) {
          Seq(("Error", false))
        } else {
          Nil
        }
      }
    }

    val sortableColumnHeaders = headerAndCss.filter {
      case (_, sortable) => sortable
    }.map { case (title, _) => title }

    // sort column must be one of sortable columns of the table
    require(sortableColumnHeaders.contains(sortColumn),
      s"Sorting is not allowed on this column: $sortColumn")

    val headerRow: Seq[Node] = {
      headerAndCss.map { case (header, sortable) =>
        if (header == sortColumn) {
          val headerLink = Unparsed(
            parameterPath +
              s"&$tableTag.sort=${URLEncoder.encode(header, UTF_8.name())}" +
              s"&$tableTag.desc=${!sortDesc}" +
              s"&$tableTag.pageSize=$pageSize" +
              s"#$tableTag")
          val arrow = if (sortDesc) "&#x25BE;" else "&#x25B4;"

          <th>
            <a href={headerLink}>
              <span>
                {header}&nbsp;{Unparsed(arrow)}
              </span>
            </a>
          </th>
        } else {
          if (sortable) {
            val headerLink = Unparsed(
              parameterPath +
                s"&$tableTag.sort=${URLEncoder.encode(header, UTF_8.name())}" +
                s"&$tableTag.pageSize=$pageSize" +
                s"#$tableTag")

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

  override def row(query: StructuredStreamingRow): Seq[Node] = {
    val streamingQuery = query.streamingUIData
    val statisticsLink = "%s/%s/statistics?id=%s"
      .format(SparkUIUtils.prependBaseUri(request, parent.basePath), parent.prefix,
        streamingQuery.runId)

    def details(detail: Any): Seq[Node] = {
      if (isActive) {
        return Seq.empty[Node]
      }
      val detailString = detail.asInstanceOf[String]
      val isMultiline = detailString.indexOf('\n') >= 0
      val summary = StringEscapeUtils.escapeHtml4(
        if (isMultiline) detailString.substring(0, detailString.indexOf('\n')) else detailString
      )
      val details = SparkUIUtils.detailsUINode(isMultiline, detailString)
      <td>{summary}{details}</td>
    }

    <tr>
      <td>{UIUtils.getQueryName(streamingQuery)}</td>
      <td>{UIUtils.getQueryStatus(streamingQuery)}</td>
      <td>{streamingQuery.id}</td>
      <td><a href={statisticsLink}>{streamingQuery.runId}</a></td>
      <td>{SparkUIUtils.formatDate(streamingQuery.startTimestamp)}</td>
      <td>{query.duration}</td>
      <td>{withNoProgress(streamingQuery, {query.avgInput.formatted("%.2f")}, "NaN")}</td>
      <td>{withNoProgress(streamingQuery, {query.avgProcess.formatted("%.2f")}, "NaN")}</td>
      <td>{withNoProgress(streamingQuery, {streamingQuery.lastProgress.batchId}, "NaN")}</td>
      {details(streamingQuery.exception.getOrElse("-"))}
    </tr>
  }
}

case class StructuredStreamingRow(
    duration: String,
    avgInput: Double,
    avgProcess: Double,
    streamingUIData: StreamingQueryUIData)

class StreamingQueryDataSource(uiData: Seq[StreamingQueryUIData], sortColumn: String, desc: Boolean,
    pageSize: Int, isActive: Boolean) extends PagedDataSource[StructuredStreamingRow](pageSize) {

  // convert StreamingQueryUIData to StreamingRow to provide required data for sorting and sort it
  private val data = uiData.map(streamingRow).sorted(ordering(sortColumn, desc))

  override def dataSize: Int = data.size

  override def sliceData(from: Int, to: Int): Seq[StructuredStreamingRow] = data.slice(from, to)

  private def streamingRow(query: StreamingQueryUIData): StructuredStreamingRow = {
    val duration = if (isActive) {
      SparkUIUtils.formatDurationVerbose(System.currentTimeMillis() - query.startTimestamp)
    } else {
      withNoProgress(query, {
        val endTimeMs = query.lastProgress.timestamp
        SparkUIUtils.formatDurationVerbose(parseProgressTimestamp(endTimeMs) - query.startTimestamp)
      }, "-")
    }

    val avgInput = (query.recentProgress.map(p => withNumberInvalid(p.inputRowsPerSecond)).sum /
      query.recentProgress.length)

    val avgProcess = (query.recentProgress.map(p =>
      withNumberInvalid(p.processedRowsPerSecond)).sum / query.recentProgress.length)

    StructuredStreamingRow(duration, avgInput, avgProcess, query)
  }

  private def ordering(sortColumn: String, desc: Boolean): Ordering[StructuredStreamingRow] = {
    val ordering: Ordering[StructuredStreamingRow] = sortColumn match {
      case "Name" => Ordering.by(q => UIUtils.getQueryName(q.streamingUIData))
      case "ID" => Ordering.by(_.streamingUIData.id)
      case "Run ID" => Ordering.by(_.streamingUIData.runId)
      case "Start Time" => Ordering.by(_.streamingUIData.startTimestamp)
      case "Duration" => Ordering.by(_.duration)
      case "Avg Input /sec" => Ordering.by(_.avgInput)
      case "Avg Process /sec" => Ordering.by(_.avgProcess)
      case "Lastest Batch" => Ordering.by(_.streamingUIData.lastProgress.batchId)
      case unknownColumn => throw new IllegalArgumentException(s"Unknown Column: $unknownColumn")
    }
    if (desc) {
      ordering.reverse
    } else {
      ordering
    }
  }
}
