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

import scala.collection.mutable
import scala.xml.Node

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
    val (activeQueries, inactiveQueries) =
      parent.store.allQueryUIData.partition(_.summary.isActive)

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
    val page = Option(request.getParameter(s"$tableTag.page")).map(_.toInt).getOrElse(1)

    try {
      new StreamingQueryPagedTable(
        request,
        parent,
        data,
        tableTag,
        isActive,
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

private[ui] class StreamingQueryPagedTable(
    request: HttpServletRequest,
    parent: StreamingQueryTab,
    data: Seq[StreamingQueryUIData],
    tableTag: String,
    isActive: Boolean,
    basePath: String,
    subPath: String) extends PagedTable[StructuredStreamingRow] {

  private val (sortColumn, sortDesc, pageSize) = getTableParameters(request, tableTag, "Start Time")
  private val parameterPath = s"$basePath/$subPath/?${getParameterOtherTable(request, tableTag)}"
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
    val headerAndCss: Seq[(String, Boolean, Option[String])] = {
      Seq(
        ("Name", true, None),
        ("Status", true, None),
        ("ID", true, None),
        ("Run ID", true, None),
        ("Start Time", true, None),
        ("Duration", true, None),
        ("Avg Input /sec", true, None),
        ("Avg Process /sec", true, None),
        ("Latest Batch", true, None)) ++ {
        if (!isActive) {
          Seq(("Error", false, None))
        } else {
          Nil
        }
      }
    }
    isSortColumnValid(headerAndCss, sortColumn)

    headerRow(headerAndCss, sortDesc, pageSize, sortColumn, parameterPath, tableTag, tableTag)
  }

  override def row(query: StructuredStreamingRow): Seq[Node] = {
    val streamingQuery = query.streamingUIData
    val statisticsLink = "%s/%s/statistics/?id=%s"
      .format(SparkUIUtils.prependBaseUri(request, parent.basePath), parent.prefix,
        streamingQuery.summary.runId)

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
      <td>{streamingQuery.summary.id}</td>
      <td><a href={statisticsLink}>{streamingQuery.summary.runId}</a></td>
      <td>{SparkUIUtils.formatDate(streamingQuery.summary.startTimestamp)}</td>
      <td>{SparkUIUtils.formatDurationVerbose(query.duration)}</td>
      <td>{withNoProgress(streamingQuery, {"%.2f".format(query.avgInput)}, "NaN")}</td>
      <td>{withNoProgress(streamingQuery, {"%.2f".format(query.avgProcess)}, "NaN")}</td>
      <td>{withNoProgress(streamingQuery, {streamingQuery.lastProgress.batchId}, "NaN")}</td>
      {details(streamingQuery.summary.exception.getOrElse("-"))}
    </tr>
  }
}

private[ui] case class StructuredStreamingRow(
    duration: Long,
    avgInput: Double,
    avgProcess: Double,
    streamingUIData: StreamingQueryUIData)

private[ui] class StreamingQueryDataSource(
    uiData: Seq[StreamingQueryUIData],
    sortColumn: String,
    desc: Boolean,
    pageSize: Int,
    isActive: Boolean) extends PagedDataSource[StructuredStreamingRow](pageSize) {

  // convert StreamingQueryUIData to StreamingRow to provide required data for sorting and sort it
  private val data = uiData.map(streamingRow).sorted(ordering(sortColumn, desc))

  override def dataSize: Int = data.size

  override def sliceData(from: Int, to: Int): Seq[StructuredStreamingRow] = data.slice(from, to)

  private def streamingRow(uiData: StreamingQueryUIData): StructuredStreamingRow = {
    val duration = if (isActive) {
      System.currentTimeMillis() - uiData.summary.startTimestamp
    } else {
      withNoProgress(uiData, {
        val endTimeMs = uiData.lastProgress.timestamp
        parseProgressTimestamp(endTimeMs) - uiData.summary.startTimestamp
      }, 0)
    }

    val avgInput = (uiData.recentProgress.map(p => withNumberInvalid(p.inputRowsPerSecond)).sum /
      uiData.recentProgress.length)

    val avgProcess = (uiData.recentProgress.map(p =>
      withNumberInvalid(p.processedRowsPerSecond)).sum / uiData.recentProgress.length)

    StructuredStreamingRow(duration, avgInput, avgProcess, uiData)
  }

  private def ordering(sortColumn: String, desc: Boolean): Ordering[StructuredStreamingRow] = {
    val ordering: Ordering[StructuredStreamingRow] = sortColumn match {
      case "Name" => Ordering.by(row => UIUtils.getQueryName(row.streamingUIData))
      case "Status" => Ordering.by(row => UIUtils.getQueryStatus(row.streamingUIData))
      case "ID" => Ordering.by(_.streamingUIData.summary.id)
      case "Run ID" => Ordering.by(_.streamingUIData.summary.runId)
      case "Start Time" => Ordering.by(_.streamingUIData.summary.startTimestamp)
      case "Duration" => Ordering.by(_.duration)
      case "Avg Input /sec" => Ordering.by(_.avgInput)
      case "Avg Process /sec" => Ordering.by(_.avgProcess)
      case "Latest Batch" => Ordering.by(_.streamingUIData.lastProgress.batchId)
      case unknownColumn => throw new IllegalArgumentException(s"Unknown Column: $unknownColumn")
    }
    if (desc) {
      ordering.reverse
    } else {
      ordering
    }
  }
}
