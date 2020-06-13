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

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.apache.commons.text.StringEscapeUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.ui.UIUtils._
import org.apache.spark.ui.{UIUtils => SparkUIUtils, WebUIPage}

private[ui] class StreamingQueryPage(parent: StreamingQueryTab)
    extends WebUIPage("") with Logging {

  override def render(request: HttpServletRequest): Seq[Node] = {
    val content = generateStreamingQueryTable(request)
    SparkUIUtils.headerSparkPage(request, "Streaming Query", content, parent)
  }

  def generateDataRow(request: HttpServletRequest, queryActive: Boolean)
    (query: StreamingQueryUIData): Seq[Node] = {

    def details(detail: Any): Seq[Node] = {
      if (queryActive) {
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

    val statisticsLink = "%s/%s/statistics?id=%s"
      .format(SparkUIUtils.prependBaseUri(request, parent.basePath), parent.prefix, query.runId)

    val name = UIUtils.getQueryName(query)
    val status = UIUtils.getQueryStatus(query)
    val duration = if (queryActive) {
      System.currentTimeMillis() - query.startTimestamp
    } else {
      withNoProgress(query, {
        val endTimeMs = query.lastProgress.timestamp
        parseProgressTimestamp(endTimeMs) - query.startTimestamp
      }, 0)
    }

    <tr>
      <td> {name} </td>
      <td> {status} </td>
      <td> {query.id} </td>
      <td> <a href={statisticsLink}> {query.runId} </a> </td>
      <td> {SparkUIUtils.formatDate(query.startTimestamp)} </td>
      <td sorttable_customkey={duration.toString}>
        {SparkUIUtils.formatDurationVerbose(duration)}
      </td>
      <td> {withNoProgress(query, {
        (query.recentProgress.map(p => withNumberInvalid(p.inputRowsPerSecond)).sum /
          query.recentProgress.length).formatted("%.2f") }, "NaN")}
      </td>
      <td> {withNoProgress(query, {
        (query.recentProgress.map(p => withNumberInvalid(p.processedRowsPerSecond)).sum /
          query.recentProgress.length).formatted("%.2f") }, "NaN")}
      </td>
      <td> {withNoProgress(query, { query.lastProgress.batchId }, "NaN")} </td>
      {details(query.exception.getOrElse("-"))}
    </tr>
  }

  private def generateStreamingQueryTable(request: HttpServletRequest): Seq[Node] = {
    val (activeQueries, inactiveQueries) = parent.statusListener.allQueryStatus
      .partition(_.isActive)
    val activeQueryTables = if (activeQueries.nonEmpty) {
      val headerRow = Seq(
        "Name", "Status", "Id", "Run ID", "Start Time", "Duration", "Avg Input /sec",
        "Avg Process /sec", "Lastest Batch")

      val headerCss = Seq("", "", "", "", "", "sorttable_numeric", "sorttable_numeric",
        "sorttable_numeric", "")
      // header classes size must be equal to header row size
      assert(headerRow.size == headerCss.size)

      Some(SparkUIUtils.listingTable(headerRow, generateDataRow(request, queryActive = true),
        activeQueries, true, Some("activeQueries-table"), headerCss, false))
    } else {
      None
    }

    val inactiveQueryTables = if (inactiveQueries.nonEmpty) {
      val headerRow = Seq(
        "Name", "Status", "Id", "Run ID", "Start Time", "Duration", "Avg Input /sec",
        "Avg Process /sec", "Lastest Batch", "Error")

      val headerCss = Seq("", "", "", "", "", "sorttable_numeric", "sorttable_numeric",
        "sorttable_numeric", "", "")
      assert(headerRow.size == headerCss.size)

      Some(SparkUIUtils.listingTable(headerRow, generateDataRow(request, queryActive = false),
        inactiveQueries, true, Some("completedQueries-table"), headerCss, false))
    } else {
      None
    }

    // scalastyle:off
    val content =
      <span id="active" class="collapse-aggregated-activeQueries collapse-table"
            onClick="collapseTable('collapse-aggregated-activeQueries','aggregated-activeQueries')">
        <h5 id="activequeries">
          <span class="collapse-table-arrow arrow-open"></span>
          <a>Active Streaming Queries ({activeQueries.length})</a>
        </h5>
      </span> ++
      <div>
        <ul class="aggregated-activeQueries collapsible-table">
          {activeQueryTables.getOrElse(Seq.empty[Node])}
        </ul>
      </div> ++
      <span id="completed" class="collapse-aggregated-completedQueries collapse-table"
            onClick="collapseTable('collapse-aggregated-completedQueries','aggregated-completedQueries')">
        <h5 id="completedqueries">
          <span class="collapse-table-arrow arrow-open"></span>
          <a>Completed Streaming Queries ({inactiveQueries.length})</a>
        </h5>
      </span> ++
      <div>
        <ul class="aggregated-completedQueries collapsible-table">
          {inactiveQueryTables.getOrElse(Seq.empty[Node])}
        </ul>
      </div>
    // scalastyle:on

    content
  }
}
