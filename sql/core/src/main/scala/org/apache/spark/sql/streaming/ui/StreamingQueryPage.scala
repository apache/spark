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

import java.text.SimpleDateFormat
import java.util.TimeZone
import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.apache.commons.lang3.StringEscapeUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.{QuerySummary, StreamQueryStore}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.streaming.ui.UIUtils._
import org.apache.spark.ui.{UIUtils => SparkUIUtils, WebUIPage}

class StreamingQueryPage(parent: StreamingQueryTab, store: StreamQueryStore)
  extends WebUIPage("") with Logging {
  val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  df.setTimeZone(TimeZone.getDefault)

  override def render(request: HttpServletRequest): Seq[Node] = {
    val content = generateStreamingQueryTable(request)
    SparkUIUtils.headerSparkPage(request, "Streaming Query", content, parent)
  }

  def generateDataRow(request: HttpServletRequest, queryActive: Boolean)
    (streamQuery: (StreamingQuery, Long)): Seq[Node] = {

    val (query, timeSinceStart) = streamQuery
    def details(detail: Any): Seq[Node] = {
      val s = detail.asInstanceOf[String]
      val isMultiline = s.indexOf('\n') >= 0
      val summary = StringEscapeUtils.escapeHtml4(
        if (isMultiline) s.substring(0, s.indexOf('\n')) else s
      )
      val details = if (isMultiline) {
        // scalastyle:off
        <span onclick="this.parentNode.querySelector('.stacktrace-details').classList.toggle('collapsed')"
              class="expand-details">
          +details
        </span> ++
          <div class="stacktrace-details collapsed">
            <pre>{s}</pre>
          </div>
        // scalastyle:on
      } else {
        ""
      }
      <td>{summary}{details}</td>
    }

    val statisticsLink = "%s/%s/statistics?id=%s"
      .format(SparkUIUtils.prependBaseUri(request, parent.basePath), parent.prefix, query.runId)

    val name = UIUtils.getQueryName(query)
    val status = UIUtils.getQueryStatus(query)
    val duration = if (queryActive) {
      SparkUIUtils.formatDurationVerbose(System.currentTimeMillis() - timeSinceStart)
    } else {
      withNoProgress(query, {
        val endTimeMs = query.lastProgress.timestamp
        val startTimeMs = query.recentProgress.head.timestamp
        SparkUIUtils.formatDurationVerbose(
          df.parse(endTimeMs).getTime - df.parse(startTimeMs).getTime)
      }, "-")
    }

    <tr>
      <td> {name} </td>
      <td> {status} </td>
      <td> {query.id} </td>
      <td> <a href={statisticsLink}> {query.runId} </a> </td>
      <td> {SparkUIUtils.formatDate(timeSinceStart)} </td>
      <td> {duration} </td>
      <td> {withNoProgress(query, {
        (query.recentProgress.map(p => withNumberInvalid(p.inputRowsPerSecond)).sum /
          query.recentProgress.length).formatted("%.2f") }, "NaN")}
      </td>
      <td> {withNoProgress(query, {
        (query.recentProgress.map(p => withNumberInvalid(p.processedRowsPerSecond)).sum /
          query.recentProgress.length).formatted("%.2f") }, "NaN")}
      </td>
      <td> {withNoProgress(query,
        { query.getQuerySummary.getMetric(QuerySummary.TOTAL_INPUT_RECORDS, 0L) }, "NaN")} </td>
      <td> {withNoProgress(query, { query.lastProgress.batchId }, "NaN")} </td>
      {details(withNoProgress(query, { query.exception.map(_.message).getOrElse("-") }, "-"))}
    </tr>
  }

  private def generateStreamingQueryTable(request: HttpServletRequest): Seq[Node] = {
    val (activeQueries, inactiveQueries) = store.allStreamQueries.partition(_._1.isActive)
    val activeQueryTables = if (activeQueries.nonEmpty) {
      val headerRow = Seq(
        "Query Name", "Status", "Id", "Run ID", "Submit Time", "Duration", "Avg Input /sec",
        "Avg Process /sec", "Total Input Rows", "Last Batch ID", "Error")

      Some(SparkUIUtils.listingTable(headerRow, generateDataRow(request, queryActive = true),
        activeQueries, true, None, Seq(null), false))
    } else {
      None
    }

    val inactiveQueryTables = if (inactiveQueries.nonEmpty) {
      val headerRow = Seq(
        "Query Name", "Status", "Id", "Run ID", "Submit Time", "Duration", "Avg Input /sec",
        "Avg Process /sec", "Total Input Rows", "Last Batch ID", "Error")

      Some(SparkUIUtils.listingTable(headerRow, generateDataRow(request, queryActive = false),
        inactiveQueries, true, None, Seq(null), false))
    } else {
      None
    }

    val content =
      <h5 id="activequeries">Active Streaming Queries ({activeQueries.length})</h5> ++
        <div>
          <ul class="unstyled">
            {activeQueryTables.getOrElse("No active streaming query has been generated yet.")}
          </ul>
        </div> ++
        <h5 id="completedqueries">Completed Streaming Queries ({inactiveQueries.length})</h5> ++
        <div>
          <ul class="unstyled">
            {inactiveQueryTables.getOrElse("No streaming query has completed yet.")}
          </ul>
        </div>

    content
  }
}
