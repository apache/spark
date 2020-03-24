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

import java.{util => ju}
import java.lang.{Long => JLong}
import java.util.UUID
import javax.servlet.http.HttpServletRequest

import scala.xml.{Node, Unparsed}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.ui.UIUtils._
import org.apache.spark.ui.{GraphUIData, JsCollector, UIUtils => SparkUIUtils, WebUIPage}

private[ui] class StreamingQueryStatisticsPage(parent: StreamingQueryTab)
  extends WebUIPage("statistics") with Logging {

  def generateLoadResources(request: HttpServletRequest): Seq[Node] = {
    // scalastyle:off
    <script src={SparkUIUtils.prependBaseUri(request, "/static/d3.min.js")}></script>
        <link rel="stylesheet" href={SparkUIUtils.prependBaseUri(request, "/static/streaming-page.css")} type="text/css"/>
      <script src={SparkUIUtils.prependBaseUri(request, "/static/streaming-page.js")}></script>
      <script src={SparkUIUtils.prependBaseUri(request, "/static/structured-streaming-page.js")}></script>
    // scalastyle:on
  }

  override def render(request: HttpServletRequest): Seq[Node] = {
    val parameterId = request.getParameter("id")
    require(parameterId != null && parameterId.nonEmpty, "Missing id parameter")

    val query = parent.statusListener.allQueryStatus.find { case q =>
      q.runId.equals(UUID.fromString(parameterId))
    }.getOrElse(throw new IllegalArgumentException(s"Failed to find streaming query $parameterId"))

    val resources = generateLoadResources(request)
    val basicInfo = generateBasicInfo(query)
    val content =
      resources ++
        basicInfo ++
        generateStatTable(query)
    SparkUIUtils.headerSparkPage(request, "Streaming Query Statistics", content, parent)
  }

  def generateTimeMap(times: Seq[Long]): Seq[Node] = {
    val js = "var timeFormat = {};\n" + times.map { time =>
      val formattedTime = SparkUIUtils.formatBatchTime(time, 1, showYYYYMMSS = false)
      s"timeFormat[$time] = '$formattedTime';"
    }.mkString("\n")

    <script>{Unparsed(js)}</script>
  }

  def generateTimeTipStrings(values: Array[(Long, Long)]): Seq[Node] = {
    val js = "var timeTipStrings = {};\n" + values.map { case (batchId, time) =>
      val formattedTime = SparkUIUtils.formatBatchTime(time, 1, showYYYYMMSS = false)
      s"timeTipStrings[$time] = 'batch $batchId ($formattedTime)';"
    }.mkString("\n")

    <script>{Unparsed(js)}</script>
  }

  def generateFormattedTimeTipStrings(values: Array[(Long, Long)]): Seq[Node] = {
    val js = "var formattedTimeTipStrings = {};\n" + values.map { case (batchId, time) =>
      val formattedTime = SparkUIUtils.formatBatchTime(time, 1, showYYYYMMSS = false)
      s"""formattedTimeTipStrings["$formattedTime"] = 'batch $batchId ($formattedTime)';"""
    }.mkString("\n")

    <script>{Unparsed(js)}</script>
  }

  def generateTimeToValues(values: Array[(Long, ju.Map[String, JLong])]): Seq[Node] = {
    val durationDataPadding = SparkUIUtils.durationDataPadding(values)
    val js = "var formattedTimeToValues = {};\n" + durationDataPadding.map { case (x, y) =>
      val s = y.toSeq.sortBy(_._1).map(e => s""""${e._2}"""").mkString("[", ",", "]")
      val formattedTime = SparkUIUtils.formatBatchTime(x, 1, showYYYYMMSS = false)
      s"""formattedTimeToValues["$formattedTime"] = $s;"""
    }.mkString("\n")

    <script>{Unparsed(js)}</script>
  }

  def generateBasicInfo(query: StreamingQueryUIData): Seq[Node] = {
    val duration = if (query.isActive) {
      SparkUIUtils.formatDurationVerbose(System.currentTimeMillis() - query.startTimestamp)
    } else {
      withNoProgress(query, {
        val end = query.lastProgress.timestamp
        val start = query.recentProgress.head.timestamp
        SparkUIUtils.formatDurationVerbose(
          parseProgressTimestamp(end) - parseProgressTimestamp(start))
      }, "-")
    }

    val name = UIUtils.getQueryName(query)
    val numBatches = withNoProgress(query, { query.lastProgress.batchId + 1L }, 0)
    <div>Running batches for
      <strong>
        {duration}
      </strong>
      since
      <strong>
        {SparkUIUtils.formatDate(query.startTimestamp)}
      </strong>
      (<strong>{numBatches}</strong> completed batches)
    </div>
    <br />
    <div><strong>Name: </strong>{name}</div>
    <div><strong>Id: </strong>{query.id}</div>
    <div><strong>RunId: </strong>{query.runId}</div>
    <br />
  }

  def generateStatTable(query: StreamingQueryUIData): Seq[Node] = {
    val batchToTimestamps = withNoProgress(query,
      query.recentProgress.map(p => (p.batchId, parseProgressTimestamp(p.timestamp))),
      Array.empty[(Long, Long)])
    val batchTimes = batchToTimestamps.map(_._2)
    val minBatchTime =
      withNoProgress(query, parseProgressTimestamp(query.recentProgress.head.timestamp), 0L)
    val maxBatchTime =
      withNoProgress(query, parseProgressTimestamp(query.lastProgress.timestamp), 0L)
    val maxRecordRate =
      withNoProgress(query, query.recentProgress.map(_.inputRowsPerSecond).max, 0L)
    val minRecordRate = 0L
    val maxProcessRate =
      withNoProgress(query, query.recentProgress.map(_.processedRowsPerSecond).max, 0L)

    val minProcessRate = 0L
    val maxRows = withNoProgress(query, query.recentProgress.map(_.numInputRows).max, 0L)
    val minRows = 0L
    val maxBatchDuration = withNoProgress(query, query.recentProgress.map(_.batchDuration).max, 0L)
    val minBatchDuration = 0L

    val inputRateData = withNoProgress(query,
      query.recentProgress.map(p => (parseProgressTimestamp(p.timestamp),
        withNumberInvalid { p.inputRowsPerSecond })), Array.empty[(Long, Double)])
    val processRateData = withNoProgress(query,
      query.recentProgress.map(p => (parseProgressTimestamp(p.timestamp),
        withNumberInvalid { p.processedRowsPerSecond })), Array.empty[(Long, Double)])
    val inputRowsData = withNoProgress(query,
      query.recentProgress.map(p => (parseProgressTimestamp(p.timestamp),
        withNumberInvalid { p.numInputRows })), Array.empty[(Long, Double)])
    val batchDurations = withNoProgress(query,
      query.recentProgress.map(p => (parseProgressTimestamp(p.timestamp),
        withNumberInvalid { p.batchDuration })), Array.empty[(Long, Double)])
    val operationDurationData = withNoProgress(
      query,
      query.recentProgress.map { p =>
        val durationMs = p.durationMs
        // remove "triggerExecution" as it count the other operation duration.
        durationMs.remove("triggerExecution")
        (parseProgressTimestamp(p.timestamp), durationMs)
      },
      Array.empty[(Long, ju.Map[String, JLong])])

    val jsCollector = new JsCollector
    val graphUIDataForInputRate =
      new GraphUIData(
        "input-rate-timeline",
        "input-rate-histogram",
        inputRateData,
        minBatchTime,
        maxBatchTime,
        minRecordRate,
        maxRecordRate,
        "records/sec")
    graphUIDataForInputRate.generateDataJs(jsCollector)

    val graphUIDataForProcessRate =
      new GraphUIData(
        "process-rate-timeline",
        "process-rate-histogram",
        processRateData,
        minBatchTime,
        maxBatchTime,
        minProcessRate,
        maxProcessRate,
        "records/sec")
    graphUIDataForProcessRate.generateDataJs(jsCollector)

    val graphUIDataForInputRows =
      new GraphUIData(
        "input-rows-timeline",
        "input-rows-histogram",
        inputRowsData,
        minBatchTime,
        maxBatchTime,
        minRows,
        maxRows,
        "records")
    graphUIDataForInputRows.generateDataJs(jsCollector)

    val graphUIDataForBatchDuration =
      new GraphUIData(
        "batch-duration-timeline",
        "batch-duration-histogram",
        batchDurations,
        minBatchTime,
        maxBatchTime,
        minBatchDuration,
        maxBatchDuration,
        "ms")
    graphUIDataForBatchDuration.generateDataJs(jsCollector)

    val graphUIDataForDuration =
      new GraphUIData(
        "duration-area-stack",
        "",
        Seq.empty[(Long, Double)],
        0L,
        0L,
        0L,
        0L,
        "ms")

    val table = if (query.lastProgress != null) {
      // scalastyle:off
      <table id="stat-table" class="table table-bordered" style="width: auto">
        <thead>
          <tr>
            <th style="width: 160px;"></th>
            <th style="width: 492px;">Timelines</th>
            <th style="width: 350px;">Histograms</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td style="vertical-align: middle;">
              <div style="width: 160px;">
                <div><strong>Input Rate {SparkUIUtils.tooltip("The aggregate (across all sources) rate of data arriving.", "right")}</strong></div>
              </div>
            </td>
            <td class="timeline">{graphUIDataForInputRate.generateTimelineHtml(jsCollector)}</td>
            <td class="histogram">{graphUIDataForInputRate.generateHistogramHtml(jsCollector)}</td>
          </tr>
          <tr>
            <td style="vertical-align: middle;">
              <div style="width: 160px;">
                <div><strong>Process Rate {SparkUIUtils.tooltip("The aggregate (across all sources) rate at which Spark is processing data.", "right")}</strong></div>
              </div>
            </td>
            <td class="timeline">{graphUIDataForProcessRate.generateTimelineHtml(jsCollector)}</td>
            <td class="histogram">{graphUIDataForProcessRate.generateHistogramHtml(jsCollector)}</td>
          </tr>
          <tr>
            <td style="vertical-align: middle;">
              <div style="width: 160px;">
                <div><strong>Input Rows {SparkUIUtils.tooltip("The aggregate (across all sources) number of records processed in a trigger.", "right")}</strong></div>
              </div>
            </td>
            <td class="timeline">{graphUIDataForInputRows.generateTimelineHtml(jsCollector)}</td>
            <td class="histogram">{graphUIDataForInputRows.generateHistogramHtml(jsCollector)}</td>
          </tr>
          <tr>
            <td style="vertical-align: middle;">
              <div style="width: 160px;">
                <div><strong>Batch Duration {SparkUIUtils.tooltip("The process duration of each batch.", "right")}</strong></div>
              </div>
            </td>
            <td class="timeline">{graphUIDataForBatchDuration.generateTimelineHtml(jsCollector)}</td>
            <td class="histogram">{graphUIDataForBatchDuration.generateHistogramHtml(jsCollector)}</td>
          </tr>
          <tr>
            <td style="vertical-align: middle;">
              <div style="width: auto;">
                <div><strong>Operation Duration {SparkUIUtils.tooltip("The amount of time taken to perform various operations in milliseconds.", "right")}</strong></div>
              </div>
            </td>
            <td class="duration-area-stack" colspan="2">{graphUIDataForDuration.generateAreaStackHtmlWithData(jsCollector, operationDurationData)}</td>
          </tr>
        </tbody>
      </table>
    } else {
      <div id="empty-streaming-query-message">
        <b>No visualization information available.</b>
      </div>
      // scalastyle:on
    }

    generateTimeToValues(operationDurationData) ++
      generateFormattedTimeTipStrings(batchToTimestamps) ++
      generateTimeMap(batchTimes) ++ generateTimeTipStrings(batchToTimestamps) ++
      table ++ jsCollector.toHtml
  }
}
