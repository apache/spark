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
import java.text.SimpleDateFormat
import java.util.UUID
import javax.servlet.http.HttpServletRequest

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashSet}
import scala.xml.{Node, Unparsed}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.DateTimeUtils.getTimeZone
import org.apache.spark.sql.execution.streaming.QuerySummary
import org.apache.spark.sql.execution.ui.SQLTab
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.streaming.ui.UIUtils._
import org.apache.spark.ui.{UIUtils => SparkUIUtils, WebUIPage}

class StreamingQueryStatisticsPage(
    parent: SQLTab,
    store: Option[HashSet[(StreamingQuery, Long)]])
  extends WebUIPage("streaming/statistics") with Logging {
  val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  df.setTimeZone(getTimeZone("UTC"))

  def generateLoadResources(request: HttpServletRequest): Seq[Node] = {
    // scalastyle:off
    <script src={SparkUIUtils.prependBaseUri(request, "/static/d3.min.js")}></script>
        <link rel="stylesheet" href={SparkUIUtils.prependBaseUri(request, "/static/sql/streaming/streaming-page.css")} type="text/css"/>
      <script src={SparkUIUtils.prependBaseUri(request, "/static/sql/streaming/streaming-page.js")}></script>
    // scalastyle:on
  }

  override def render(request: HttpServletRequest): Seq[Node] = {
    val parameterId = UIUtils.stripXSS(request.getParameter("id"))
    require(parameterId != null && parameterId.nonEmpty, "Missing id parameter")

    val (query, timeSinceStart) = if (store.nonEmpty) {
      store.get.find(i => i._1.runId.equals(UUID.fromString(parameterId)))
        .getOrElse(throw new Exception(s"Can not find streaming query $parameterId"))
    } else {
      throw new Exception(s"Can not find streaming query $parameterId")
    }

    val resources = generateLoadResources(request)
    val basicInfo = generateBasicInfo(query, timeSinceStart)
    val content =
      store.synchronized { // make sure all parts in this page are consistent
        resources ++
          basicInfo ++
          generateStatTable(query)
      }
    SparkUIUtils.headerSparkPage(request, "Streaming Query Statistics", content, parent)
  }

  def generateTimeMap(times: Seq[Long]): Seq[Node] = {
    val js = "var timeFormat = {};\n" + times.map { time =>
      val formattedTime =
        UIUtils.formatBatchTime(time, 1, showYYYYMMSS = false)
      s"timeFormat[$time] = '$formattedTime';"
    }.mkString("\n")

    <script>{Unparsed(js)}</script>
  }

  def generateVar(values: Array[(Long, ju.Map[String, JLong])]): Seq[Node] = {
    val js = "var timeToValues = {};\n" + values.map { case (x, y) =>
      val s = y.asScala.toSeq.sortBy(_._1).map(e => s""""${e._2.toDouble}"""")
        .mkString("[", ",", "]")
      s"""timeToValues["${UIUtils.formatBatchTime(x, 1, showYYYYMMSS = false)}"] = $s;"""
    }.mkString("\n")

    <script>{Unparsed(js)}</script>
  }

  def generateBasicInfo(query: StreamingQuery, timeSinceStart: Long): Seq[Node] = {
    val duration = if (query.isActive) {
      SparkUIUtils.formatDurationVerbose(System.currentTimeMillis() - timeSinceStart)
    } else {
      withNoProgress(query, {
        val end = query.lastProgress.timestamp
        val start = query.recentProgress.head.timestamp
        SparkUIUtils.formatDurationVerbose(
          df.parse(end).getTime - df.parse(start).getTime)
      }, "-")
    }

    <div>Running batches for
      <strong>
        {duration}
      </strong>
      since
      <strong>
        {SparkUIUtils.formatDate(timeSinceStart)}
      </strong>
      (<strong>{withNoProgress(query, { query.lastProgress.batchId + 1L }, "NaN")}</strong>
      completed batches,
      <strong>{query.getQuerySummary.getMetric(QuerySummary.TOTAL_INPUT_RECORDS, 0L)}
      </strong> records)
    </div>
    <br />
  }

  def generateStatTable(query: StreamingQuery): Seq[Node] = {
    val batchTimes = withNoProgress(query,
      query.recentProgress.map(p => df.parse(p.timestamp).getTime), Array.empty[Long])
    val minBatchTime = withNoProgress(query, df.parse(query.recentProgress.head.timestamp).getTime,
      0L)
    val maxBatchTime = withNoProgress(query, df.parse(query.lastProgress.timestamp).getTime,
      0L)
    val maxRecordRate = withNoProgress(query, query.recentProgress.map(_.inputRowsPerSecond).max,
      0L)
    val minRecordRate = 0L
    val maxProcessRate = withNoProgress(query,
      query.recentProgress.map(_.processedRowsPerSecond).max, 0L)
    val minProcessRate = 0L
    val maxRows = withNoProgress(query, query.recentProgress.map(_.numInputRows).max, 0L)
    val minRows = 0L
    val maxBatchDuration = withNoProgress(query, query.recentProgress.map(_.batchDuration).max, 0L)
    val minBatchDuration = 0L

    val inputRateData = withNoProgress(query, query.recentProgress.map(p =>
      (df.parse(p.timestamp).getTime, withNumberInvalid { p.inputRowsPerSecond })),
      Array.empty[(Long, Double)])
    val processRateData = withNoProgress(query, query.recentProgress.map(p =>
      (df.parse(p.timestamp).getTime, withNumberInvalid { p.processedRowsPerSecond })),
      Array.empty[(Long, Double)])
    val inputRowsData = withNoProgress(query, query.recentProgress.map(p =>
      (df.parse(p.timestamp).getTime, withNumberInvalid { p.numInputRows })),
      Array.empty[(Long, Double)])
    val batchDurations = withNoProgress(query, query.recentProgress.map(p =>
      (df.parse(p.timestamp).getTime, withNumberInvalid { p.batchDuration })),
      Array.empty[(Long, Double)])
    val operationDurationData = withNoProgress(query, query.recentProgress.map { case p =>
      val durationMs = p.durationMs
      durationMs.remove("triggerExecution")
      (df.parse(p.timestamp).getTime, durationMs)},
      Array.empty[(Long, ju.Map[String, JLong])])
    val operationLabels = withNoProgress(query, {
        val durationKeys = query.lastProgress.durationMs.keySet()
        // remove "triggerExecution" as it count the other operation duration.
        durationKeys.remove("triggerExecution")
        durationKeys.asScala.toSeq.sorted
      }, Seq.empty[String])

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

    val table =
    // scalastyle:off
      <table id="stat-table" class="table table-bordered" style="width: auto">
        <thead>
          <tr>
            <th style="width: 160px;"></th>
            <th style="width: 492px;">Timelines</th>
            <th style="width: 350px;">Histograms</th></tr>
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
              <div style="width: 160px;">
                <div><strong>Operation Duration {SparkUIUtils.tooltip("The amount of time taken to perform various operations in milliseconds.", "right")}</strong></div>
              </div>
            </td>
            <td class="duration-area-stack" colspan="2">{graphUIDataForDuration.generateAreaStackHtmlWithData(jsCollector, operationDurationData, operationLabels)}</td>
          </tr>
        </tbody>
      </table>
    // scalastyle:on

    generateVar(operationDurationData) ++ generateTimeMap(batchTimes) ++ table ++ jsCollector.toHtml
  }
}

class JsCollector {

  private var variableId = 0

  /**
   * Return the next unused JavaScript variable name
   */
  def nextVariableName: String = {
    variableId += 1
    "v" + variableId
  }

  /**
   * JavaScript statements that will execute before `statements`
   */
  private val preparedStatements = ArrayBuffer[String]()

  /**
   * JavaScript statements that will execute after `preparedStatements`
   */
  private val statements = ArrayBuffer[String]()

  def addPreparedStatement(js: String): Unit = {
    preparedStatements += js
  }

  def addStatement(js: String): Unit = {
    statements += js
  }

  /**
   * Generate a html snippet that will execute all scripts when the DOM has finished loading.
   */
  def toHtml: Seq[Node] = {
    val js =
      s"""
         |$$(document).ready(function() {
         |    ${preparedStatements.mkString("\n")}
         |    ${statements.mkString("\n")}
         |});""".stripMargin

    <script>{Unparsed(js)}</script>
  }
}

class GraphUIData(
    timelineDivId: String,
    histogramDivId: String,
    data: Seq[(Long, Double)],
    minX: Long,
    maxX: Long,
    minY: Double,
    maxY: Double,
    unitY: String,
    batchInterval: Option[Double] = None) {

  private var dataJavaScriptName: String = _

  def generateDataJs(jsCollector: JsCollector): Unit = {
    val jsForData = data.map { case (x, y) =>
      s"""{"x": $x, "y": $y}"""
    }.mkString("[", ",", "]")
    dataJavaScriptName = jsCollector.nextVariableName
    jsCollector.addPreparedStatement(s"var $dataJavaScriptName = $jsForData;")
  }

  def generateTimelineHtml(jsCollector: JsCollector): Seq[Node] = {
    jsCollector.addPreparedStatement(s"registerTimeline($minY, $maxY);")
    if (batchInterval.isDefined) {
      jsCollector.addStatement(
        "drawTimeline(" +
          s"'#$timelineDivId', $dataJavaScriptName, $minX, $maxX, $minY, $maxY, '$unitY'," +
          s" ${batchInterval.get}" +
          ");")
    } else {
      jsCollector.addStatement(
        s"drawTimeline('#$timelineDivId', $dataJavaScriptName, $minX, $maxX, $minY, $maxY," +
          s" '$unitY');")
    }
    <div id={timelineDivId}></div>
  }

  def generateHistogramHtml(jsCollector: JsCollector): Seq[Node] = {
    val histogramData = s"$dataJavaScriptName.map(function(d) { return d.y; })"
    jsCollector.addPreparedStatement(s"registerHistogram($histogramData, $minY, $maxY);")
    if (batchInterval.isDefined) {
      jsCollector.addStatement(
        "drawHistogram(" +
          s"'#$histogramDivId', $histogramData, $minY, $maxY, '$unitY', ${batchInterval.get}" +
          ");")
    } else {
      jsCollector.addStatement(
        s"drawHistogram('#$histogramDivId', $histogramData, $minY, $maxY, '$unitY');")
    }
    <div id={histogramDivId}></div>
  }

  def generateAreaStackHtmlWithData(
      jsCollector: JsCollector,
      values: Array[(Long, ju.Map[String, JLong])],
      operationLabels: Seq[String]): Seq[Node] = {
    val jsForData = values.map { case (x, y) =>
      val s = y.asScala.toSeq.sortBy(_._1).map(e => s""""${e._1}": "${e._2.toDouble}"""")
        .mkString(",")
      s"""{x: "${UIUtils.formatBatchTime(x, 1, showYYYYMMSS = false)}", $s}"""
    }.mkString("[", ",", "]")
    val jsForLabels = operationLabels.mkString("[\"", "\",\"", "\"]")
    val (maxX, minX, maxY, minY) = if (values != null && values.length > 0) {
      (values.map(_._1.toLong).max,
        values.map(_._1.toLong).min,
        values.map(_._2.asScala.toSeq.map(_._2.toLong).sum).max,
        values.map(_._2.asScala.toSeq.map(_._2.toLong).sum).min)
    } else {
      (0L, 0L, 0L, 0L)
    }

    dataJavaScriptName = jsCollector.nextVariableName
    jsCollector.addPreparedStatement(s"var $dataJavaScriptName = $jsForData;")
    val labels = jsCollector.nextVariableName
    jsCollector.addPreparedStatement(s"var $labels = $jsForLabels;")
    jsCollector.addStatement(
      s"drawAreaStack('#$timelineDivId', $labels, $dataJavaScriptName, $minX, $maxX, $minY, $maxY)")
    <div id={timelineDivId}></div>
  }
}
