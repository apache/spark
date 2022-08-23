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
import java.util.Locale
import javax.servlet.http.HttpServletRequest

import scala.collection.JavaConverters._
import scala.xml.{Node, NodeBuffer, Unparsed}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.state.StateStoreProvider
import org.apache.spark.sql.internal.SQLConf.STATE_STORE_PROVIDER_CLASS
import org.apache.spark.sql.internal.StaticSQLConf.ENABLED_STREAMING_UI_CUSTOM_METRIC_LIST
import org.apache.spark.sql.streaming.ui.UIUtils._
import org.apache.spark.ui.{GraphUIData, JsCollector, UIUtils => SparkUIUtils, WebUIPage}

private[ui] class StreamingQueryStatisticsPage(parent: StreamingQueryTab)
  extends WebUIPage("statistics") with Logging {

  // State store provider implementation mustn't do any heavyweight initialiation in constructor
  // but in its init method.
  private val supportedCustomMetrics = StateStoreProvider.create(
    parent.parent.conf.get(STATE_STORE_PROVIDER_CLASS)).supportedCustomMetrics
  logDebug(s"Supported custom metrics: $supportedCustomMetrics")

  private val enabledCustomMetrics =
    parent.parent.conf.get(ENABLED_STREAMING_UI_CUSTOM_METRIC_LIST).map(_.toLowerCase(Locale.ROOT))
  logDebug(s"Enabled custom metrics: $enabledCustomMetrics")

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

    val query = parent.store.allQueryUIData.find { uiData =>
      uiData.summary.runId.equals(parameterId)
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

  def generateBasicInfo(uiData: StreamingQueryUIData): Seq[Node] = {
    val duration = if (uiData.summary.isActive) {
      val durationMs = System.currentTimeMillis() - uiData.summary.startTimestamp
      SparkUIUtils.formatDurationVerbose(durationMs)
    } else {
      withNoProgress(uiData, {
        val end = uiData.lastProgress.timestamp
        val start = uiData.recentProgress.head.timestamp
        SparkUIUtils.formatDurationVerbose(
          parseProgressTimestamp(end) - parseProgressTimestamp(start))
      }, "-")
    }

    val name = UIUtils.getQueryName(uiData)
    val numBatches = withNoProgress(uiData, { uiData.lastProgress.batchId + 1L }, 0)
    <div>Running batches for
      <strong>
        {duration}
      </strong>
      since
      <strong>
        {SparkUIUtils.formatDate(uiData.summary.startTimestamp)}
      </strong>
      (<strong>{numBatches}</strong> completed batches)
    </div>
    <br />
    <div><strong>Name: </strong>{name}</div>
    <div><strong>Id: </strong>{uiData.summary.id}</div>
    <div><strong>RunId: </strong>{uiData.summary.runId}</div>
    <br />
  }

  def generateWatermark(
      query: StreamingQueryUIData,
      minBatchTime: Long,
      maxBatchTime: Long,
      jsCollector: JsCollector): Seq[Node] = {
    // This is made sure on caller side but put it here to be defensive
    require(query.lastProgress != null)
    if (query.lastProgress.eventTime.containsKey("watermark")) {
      val watermarkData = query.recentProgress.flatMap { p =>
        val batchTimestamp = parseProgressTimestamp(p.timestamp)
        val watermarkValue = parseProgressTimestamp(p.eventTime.get("watermark"))
        if (watermarkValue > 0L) {
          // seconds
          Some((batchTimestamp, ((batchTimestamp - watermarkValue) / 1000.0)))
        } else {
          None
        }
      }

      if (watermarkData.nonEmpty) {
        val maxWatermark = watermarkData.maxBy(_._2)._2
        val graphUIDataForWatermark =
          new GraphUIData(
            "watermark-gap-timeline",
            "watermark-gap-histogram",
            watermarkData,
            minBatchTime,
            maxBatchTime,
            0,
            maxWatermark,
            "seconds")
        graphUIDataForWatermark.generateDataJs(jsCollector)

        // scalastyle:off
        <tr>
          <td style="vertical-align: middle;">
            <div style="width: 160px;">
              <div><strong>Global Watermark Gap {SparkUIUtils.tooltip("The gap between batch timestamp and global watermark for the batch.", "right")}</strong></div>
            </div>
          </td>
          <td class="watermark-gap-timeline">{graphUIDataForWatermark.generateTimelineHtml(jsCollector)}</td>
          <td class="watermark-gap-histogram">{graphUIDataForWatermark.generateHistogramHtml(jsCollector)}</td>
        </tr>
        // scalastyle:on
      } else {
        Seq.empty[Node]
      }
    } else {
      Seq.empty[Node]
    }
  }

  def generateAggregatedStateOperators(
      query: StreamingQueryUIData,
      minBatchTime: Long,
      maxBatchTime: Long,
      jsCollector: JsCollector): NodeBuffer = {
    // This is made sure on caller side but put it here to be defensive
    require(query.lastProgress != null)
    if (query.lastProgress.stateOperators.nonEmpty) {
      val numRowsTotalData = query.recentProgress.map(p => (parseProgressTimestamp(p.timestamp),
        p.stateOperators.map(_.numRowsTotal).sum.toDouble))
      val maxNumRowsTotal = numRowsTotalData.maxBy(_._2)._2

      val numRowsUpdatedData = query.recentProgress.map(p => (parseProgressTimestamp(p.timestamp),
        p.stateOperators.map(_.numRowsUpdated).sum.toDouble))
      val maxNumRowsUpdated = numRowsUpdatedData.maxBy(_._2)._2

      val memoryUsedBytesData = query.recentProgress.map(p => (parseProgressTimestamp(p.timestamp),
        p.stateOperators.map(_.memoryUsedBytes).sum.toDouble))
      val maxMemoryUsedBytes = memoryUsedBytesData.maxBy(_._2)._2

      val numRowsDroppedByWatermarkData = query.recentProgress
        .map(p => (parseProgressTimestamp(p.timestamp),
          p.stateOperators.map(_.numRowsDroppedByWatermark).sum.toDouble))
      val maxNumRowsDroppedByWatermark = numRowsDroppedByWatermarkData.maxBy(_._2)._2

      val graphUIDataForNumberTotalRows =
        new GraphUIData(
          "aggregated-num-total-state-rows-timeline",
          "aggregated-num-total-state-rows-histogram",
          numRowsTotalData,
          minBatchTime,
          maxBatchTime,
          0,
          maxNumRowsTotal,
          "records")
      graphUIDataForNumberTotalRows.generateDataJs(jsCollector)

      val graphUIDataForNumberUpdatedRows =
        new GraphUIData(
          "aggregated-num-updated-state-rows-timeline",
          "aggregated-num-updated-state-rows-histogram",
          numRowsUpdatedData,
          minBatchTime,
          maxBatchTime,
          0,
          maxNumRowsUpdated,
          "records")
      graphUIDataForNumberUpdatedRows.generateDataJs(jsCollector)

      val graphUIDataForMemoryUsedBytes =
        new GraphUIData(
          "aggregated-state-memory-used-bytes-timeline",
          "aggregated-state-memory-used-bytes-histogram",
          memoryUsedBytesData,
          minBatchTime,
          maxBatchTime,
          0,
          maxMemoryUsedBytes,
          "bytes")
      graphUIDataForMemoryUsedBytes.generateDataJs(jsCollector)

      val graphUIDataForNumRowsDroppedByWatermark =
        new GraphUIData(
          "aggregated-num-rows-dropped-by-watermark-timeline",
          "aggregated-num-rows-dropped-by-watermark-histogram",
          numRowsDroppedByWatermarkData,
          minBatchTime,
          maxBatchTime,
          0,
          maxNumRowsDroppedByWatermark,
          "records")
      graphUIDataForNumRowsDroppedByWatermark.generateDataJs(jsCollector)

      val result =
        // scalastyle:off
        <tr>
          <td style="vertical-align: middle;">
            <div style="width: 160px;">
              <div><strong>Aggregated Number Of Total State Rows {SparkUIUtils.tooltip("Aggregated number of total state rows.", "right")}</strong></div>
            </div>
          </td>
          <td class={"aggregated-num-total-state-rows-timeline"}>{graphUIDataForNumberTotalRows.generateTimelineHtml(jsCollector)}</td>
          <td class={"aggregated-num-total-state-rows-histogram"}>{graphUIDataForNumberTotalRows.generateHistogramHtml(jsCollector)}</td>
        </tr>
        <tr>
          <td style="vertical-align: middle;">
            <div style="width: 160px;">
              <div><strong>Aggregated Number Of Updated State Rows {SparkUIUtils.tooltip("Aggregated number of updated state rows.", "right")}</strong></div>
            </div>
          </td>
          <td class={"aggregated-num-updated-state-rows-timeline"}>{graphUIDataForNumberUpdatedRows.generateTimelineHtml(jsCollector)}</td>
          <td class={"aggregated-num-updated-state-rows-histogram"}>{graphUIDataForNumberUpdatedRows.generateHistogramHtml(jsCollector)}</td>
        </tr>
        <tr>
          <td style="vertical-align: middle;">
            <div style="width: 160px;">
              <div><strong>Aggregated State Memory Used In Bytes {SparkUIUtils.tooltip("Aggregated state memory used in bytes.", "right")}</strong></div>
            </div>
          </td>
          <td class={"aggregated-state-memory-used-bytes-timeline"}>{graphUIDataForMemoryUsedBytes.generateTimelineHtml(jsCollector)}</td>
          <td class={"aggregated-state-memory-used-bytes-histogram"}>{graphUIDataForMemoryUsedBytes.generateHistogramHtml(jsCollector)}</td>
        </tr>
        <tr>
          <td style="vertical-align: middle;">
            <div style="width: 160px;">
              <div><strong>Aggregated Number Of Rows Dropped By Watermark {SparkUIUtils.tooltip("Accumulates all input rows being dropped in stateful operators by watermark. 'Inputs' are relative to operators.", "right")}</strong></div>
            </div>
          </td>
          <td class={"aggregated-num-rows-dropped-by-watermark-timeline"}>{graphUIDataForNumRowsDroppedByWatermark.generateTimelineHtml(jsCollector)}</td>
          <td class={"aggregated-num-rows-dropped-by-watermark-histogram"}>{graphUIDataForNumRowsDroppedByWatermark.generateHistogramHtml(jsCollector)}</td>
        </tr>
        // scalastyle:on

      if (enabledCustomMetrics.nonEmpty) {
        result ++= generateAggregatedCustomMetrics(query, minBatchTime, maxBatchTime, jsCollector)
      }
      result
    } else {
      new NodeBuffer()
    }
  }

  def generateAggregatedCustomMetrics(
      query: StreamingQueryUIData,
      minBatchTime: Long,
      maxBatchTime: Long,
      jsCollector: JsCollector): NodeBuffer = {
    val result: NodeBuffer = new NodeBuffer

    // This is made sure on caller side but put it here to be defensive
    require(query.lastProgress.stateOperators.nonEmpty)
    query.lastProgress.stateOperators.head.customMetrics.keySet().asScala
      .filter(m => enabledCustomMetrics.contains(m.toLowerCase(Locale.ROOT))).map { metricName =>
        val data = query.recentProgress.map(p => (parseProgressTimestamp(p.timestamp),
          p.stateOperators.map(_.customMetrics.get(metricName).toDouble).sum))
        val max = data.maxBy(_._2)._2
        val metric = supportedCustomMetrics.find(_.name.equalsIgnoreCase(metricName)).get

        val graphUIData =
          new GraphUIData(
            s"aggregated-$metricName-timeline",
            s"aggregated-$metricName-histogram",
            data,
            minBatchTime,
            maxBatchTime,
            0,
            max,
            "")
        graphUIData.generateDataJs(jsCollector)

        result ++=
          // scalastyle:off
          <tr>
            <td style="vertical-align: middle;">
              <div style="width: 240px;">
                <div><strong>Aggregated Custom Metric {s"$metricName"} {SparkUIUtils.tooltip(metric.desc, "right")}</strong></div>
              </div>
            </td>
            <td class={s"aggregated-$metricName-timeline"}>{graphUIData.generateTimelineHtml(jsCollector)}</td>
            <td class={s"aggregated-$metricName-histogram"}>{graphUIData.generateHistogramHtml(jsCollector)}</td>
          </tr>
          // scalastyle:on
      }

    result
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
          {generateWatermark(query, minBatchTime, maxBatchTime, jsCollector)}
          {generateAggregatedStateOperators(query, minBatchTime, maxBatchTime, jsCollector)}
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
