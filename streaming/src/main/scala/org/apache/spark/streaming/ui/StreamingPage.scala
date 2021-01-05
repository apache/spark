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

package org.apache.spark.streaming.ui

import java.util.concurrent.TimeUnit
import javax.servlet.http.HttpServletRequest

import scala.collection.mutable
import scala.xml.{Node, Unparsed}

import org.apache.spark.internal.Logging
import org.apache.spark.ui.{GraphUIData, JsCollector, UIUtils => SparkUIUtils, WebUIPage}
import org.apache.spark.util.Utils

/**
 * A helper class for "scheduling delay", "processing time" and "total delay" to generate data that
 * will be used in the timeline and histogram graphs.
 *
 * @param data (batchTime, milliseconds). "milliseconds" is something like "processing time".
 */
private[ui] class MillisecondsStatUIData(data: Seq[(Long, Long)]) {

  /**
   * Converting the original data as per `unit`.
   */
  def timelineData(unit: TimeUnit): Seq[(Long, Double)] =
    data.map(x => x._1 -> UIUtils.convertToTimeUnit(x._2, unit))

  /**
   * Converting the original data as per `unit`.
   */
  def histogramData(unit: TimeUnit): Seq[Double] =
    data.map(x => UIUtils.convertToTimeUnit(x._2, unit))

  val avg: Option[Long] = if (data.isEmpty) None else Some(data.map(_._2).sum / data.size)

  val formattedAvg: String = StreamingPage.formatDurationOption(avg)

  val max: Option[Long] = if (data.isEmpty) None else Some(data.map(_._2).max)
}

/**
 * A helper class for "input rate" to generate data that will be used in the timeline and histogram
 * graphs.
 *
 * @param data (batch time, record rate).
 */
private[ui] class RecordRateUIData(val data: Seq[(Long, Double)]) {

  val avg: Option[Double] = if (data.isEmpty) None else Some(data.map(_._2).sum / data.size)

  val formattedAvg: String = avg.map(_.formatted("%.2f")).getOrElse("-")

  val max: Option[Double] = if (data.isEmpty) None else Some(data.map(_._2).max)
}

/** Page for Spark Web UI that shows statistics of a streaming job */
private[ui] class StreamingPage(parent: StreamingTab)
  extends WebUIPage("") with Logging {

  import StreamingPage._

  private val listener = parent.listener

  private def startTime: Long = listener.startTime

  /** Render the page */
  def render(request: HttpServletRequest): Seq[Node] = {
    val resources = generateLoadResources(request)
    val onClickTimelineFunc = generateOnClickTimelineFunction()
    val basicInfo = generateBasicInfo()
    val content = resources ++
      onClickTimelineFunc ++ basicInfo ++
      listener.synchronized {
        generateStatTable() ++
          generateBatchListTables(request)
      }
    SparkUIUtils.headerSparkPage(request, "Streaming Statistics", content, parent)
  }

  /**
   * Generate html that will load css/js files for StreamingPage
   */
  private def generateLoadResources(request: HttpServletRequest): Seq[Node] = {
    // scalastyle:off
    <script src={SparkUIUtils.prependBaseUri(request, "/static/d3.min.js")}></script>
      <link rel="stylesheet" href={SparkUIUtils.prependBaseUri(request, "/static/streaming-page.css")} type="text/css"/>
      <script src={SparkUIUtils.prependBaseUri(request, "/static/streaming-page.js")}></script>
    // scalastyle:on
  }

  /** Generate html that will set onClickTimeline declared in streaming-page.js */
  private def generateOnClickTimelineFunction(): Seq[Node] = {
    val js = "onClickTimeline = getOnClickTimelineFunction();"
    <script>{Unparsed(js)}</script>
  }

  /** Generate basic information of the streaming program */
  private def generateBasicInfo(): Seq[Node] = {
    val timeSinceStart = System.currentTimeMillis() - startTime
    <div>Running batches of
      <strong>
        {SparkUIUtils.formatDurationVerbose(listener.batchDuration)}
      </strong>
      for
      <strong>
        {SparkUIUtils.formatDurationVerbose(timeSinceStart)}
      </strong>
      since
      <strong>
        {SparkUIUtils.formatDate(startTime)}
      </strong>
      (<strong>{listener.numTotalCompletedBatches}</strong>
      completed batches, <strong>{listener.numTotalReceivedRecords}</strong> records)
    </div>
    <br />
  }

  /**
   * Generate a global "timeFormat" dictionary in the JavaScript to store the time and its formatted
   * string. Because we cannot specify a timezone in JavaScript, to make sure the server and client
   * use the same timezone, we use the "timeFormat" dictionary to format all time values used in the
   * graphs.
   *
   * @param times all time values that will be used in the graphs.
   */
  private def generateTimeMap(times: Seq[Long]): Seq[Node] = {
    val js = "var timeFormat = {};\n" + times.map { time =>
      val formattedTime =
        SparkUIUtils.formatBatchTime(time, listener.batchDuration, showYYYYMMSS = false)
      s"timeFormat[$time] = '$formattedTime';"
    }.mkString("\n")

    <script>{Unparsed(js)}</script>
  }

  private def generateTimeTipStrings(times: Seq[Long]): Seq[Node] = {
    // We leverage timeFormat as the value would be same as timeFormat. This means it is
    // sensitive to the order - generateTimeMap should be called earlier than this.
    val js = "var timeTipStrings = {};\n" + times.map { time =>
      s"timeTipStrings[$time] = timeFormat[$time];"
    }.mkString("\n")

    <script>{Unparsed(js)}</script>
  }

  private def generateStatTable(): Seq[Node] = {
    val batches = listener.retainedBatches

    val batchTimes = batches.map(_.batchTime.milliseconds)
    val minBatchTime = if (batchTimes.isEmpty) startTime else batchTimes.min
    val maxBatchTime = if (batchTimes.isEmpty) startTime else batchTimes.max

    val recordRateForAllStreams = new RecordRateUIData(batches.map { batchInfo =>
      (batchInfo.batchTime.milliseconds, batchInfo.numRecords * 1000.0 / listener.batchDuration)
    })

    val schedulingDelay = new MillisecondsStatUIData(batches.flatMap { batchInfo =>
      batchInfo.schedulingDelay.map(batchInfo.batchTime.milliseconds -> _)
    })
    val processingTime = new MillisecondsStatUIData(batches.flatMap { batchInfo =>
      batchInfo.processingDelay.map(batchInfo.batchTime.milliseconds -> _)
    })
    val totalDelay = new MillisecondsStatUIData(batches.flatMap { batchInfo =>
      batchInfo.totalDelay.map(batchInfo.batchTime.milliseconds -> _)
    })

    // Use the max value of "schedulingDelay", "processingTime", and "totalDelay" to make the
    // Y axis ranges same.
    val _maxTime =
      (for (m1 <- schedulingDelay.max; m2 <- processingTime.max; m3 <- totalDelay.max) yield
        m1 max m2 max m3).getOrElse(0L)
    // Should start at 0
    val minTime = 0L
    val (maxTime, normalizedUnit) = UIUtils.normalizeDuration(_maxTime)
    val formattedUnit = UIUtils.shortTimeUnitString(normalizedUnit)

    // Use the max input rate for all InputDStreams' graphs to make the Y axis ranges same.
    // If it's not an integral number, just use its ceil integral number.
    val maxRecordRate = recordRateForAllStreams.max.map(_.ceil.toLong).getOrElse(0L)
    val minRecordRate = 0L

    val batchInterval = UIUtils.convertToTimeUnit(listener.batchDuration, normalizedUnit)

    val jsCollector = new JsCollector

    val graphUIDataForRecordRateOfAllStreams =
      new GraphUIData(
        "all-stream-records-timeline",
        "all-stream-records-histogram",
        recordRateForAllStreams.data,
        minBatchTime,
        maxBatchTime,
        minRecordRate,
        maxRecordRate,
        "records/sec")
    graphUIDataForRecordRateOfAllStreams.generateDataJs(jsCollector)

    val graphUIDataForSchedulingDelay =
      new GraphUIData(
        "scheduling-delay-timeline",
        "scheduling-delay-histogram",
        schedulingDelay.timelineData(normalizedUnit),
        minBatchTime,
        maxBatchTime,
        minTime,
        maxTime,
        formattedUnit)
    graphUIDataForSchedulingDelay.generateDataJs(jsCollector)

    val graphUIDataForProcessingTime =
      new GraphUIData(
        "processing-time-timeline",
        "processing-time-histogram",
        processingTime.timelineData(normalizedUnit),
        minBatchTime,
        maxBatchTime,
        minTime,
        maxTime,
        formattedUnit, Some(batchInterval))
    graphUIDataForProcessingTime.generateDataJs(jsCollector)

    val graphUIDataForTotalDelay =
      new GraphUIData(
        "total-delay-timeline",
        "total-delay-histogram",
        totalDelay.timelineData(normalizedUnit),
        minBatchTime,
        maxBatchTime,
        minTime,
        maxTime,
        formattedUnit)
    graphUIDataForTotalDelay.generateDataJs(jsCollector)

    // It's false before the user registers the first InputDStream
    val hasStream = listener.streamIds.nonEmpty

    val numCompletedBatches = listener.retainedCompletedBatches.size
    val numActiveBatches = batchTimes.length - numCompletedBatches
    val numReceivers = listener.numInactiveReceivers + listener.numActiveReceivers
    val table =
      // scalastyle:off
      <table id="stat-table" class="table table-bordered" style="width: auto">
      <thead>
        <tr>
          <th style="width: 160px;"></th>
          <th style="width: 492px;">Timelines (Last {batchTimes.length} batches, {numActiveBatches} active, {numCompletedBatches} completed)</th>
          <th style="width: 350px;">Histograms</th></tr>
      </thead>
      <tbody>
        <tr>
          <td style="vertical-align: middle;">
            <div style="width: 160px;">
              <div>
              {
                if (hasStream) {
                  <span class="expand-input-rate">
                    <span class="expand-input-rate-arrow arrow-closed"></span>
                    <a data-toggle="tooltip" title="Show/hide details of each receiver" data-placement="top">
                      <strong>Input Rate</strong>
                    </a>
                  </span>
                } else {
                  <strong>Input Rate</strong>
                }
              }
              </div>
              {
                if (numReceivers > 0) {
                  <div>Receivers: {listener.numActiveReceivers} / {numReceivers} active</div>
                }
              }
              <div>Avg: {recordRateForAllStreams.formattedAvg} records/sec</div>
            </div>
          </td>
          <td class="timeline">{graphUIDataForRecordRateOfAllStreams.generateTimelineHtml(jsCollector)}</td>
          <td class="histogram">{graphUIDataForRecordRateOfAllStreams.generateHistogramHtml(jsCollector)}</td>
        </tr>
      {if (hasStream) {
        <tr id="inputs-table" style="display: none;" >
          <td colspan="3">
            {generateInputDStreamsTable(jsCollector, minBatchTime, maxBatchTime, minRecordRate, maxRecordRate)}
          </td>
        </tr>
      }}
        <tr>
          <td style="vertical-align: middle;">
            <div style="width: 160px;">
              <div><strong>Scheduling Delay {SparkUIUtils.tooltip("Time taken by Streaming scheduler to submit jobs of a batch", "top")}</strong></div>
              <div>Avg: {schedulingDelay.formattedAvg}</div>
            </div>
          </td>
          <td class="timeline">{graphUIDataForSchedulingDelay.generateTimelineHtml(jsCollector)}</td>
          <td class="histogram">{graphUIDataForSchedulingDelay.generateHistogramHtml(jsCollector)}</td>
        </tr>
        <tr>
          <td style="vertical-align: middle;">
            <div style="width: 160px;">
              <div><strong>Processing Time {SparkUIUtils.tooltip("Time taken to process all jobs of a batch", "top")}</strong></div>
              <div>Avg: {processingTime.formattedAvg}</div>
            </div>
          </td>
          <td class="timeline">{graphUIDataForProcessingTime.generateTimelineHtml(jsCollector)}</td>
          <td class="histogram">{graphUIDataForProcessingTime.generateHistogramHtml(jsCollector)}</td>
        </tr>
        <tr>
          <td style="vertical-align: middle;">
            <div style="width: 160px;">
              <div><strong>Total Delay {SparkUIUtils.tooltip("Total time taken to handle a batch", "top")}</strong></div>
              <div>Avg: {totalDelay.formattedAvg}</div>
            </div>
          </td>
          <td class="timeline">{graphUIDataForTotalDelay.generateTimelineHtml(jsCollector)}</td>
          <td class="histogram">{graphUIDataForTotalDelay.generateHistogramHtml(jsCollector)}</td>
        </tr>
      </tbody>
    </table>
    // scalastyle:on

    generateTimeMap(batchTimes) ++ generateTimeTipStrings(batchTimes) ++ table ++
      jsCollector.toHtml
  }

  private def generateInputDStreamsTable(
      jsCollector: JsCollector,
      minX: Long,
      maxX: Long,
      minY: Double,
      maxY: Double): Seq[Node] = {
    val maxYCalculated = listener.receivedRecordRateWithBatchTime.values
      .flatMap { case streamAndRates => streamAndRates.map { case (_, recordRate) => recordRate } }
      .reduceOption[Double](math.max)
      .map(_.ceil.toLong)
      .getOrElse(0L)

    val content: Seq[Node] = listener.receivedRecordRateWithBatchTime.toList.sortBy(_._1).flatMap {
      case (streamId, recordRates) =>
        generateInputDStreamRow(
          jsCollector, streamId, recordRates, minX, maxX, minY, maxYCalculated)
    }

    // scalastyle:off
    <table class="table table-bordered" style="width: auto">
      <thead>
        <tr>
          <th style="width: 151px;"></th>
          <th style="width: 167px; padding: 8px 0 8px 0"><div style="margin: 0 8px 0 8px">Status</div></th>
          <th style="width: 167px; padding: 8px 0 8px 0"><div style="margin: 0 8px 0 8px">Executor ID / Host</div></th>
          <th style="width: 166px; padding: 8px 0 8px 0"><div style="margin: 0 8px 0 8px">Last Error Time</div></th>
          <th>Last Error Message</th>
        </tr>
      </thead>
      <tbody>
        {content}
      </tbody>
    </table>
    // scalastyle:on
  }

  private def generateInputDStreamRow(
      jsCollector: JsCollector,
      streamId: Int,
      recordRates: Seq[(Long, Double)],
      minX: Long,
      maxX: Long,
      minY: Double,
      maxY: Double): Seq[Node] = {
    // If this is a ReceiverInputDStream, we need to show the receiver info. Or we only need the
    // InputDStream name.
    val receiverInfo = listener.receiverInfo(streamId)
    val receiverName = receiverInfo.map(_.name).
      orElse(listener.streamName(streamId)).getOrElse(s"Stream-$streamId")
    val receiverActive = receiverInfo.map { info =>
      if (info.active) "ACTIVE" else "INACTIVE"
    }.getOrElse(emptyCell)
    val receiverLocation = receiverInfo.map { info =>
      val executorId = if (info.executorId.isEmpty) emptyCell else info.executorId
      val location = if (info.location.isEmpty) emptyCell else info.location
      s"$executorId / $location"
    }.getOrElse(emptyCell)
    val receiverLastError = receiverInfo.map { info =>
      val msg = s"${info.lastErrorMessage} - ${info.lastError}"
      if (msg.length > 100) msg.take(97) + "..." else msg
    }.getOrElse(emptyCell)
    val receiverLastErrorTime = receiverInfo.map {
      r => if (r.lastErrorTime < 0) "-" else SparkUIUtils.formatDate(r.lastErrorTime)
    }.getOrElse(emptyCell)
    val receivedRecords = new RecordRateUIData(recordRates)

    val graphUIDataForRecordRate =
      new GraphUIData(
        s"stream-$streamId-records-timeline",
        s"stream-$streamId-records-histogram",
        receivedRecords.data,
        minX,
        maxX,
        minY,
        maxY,
        "records/sec")
    graphUIDataForRecordRate.generateDataJs(jsCollector)

    <tr>
      <td rowspan="2" style="vertical-align: middle; width: 151px;">
        <div style="width: 151px;">
          <div style="word-wrap: break-word;"><strong>{receiverName}</strong></div>
          <div>Avg: {receivedRecords.formattedAvg} records/sec</div>
        </div>
      </td>
      <td>{receiverActive}</td>
      <td>{receiverLocation}</td>
      <td>{receiverLastErrorTime}</td>
      <td><div style="width: 342px;">{receiverLastError}</div></td>
    </tr>
    <tr>
      <td colspan="3" class="timeline">
        {graphUIDataForRecordRate.generateTimelineHtml(jsCollector)}
      </td>
      <td class="histogram">{graphUIDataForRecordRate.generateHistogramHtml(jsCollector)}</td>
    </tr>
  }

  private def streamingTable(request: HttpServletRequest, batches: Seq[BatchUIData],
      tableTag: String): Seq[Node] = {
    val interval: Long = listener.batchDuration
    val streamingPage = Option(request.getParameter(s"$tableTag.page")).map(_.toInt).getOrElse(1)

    try {
      new StreamingPagedTable(
        request,
        tableTag,
        batches,
        SparkUIUtils.prependBaseUri(request, parent.basePath),
        "streaming",
        interval
      ).table(streamingPage)
    } catch {
      case e @ (_: IllegalArgumentException | _: IndexOutOfBoundsException) =>
        <div class="alert alert-error">
          <p>Error while rendering streaming table:</p>
          <pre>
            {Utils.exceptionString(e)}
          </pre>
        </div>
    }
  }

  private def generateBatchListTables(request: HttpServletRequest): Seq[Node] = {
    val runningBatches = listener.runningBatches.sortBy(_.batchTime.milliseconds).reverse
    val waitingBatches = listener.waitingBatches.sortBy(_.batchTime.milliseconds).reverse
    val completedBatches = listener.retainedCompletedBatches.
      sortBy(_.batchTime.milliseconds).reverse

    val content = mutable.ListBuffer[Node]()

    if (runningBatches.nonEmpty) {
      content ++=
        <div class="row">
          <div class="col-12">
            <span id="runningBatches" class="collapse-aggregated-runningBatches collapse-table"
                  onClick="collapseTable('collapse-aggregated-runningBatches',
                  'aggregated-runningBatches')">
              <h4>
                <span class="collapse-table-arrow arrow-open"></span>
                <a>Running Batches ({runningBatches.size})</a>
              </h4>
            </span>
            <div class="aggregated-runningBatches collapsible-table">
              { streamingTable(request, runningBatches, "runningBatches") }
            </div>
          </div>
        </div>
    }

    if (waitingBatches.nonEmpty) {
      content ++=
        <div class="row">
          <div class="col-12">
            <span id="waitingBatches" class="collapse-aggregated-waitingBatches collapse-table"
                  onClick="collapseTable('collapse-aggregated-waitingBatches',
                  'aggregated-waitingBatches')">
              <h4>
                <span class="collapse-table-arrow arrow-open"></span>
                <a>Waiting Batches ({waitingBatches.size})</a>
              </h4>
            </span>
            <div class="aggregated-waitingBatches collapsible-table">
              { streamingTable(request, waitingBatches, "waitingBatches") }
            </div>
          </div>
        </div>
    }

    if (completedBatches.nonEmpty) {
      content ++=
        <div class="row">
          <div class="col-12">
            <span id="completedBatches" class="collapse-aggregated-completedBatches collapse-table"
                  onClick="collapseTable('collapse-aggregated-completedBatches',
                  'aggregated-completedBatches')">
              <h4>
                <span class="collapse-table-arrow arrow-open"></span>
                <a>Completed Batches (last {completedBatches.size}
                  out of {listener.numTotalCompletedBatches})</a>
              </h4>
            </span>
            <div class="aggregated-completedBatches collapsible-table">
              { streamingTable(request, completedBatches, "completedBatches") }
            </div>
          </div>
        </div>
    }
    content
  }
}

private[ui] object StreamingPage {
  val BLACK_RIGHT_TRIANGLE_HTML = "&#9654;"
  val BLACK_DOWN_TRIANGLE_HTML = "&#9660;"

  val emptyCell = "-"

  /**
   * Returns a human-readable string representing a duration such as "5 second 35 ms"
   */
  def formatDurationOption(msOption: Option[Long]): String = {
    msOption.map(SparkUIUtils.formatDurationVerbose).getOrElse(emptyCell)
  }

}

