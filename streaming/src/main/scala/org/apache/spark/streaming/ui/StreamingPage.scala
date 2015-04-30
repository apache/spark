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

import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.TimeUnit
import javax.servlet.http.HttpServletRequest

import scala.collection.mutable.ArrayBuffer
import scala.xml.{Node, Unparsed}

import org.apache.spark.Logging
import org.apache.spark.ui._
import org.apache.spark.ui.UIUtils._

/**
 * @param divId the `id` used in the html `div` tag
 * @param data the data for the timeline graph
 * @param minX the min value of X axis
 * @param maxX the max value of X axis
 * @param minY the min value of Y axis
 * @param maxY the max value of Y axis
 * @param unitY the unit of Y axis
 * @param batchInterval if `batchInterval` is not None, we will draw a line for `batchInterval` in
 *                      the graph
 */
private[ui] class TimelineUIData(divId: String, data: Seq[(Long, _)], minX: Long, maxX: Long,
    minY: Double, maxY: Double, unitY: String, batchInterval: Option[Double] = None) {

  def toHtml(jsCollector: JsCollector): Seq[Node] = {
    val jsForData = data.map { case (x, y) =>
      s"""{"x": $x, "y": $y}"""
    }.mkString("[", ",", "]")
    jsCollector.addPreparedStatement(s"registerTimeline($minY, $maxY);")
    if (batchInterval.isDefined) {
      jsCollector.addStatement(
        "drawTimeline(" +
          s"'#$divId', $jsForData, $minX, $maxX, $minY, $maxY, '$unitY', ${batchInterval.get}" +
          ");")
    } else {
      jsCollector.addStatement(
        s"drawTimeline('#$divId', $jsForData, $minX, $maxX, $minY, $maxY, '$unitY');")
    }
    <div id={divId}></div>
  }
}

/**
 * @param divId the `id` used in the html `div` tag
 * @param data the data for the histogram graph
 * @param minY the min value of Y axis
 * @param maxY the max value of Y axis
 * @param unitY the unit of Y axis
 * @param batchInterval if `batchInterval` is not None, we will draw a line for `batchInterval` in
 *                      the graph
 */
private[ui] class HistogramUIData(
    divId: String, data: Seq[_], minY: Double, maxY: Double, unitY: String,
    batchInterval: Option[Double] = None) {

  def toHtml(jsCollector: JsCollector): Seq[Node] = {
    val jsForData = data.mkString("[", ",", "]")
    jsCollector.addPreparedStatement(s"registerHistogram($jsForData, $minY, $maxY);")
    if (batchInterval.isDefined) {
      jsCollector.addStatement(
        "drawHistogram(" +
          s"'#$divId', $jsForData, $minY, $maxY, '$unitY', ${batchInterval.get}" +
          ");")
    } else {
      jsCollector.addStatement(s"drawHistogram('#$divId', $jsForData, $minY, $maxY, '$unitY');")
    }
    <div id={divId}></div>
  }
}

/**
 * @param data (batchTime, milliseconds). "milliseconds" is something like "processing time".
 */
private[ui] class MillisecondsStatUIData(data: Seq[(Long, Long)]) {

  /**
   * Converting the original data as per `unit`.
   */
  def timelineData(unit: TimeUnit): Seq[(Long, Double)] =
    data.map(x => x._1 -> StreamingPage.convertToTimeUnit(x._2, unit))

  /**
   * Converting the original data as per `unit`.
   */
  def histogramData(unit: TimeUnit): Seq[Double] =
    data.map(x => StreamingPage.convertToTimeUnit(x._2, unit))

  val avg: Option[Long] = if (data.isEmpty) None else Some(data.map(_._2).sum / data.size)

  val formattedAvg: String = StreamingPage.formatDurationOption(avg)

  val max: Option[Long] = if (data.isEmpty) None else Some(data.map(_._2).max)
}

/**
 * @param data (batchTime, event-rate).
 */
private[ui] class EventRateUIData(val data: Seq[(Long, Double)]) {

  val avg: Option[Double] = if (data.isEmpty) None else Some(data.map(_._2).sum / data.size)

  val formattedAvg: String = avg.map(_.formatted("%.2f")).getOrElse("-")

  val max: Option[Double] = if (data.isEmpty) None else Some(data.map(_._2).max)
}

/** Page for Spark Web UI that shows statistics of a streaming job */
private[ui] class StreamingPage(parent: StreamingTab)
  extends WebUIPage("") with Logging {

  import StreamingPage._

  private val listener = parent.listener
  private val startTime = System.currentTimeMillis()

  /** Render the page */
  def render(request: HttpServletRequest): Seq[Node] = {
    val resources = generateLoadResources()
    val basicInfo = generateBasicInfo()
    val content = resources ++
      basicInfo ++
      listener.synchronized {
        generateStatTable() ++
          generateBatchListTables()
      }
    UIUtils.headerSparkPage("Streaming Statistics", content, parent, Some(5000))
  }

  /**
   * Generate html that will load css/js files for StreamingPage
   */
  private def generateLoadResources(): Seq[Node] = {
    // scalastyle:off
    <script src={UIUtils.prependBaseUri("/static/d3.min.js")}></script>
      <link rel="stylesheet" href={UIUtils.prependBaseUri("/static/streaming-page.css")} type="text/css"/>
      <script src={UIUtils.prependBaseUri("/static/streaming-page.js")}></script>
    // scalastyle:on
  }

  /** Generate basic information of the streaming program */
  private def generateBasicInfo(): Seq[Node] = {
    val timeSinceStart = System.currentTimeMillis() - startTime
    <div>Running batches of
      <strong>
        {formatDurationVerbose(listener.batchDuration)}
      </strong>
      for
      <strong>
        {formatDurationVerbose(timeSinceStart)}
      </strong>
      since
      <strong>
        {UIUtils.formatDate(startTime)}
      </strong>
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
    val dateFormat = new SimpleDateFormat("HH:mm:ss")
    val js = "var timeFormat = {};\n" + times.map { time =>
      val formattedTime = dateFormat.format(new Date(time))
      s"timeFormat[$time] = '$formattedTime';"
    }.mkString("\n")

    <script>{Unparsed(js)}</script>
  }

  private def generateStatTable(): Seq[Node] = {
    val batches = listener.retainedBatches

    val batchTimes = batches.map(_.batchTime.milliseconds)
    val minBatchTime = if (batchTimes.isEmpty) startTime else batchTimes.min
    val maxBatchTime = if (batchTimes.isEmpty) startTime else batchTimes.max

    val eventRateForAllReceivers = new EventRateUIData(batches.map { batchInfo =>
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

    // Use the max input rate for all receivers' graphs to make the Y axis ranges same.
    // If it's not an integral number, just use its ceil integral number.
    val maxEventRate = eventRateForAllReceivers.max.map(_.ceil.toLong).getOrElse(0L)
    val minEventRate = 0L

    // JavaScript to show/hide the receiver sub table.
    val triangleJs =
      s"""$$('#inputs-table').toggle('collapsed');
         |var status = false;
         |if ($$(this).html() == '$BLACK_RIGHT_TRIANGLE_HTML') {
         |$$(this).html('$BLACK_DOWN_TRIANGLE_HTML');status = true;}
         |else {$$(this).html('$BLACK_RIGHT_TRIANGLE_HTML');status  = false;}
         |window.history.pushState('',
         |    document.title, window.location.pathname + '?show-receivers-detail=' + status);"""
        .stripMargin.replaceAll("\\n", "") // it must be only one single line

    val batchInterval = StreamingPage.convertToTimeUnit(listener.batchDuration, normalizedUnit)

    val jsCollector = new JsCollector

    val timelineDataForEventRateOfAllReceivers =
      new TimelineUIData(
        "all-receiver-events-timeline",
        eventRateForAllReceivers.data,
        minBatchTime,
        maxBatchTime,
        minEventRate,
        maxEventRate,
        "events/sec").toHtml(jsCollector)

    val histogramDataForEventRateOfAllReceivers =
      new HistogramUIData(
        "all-receiver-events-histogram",
        eventRateForAllReceivers.data.map(_._2),
        minEventRate,
        maxEventRate,
        "events/sec").toHtml(jsCollector)

    val timelineDataForSchedulingDelay =
      new TimelineUIData(
        "scheduling-delay-timeline",
        schedulingDelay.timelineData(normalizedUnit),
        minBatchTime,
        maxBatchTime,
        minTime,
        maxTime,
        formattedUnit).toHtml(jsCollector)

    val histogramDataForSchedulingDelay =
      new HistogramUIData(
        "scheduling-delay-histogram",
        schedulingDelay.histogramData(normalizedUnit),
        minTime,
        maxTime,
        formattedUnit).toHtml(jsCollector)

    val timelineDataForProcessingTime =
      new TimelineUIData(
        "processing-time-timeline",
        processingTime.timelineData(normalizedUnit),
        minBatchTime,
        maxBatchTime,
        minTime,
        maxTime,
        formattedUnit, Some(batchInterval)).toHtml(jsCollector)

    val histogramDataForProcessingTime =
      new HistogramUIData(
        "processing-time-histogram",
        processingTime.histogramData(normalizedUnit),
        minTime,
        maxTime,
        formattedUnit, Some(batchInterval)).toHtml(jsCollector)

    val timelineDataForTotalDelay =
      new TimelineUIData(
        "total-delay-timeline",
        totalDelay.timelineData(normalizedUnit),
        minBatchTime,
        maxBatchTime,
        minTime,
        maxTime,
        formattedUnit).toHtml(jsCollector)

    val histogramDataForTotalDelay =
      new HistogramUIData(
        "total-delay-histogram",
        totalDelay.histogramData(normalizedUnit),
        minTime,
        maxTime,
        formattedUnit).toHtml(jsCollector)

    val numCompletedBatches = listener.retainedCompletedBatches.size
    val numActiveBatches = batchTimes.length - numCompletedBatches
    val table =
      // scalastyle:off
      <table id="stat-table" class="table table-bordered" style="width: auto">
      <thead>
        <tr>
          <th style="width: 160px;"></th>
          <th style="width: 492px;">Timelines (Last {batchTimes.length} batches, {numActiveBatches} active, {numCompletedBatches} completed)</th>
          <th style="width: 300px;">Histograms</th></tr>
      </thead>
      <tbody>
        <tr>
          <td style="vertical-align: middle;">
            <div style="width: 160px;">
              <div>
                <span id="triangle" onclick={Unparsed(triangleJs)}>{Unparsed(BLACK_RIGHT_TRIANGLE_HTML)}</span>
                <strong>Input Rate</strong>
              </div>
              <div>Avg: {eventRateForAllReceivers.formattedAvg} events/sec</div>
            </div>
          </td>
          <td class="timeline">{timelineDataForEventRateOfAllReceivers}</td>
          <td class="histogram">{histogramDataForEventRateOfAllReceivers}</td>
        </tr>
        <tr id="inputs-table" style="display: none;" >
          <td colspan="3">
          {generateInputReceiversTable(jsCollector, minBatchTime, maxBatchTime, minEventRate, maxEventRate)}
          </td>
        </tr>
        <tr>
          <td style="vertical-align: middle;">
            <div style="width: 160px;">
              <div><strong>Streaming Scheduling Delay</strong></div>
              <div>Avg: {schedulingDelay.formattedAvg}</div>
            </div>
          </td>
          <td class="timeline">{timelineDataForSchedulingDelay}</td>
          <td class="histogram">{histogramDataForSchedulingDelay}</td>
        </tr>
        <tr>
          <td style="vertical-align: middle;">
            <div style="width: 160px;">
              <div><strong>Processing Time</strong></div>
              <div>Avg: {processingTime.formattedAvg}</div>
            </div>
          </td>
          <td class="timeline">{timelineDataForProcessingTime}</td>
          <td class="histogram">{histogramDataForProcessingTime}</td>
        </tr>
        <tr>
          <td style="vertical-align: middle;">
            <div style="width: 160px;">
              <div><strong>Total Delay</strong></div>
              <div>Avg: {totalDelay.formattedAvg}</div>
            </div>
          </td>
          <td class="timeline">{timelineDataForTotalDelay}</td>
          <td class="histogram">{histogramDataForTotalDelay}</td>
        </tr>
      </tbody>
    </table>
    // scalastyle:on

    generateTimeMap(batchTimes) ++ table ++ jsCollector.toHtml
  }

  private def generateInputReceiversTable(
      jsCollector: JsCollector,
      minX: Long,
      maxX: Long,
      minY: Double,
      maxY: Double): Seq[Node] = {
    val content = listener.allReceivers.map { receiverId =>
      generateInputReceiverRow(jsCollector, receiverId, minX, maxX, minY, maxY)
    }.foldLeft[Seq[Node]](Nil)(_ ++ _)

    // scalastyle:off
    <table class="table table-bordered" style="width: auto">
      <thead>
        <tr>
          <th style="width: 151px;"></th>
          <th style="width: 167px; padding: 8px 0 8px 0"><div style="margin: 0 8px 0 8px">Status</div></th>
          <th style="width: 167px; padding: 8px 0 8px 0"><div style="margin: 0 8px 0 8px">Location</div></th>
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

  private def generateInputReceiverRow(
      jsCollector: JsCollector,
      receiverId: Int,
      minX: Long,
      maxX: Long,
      minY: Double,
      maxY: Double): Seq[Node] = {
    val receiverInfo = listener.receiverInfo(receiverId)
    val receiverName = receiverInfo.map(_.name).getOrElse(s"Receiver-$receiverId")
    val receiverActive = receiverInfo.map { info =>
      if (info.active) "ACTIVE" else "INACTIVE"
    }.getOrElse(emptyCell)
    val receiverLocation = receiverInfo.map(_.location).getOrElse(emptyCell)
    val receiverLastError = listener.receiverInfo(receiverId).map { info =>
      val msg = s"${info.lastErrorMessage} - ${info.lastError}"
      if (msg.size > 100) msg.take(97) + "..." else msg
    }.getOrElse(emptyCell)
    val receiverLastErrorTime = receiverInfo.map {
      r => if (r.lastErrorTime < 0) "-" else UIUtils.formatDate(r.lastErrorTime)
    }.getOrElse(emptyCell)
    val receivedRecords =
      new EventRateUIData(listener.receivedEventRateWithBatchTime.get(receiverId).getOrElse(Seq()))

    val timelineForEventRate =
      new TimelineUIData(
        s"receiver-$receiverId-events-timeline",
        receivedRecords.data,
        minX,
        maxX,
        minY,
        maxY,
        "events/sec").toHtml(jsCollector)

    val histogramForEventsRate =
      new HistogramUIData(
        s"receiver-$receiverId-events-histogram",
        receivedRecords.data.map(_._2),
        minY,
        maxY,
        "events/sec").toHtml(jsCollector)

    <tr>
      <td rowspan="2" style="vertical-align: middle; width: 151px;">
        <div style="width: 151px;">
          <div><strong>{receiverName}</strong></div>
          <div>Avg: {receivedRecords.formattedAvg} events/sec</div>
        </div>
      </td>
      <td>{receiverActive}</td>
      <td>{receiverLocation}</td>
      <td>{receiverLastErrorTime}</td>
      <td><div style="width: 292px;">{receiverLastError}</div></td>
    </tr>
    <tr>
      <td colspan="3" class="timeline">
        {timelineForEventRate}
      </td>
      <td class="histogram">{histogramForEventsRate}</td>
    </tr>
  }

  private def generateBatchListTables(): Seq[Node] = {
    val runningBatches = listener.runningBatches.sortBy(_.batchTime.milliseconds).reverse
    val waitingBatches = listener.waitingBatches.sortBy(_.batchTime.milliseconds).reverse
    val completedBatches = listener.retainedCompletedBatches.
      sortBy(_.batchTime.milliseconds).reverse

    val activeBatchesContent = {
      <h4 id="active">Active Batches ({runningBatches.size + waitingBatches.size})</h4> ++
        new ActiveBatchTable(runningBatches, waitingBatches).toNodeSeq
    }

    val completedBatchesContent = {
      <h4 id="completed">
        Completed Batches (last {completedBatches.size} out of {listener.numTotalCompletedBatches})
      </h4> ++
        new CompletedBatchTable(completedBatches).toNodeSeq
    }

    activeBatchesContent ++ completedBatchesContent
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
    msOption.map(formatDurationVerbose).getOrElse(emptyCell)
  }

  /**
   * Convert `milliseconds` to the specified `unit`. We cannot use `TimeUnit.convert` because it
   * will discard the fractional part.
   */
  def convertToTimeUnit(milliseconds: Long, unit: TimeUnit): Double =  unit match {
    case TimeUnit.NANOSECONDS => milliseconds * 1000 * 1000
    case TimeUnit.MICROSECONDS => milliseconds * 1000
    case TimeUnit.MILLISECONDS => milliseconds
    case TimeUnit.SECONDS => milliseconds / 1000.0
    case TimeUnit.MINUTES => milliseconds / 1000.0 / 60.0
    case TimeUnit.HOURS => milliseconds / 1000.0 / 60.0 / 60.0
    case TimeUnit.DAYS => milliseconds / 1000.0 / 60.0 / 60.0 / 24.0
  }
}

/**
 * A helper class that allows the user to add JavaScript statements which will be executed when the
 * DOM has finished loading.
 */
private[ui] class JsCollector {
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
         |$$(document).ready(function(){
         |    ${preparedStatements.mkString("\n")}
         |    ${statements.mkString("\n")}
         |});""".stripMargin

   <script>{Unparsed(js)}</script>
  }
}

