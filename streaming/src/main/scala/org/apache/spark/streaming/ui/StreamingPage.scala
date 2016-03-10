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

import scala.collection.mutable.ArrayBuffer
import scala.xml.{Node, Unparsed}

import org.apache.spark.Logging
import org.apache.spark.ui.{UIUtils => SparkUIUtils, _}

/**
 * A helper class to generate JavaScript and HTML for both timeline and histogram graphs.
 *
 * @param timelineDivId the timeline `id` used in the html `div` tag
 * @param histogramDivId the timeline `id` used in the html `div` tag
 * @param dataSets the data sets for the graph; each data set typically represents a line
 * @param minX the min value of X axis
 * @param maxX the max value of X axis
 * @param minY the min value of Y axis
 * @param maxY the max value of Y axis
 * @param unitY the unit of Y axis
 * @param batchInterval if `batchInterval` is not None, we will draw a line for `batchInterval` in
 *                      the graph
 */
private[ui] class GraphUIData(
    timelineDivId: String,
    histogramDivId: String,
    dataSets: Seq[Seq[(Long, Double)]],
    minX: Long,
    maxX: Long,
    minY: Double,
    maxY: Double,
    unitY: String,
    batchInterval: Option[Double] = None) {

  private var dataJavaScriptName: String = _

  def generateDataJs(jsCollector: JsCollector): Unit = {
    // jsForData then is a 2-dimensional array
    val jsForData = dataSets.map { dataSet =>
      dataSet.map { case (x, y) =>
        s"""{"x": $x, "y": $y}"""
      }.mkString("[", ",", "]")
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
    // we only generate histogram for the 0th data-set, i.e., the event rate data-set
    val histogramData = s"$dataJavaScriptName[0].map(function(d) { return d.y; })"
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
}

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
    SparkUIUtils.headerSparkPage("Streaming Statistics", content, parent, Some(5000))
  }

  /**
   * Generate html that will load css/js files for StreamingPage
   */
  private def generateLoadResources(): Seq[Node] = {
    // scalastyle:off
    <script src={SparkUIUtils.prependBaseUri("/static/d3.min.js")}></script>
      <link rel="stylesheet" href={SparkUIUtils.prependBaseUri("/static/streaming/streaming-page.css")} type="text/css"/>
      <script src={SparkUIUtils.prependBaseUri("/static/streaming/streaming-page.js")}></script>
    // scalastyle:on
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
        UIUtils.formatBatchTime(time, listener.batchDuration, showYYYYMMSS = false)
      s"timeFormat[$time] = '$formattedTime';"
    }.mkString("\n")

    <script>{Unparsed(js)}</script>
  }

  private def generateStatTable(): Seq[Node] = {
    val batches = listener.retainedBatches

    val batchTimes = batches.map(_.batchTime.milliseconds)
    val minBatchTime = if (batchTimes.isEmpty) startTime else batchTimes.min
    val maxBatchTime = if (batchTimes.isEmpty) startTime else batchTimes.max

    val eventRateForAllStreams = new EventRateUIData(batches.map { batchInfo =>
      (batchInfo.batchTime.milliseconds, batchInfo.numRecords * 1000.0 / listener.batchDuration)
    })

    // Use the max input rate for all InputDStreams' graphs to make the Y axis ranges same.
    // If it's not an integral number, just use its ceil integral number.
    val maxEventRate: Long = eventRateForAllStreams.max.map(_.ceil.toLong).getOrElse(0L)
    val minEventRate: Long = 0L

    val numRecordsLimitForAllStreamsOption = if (listener.allStreamsUnderRateControl) {
      val uiDate = new EventRateUIData(batches.map { batchInfo =>
        (batchInfo.batchTime.milliseconds, {
          val numRecordsLimitRate = batchInfo.numRecordsLimitOption
                                    .getOrElse(Long.MaxValue) * 1000.0 / listener.batchDuration
          StreamingPage.limitRateVisibleBoundTo(maxEventRate, numRecordsLimitRate)
        })
      })
      Some(uiDate)
    } else {
      None
    }

    // Cal maxY of limit rates from all batches
    val maxNumRecordsLimitRate: Long = if (numRecordsLimitForAllStreamsOption.isDefined) {
      numRecordsLimitForAllStreamsOption.get.max.map(_.ceil.toLong).getOrElse(0L)
    }
    else {
      0L
    }

    // Cal maxY from maxEventRate and maxNumRecordsLimitRate
    val maxEventRateOrNumRecordsLimitRate = maxEventRate.max(maxNumRecordsLimitRate)

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

    val batchInterval = UIUtils.convertToTimeUnit(listener.batchDuration, normalizedUnit)

    val jsCollector = new JsCollector

    val graphUIDataForEventRateOfAllStreams =
      new GraphUIData(
        "all-stream-events-timeline",
        "all-stream-events-histogram",
        if (listener.allStreamsUnderRateControl) {
          // All streams are under rate control, so we display the rate-limit line
          Seq(eventRateForAllStreams.data) ++
          StreamingPage.divideIntoLinesByMaxY(numRecordsLimitForAllStreamsOption.get.data,
                                              maxEventRateOrNumRecordsLimitRate)
        }
        else {
          // Not all streams are under rate control, so we won't display the rate-limit line
          Seq(eventRateForAllStreams.data)
        },
        minBatchTime,
        maxBatchTime,
        minEventRate,
        maxEventRateOrNumRecordsLimitRate,
        "events/sec")
    graphUIDataForEventRateOfAllStreams.generateDataJs(jsCollector)

    val graphUIDataForSchedulingDelay =
      new GraphUIData(
        "scheduling-delay-timeline",
        "scheduling-delay-histogram",
        Seq(schedulingDelay.timelineData(normalizedUnit)),
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
        Seq(processingTime.timelineData(normalizedUnit)),
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
        Seq(totalDelay.timelineData(normalizedUnit)),
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
                    <a data-toggle="tooltip" title="Show/hide details of each receiver" data-placement="right">
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
              <div>Avg: {eventRateForAllStreams.formattedAvg} events/sec</div>
            </div>
          </td>
          <td class="timeline">{graphUIDataForEventRateOfAllStreams.generateTimelineHtml(jsCollector)}</td>
          <td class="histogram">{graphUIDataForEventRateOfAllStreams.generateHistogramHtml(jsCollector)}</td>
        </tr>
      {if (hasStream) {
        <tr id="inputs-table" style="display: none;" >
          <td colspan="3">
            {generateInputDStreamsTable(jsCollector, minBatchTime, maxBatchTime, minEventRate, maxEventRate)}
          </td>
        </tr>
      }}
        <tr>
          <td style="vertical-align: middle;">
            <div style="width: 160px;">
              <div><strong>Scheduling Delay {SparkUIUtils.tooltip("Time taken by Streaming scheduler to submit jobs of a batch", "right")}</strong></div>
              <div>Avg: {schedulingDelay.formattedAvg}</div>
            </div>
          </td>
          <td class="timeline">{graphUIDataForSchedulingDelay.generateTimelineHtml(jsCollector)}</td>
          <td class="histogram">{graphUIDataForSchedulingDelay.generateHistogramHtml(jsCollector)}</td>
        </tr>
        <tr>
          <td style="vertical-align: middle;">
            <div style="width: 160px;">
              <div><strong>Processing Time {SparkUIUtils.tooltip("Time taken to process all jobs of a batch", "right")}</strong></div>
              <div>Avg: {processingTime.formattedAvg}</div>
            </div>
          </td>
          <td class="timeline">{graphUIDataForProcessingTime.generateTimelineHtml(jsCollector)}</td>
          <td class="histogram">{graphUIDataForProcessingTime.generateHistogramHtml(jsCollector)}</td>
        </tr>
        <tr>
          <td style="vertical-align: middle;">
            <div style="width: 160px;">
              <div><strong>Total Delay {SparkUIUtils.tooltip("Total time taken to handle a batch", "right")}</strong></div>
              <div>Avg: {totalDelay.formattedAvg}</div>
            </div>
          </td>
          <td class="timeline">{graphUIDataForTotalDelay.generateTimelineHtml(jsCollector)}</td>
          <td class="histogram">{graphUIDataForTotalDelay.generateHistogramHtml(jsCollector)}</td>
        </tr>
      </tbody>
    </table>
    // scalastyle:on

    generateTimeMap(batchTimes) ++ table ++ jsCollector.toHtml
  }

  private def generateInputDStreamsTable(
      jsCollector: JsCollector,
      minX: Long,
      maxX: Long,
      minY: Double,
      maxY: Double): Seq[Node] = {
    val receivedEventRateAndLimitRateWithBatchTime =
      listener.receivedEventRateAndLimitRateWithBatchTime

    val maxYOfEventRate = receivedEventRateAndLimitRateWithBatchTime.values
      .flatMap { case streamAndEventRatesAndLimitRateOptions =>
        streamAndEventRatesAndLimitRateOptions.map {
          case (_, eventRate, _) =>
            eventRate
        }
      }
      .reduceOption[Double](math.max)
      .map(_.ceil.toLong)
      .getOrElse(0L)

    val maxYOfLimitRate = receivedEventRateAndLimitRateWithBatchTime.values
      .flatMap { case streamAndEventRatesAndLimitRateOptions =>
        streamAndEventRatesAndLimitRateOptions.map {
          case (_, _, limitRate) =>
            limitRate.getOrElse(Double.MaxValue)
        }
      }
      .reduceOption[Double](math.max)
      .map(_.ceil.toLong)
      .getOrElse(0L)

    val maxYCalculated = StreamingPage.limitRateVisibleBoundTo(maxYOfEventRate, maxYOfLimitRate)

    val content = receivedEventRateAndLimitRateWithBatchTime.toList.sortBy(_._1).map {
      case (streamId, eventRatesAndLimitRates) => generateInputDStreamRow(jsCollector, streamId,
                                      eventRatesAndLimitRates, minX, maxX, minY, maxYCalculated)
    }.foldLeft[Seq[Node]](Nil)(_ ++ _)

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
      eventRatesAndLimitRates: Seq[(Long, Double, Option[Double])],
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
    val receivedRecords = new EventRateUIData(eventRatesAndLimitRates.map(e => (e._1, e._2)))
    val receivedRecordsLimitOption =
      if (listener.streamUnderRateControl(streamId).getOrElse(false)) {
        Some(new EventRateUIData(
          eventRatesAndLimitRates.map(e => (e._1, maxY.min(e._3.getOrElse(Double.MaxValue))))
        ))
      } else {
        None
      }

    val graphUIDataForEventRate =
      new GraphUIData(
        s"stream-$streamId-events-timeline",
        s"stream-$streamId-events-histogram",
        if (receivedRecordsLimitOption.isDefined) {
          // This stream is under rate control, so we display the rate-limit line
          Seq(receivedRecords.data) ++
          StreamingPage.divideIntoLinesByMaxY(receivedRecordsLimitOption.get.data, maxY)
        } else {
          // This stream is not under rate control, so we won't display the rate-limit line
          Seq(receivedRecords.data)
        },
        minX,
        maxX,
        minY,
        maxY,
        "events/sec")
    graphUIDataForEventRate.generateDataJs(jsCollector)

    <tr>
      <td rowspan="2" style="vertical-align: middle; width: 151px;">
        <div style="width: 151px;">
          <div style="word-wrap: break-word;"><strong>{receiverName}</strong></div>
          <div>Avg: {receivedRecords.formattedAvg} events/sec</div>
        </div>
      </td>
      <td>{receiverActive}</td>
      <td>{receiverLocation}</td>
      <td>{receiverLastErrorTime}</td>
      <td><div style="width: 342px;">{receiverLastError}</div></td>
    </tr>
    <tr>
      <td colspan="3" class="timeline">
        {graphUIDataForEventRate.generateTimelineHtml(jsCollector)}
      </td>
      <td class="histogram">{graphUIDataForEventRate.generateHistogramHtml(jsCollector)}</td>
    </tr>
  }

  private def generateBatchListTables(): Seq[Node] = {
    val runningBatches = listener.runningBatches.sortBy(_.batchTime.milliseconds).reverse
    val waitingBatches = listener.waitingBatches.sortBy(_.batchTime.milliseconds).reverse
    val completedBatches = listener.retainedCompletedBatches.
      sortBy(_.batchTime.milliseconds).reverse

    val activeBatchesContent = {
      <h4 id="active">Active Batches ({runningBatches.size + waitingBatches.size})</h4> ++
        new ActiveBatchTable(runningBatches, waitingBatches, listener.batchDuration,
                             listener.allStreamsUnderRateControl).toNodeSeq
    }

    val completedBatchesContent = {
      <h4 id="completed">
        Completed Batches (last {completedBatches.size} out of {listener.numTotalCompletedBatches})
      </h4> ++
        new CompletedBatchTable(completedBatches, listener.batchDuration,
                                listener.allStreamsUnderRateControl).toNodeSeq
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
    msOption.map(SparkUIUtils.formatDurationVerbose).getOrElse(emptyCell)
  }

  val VISIBLE_BOUND_MULTIPLIER = 2

  def limitRateVisibleBoundTo(eventRate: Double, limitRate: Double): Double = {
    if (limitRate > eventRate * VISIBLE_BOUND_MULTIPLIER) {
      eventRate * VISIBLE_BOUND_MULTIPLIER
    }
    else {
      limitRate
    }
  }

  /**
   * @return a Seq of dashed, solid, dashed, solid, dashed... lines
   */
  def divideIntoLinesByMaxY(rateLimits: Seq[(Long, Double)], maxY: Double)
    : Seq[Seq[(Long, Double)]] = {
    if (rateLimits.length <= 1) {
      Seq(rateLimits)
    }
    else {
      var ret: Seq[Seq[(Long, Double)]] = Seq()
      val array = rateLimits.toArray

      var consecutiveMaxY = array(0)._2 == maxY && array(1)._2 == maxY
      if (!consecutiveMaxY) {
        // This ensures that the first returned line is a dashed one
        ret = ret :+ Seq()
      }
      var startIdx = 0

      // Each iteration adds a dashed or solid line
      while (startIdx < array.length) {
        var stopIdx = startIdx
        if (consecutiveMaxY) {
          // Calculate the (inclusive) stopIdx for a dashed line
          while (stopIdx + 1 < array.length && array(stopIdx + 1)._2 == maxY) {
            stopIdx += 1
          }
        }
        else {
          // Calculate the (inclusive) stopIdx for a solid line
          while (stopIdx < array.length - 1 &&
                 (array(stopIdx)._2 != maxY || array(stopIdx + 1)._2 != maxY)) {
            stopIdx += 1
          }
        }
        ret = ret :+ array.slice(startIdx, stopIdx + 1).toSeq
        startIdx = if (stopIdx + 1 < array.length) stopIdx else array.length
        consecutiveMaxY = !consecutiveMaxY
      }

      ret
    }
  }
}

/**
 * A helper class that allows the user to add JavaScript statements which will be executed when the
 * DOM has finished loading.
 */
private[ui] class JsCollector {

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
         |$$(document).ready(function(){
         |    ${preparedStatements.mkString("\n")}
         |    ${statements.mkString("\n")}
         |});""".stripMargin

   <script>{Unparsed(js)}</script>
  }
}

