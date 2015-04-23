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

import javax.servlet.http.HttpServletRequest

import scala.collection.mutable.ArrayBuffer
import scala.xml.{Node, Unparsed}

import org.apache.spark.Logging
import org.apache.spark.ui._
import org.apache.spark.ui.UIUtils._
import org.apache.spark.util.Distribution

/**
 * @param divId the `id` used in the html `div` tag
 * @param data the data for the timeline graph
 * @param minY the min value of Y axis
 * @param maxY the max value of Y axis
 * @param unitY the unit of Y axis
 */
private[ui] case class TimelineUIData(divId: String, data: Seq[(Long, _)], minX: Long, maxX: Long,
    minY: Long, maxY: Long, unitY: String) {

  def toHtml(jsCollector: JsCollector): Seq[Node] = {
    val jsForData = data.map { case (x, y) =>
      s"""{"x": $x, "y": $y}"""
    }.mkString("[", ",", "]")
    jsCollector.addStatement(
      s"drawTimeline('#$divId', $jsForData, $minX, $maxX, $minY, $maxY, '$unitY');")

    <div id={divId}></div>
  }
}

/**
 * @param divId the `id` used in the html `div` tag
 * @param data the data for the distribution graph
 * @param minY the min value of Y axis
 * @param maxY the max value of Y axis
 * @param unitY the unit of Y axis
 */
private[ui] case class DistributionUIData(
    divId: String, data: Seq[_], minY: Long, maxY: Long, unitY: String) {

  def toHtml(jsCollector: JsCollector): Seq[Node] = {
    val jsForData = data.mkString("[", ",", "]")
    jsCollector.addPreparedStatement(s"prepareDistribution($jsForData, $minY, $maxY);")
    jsCollector.addStatement(s"drawDistribution('#$divId', $jsForData, $minY, $maxY, '$unitY');")

    <div id={divId}></div>
  }
}

private[ui] case class LongStreamingUIData(data: Seq[(Long, Long)]) {

  val avg: Option[Long] = if (data.isEmpty) None else Some(data.map(_._2).sum / data.size)

  val max: Option[Long] = if (data.isEmpty) None else Some(data.map(_._2).max)
}

private[ui] case class DoubleStreamingUIData(data: Seq[(Long, Double)]) {

  val avg: Option[Double] = if (data.isEmpty) None else Some(data.map(_._2).sum / data.size)

  val max: Option[Double] = if (data.isEmpty) None else Some(data.map(_._2).max)
}

/** Page for Spark Web UI that shows statistics of a streaming job */
private[ui] class StreamingPage(parent: StreamingTab)
  extends WebUIPage("") with Logging {

  import StreamingPage._

  private val listener = parent.listener
  private val startTime = System.currentTimeMillis()
  private val emptyCell = "-"

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
      </strong>.
    </div>
  }

  private def generateStatTable(): Seq[Node] = {
    val batchInfos = listener.retainedBatches

    val batchTimes = batchInfos.map(_.batchTime.milliseconds)
    val minBatchTime = if (batchTimes.isEmpty) startTime else batchTimes.min
    val maxBatchTime = if (batchTimes.isEmpty) startTime else batchTimes.max

    val eventRateForAllReceivers = DoubleStreamingUIData(batchInfos.map { batchInfo =>
      (batchInfo.batchTime.milliseconds, batchInfo.numRecords * 1000.0 / listener.batchDuration)
    })

    val schedulingDelay = LongStreamingUIData(batchInfos.flatMap { batchInfo =>
      batchInfo.schedulingDelay.map(batchInfo.batchTime.milliseconds -> _)
    })
    val processingTime = LongStreamingUIData(batchInfos.flatMap { batchInfo =>
      batchInfo.processingDelay.map(batchInfo.batchTime.milliseconds -> _)
    })
    val totalDelay = LongStreamingUIData(batchInfos.flatMap { batchInfo =>
      batchInfo.totalDelay.map(batchInfo.batchTime.milliseconds -> _)
    })

    val jsCollector = new JsCollector

    // Use the max value of "schedulingDelay", "processingTime", and "totalDelay" to make the
    // Y axis ranges same.
    val maxTime =
      (for (m1 <- schedulingDelay.max; m2 <- processingTime.max; m3 <- totalDelay.max) yield
        m1 max m2 max m3).getOrElse(0L)
    List(1, 2, 3).sum
    // Should start at 0
    val minTime = 0L

    // Use the max input rate for all receivers' graphs to make the Y axis ranges same.
    // If it's not an integral number, just use its ceil integral number.
    val maxEventRate = eventRateForAllReceivers.max.map(_.ceil.toLong).getOrElse(0L)
    val minEventRate = 0L

    val triangleJs =
      s"""$$('#inputs-table').toggle('collapsed');
         |if ($$(this).html() == '$BLACK_RIGHT_TRIANGLE_HTML')
         |$$(this).html('$BLACK_DOWN_TRIANGLE_HTML');
         |else $$(this).html('$BLACK_RIGHT_TRIANGLE_HTML');""".stripMargin.replaceAll("\\n", "")

    val timelineDataForEventRateOfAllReceivers =
      TimelineUIData(
        "all-receiver-events-timeline",
        eventRateForAllReceivers.data,
        minBatchTime,
        maxBatchTime,
        minEventRate,
        maxEventRate,
        "events/sec").toHtml(jsCollector)

    val distributionDataForEventRateOfAllReceivers =
      DistributionUIData(
        "all-receiver-events-distribution",
        eventRateForAllReceivers.data.map(_._2),
        minEventRate,
        maxEventRate,
        "events/sec").toHtml(jsCollector)

    val timelineDataForSchedulingDelay =
      TimelineUIData(
        "scheduling-delay-timeline",
        schedulingDelay.data,
        minBatchTime,
        maxBatchTime,
        minTime,
        maxTime,
        "ms").toHtml(jsCollector)

    val distributionDataForSchedulingDelay =
      DistributionUIData(
        "scheduling-delay-distribution",
        schedulingDelay.data.map(_._2),
        minTime,
        maxTime,
        "ms").toHtml(jsCollector)

    val timelineDataForProcessingTime =
      TimelineUIData(
        "processing-time-timeline",
        processingTime.data,
        minBatchTime,
        maxBatchTime,
        minTime,
        maxTime,
        "ms").toHtml(jsCollector)

    val distributionDataForProcessingTime =
      DistributionUIData(
        "processing-time-distribution",
        processingTime.data.map(_._2),
        minTime,
        maxTime,
        "ms").toHtml(jsCollector)

    val timelineDataForTotalDelay =
      TimelineUIData(
        "total-delay-timeline",
        totalDelay.data,
        minBatchTime,
        maxBatchTime,
        minTime,
        maxTime,
        "ms").toHtml(jsCollector)

    val distributionDataForTotalDelay =
      DistributionUIData(
        "total-delay-distribution",
        totalDelay.data.map(_._2),
        minTime,
        maxTime,
        "ms").toHtml(jsCollector)

    val table =
      // scalastyle:off
      <table class="table table-bordered">
      <thead>
        <tr><th></th><th>Timelines</th><th>Distributions</th></tr>
      </thead>
      <tbody>
        <tr>
          <td style="vertical-align: middle;">
            <div>
              <span onclick={Unparsed(triangleJs)}>{Unparsed(BLACK_RIGHT_TRIANGLE_HTML)}</span>
              <strong>Input Rate</strong>
            </div>
            <div>Avg: {eventRateForAllReceivers.avg.map(_.formatted("%.2f")).getOrElse(emptyCell)} events/sec</div>
          </td>
          <td>{timelineDataForEventRateOfAllReceivers}</td>
          <td>{distributionDataForEventRateOfAllReceivers}</td>
        </tr>
        <tr id="inputs-table" style="display: none;" >
          <td colspan="3">
          {generateInputReceiversTable(jsCollector, minBatchTime, maxBatchTime, minEventRate, maxEventRate)}
          </td>
        </tr>
        <tr>
          <td style="vertical-align: middle;">
            <div><strong>Scheduling Delay</strong></div>
            <div>Avg: {formatDurationOption(schedulingDelay.avg)}</div>
          </td>
          <td>{timelineDataForSchedulingDelay}</td>
          <td>{distributionDataForSchedulingDelay}</td>
        </tr>
        <tr>
          <td style="vertical-align: middle;">
            <div><strong>Processing Time</strong></div>
            <div>Avg: {formatDurationOption(processingTime.avg)}</div>
          </td>
          <td>{timelineDataForProcessingTime}</td>
          <td>{distributionDataForProcessingTime}</td>
        </tr>
        <tr>
          <td style="vertical-align: middle;">
            <div><strong>Total Delay</strong></div>
            <div>Avg: {formatDurationOption(totalDelay.avg)}</div>
          </td>
          <td>{timelineDataForTotalDelay}</td>
          <td>{distributionDataForTotalDelay}</td>
        </tr>
      </tbody>
    </table>
    // scalastyle:on

    table ++ jsCollector.toHtml
  }

  private def generateInputReceiversTable(
      jsCollector: JsCollector,
      minX: Long,
      maxX: Long,
      minY: Long,
      maxY: Long): Seq[Node] = {
    val content = listener.receivedRecordsDistributions.map { case (receiverId, distribution) =>
      generateInputReceiverRow(jsCollector, receiverId, distribution, minX, maxX, minY, maxY)
    }.foldLeft[Seq[Node]](Nil)(_ ++ _)

    <table class="table table-bordered">
      <thead>
        <tr>
          <th></th>
          <th>Status</th>
          <th>Location</th>
          <th>Last Error Time</th>
          <th>Last Error Message</th>
        </tr>
      </thead>
      <tbody>
        {content}
      </tbody>
    </table>
  }

  private def generateInputReceiverRow(
      jsCollector: JsCollector,
      receiverId: Int,
      distribution: Option[Distribution],
      minX: Long,
      maxX: Long,
      minY: Long,
      maxY: Long): Seq[Node] = {
    val avgReceiverEvents = distribution.map(_.statCounter.mean.toLong)
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
    val receiverLastErrorTime =
      listener.receiverLastErrorTime(receiverId).map(UIUtils.formatDate).getOrElse(emptyCell)
    val receivedRecords = listener.receivedRecordsWithBatchTime.get(receiverId).getOrElse(Seq())

    val timelineForEventRate =
      TimelineUIData(
        s"receiver-$receiverId-events-timeline",
        receivedRecords,
        minX,
        maxX,
        minY,
        maxY,
        "events/sec").toHtml(jsCollector)

    val distributionForEventsRate =
      DistributionUIData(
        s"receiver-$receiverId-events-distribution",
        receivedRecords.map(_._2),
        minY,
        maxY,
        "events/sec").toHtml(jsCollector)

    // scalastyle:off
    <tr>
      <td rowspan="2" style="vertical-align: middle;">
        <div>
          <strong>{receiverName}</strong>
        </div>
        <div>Avg: {avgReceiverEvents.map(_.toString).getOrElse(emptyCell)} events/sec</div>
      </td>
      <td>{receiverActive}</td>
      <td>{receiverLocation}</td>
      <td>{receiverLastErrorTime}</td>
      <td>{receiverLastError}</td>
    </tr>
      <tr>
        <td colspan="3">
          {timelineForEventRate}
        </td>
        <td>{distributionForEventsRate}</td>
      </tr>
    // scalastyle:on
  }

  /**
   * Returns a human-readable string representing a duration such as "5 second 35 ms"
   */
  private def formatDurationOption(msOption: Option[Long]): String = {
    msOption.map(formatDurationVerbose).getOrElse(emptyCell)
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

private object StreamingPage {
  val BLACK_RIGHT_TRIANGLE_HTML = "&#9654;"
  val BLACK_DOWN_TRIANGLE_HTML = "&#9660;"
}

private[ui] class JsCollector {
  private val preparedStatements = ArrayBuffer[String]()
  private val statements = ArrayBuffer[String]()

  def addPreparedStatement(js: String): Unit = {
    preparedStatements += js
  }

  def addStatement(js: String): Unit = {
    statements += js
  }

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

