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

import java.util.Calendar
import javax.servlet.http.HttpServletRequest

import scala.collection.mutable.ArrayBuffer
import scala.xml.{Node, Unparsed}

import org.apache.spark.Logging
import org.apache.spark.streaming.Time
import org.apache.spark.ui._
import org.apache.spark.ui.UIUtils._
import org.apache.spark.util.Distribution

/** Page for Spark Web UI that shows statistics of a streaming job */
private[ui] class StreamingPage(parent: StreamingTab)
  extends WebUIPage("") with Logging {

  import StreamingPage._

  private val listener = parent.listener
  private val startTime = System.currentTimeMillis()
  private val emptyCell = "-"

  /** Render the page */
  def render(request: HttpServletRequest): Seq[Node] = {
    val content = listener.synchronized {
      generateLoadResources() ++
      generateBasicStats() ++
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

  /** Generate basic stats of the streaming program */
  private def generateBasicStats(): Seq[Node] = {
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
    val avgSchedulingDelay = listener.schedulingDelayDistribution.map(_.statCounter.mean.toLong)
    val avgProcessingTime = listener.processingDelayDistribution.map(_.statCounter.mean.toLong)
    val avgTotalDelay = listener.totalDelayDistribution.map(_.statCounter.mean.toLong)
    val avgReceiverEvents =
      listener.receivedRecordsDistributions.mapValues(_.map(_.statCounter.mean.toLong))
    val avgAllReceiverEvents = avgReceiverEvents.values.flatMap(x => x).sum
    val receivedRecords = listener.allReceivedRecordsWithBatchTime

    val completedBatches = listener.retainedCompletedBatches
    val processingTime = completedBatches.
      flatMap(batchInfo => batchInfo.processingDelay.map(batchInfo.batchTime.milliseconds -> _)).
      sortBy(_._1)
    val schedulingDelay = completedBatches.
      flatMap(batchInfo => batchInfo.schedulingDelay.map(batchInfo.batchTime.milliseconds -> _)).
      sortBy(_._1)
    val totalDelay = completedBatches.
      flatMap(batchInfo => batchInfo.totalDelay.map(batchInfo.batchTime.milliseconds -> _)).
      sortBy(_._1)
    val jsCollector = ArrayBuffer[String]()

    val triangleJs =
      s"""$$('#inputs-table').toggle('collapsed');
         |if ($$(this).html() == '$BLACK_RIGHT_TRIANGLE_HTML')
         |$$(this).html('$BLACK_DOWN_TRIANGLE_HTML');
         |else $$(this).html('$BLACK_RIGHT_TRIANGLE_HTML');""".stripMargin.replaceAll("\\n", "")

    val table =
      // scalastyle:off
      <table class="table table-bordered">
      <thead>
        <tr><th></th><th>Timelines</th><th>Distributions</th></tr>
      </thead>
      <tbody>
        <tr>
          <td>
            <div>
              <span onclick={Unparsed(triangleJs)}>{Unparsed(BLACK_RIGHT_TRIANGLE_HTML)}</span>
              <strong>Input Rate</strong>
            </div>
            <div>Avg: {avgAllReceiverEvents} events/sec</div>
          </td>
          <td>{generateTimeline(jsCollector, "all-receiver-events-timeline", receivedRecords, "#")}</td>
          <td>{generateDistribution(jsCollector, "all-receiver-events-distribution", receivedRecords.map(_._2), "#")}</td>
        </tr>
        <tr id="inputs-table" style="display: none;" >
          <td colspan="3">
          {generateInputReceiversTable(jsCollector)}
          </td>
        </tr>
        <tr>
          <td>
            <div><strong>Processing Time</strong></div>
            <div>Avg: {formatDurationOption(avgProcessingTime)}</div>
          </td>
          <td>{generateTimeline(jsCollector, "processing-time-timeline", processingTime, "ms")}</td>
          <td>{generateDistribution(jsCollector, "processing-time-distribution", processingTime.map(_._2), "ms")}</td>
        </tr>
        <tr>
          <td>
            <div><strong>Scheduling Delay</strong></div>
            <div>Avg: {formatDurationOption(avgSchedulingDelay)}</div>
          </td>
          <td>{generateTimeline(jsCollector, "scheduling-delay-timeline", schedulingDelay, "ms")}</td>
          <td>{generateDistribution(jsCollector, "scheduling-delay-distribution", schedulingDelay.map(_._2), "ms")}</td>
        </tr>
        <tr>
          <td>
            <div><strong>Total Delay</strong></div>
            <div>Avg: {formatDurationOption(avgTotalDelay)}</div>
          </td>
          <td>{generateTimeline(jsCollector, "total-delay-timeline", totalDelay, "ms")}</td>
          <td>{generateDistribution(jsCollector, "total-delay-distribution", totalDelay.map(_._2), "ms")}</td>
        </tr>
      </tbody>
    </table>
    // scalastyle:on

    val js =
      s"""
         |$$(document).ready(function(){
         |    ${jsCollector.mkString("\n")}
         |});""".stripMargin
    table ++ <script>{Unparsed(js)}</script>
  }

  private def generateInputReceiversTable(jsCollector: ArrayBuffer[String]): Seq[Node] = {
    val content = listener.receivedRecordsDistributions.map { case (receiverId, distribution) =>
      generateInputReceiverRow(jsCollector, receiverId, distribution)
    }.reduce(_ ++ _)

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
      jsCollector: ArrayBuffer[String], receiverId: Int, distribution: Option[Distribution]):
    Seq[Node] = {
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
      listener.receiverLastErrorTimeo(receiverId).map(UIUtils.formatDate).getOrElse(emptyCell)
    val receivedRecords = listener.receivedRecordsWithBatchTime.get(receiverId).getOrElse(Seq())

    // scalastyle:off
    <tr>
      <td rowspan="2">
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
          {generateTimeline(jsCollector, s"receiver-$receiverId-events-timeline", receivedRecords, "#")}
        </td>
        <td>{generateDistribution(jsCollector, s"receiver-$receiverId-events-distribution", receivedRecords.map(_._2), "#")}</td>
      </tr>
    // scalastyle:on
  }

  private def generateTimeline(
      jsCollector: ArrayBuffer[String], divId: String, data: Seq[(Long, _)], unit: String):
    Seq[Node] = {
    val jsForData = data.map { case (x, y) =>
      s"""{"x": $x, "y": $y}"""
    }.mkString("[", ",", "]")
    jsCollector += s"drawTimeline('#$divId', $jsForData, '$unit');"

    <div id={divId}></div>
  }

  private def generateDistribution(
      jsCollector: ArrayBuffer[String], divId: String, data: Seq[_], unit: String): Seq[Node] = {
    val jsForData = data.mkString("[", ",", "]")
    jsCollector += s"drawDistribution('#$divId', $jsForData, '$unit');"

    <div id={divId}></div>
  }


  /** Generate stats of data received by the receivers in the streaming program */
  private def generateReceiverStats(): Seq[Node] = {
    val receivedRecordDistributions = listener.receivedRecordsDistributions
    val lastBatchReceivedRecord = listener.lastReceivedBatchRecords
    val table = if (receivedRecordDistributions.size > 0) {
      val headerRow = Seq(
        "Receiver",
        "Status",
        "Location",
        "Events in last batch\n[" + formatDate(Calendar.getInstance().getTime()) + "]",
        "Minimum rate\n[events/sec]",
        "Median rate\n[events/sec]",
        "Maximum rate\n[events/sec]",
        "Last Error"
      )
      val dataRows = (0 until listener.numReceivers).map { receiverId =>
        val receiverInfo = listener.receiverInfo(receiverId)
        val receiverName = receiverInfo.map(_.name).getOrElse(s"Receiver-$receiverId")
        val receiverActive = receiverInfo.map { info =>
          if (info.active) "ACTIVE" else "INACTIVE"
        }.getOrElse(emptyCell)
        val receiverLocation = receiverInfo.map(_.location).getOrElse(emptyCell)
        val receiverLastBatchRecords = formatNumber(lastBatchReceivedRecord(receiverId))
        val receivedRecordStats = receivedRecordDistributions(receiverId).map { d =>
          d.getQuantiles(Seq(0.0, 0.5, 1.0)).map(r => formatNumber(r.toLong))
        }.getOrElse {
          Seq(emptyCell, emptyCell, emptyCell, emptyCell, emptyCell)
        }
        val receiverLastError = listener.receiverInfo(receiverId).map { info =>
          val msg = s"${info.lastErrorMessage} - ${info.lastError}"
          if (msg.size > 100) msg.take(97) + "..." else msg
        }.getOrElse(emptyCell)
        Seq(receiverName, receiverActive, receiverLocation, receiverLastBatchRecords) ++
          receivedRecordStats ++ Seq(receiverLastError)
      }
      Some(listingTable(headerRow, dataRows))
    } else {
      None
    }

    val content =
      <h5>Receiver Statistics</h5> ++
      <div>{table.getOrElse("No receivers")}</div>

    content
  }

  /** Generate stats of batch jobs of the streaming program */
  private def generateBatchStatsTable(): Seq[Node] = {
    val numBatches = listener.retainedCompletedBatches.size
    val lastCompletedBatch = listener.lastCompletedBatch
    val table = if (numBatches > 0) {
      val processingDelayQuantilesRow = {
        Seq(
          "Processing Time",
          formatDurationOption(lastCompletedBatch.flatMap(_.processingDelay))
        ) ++ getQuantiles(listener.processingDelayDistribution)
      }
      val schedulingDelayQuantilesRow = {
        Seq(
          "Scheduling Delay",
          formatDurationOption(lastCompletedBatch.flatMap(_.schedulingDelay))
        ) ++ getQuantiles(listener.schedulingDelayDistribution)
      }
      val totalDelayQuantilesRow = {
        Seq(
          "Total Delay",
          formatDurationOption(lastCompletedBatch.flatMap(_.totalDelay))
        ) ++ getQuantiles(listener.totalDelayDistribution)
      }
      val headerRow = Seq("Metric", "Last batch", "Minimum", "25th percentile",
        "Median", "75th percentile", "Maximum")
      val dataRows: Seq[Seq[String]] = Seq(
        processingDelayQuantilesRow,
        schedulingDelayQuantilesRow,
        totalDelayQuantilesRow
      )
      Some(listingTable(headerRow, dataRows))
    } else {
      None
    }

    val content =
      <h5>Batch Processing Statistics</h5> ++
      <div>
        <ul class="unstyled">
          {table.getOrElse("No statistics have been generated yet.")}
        </ul>
      </div>

    content
  }

  def generateTimeRow(title: String, times: Seq[(Time, Long)]): Unit = {

  }


  /**
   * Returns a human-readable string representing a duration such as "5 second 35 ms"
   */
  private def formatDurationOption(msOption: Option[Long]): String = {
    msOption.map(formatDurationVerbose).getOrElse(emptyCell)
  }

  /** Get quantiles for any time distribution */
  private def getQuantiles(timeDistributionOption: Option[Distribution]) = {
    timeDistributionOption.get.getQuantiles().map { ms => formatDurationVerbose(ms.toLong) }
  }

  /** Generate HTML table from string data */
  private def listingTable(headers: Seq[String], data: Seq[Seq[String]]) = {
    def generateDataRow(data: Seq[String]): Seq[Node] = {
      <tr> {data.map(d => <td>{d}</td>)} </tr>
    }
    UIUtils.listingTable(headers, generateDataRow, data, fixedWidth = true)
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

