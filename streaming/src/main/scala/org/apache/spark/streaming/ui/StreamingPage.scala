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

import scala.xml.Node

import org.apache.spark.Logging
import org.apache.spark.ui._
import org.apache.spark.ui.UIUtils._
import org.apache.spark.util.Distribution

/** Page for Spark Web UI that shows statistics of a streaming job */
private[ui] class StreamingPage(parent: StreamingTab)
  extends WebUIPage("") with Logging {

  private val listener = parent.listener
  private val startTime = System.currentTimeMillis()
  private val emptyCell = "-"

  /** Render the page */
  def render(request: HttpServletRequest): Seq[Node] = {
    val content = listener.synchronized {
      generateBasicStats() ++ <br></br> ++
      <h4>Statistics over last {listener.retainedCompletedBatches.size} processed batches</h4> ++
      generateReceiverStats() ++
      generateBatchStatsTable() ++
      generateBatchListTables()
    }
    UIUtils.headerSparkPage("Streaming", content, parent, Some(5000))
  }

  /** Generate basic stats of the streaming program */
  private def generateBasicStats(): Seq[Node] = {
    val timeSinceStart = System.currentTimeMillis() - startTime
    // scalastyle:off
    <ul class ="unstyled">
      <li>
        <strong>Started at: </strong> {UIUtils.formatDate(startTime)}
      </li>
      <li>
        <strong>Time since start: </strong>{formatDurationVerbose(timeSinceStart)}
      </li>
      <li>
        <strong>Network receivers: </strong>{listener.numReceivers}
      </li>
      <li>
        <strong>Batch interval: </strong>{formatDurationVerbose(listener.batchDuration)}
      </li>
      <li>
        <a href="#completed"><strong>Completed batches: </strong></a>{listener.numTotalCompletedBatches}
      </li>
      <li>
        <a href="#active"><strong>Active batches: </strong></a>{listener.numUnprocessedBatches}
      </li>
      <li>
        <strong>Received events: </strong>{listener.numTotalReceivedRecords}
      </li>
      <li>
        <strong>Processed events: </strong>{listener.numTotalProcessedRecords}
      </li>
    </ul>
    // scalastyle:on
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
      val dataRows = listener.receiverIds().map { receiverId =>
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
      }.toSeq
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

