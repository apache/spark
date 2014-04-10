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

import java.util.{Calendar, Locale}
import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.apache.spark.Logging
import org.apache.spark.ui._
import org.apache.spark.util.Distribution

/** Page for Spark Web UI that shows statistics of a streaming job */
private[ui] class StreamingPage(parent: StreamingTab)
  extends UIPage("") with Logging {

  private val ssc = parent.ssc
  private val sc = ssc.sparkContext
  private val sparkUI = sc.ui
  private val listener = new StreamingProgressListener(ssc)
  private val calendar = Calendar.getInstance()
  private val startTime = calendar.getTime()
  private val emptyCellTest = "-"

  ssc.addStreamingListener(listener)
  parent.attachPage(this)

  /** Render the page */
  override def render(request: HttpServletRequest): Seq[Node] = {
    val content =
      generateBasicStats() ++
      <br></br><h4>Statistics over last {listener.completedBatches.size} processed batches</h4> ++
      generateNetworkStatsTable() ++
      generateBatchStatsTable()
    UIUtils.headerSparkPage(
      content, sparkUI.basePath, sc.appName, "Streaming", sparkUI.getTabs, parent, Some(5000))
  }

  /** Generate basic stats of the streaming program */
  private def generateBasicStats(): Seq[Node] = {
    val timeSinceStart = System.currentTimeMillis() - startTime.getTime
    <ul class ="unstyled">
      <li>
        <strong>Started at: </strong> {startTime.toString}
      </li>
      <li>
        <strong>Time since start: </strong>{msDurationToString(timeSinceStart)}
      </li>
      <li>
        <strong>Network receivers: </strong>{listener.numNetworkReceivers}
      </li>
      <li>
        <strong>Batch interval: </strong>{msDurationToString(listener.batchDuration)}
      </li>
      <li>
        <strong>Processed batches: </strong>{listener.numTotalCompletedBatches}
      </li>
      <li>
        <strong>Waiting batches: </strong>{listener.numUnprocessedBatches}
      </li>
    </ul>
  }

  /** Generate stats of data received over the network the streaming program */
  private def generateNetworkStatsTable(): Seq[Node] = {
    val receivedRecordDistributions = listener.receivedRecordsDistributions
    val lastBatchReceivedRecord = listener.lastReceivedBatchRecords
    val table = if (receivedRecordDistributions.size > 0) {
      val headerRow = Seq(
        "Receiver",
        "Location",
        "Records in last batch",
        "Minimum rate\n[records/sec]",
        "25th percentile rate\n[records/sec]",
        "Median rate\n[records/sec]",
        "75th percentile rate\n[records/sec]",
        "Maximum rate\n[records/sec]"
      )
      val dataRows = (0 until listener.numNetworkReceivers).map { receiverId =>
        val receiverInfo = listener.receiverInfo(receiverId)
        val receiverName = receiverInfo.map(_.toString).getOrElse(s"Receiver-$receiverId")
        val receiverLocation = receiverInfo.map(_.location).getOrElse(emptyCellTest)
        val receiverLastBatchRecords = numberToString(lastBatchReceivedRecord(receiverId))
        val receivedRecordStats = receivedRecordDistributions(receiverId).map { d =>
          d.getQuantiles().map(r => numberToString(r.toLong))
        }.getOrElse {
          Seq(emptyCellTest, emptyCellTest, emptyCellTest, emptyCellTest, emptyCellTest)
        }
        Seq(receiverName, receiverLocation, receiverLastBatchRecords) ++ receivedRecordStats
      }
      Some(listingTable(headerRow, dataRows, fixedWidth = true))
    } else {
      None
    }

    val content =
      <h5>Network Input Statistics</h5> ++
      <div>{table.getOrElse("No network receivers")}</div>

    content
  }

  /** Generate stats of batch jobs of the streaming program */
  private def generateBatchStatsTable(): Seq[Node] = {
    val numBatches = listener.completedBatches.size
    val lastCompletedBatch = listener.lastCompletedBatch
    val table = if (numBatches > 0) {
      val processingDelayQuantilesRow = {
        Seq(
          "Processing Time",
          msDurationToString(lastCompletedBatch.flatMap(_.processingDelay))
        ) ++ getQuantiles(listener.processingDelayDistribution)
      }
      val schedulingDelayQuantilesRow = {
        Seq(
          "Scheduling Delay",
          msDurationToString(lastCompletedBatch.flatMap(_.schedulingDelay))
        ) ++ getQuantiles(listener.schedulingDelayDistribution)
      }
      val totalDelayQuantilesRow = {
        Seq(
          "Total Delay",
          msDurationToString(lastCompletedBatch.flatMap(_.totalDelay))
        ) ++ getQuantiles(listener.totalDelayDistribution)
      }
      val headerRow = Seq("Metric", "Last batch", "Minimum", "25th percentile",
        "Median", "75th percentile", "Maximum")
      val dataRows: Seq[Seq[String]] = Seq(
        processingDelayQuantilesRow,
        schedulingDelayQuantilesRow,
        totalDelayQuantilesRow
      )
      Some(listingTable(headerRow, dataRows, fixedWidth = true))
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
   * Returns a human-readable string representing a number
   */
  private def numberToString(records: Double): String = {
    val trillion = 1e12
    val billion = 1e9
    val million = 1e6
    val thousand = 1e3

    val (value, unit) = {
      if (records >= 2*trillion) {
        (records / trillion, " T")
      } else if (records >= 2*billion) {
        (records / billion, " B")
      } else if (records >= 2*million) {
        (records / million, " M")
      } else if (records >= 2*thousand) {
        (records / thousand, " K")
      } else {
        (records, "")
      }
    }
    "%.1f%s".formatLocal(Locale.US, value, unit)
  }

  /**
   * Returns a human-readable string representing a duration such as "5 second 35 ms"
   */
  private def msDurationToString(ms: Long): String = {
    try {
      val second = 1000L
      val minute = 60 * second
      val hour = 60 * minute
      val day = 24 * hour
      val week = 7 * day
      val year = 365 * day

      def toString(num: Long, unit: String): String = {
        if (num == 0) {
          ""
        } else if (num == 1) {
          s"$num $unit"
        } else {
          s"$num ${unit}s"
        }
      }

      val millisecondsString = if (ms >= second && ms % second == 0) "" else s"${ms % second} ms"
      val secondString = toString((ms % minute) / second, "second")
      val minuteString = toString((ms % hour) / minute, "minute")
      val hourString = toString((ms % day) / hour, "hour")
      val dayString = toString((ms % week) / day, "day")
      val weekString = toString((ms % year) / week, "week")
      val yearString = toString(ms / year, "year")

      Seq(
        second -> millisecondsString,
        minute -> s"$secondString $millisecondsString",
        hour -> s"$minuteString $secondString",
        day -> s"$hourString $minuteString $secondString",
        week -> s"$dayString $hourString $minuteString",
        year -> s"$weekString $dayString $hourString"
      ).foreach { case (durationLimit, durationString) =>
        if (ms < durationLimit) {
          // if time is less than the limit (upto year)
          return durationString
        }
      }
      // if time is more than a year
      return s"$yearString $weekString $dayString"
    } catch {
      case e: Exception =>
        logError("Error converting time to string", e)
        // if there is some error, return blank string
        return ""
    }
  }

  /**
   * Returns a human-readable string representing a duration such as "5 second 35 ms"
   */
  private def msDurationToString(msOption: Option[Long]): String = {
    msOption.map(msDurationToString).getOrElse(emptyCellTest)
  }

  /** Get quantiles for any time distribution */
  private def getQuantiles(timeDistributionOption: Option[Distribution]) = {
    timeDistributionOption.get.getQuantiles().map { ms => msDurationToString(ms.toLong) }
  }

  /** Generate an HTML table constructed by generating a row for each object in a sequence. */
  def listingTable[T](
      headerRow: Seq[String],
      dataRows: Seq[Seq[String]],
      fixedWidth: Boolean = false
    ): Seq[Node] = {

    val colWidth = 100.toDouble / headerRow.size
    val colWidthAttr = if (fixedWidth) colWidth + "%" else ""
    var tableClass = "table table-bordered table-striped table-condensed sortable"
    if (fixedWidth) {
      tableClass += " table-fixed"
    }

    def generateHeaderRow(header: Seq[String]): Seq[Node] = {
      headerRow.map { case h =>
        <th width={colWidthAttr}>
          <ul class ="unstyled">
            { h.split("\n").map { case t => <li> {t} </li> } }
          </ul>
        </th>
      }
    }

    def generateDataRow(data: Seq[String]): Seq[Node] = {
      <tr> {data.map(d => <td>{d}</td>)} </tr>
    }

    <table class={tableClass}>
      <thead>{generateHeaderRow(headerRow)}</thead>
      <tbody>
        {dataRows.map(r => generateDataRow(r))}
      </tbody>
    </table>
  }
}

