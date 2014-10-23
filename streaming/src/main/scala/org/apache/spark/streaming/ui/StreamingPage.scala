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
import org.apache.spark.streaming.scheduler.ReceiverInfo


/** Page for Spark Web UI that shows statistics of a streaming job */
private[ui] class StreamingPage(parent: StreamingTab)
  extends WebUIPage("") with Logging {

  private val listener = parent.listener
  private val startTime = Calendar.getInstance().getTime()
  private val empty = "-"

  /** Render the page */
  def render(request: HttpServletRequest): Seq[Node] = {
    val content =
      generateBasicStats() ++ <br></br> ++
      <h4>Statistics over last {listener.retainedCompletedBatches.size} processed batches</h4> ++
      generateReceiverStats() ++
      generateBatchStatsTable()
    UIUtils.headerSparkPage("Streaming", content, parent, Some(5000))
  }

  private val batchStatsTable: UITable[(String, Option[Long], Option[Seq[Double]])] = {
    val t = new UITableBuilder[(String, Option[Long], Option[Seq[Double]])]()
    t.col("Metric") { _._1 }
    t.optDurationCol("Last batch") { _._2 }
    t.optDurationCol("Minimum") { _._3.map(_(0).toLong) }
    t.optDurationCol("25th percentile") { _._3.map(_(1).toLong) }
    t.optDurationCol("Median") { _._3.map(_(2).toLong) }
    t.optDurationCol("75th percentile") { _._3.map(_(3).toLong) }
    t.optDurationCol("Maximum") { _._3.map(_(4).toLong) }
    t.build()
  }

  /** Generate basic stats of the streaming program */
  private def generateBasicStats(): Seq[Node] = {
    val timeSinceStart = System.currentTimeMillis() - startTime.getTime
    <ul class ="unstyled">
      <li>
        <strong>Started at: </strong> {startTime.toString}
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
        <strong>Processed batches: </strong>{listener.numTotalCompletedBatches}
      </li>
      <li>
        <strong>Waiting batches: </strong>{listener.numUnprocessedBatches}
      </li>
    </ul>
  }

  /** Generate stats of data received by the receivers in the streaming program */
  private def generateReceiverStats(): Seq[Node] = {
    val receivedRecordDistributions = listener.receivedRecordsDistributions
    val lastBatchReceivedRecord = listener.lastReceivedBatchRecords
    val table = if (receivedRecordDistributions.size > 0) {
      val tableRenderer: UITable[(Int, Option[ReceiverInfo])] = {
        val t = new UITableBuilder[(Int, Option[ReceiverInfo])]()
        t.col("Receiver") { case (receiverId, receiverInfo) =>
          receiverInfo.map(_.name).getOrElse(s"Receiver-$receiverId")
        }
        t.col("Status") { case (_, receiverInfo) =>
          receiverInfo.map { info => if (info.active) "ACTIVE" else "INACTIVE" }.getOrElse(empty)
        }
        t.col("Location") { case (_, receiverInfo) =>
          receiverInfo.map(_.location).getOrElse(empty)
        }
        t.col("Records in last batch\n[" + formatDate(Calendar.getInstance().getTime()) + "]") {
          case (receiverId, _) => formatNumber(lastBatchReceivedRecord(receiverId))
        }
        t.col("Minimum rate\n[records/sec]") { case (receiverId, _) =>
          receivedRecordDistributions(receiverId).map {
            _.getQuantiles(Seq(0.0)).map(formatNumber).head
          }.getOrElse(empty)
        }
        t.col("Median rate\n[records/sec]") { case (receiverId, _) =>
          receivedRecordDistributions(receiverId).map {
            _.getQuantiles(Seq(0.5)).map(formatNumber).head
          }.getOrElse(empty)
        }
        t.col("Maximum rate\n[records/sec]") { case (receiverId, _) =>
          receivedRecordDistributions(receiverId).map {
            _.getQuantiles(Seq(1.0)).map(formatNumber).head
          }.getOrElse(empty)
        }
        t.col("Last Error") { case (_, receiverInfo) =>
          receiverInfo.map { info =>
            val msg = s"${info.lastErrorMessage} - ${info.lastError}"
            if (msg.size > 100) msg.take(97) + "..." else msg
          }.getOrElse(empty)
        }
        t.build()
      }

      val dataRows = (0 until listener.numReceivers).map { id => (id, listener.receiverInfo(id)) }
      Some(tableRenderer.render(dataRows))
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
      val rows: Seq[(String, Option[Long], Option[Seq[Double]])] = Seq(
        ("Processing Time",
          lastCompletedBatch.flatMap(_.processingDelay),
          listener.processingDelayDistribution.map(_.getQuantiles())),
        ("Scheduling Delay",
          lastCompletedBatch.flatMap(_.schedulingDelay),
          listener.schedulingDelayDistribution.map(_.getQuantiles())),
        ("Total Delay",
          lastCompletedBatch.flatMap(_.totalDelay),
          listener.totalDelayDistribution.map(_.getQuantiles()))
      )
      Some(batchStatsTable.render(rows))
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
}
