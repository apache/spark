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

import scala.collection.mutable.{HashMap, Queue}
import scala.xml.Node

import javax.servlet.http.HttpServletRequest
import org.eclipse.jetty.servlet.ServletContextHandler

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.streaming.scheduler._
import org.apache.spark.ui.{ServerInfo, SparkUI}
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.util.{Distribution, Utils}
import java.util.{Calendar, Locale}

private[ui] class StreamingUIListener(ssc: StreamingContext) extends StreamingListener {

  private val waitingBatchInfos = new HashMap[Time, BatchInfo]
  private val runningBatchInfos = new HashMap[Time, BatchInfo]
  private val completedaBatchInfos = new Queue[BatchInfo]
  private val batchInfoLimit = ssc.conf.getInt("spark.steaming.ui.maxBatches", 100)
  private var totalCompletedBatches = 0L
  private val receiverInfos = new HashMap[Int, ReceiverInfo]

  val batchDuration = ssc.graph.batchDuration.milliseconds

  override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted) = {
    synchronized {
      receiverInfos.put(receiverStarted.receiverInfo.streamId, receiverStarted.receiverInfo)
    }
  }

  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted) = synchronized {
    runningBatchInfos(batchSubmitted.batchInfo.batchTime) = batchSubmitted.batchInfo
  }

  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted) = synchronized {
    runningBatchInfos(batchStarted.batchInfo.batchTime) = batchStarted.batchInfo
    waitingBatchInfos.remove(batchStarted.batchInfo.batchTime)
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) = synchronized {
    waitingBatchInfos.remove(batchCompleted.batchInfo.batchTime)
    runningBatchInfos.remove(batchCompleted.batchInfo.batchTime)
    completedaBatchInfos.enqueue(batchCompleted.batchInfo)
    if (completedaBatchInfos.size > batchInfoLimit) completedaBatchInfos.dequeue()
    totalCompletedBatches += 1L
  }

  def numNetworkReceivers = synchronized {
    ssc.graph.getNetworkInputStreams().size
  }

  def numTotalCompletedBatches: Long = synchronized {
    totalCompletedBatches
  }
  
  def numUnprocessedBatches: Long = synchronized {
    waitingBatchInfos.size + runningBatchInfos.size
  }

  def waitingBatches: Seq[BatchInfo] = synchronized {
    waitingBatchInfos.values.toSeq
  }

  def runningBatches: Seq[BatchInfo] = synchronized {
    runningBatchInfos.values.toSeq
  }

  def completedBatches: Seq[BatchInfo] = synchronized {
    completedaBatchInfos.toSeq
  }

  def processingDelayDistribution: Option[Distribution] = synchronized {
    extractDistribution(_.processingDelay)
  }

  def schedulingDelayDistribution: Option[Distribution] = synchronized {
    extractDistribution(_.schedulingDelay)
  }

  def totalDelayDistribution: Option[Distribution] = synchronized {
    extractDistribution(_.totalDelay)
  }

  def receivedRecordsDistributions: Map[Int, Option[Distribution]] = synchronized {
    val latestBatchInfos = allBatches.reverse.take(batchInfoLimit)
    val latestBlockInfos = latestBatchInfos.map(_.receivedBlockInfo)
    (0 until numNetworkReceivers).map { receiverId =>
      val blockInfoOfParticularReceiver = latestBlockInfos.map { batchInfo =>
        batchInfo.get(receiverId).getOrElse(Array.empty)
      }
      val recordsOfParticularReceiver = blockInfoOfParticularReceiver.map { blockInfo =>
        // calculate records per second for each batch
        blockInfo.map(_.numRecords).sum.toDouble * 1000 / batchDuration
      }
      val distributionOption = Distribution(recordsOfParticularReceiver)
      (receiverId, distributionOption)
    }.toMap
  }

  def lastReceivedBatchRecords: Map[Int, Long] = {
    val lastReceivedBlockInfoOption = lastReceivedBatch.map(_.receivedBlockInfo)
    lastReceivedBlockInfoOption.map { lastReceivedBlockInfo =>
      (0 until numNetworkReceivers).map { receiverId =>
        (receiverId, lastReceivedBlockInfo(receiverId).map(_.numRecords).sum)
      }.toMap
    }.getOrElse {
      (0 until numNetworkReceivers).map(receiverId => (receiverId, 0L)).toMap
    }
  }

  def receiverInfo(receiverId: Int): Option[ReceiverInfo] = {
    receiverInfos.get(receiverId)
  }

  def lastCompletedBatch: Option[BatchInfo] = {
    completedaBatchInfos.sortBy(_.batchTime)(Time.ordering).lastOption
  }

  def lastReceivedBatch: Option[BatchInfo] = {
    allBatches.lastOption
  }

  private def allBatches: Seq[BatchInfo] = synchronized {
    (waitingBatchInfos.values.toSeq ++
      runningBatchInfos.values.toSeq ++ completedaBatchInfos).sortBy(_.batchTime)(Time.ordering)
  }

  private def extractDistribution(getMetric: BatchInfo => Option[Long]): Option[Distribution] = {
    Distribution(completedaBatchInfos.flatMap(getMetric(_)).map(_.toDouble))
  }
}


private[ui] class StreamingPage(parent: StreamingUI) extends Logging {

  private val listener = parent.listener
  private val calendar = Calendar.getInstance()
  private val startTime = calendar.getTime()
  private val emptyCellTest = "-"

  def render(request: HttpServletRequest): Seq[Node] = {

    val content =
     generateBasicStats() ++
     <br></br><h4>Statistics over last {listener.completedBatches.size} processed batches</h4> ++
     generateNetworkStatsTable() ++
     generateBatchStatsTable()
    UIUtils.headerStreamingPage(content, "", parent.appName, "Spark Streaming Overview")
  }

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

  private def generateNetworkStatsTable(): Seq[Node] = {
    val receivedRecordDistributions = listener.receivedRecordsDistributions
    val lastBatchReceivedRecord = listener.lastReceivedBatchRecords
    val table = if (receivedRecordDistributions.size > 0) {
      val headerRow = Seq(
        "Receiver",
        "Location",
        s"Records in last batch",
        "Minimum rate [records/sec]",
        "25th percentile rate [records/sec]",
        "Median rate [records/sec]",
        "75th percentile rate [records/sec]",
        "Maximum rate [records/sec]"
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
        Seq(receiverName, receiverLocation, receiverLastBatchRecords) ++
          receivedRecordStats
      }
      Some(UIUtils.listingTable(headerRow, dataRows, fixedWidth = true))
    } else {
      None
    }

    val content =
      <h5>Network Input Statistics</h5> ++
      <div>{table.getOrElse("No network receivers")}</div>

    content
  }

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
      Some(UIUtils.listingTable(headerRow, dataRows, fixedWidth = true))
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

  private def getQuantiles(timeDistributionOption: Option[Distribution]) = {
    timeDistributionOption.get.getQuantiles().map { ms => msDurationToString(ms.toLong) }
  }

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
      ).foreach {
        case (durationLimit, durationString) if (ms < durationLimit) =>
          return durationString
        case e: Any => // matcherror is thrown without this
      }
      return s"$yearString $weekString $dayString"
    } catch {
      case e: Exception =>
        logError("Error converting time to string", e)
        return ""
    }
  }

  private def msDurationToString(msOption: Option[Long]): String = {
    msOption.map(msDurationToString).getOrElse(emptyCellTest)
  }
}


private[spark] class StreamingUI(val ssc: StreamingContext) extends Logging {

  val sc = ssc.sparkContext
  val conf = sc.conf
  val appName = sc.appName
  val listener = new StreamingUIListener(ssc)
  val overviewPage = new StreamingPage(this)

  private val bindHost = Utils.localHostName()
  private val publicHost = Option(System.getenv("SPARK_PUBLIC_DNS")).getOrElse(bindHost)
  private val port = conf.getInt("spark.streaming.ui.port", StreamingUI.DEFAULT_PORT)
  private val securityManager = sc.env.securityManager
  private val handlers: Seq[ServletContextHandler] = {
    Seq(
      createServletHandler("/",
        (request: HttpServletRequest) => overviewPage.render(request), securityManager),
      createStaticHandler(SparkUI.STATIC_RESOURCE_DIR, "/static")
    )
  }

  private var serverInfo: Option[ServerInfo] = None

  ssc.addStreamingListener(listener)

  def bind() {
    try {
      serverInfo = Some(startJettyServer(bindHost, port, handlers, sc.conf))
      logInfo("Started Spark Streaming Web UI at http://%s:%d".format(publicHost, boundPort))
    } catch {
      case e: Exception =>
        logError("Failed to create Spark JettyUtils", e)
        System.exit(1)
    }
  }

  private def boundPort: Int = serverInfo.map(_.boundPort).getOrElse(-1)
}

object StreamingUI {
  val DEFAULT_PORT = 6060
}
