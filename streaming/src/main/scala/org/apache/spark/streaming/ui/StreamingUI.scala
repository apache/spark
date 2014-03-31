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
import java.util.Locale

private[spark] class StreamingUIListener(conf: SparkConf) extends StreamingListener {

  private val waitingBatchInfos = new HashMap[Time, BatchInfo]
  private val runningBatchInfos = new HashMap[Time, BatchInfo]
  private val completedaBatchInfos = new Queue[BatchInfo]
  private val batchInfoLimit = conf.getInt("spark.steaming.ui.maxBatches", 100)

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
  }

  def numNetworkReceivers: Int = synchronized {
    completedaBatchInfos.headOption.map(_.receivedBlockInfo.size).getOrElse(0)
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
    val allBatcheInfos = waitingBatchInfos.values.toSeq ++
      runningBatchInfos.values.toSeq ++ completedaBatchInfos
    val latestBatchInfos = allBatcheInfos.sortBy(_.batchTime)(Time.ordering).reverse.take(batchInfoLimit)
    val latestBlockInfos = latestBatchInfos.map(_.receivedBlockInfo)
    (0 until numNetworkReceivers).map { receiverId =>
      val blockInfoOfParticularReceiver = latestBlockInfos.map(_.get(receiverId).getOrElse(Array.empty))
      val distributionOption = Distribution(blockInfoOfParticularReceiver.map(_.map(_.numRecords).sum.toDouble))
      (receiverId, distributionOption)
    }.toMap
  }

  private def extractDistribution(getMetric: BatchInfo => Option[Long]): Option[Distribution] = {
    Distribution(completedaBatchInfos.flatMap(getMetric(_)).map(_.toDouble))
  }
}

private[spark] class StreamingUI(ssc: StreamingContext) extends Logging {

  private val sc = ssc.sparkContext
  private val conf = sc.conf
  private val appName = sc.appName
  private val bindHost = Utils.localHostName()
  private val publicHost = Option(System.getenv("SPARK_PUBLIC_DNS")).getOrElse(bindHost)
  private val port = conf.getInt("spark.streaming.ui.port", StreamingUI.DEFAULT_PORT)
  private val securityManager = sc.env.securityManager
  private val listener = new StreamingUIListener(conf)
  private val handlers: Seq[ServletContextHandler] = {
    Seq(
      createServletHandler("/",
        (request: HttpServletRequest) => render(request), securityManager),
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

  def boundPort: Int = serverInfo.map(_.boundPort).getOrElse(-1)

  private def render(request: HttpServletRequest): Seq[Node] = {
    val content = generateBatchStatsTable() ++ generateNetworkStatsTable()
    UIUtils.headerStreamingPage(content, "", appName, "Spark Streaming Overview")
  }

  private def generateBatchStatsTable(): Seq[Node] = {
    val numBatches = listener.completedBatches.size
    val table = if (numBatches > 0) {
      val processingDelayQuantilesRow =
        "Processing Times" +: getQuantiles(listener.processingDelayDistribution)
      val schedulingDelayQuantilesRow =
        "Scheduling Delay:" +: getQuantiles(listener.schedulingDelayDistribution)
      val totalDelayQuantilesRow =
        "End-to-end Delay:" +: getQuantiles(listener.totalDelayDistribution)

      val headerRow = Seq("Metric", "Min", "25th percentile",
        "Median", "75th percentile", "Max")
      val dataRows: Seq[Seq[String]] = Seq(
        processingDelayQuantilesRow,
        schedulingDelayQuantilesRow,
        totalDelayQuantilesRow
      )
      Some(UIUtils.listingTable(headerRow, dataRows, fixedWidth = true))
    } else {
      None
    }

    val batchCounts =
      <ul class="unstyled">
        <li>
          # batches being processed: {listener.runningBatches.size}
        </li>
        <li>
          # scheduled batches: {listener.waitingBatches.size}
        </li>
      </ul>

    val batchStats =
      <ul class="unstyled">
        <li>
          <h5>Statistics over last {numBatches} processed batches</h5>
        </li>
        <li>
          {table.getOrElse("No statistics have been generated yet.")}
        </li>
      </ul>

    val content =
      <h4>Batch Processing Statistics</h4> ++
      <div>{batchCounts}</div> ++
      <div>{batchStats}</div>

    content
  }

  private def generateNetworkStatsTable(): Seq[Node] = {
    val receivedRecordDistributions = listener.receivedRecordsDistributions
    val numNetworkReceivers = receivedRecordDistributions.size
    val table = if (receivedRecordDistributions.size > 0) {
      val headerRow = Seq("Receiver", "Min", "25th percentile",
        "Median", "75th percentile", "Max")
      val dataRows = (0 until numNetworkReceivers).map { receiverId =>
        val receiverName = s"Receiver-$receiverId"
        val receivedRecordStats = receivedRecordDistributions(receiverId).map { d =>
          d.getQuantiles().map(r => numberToString(r.toLong) + " records/batch")
        }.getOrElse {
          Seq("-", "-", "-", "-", "-")
        }
        receiverName +: receivedRecordStats
      }
      Some(UIUtils.listingTable(headerRow, dataRows, fixedWidth = true))
    } else {
      None
    }

    val content =
      <h4>Network Input Statistics</h4> ++
      <div>{table.getOrElse("No network receivers")}</div>

    content
  }

  private def getQuantiles(timeDistributionOption: Option[Distribution]) = {
    timeDistributionOption.get.getQuantiles().map { ms => Utils.msDurationToString(ms.toLong) }
  }

  private def numberToString(records: Double): String = {
    val trillion = 1e12
    val billion = 1e9
    val million = 1e6
    val thousand = 1e3

    val (value, unit) = {
      if (records >= 2*trillion) {
        (records / trillion, "T")
      } else if (records >= 2*billion) {
        (records / billion, "B")
      } else if (records >= 2*million) {
        (records / million, "M")
      } else if (records >= 2*thousand) {
        (records / thousand, "K")
      } else {
        (records, "")
      }
    }
    "%.1f%s".formatLocal(Locale.US, value, unit)
  }
}

object StreamingUI {
  val DEFAULT_PORT = 6060
}
