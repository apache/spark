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

import scala.collection.mutable.SynchronizedQueue
import scala.xml.Node

import javax.servlet.http.HttpServletRequest
import org.eclipse.jetty.servlet.ServletContextHandler

import org.apache.spark.Logging
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler.{BatchInfo, StreamingListener, StreamingListenerBatchCompleted}
import org.apache.spark.ui.{ServerInfo, SparkUI}
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.util.{Distribution, Utils}

private[spark] class StreamingUIListener() extends StreamingListener {

  private val batchInfos = new SynchronizedQueue[BatchInfo]
  private val maxBatchInfos = 100

  override def onBatchCompleted(batchStarted: StreamingListenerBatchCompleted) {
    batchInfos.enqueue(batchStarted.batchInfo)
    if (batchInfos.size > maxBatchInfos) batchInfos.dequeue()
  }

  def processingDelayDistribution = extractDistribution(_.processingDelay)

  def schedulingDelayDistribution = extractDistribution(_.schedulingDelay)

  def totalDelay = extractDistribution(_.totalDelay)

  def extractDistribution(getMetric: BatchInfo => Option[Long]): Option[Distribution] = {
    Distribution(batchInfos.flatMap(getMetric(_)).map(_.toDouble))
  }

  def numBatchInfos = batchInfos.size
}

private[spark] class StreamingUI(ssc: StreamingContext) extends Logging {

  private val sc = ssc.sparkContext
  private val conf = sc.conf
  private val appName = sc.appName
  private val bindHost = Utils.localHostName()
  private val publicHost = Option(System.getenv("SPARK_PUBLIC_DNS")).getOrElse(bindHost)
  private val port = conf.getInt("spark.streaming.ui.port", StreamingUI.DEFAULT_PORT)
  private val securityManager = sc.env.securityManager
  private val listener = new StreamingUIListener()
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
    val batchStatsTable = generateBatchStatsTable()
    val content = batchStatsTable
    UIUtils.headerStreamingPage(content, "", appName, "Spark Streaming Overview")
  }

  private def generateBatchStatsTable(): Seq[Node] =  {
    def getQuantiles(timeDistributionOption: Option[Distribution]) = {
      timeDistributionOption.get.getQuantiles().map { ms => Utils.msDurationToString(ms.toLong) }
    }
    val numBatches = listener.numBatchInfos
    val table = if (numBatches > 0) {
      val processingDelayQuantilesRow =
        "Processing Times" +: getQuantiles(listener.processingDelayDistribution)
      val schedulingDelayQuantilesRow =
        "Scheduling Delay:" +: getQuantiles(listener.processingDelayDistribution)
      val totalDelayQuantilesRow =
        "End-to-end Delay:" +: getQuantiles(listener.totalDelay)

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

    val content =
      <h4>Batch Processing Statistics</h4> ++
      <div>{table.getOrElse("No statistics have been generated yet.")}</div>
    content
  }
}

object StreamingUI {
  val DEFAULT_PORT = 6060
}
