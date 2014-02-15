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

package org.apache.spark.ui

import org.eclipse.jetty.server.{Handler, Server}

import org.apache.spark.{Logging, SparkContext, SparkEnv}
import org.apache.spark.ui.env.EnvironmentUI
import org.apache.spark.ui.exec.ExecutorsUI
import org.apache.spark.ui.storage.BlockManagerUI
import org.apache.spark.ui.jobs.JobProgressUI
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.util.{FileLogger, Utils}
import org.apache.spark.scheduler._

import net.liftweb.json.JsonAST._
import org.apache.spark.storage.StorageStatus
import scala.Some
import scala.Some
import org.apache.spark.scheduler.SparkListenerStorageStatusFetch
import scala.Some
import org.apache.spark.scheduler.SparkListenerJobEnd
import org.apache.spark.scheduler.SparkListenerStageSubmitted
import org.apache.spark.scheduler.SparkListenerJobStart

/** Top level user interface for Spark */
private[spark] class SparkUI(sc: SparkContext) extends Logging {
  val host = Option(System.getenv("SPARK_PUBLIC_DNS")).getOrElse(Utils.localHostName())
  val port = sc.conf.get("spark.ui.port", SparkUI.DEFAULT_PORT).toInt
  var boundPort: Option[Int] = None
  var server: Option[Server] = None

  val handlers = Seq[(String, Handler)](
    ("/static", createStaticHandler(SparkUI.STATIC_RESOURCE_DIR)),
    ("/", createRedirectHandler("/stages"))
  )
  val storage = new BlockManagerUI(sc)
  val jobs = new JobProgressUI(sc)
  val env = new EnvironmentUI(sc)
  val exec = new ExecutorsUI(sc)

  // Add MetricsServlet handlers by default
  val metricsServletHandlers = SparkEnv.get.metricsSystem.getServletHandlers

  val allHandlers = storage.getHandlers ++ jobs.getHandlers ++ env.getHandlers ++
    exec.getHandlers ++ metricsServletHandlers ++ handlers

  /** Bind the HTTP server which backs this web interface */
  def bind() {
    try {
      val (srv, usedPort) = JettyUtils.startJettyServer(host, port, allHandlers)
      logInfo("Started Spark Web UI at http://%s:%d".format(host, usedPort))
      server = Some(srv)
      boundPort = Some(usedPort)
    } catch {
      case e: Exception =>
        logError("Failed to create Spark JettyUtils", e)
        System.exit(1)
    }
  }

  /** Initialize all components of the server */
  def start() {
    // NOTE: This is decoupled from bind() because of the following dependency cycle:
    //  DAGScheduler() requires that the port of this server is known
    //  This server must register all handlers, including JobProgressUI, before binding
    //  JobProgressUI registers a listener with SparkContext, which requires sc to initialize
    storage.start()
    jobs.start()
    env.start()
    exec.start()
  }

  def stop() {
    server.foreach(_.stop())
  }

  private[spark] def appUIAddress = host + ":" + boundPort.getOrElse("-1")

}

private[spark] object SparkUI {
  val DEFAULT_PORT = "4040"
  val STATIC_RESOURCE_DIR = "org/apache/spark/ui/static"
}

/** A SparkListener for logging events, one file per job */
private[spark] class UISparkListener(name: String) extends SparkListener {
  protected val logger = new FileLogger(name)

  protected def logEvent(event: SparkListenerEvent) = {
    logger.logLine(compactRender(event.toJson))
  }

  override def onJobStart(jobStart: SparkListenerJobStart) = logger.start()

  override def onJobEnd(jobEnd: SparkListenerJobEnd) = logger.close()
}

/**
 * A SparkListener that fetches storage information from SparkEnv and logs it as an event.
 *
 * The frequency at which this occurs is by default every time a stage event is triggered.
 * This needs not necessarily be the case; a stage can be arbitrarily long, so any failure
 * in the middle of a stage causes the storage status for that stage to be lost.
 */
private[spark] class StorageStatusFetchSparkListener(
    name: String,
    sc: SparkContext)
  extends UISparkListener(name) {
  var storageStatusList: Seq[StorageStatus] = sc.getExecutorStorageStatus

  /**
   * Fetch storage information from SparkEnv, which involves a query to the driver. This is
   * expensive and should be invoked sparingly.
   */
  def fetchStorageStatus() {
    val storageStatus = sc.getExecutorStorageStatus
    val event = new SparkListenerStorageStatusFetch(storageStatus)
    onStorageStatusFetch(event)
  }

  /**
   * Update local state with fetch result, and log the appropriate event
   */
  protected def onStorageStatusFetch(storageStatusFetch: SparkListenerStorageStatusFetch) {
    storageStatusList = storageStatusFetch.storageStatusList
    logEvent(storageStatusFetch)
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) {
    fetchStorageStatus()
    logger.flush()
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) {
    fetchStorageStatus()
    logger.flush()
  }
}