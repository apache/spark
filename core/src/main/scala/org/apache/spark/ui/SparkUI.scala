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
import org.eclipse.jetty.server.handler.ContextHandlerCollection

import org.apache.spark.{Logging, SparkConf, SparkContext, SparkEnv}
import org.apache.spark.scheduler.{EventLoggingInfo, EventLoggingListener, SparkReplayerBus}
import org.apache.spark.storage.StorageStatusListener
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.ui.env.EnvironmentUI
import org.apache.spark.ui.exec.ExecutorsUI
import org.apache.spark.ui.jobs.JobProgressUI
import org.apache.spark.ui.storage.BlockManagerUI
import org.apache.spark.util.Utils

/** Top level user interface for Spark. */
private[spark] class SparkUI(
    val sc: SparkContext,
    conf: SparkConf,
    val appName: String,
    val basePath: String = "")
  extends Logging {

  import SparkUI._

  def this(sc: SparkContext) = this(sc, sc.conf, sc.appName)
  def this(conf: SparkConf, appName: String) = this(null, conf, appName)
  def this(conf: SparkConf, appName: String, basePath: String) =
    this(null, conf, appName, basePath)

  // If SparkContext is not provided, assume this UI is rendered from persisted storage
  val live = sc != null

  private val host = Option(System.getenv("SPARK_PUBLIC_DNS")).getOrElse(Utils.localHostName())
  private val port = conf.get("spark.ui.port", DEFAULT_PORT).toInt
  private var boundPort: Option[Int] = None
  private var server: Option[Server] = None
  private var started = false

  private val storage = new BlockManagerUI(this)
  private val jobs = new JobProgressUI(this)
  private val env = new EnvironmentUI(this)
  private val exec = new ExecutorsUI(this)

  private val handlers: Seq[(String, Handler)] = {
    val metricsServletHandlers = if (live) {
      SparkEnv.get.metricsSystem.getServletHandlers
    } else {
      Array[(String, Handler)]()
    }
    storage.getHandlers ++
    jobs.getHandlers ++
    env.getHandlers ++
    exec.getHandlers ++
    metricsServletHandlers ++
    Seq[(String, Handler)](
      ("/static", createStaticHandler(STATIC_RESOURCE_DIR)),
      ("/", createRedirectHandler("/stages", basePath))
    )
  }

  // The root handler that encapsulates all children handlers of this UI
  val rootHandler: ContextHandlerCollection = {
    val prefixedHandlers = handlers.map { case (relativePath, handler) =>
      JettyUtils.createContextHandler(basePath + relativePath.stripSuffix("/"), handler)
    }
    val collection = new ContextHandlerCollection
    collection.setHandlers(prefixedHandlers.toArray)
    collection
  }

  // Maintain executor storage status through Spark events
  val storageStatusListener = new StorageStatusListener

  // Only log events if this SparkUI is live
  private var eventLogger: Option[EventLoggingListener] = None

  // Only replay events if this SparkUI is not live
  private var replayerBus: Option[SparkReplayerBus] = None

  // Information needed to replay the events logged by this UI, if any
  def eventLogInfo: Option[EventLoggingInfo] =
    eventLogger.map { l => Some(l.info) }.getOrElse(None)

  /** Bind the HTTP server which backs this web interface */
  def bind() {
    try {
      val (srv, usedPort) = JettyUtils.startJettyServer(host, port, rootHandler)
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
    storage.start()
    jobs.start()
    env.start()
    exec.start()

    // Listen for events from the SparkContext if it exists, otherwise from persisted storage
    val eventBus = if (live) {
      val loggingEnabled = conf.getBoolean("spark.eventLog.enabled", false)
      if (loggingEnabled) {
        val logger = new EventLoggingListener(appName, conf)
        eventLogger = Some(logger)
        sc.listenerBus.addListener(logger)
      }
      sc.listenerBus
    } else {
      replayerBus = Some(new SparkReplayerBus(conf))
      replayerBus.get
    }

    // Storage status listener must receive events first, as other listeners depend on its state
    eventBus.addListener(storageStatusListener)
    eventBus.addListener(storage.listener)
    eventBus.addListener(jobs.listener)
    eventBus.addListener(env.listener)
    eventBus.addListener(exec.listener)
    started = true
  }

  /**
   * Reconstruct a previously persisted SparkUI from logs residing in the given directory.
   *
   * This method must be invoked after the SparkUI has started. Return true if log files
   * are found and processed.
   */
  def renderFromPersistedStorage(logDir: String): Boolean = {
    assume(!live, "Live Spark Web UI attempted to render from persisted storage!")
    assume(started, "Spark Web UI attempted to render from persisted storage before starting!")
    replayerBus.get.replay(logDir)
  }

  def stop() {
    server.foreach(_.stop())
    eventLogger.foreach(_.stop())
    logInfo("Stopped Spark Web UI at %s".format(appUIAddress))
  }

  private[spark] def appUIAddress = "http://" + host + ":" + boundPort.getOrElse("-1")

}

private[spark] object SparkUI {
  val DEFAULT_PORT = "4040"
  val STATIC_RESOURCE_DIR = "org/apache/spark/ui/static"
}
