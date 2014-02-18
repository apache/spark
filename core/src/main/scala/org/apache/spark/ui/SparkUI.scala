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

import java.io.{FileInputStream, File}

import scala.io.Source

import org.eclipse.jetty.server.{Handler, Server}

import org.apache.spark.{Logging, SparkContext, SparkEnv}
import org.apache.spark.scheduler._
import org.apache.spark.ui.env.EnvironmentUI
import org.apache.spark.ui.exec.ExecutorsUI
import org.apache.spark.ui.storage.BlockManagerUI
import org.apache.spark.ui.jobs.JobProgressUI
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.util.Utils

import net.liftweb.json._

import it.unimi.dsi.fastutil.io.FastBufferedInputStream

/** Top level user interface for Spark */
private[spark] class SparkUI(val sc: SparkContext, live: Boolean = true) extends Logging {
  val host = Option(System.getenv("SPARK_PUBLIC_DNS")).getOrElse(Utils.localHostName())
  val port = if (live) {
      sc.conf.get("spark.ui.port", SparkUI.DEFAULT_PORT).toInt
    } else {
      // While each context has only one live SparkUI, it can have many persisted ones
      // For persisted UI's, climb upwards from the configured / default port
      val nextPort = SparkUI.lastPersistedUIPort match {
        case Some(p) => p + 1
        case None => sc.conf.get("spark.ui.persisted.port", SparkUI.DEFAULT_PERSISTED_PORT).toInt
      }
      SparkUI.lastPersistedUIPort = Some(nextPort)
      nextPort
    }
  var boundPort: Option[Int] = None
  var server: Option[Server] = None

  private val handlers = Seq[(String, Handler)](
    ("/static", createStaticHandler(SparkUI.STATIC_RESOURCE_DIR)),
    ("/", createRedirectHandler("/stages"))
  )
  private val storage = new BlockManagerUI(this, live)
  private val jobs = new JobProgressUI(this, live)
  private val env = new EnvironmentUI(this, live)
  private val exec = new ExecutorsUI(this, live)

  // Add MetricsServlet handlers by default
  private val metricsServletHandlers = SparkEnv.get.metricsSystem.getServletHandlers

  private val allHandlers = storage.getHandlers ++ jobs.getHandlers ++ env.getHandlers ++
    exec.getHandlers ++ metricsServletHandlers ++ handlers

  // Maintain a gateway listener for all events to simplify event logging
  private var _gatewayListener: Option[GatewayUISparkListener] = None

  def gatewayListener = _gatewayListener.getOrElse {
    val gateway = new GatewayUISparkListener(live)
    _gatewayListener = Some(gateway)
    gateway
  }

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

    if (live) {
      // Listen for new events only if this UI is live
      sc.addSparkListener(gatewayListener)
    }
    storage.start()
    jobs.start()
    env.start()
    exec.start()
  }

  def stop() {
    server.foreach(_.stop())
    logInfo("Stopped Spark Web UI at %s".format(appUIAddress))
  }

  /**
   * Reconstruct a previously persisted SparkUI from logs residing in the given directory.
   * Return true if log files are found and processed.
   */
  def renderFromDisk(dirPath: String): Boolean = {
    // Live UI's should never invoke this
    assert(!live)

    // Check validity of the given path
    val logDir = new File(dirPath)
    if (!logDir.exists || !logDir.isDirectory) {
      logWarning("Given invalid log path %s when rendering persisted Spark Web UI!"
        .format(dirPath))
      return false
    }
    val logFiles = logDir.listFiles.filter(_.isFile)
    if (logFiles.size == 0) {
      logWarning("No logs found in given directory %s when rendering persisted Spark Web UI!"
        .format(dirPath))
      return false
    }

    // Replay events in each event log
    logFiles.foreach(processEventLog)
    true
  }

  /**
   * Replay each event in the order maintained in the given log to the gateway listener
   */
  private def processEventLog(file: File) = {
    val fileStream = new FileInputStream(file)
    val bufferedStream = new FastBufferedInputStream(fileStream)
    var currentLine = ""
    try {
      val lines = Source.fromInputStream(bufferedStream).getLines()
      lines.foreach { line =>
        currentLine = line
        val event = SparkListenerEvent.fromJson(parse(line))
        sc.dagScheduler.listenerBus.postToListeners(event, Seq(gatewayListener))
      }
    } catch {
      case e: Exception =>
        logWarning("Exception in parsing UI logs for %s".format(file.getAbsolutePath))
        logWarning(currentLine + "\n")
        logDebug(e.getMessage + e.getStackTraceString)
    }
    bufferedStream.close()
  }

  private[spark] def appUIAddress = host + ":" + boundPort.getOrElse("-1")

}

private[spark] object SparkUI {
  val DEFAULT_PORT = "4040"
  val DEFAULT_PERSISTED_PORT = "14040"
  val STATIC_RESOURCE_DIR = "org/apache/spark/ui/static"

  // Keep track of the port of the last persisted UI
  var lastPersistedUIPort: Option[Int] = None
}
