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
import org.apache.spark.ui.env.EnvironmentUI
import org.apache.spark.ui.exec.ExecutorsUI
import org.apache.spark.ui.storage.BlockManagerUI
import org.apache.spark.ui.jobs.JobProgressUI
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.util.{FileLogger, Utils}
import org.apache.spark.scheduler._
import org.apache.spark.storage.StorageStatus

import net.liftweb.json._
import net.liftweb.json.JsonAST.compactRender

import it.unimi.dsi.fastutil.io.FastBufferedInputStream

/** Top level user interface for Spark */
private[spark] class SparkUI(sc: SparkContext, fromDisk: Boolean = false) extends Logging {
  val host = Option(System.getenv("SPARK_PUBLIC_DNS")).getOrElse(Utils.localHostName())
  val port = if (!fromDisk) {
      sc.conf.get("spark.ui.port", SparkUI.DEFAULT_PORT).toInt
    } else {
      // While each context has only one live SparkUI, it can have many persisted ones
      // For persisted UI's, climb upwards from the configured / default port
      val p = SparkUI.lastPersistedPort.map(_ + 1)
        .getOrElse(sc.conf.get("spark.ui.persisted.port", SparkUI.DEFAULT_PERSISTED_PORT).toInt)
      SparkUI.lastPersistedPort = Some(p)
      p
    }
  var boundPort: Option[Int] = None
  var server: Option[Server] = None

  private val handlers = Seq[(String, Handler)](
    ("/static", createStaticHandler(SparkUI.STATIC_RESOURCE_DIR)),
    ("/", createRedirectHandler("/stages"))
  )
  private val storage = new BlockManagerUI(sc)
  private val jobs = new JobProgressUI(sc)
  private val env = new EnvironmentUI(sc)
  private val exec = new ExecutorsUI(sc)

  // Add MetricsServlet handlers by default
  private val metricsServletHandlers = SparkEnv.get.metricsSystem.getServletHandlers

  private val allHandlers = storage.getHandlers ++ jobs.getHandlers ++ env.getHandlers ++
    exec.getHandlers ++ metricsServletHandlers ++ handlers

  // Listeners are not ready until SparkUI has started
  private def listeners = Seq(storage.listener, jobs.listener, env.listener, exec.listener)

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
    logInfo("Stopped Spark Web UI at %s".format(appUIAddress))
  }

  /**
   * Reconstruct a SparkUI previously persisted from disk from the given path.
   * Return true if all required log files are found.
   */
  def renderFromDisk(dirPath: String): Boolean = {
    var success = true
    if (fromDisk) {
      val logDir = new File(dirPath)
      if (!logDir.exists || !logDir.isDirectory) {
        logWarning("Given invalid directory %s when rendering persisted Spark Web UI!"
          .format(dirPath))
        return false
      }
      listeners.map { listener =>
        val path = "%s/%s/".format(dirPath.stripSuffix("/"), listener.name)
        val dir = new File(path)
        if (dir.exists && dir.isDirectory) {
          val files = dir.listFiles
          Option(files).foreach { files => files.foreach(processPersistedEventLog(_, listener)) }
        } else {
          logWarning("%s not found when rendering persisted Spark Web UI!".format(path))
          success = false
        }
      }
    }
    success
  }

  /**
   * Register each event logged in the given file with the corresponding listener in order
   */
  private def processPersistedEventLog(file: File, listener: SparkListener) = {
    val fileStream = new FileInputStream(file)
    val bufferedStream = new FastBufferedInputStream(fileStream)
    var currentLine = ""
    try {
      val lines = Source.fromInputStream(bufferedStream).getLines()
      lines.foreach { line =>
        currentLine = line
        val listeners = Seq(listener)
        val event = SparkListenerEvent.fromJson(parse(line))
        sc.dagScheduler.listenerBus.postToListeners(event, listeners)
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
  var lastPersistedPort: Option[Int] = None
}

/** A SparkListener for logging events, one file per job */
private[spark] class UISparkListener(val name: String) extends SparkListener {
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
