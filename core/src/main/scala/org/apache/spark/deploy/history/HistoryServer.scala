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

package org.apache.spark.deploy.history

import java.net.URI
import javax.servlet.http.HttpServletRequest

import scala.collection.mutable

import org.apache.hadoop.fs.{FileStatus, Path}
import org.eclipse.jetty.servlet.ServletContextHandler

import org.apache.spark.{Logging, SecurityManager, SparkConf}
import org.apache.spark.deploy.SparkUIContainer
import org.apache.spark.ui.SparkUI
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.util.Utils
import org.apache.spark.scheduler.{ApplicationListener, ReplayListenerBus}

/**
 * A web server that re-renders SparkUIs of finished applications.
 *
 * For the standalone mode, MasterWebUI already achieves this functionality. Thus, the
 * main use case of the HistoryServer is in other deploy modes (e.g. Yarn or Mesos).
 *
 * The logging directory structure is as follows: Within the given base directory, each
 * application's event logs are maintained in the application's own sub-directory.
 *
 * @param baseLogDir The base directory in which event logs are found
 * @param requestedPort The requested port to which this server is to be bound
 */
class HistoryServer(val baseLogDir: String, requestedPort: Int, conf: SparkConf)
  extends SparkUIContainer("History Server") with Logging {

  private val host = Option(System.getenv("SPARK_PUBLIC_DNS")).getOrElse(Utils.localHostName())
  private val port = requestedPort
  private val indexPage = new IndexPage(this)
  private val fileSystem = Utils.getHadoopFileSystem(new URI(baseLogDir))
  private val securityManager = new SecurityManager(conf)

  private val handlers = Seq[ServletContextHandler](
    createStaticHandler(HistoryServer.STATIC_RESOURCE_DIR, "/static"),
    createServletHandler("/",
      (request: HttpServletRequest) => indexPage.render(request), securityMgr = securityManager)
  )

  val appIdToInfo = mutable.HashMap[String, ApplicationHistoryInfo]()

  /** Bind to the HTTP server behind this web interface */
  override def bind() {
    try {
      serverInfo = Some(startJettyServer(host, port, handlers, conf))
      logInfo("Started HistoryServer at http://%s:%d".format(host, boundPort))
    } catch {
      case e: Exception =>
        logError("Failed to create HistoryServer", e)
        System.exit(1)
    }
    checkForLogs()
  }

  /** Parse app ID from the given log path. */
  def getAppId(logPath: String): String = logPath.split("/").last

  /** Return the address of this server. */
  def getAddress = "http://" + host + ":" + boundPort

  /**
   * Check for any updated event logs.
   *
   * If a new application is found, render the associated SparkUI and remember it.
   * If an existing application is updated, re-render the associated SparkUI.
   * If an existing application is removed, remove the associated SparkUI.
   */
  def checkForLogs() {
    val logStatus = fileSystem.listStatus(new Path(baseLogDir))
    val logDirs = if (logStatus != null) logStatus.filter(_.isDir).toSeq else Seq[FileStatus]()

    // Render any missing or outdated SparkUI
    logDirs.foreach { dir =>
      val path = dir.getPath.toString
      val appId = getAppId(path)
      val lastUpdated = {
        val logFiles = fileSystem.listStatus(dir.getPath)
        if (logFiles != null) logFiles.map(_.getModificationTime).max else dir.getModificationTime
      }
      if (!appIdToInfo.contains(appId) || appIdToInfo(appId).lastUpdated < lastUpdated) {
        maybeRenderUI(appId, path, lastUpdated)
      }
    }

    // Remove any outdated SparkUIs
    val appIds = logDirs.map { dir => getAppId(dir.getPath.toString) }
    appIdToInfo.foreach { case (appId, info) =>
      if (!appIds.contains(appId)) {
        detachUI(info.ui)
        appIdToInfo.remove(appId)
      }
    }
  }

  /** Attempt to render a new SparkUI from event logs residing in the given log directory. */
  private def maybeRenderUI(appId: String, logPath: String, lastUpdated: Long) {
    val replayBus = new ReplayListenerBus(conf)
    val appListener = new ApplicationListener
    replayBus.addListener(appListener)
    val ui = new SparkUI(conf, replayBus, appId, "/history/%s".format(appId))

    // Do not call ui.bind() to avoid creating a new server for each application
    ui.start()
    val success = replayBus.replay(logPath)
    if (success) {
      attachUI(ui)
      if (!appListener.started) {
        logWarning("Application has event logs but has not started: %s".format(appId))
      }
      val appName = appListener.appName
      val startTime = appListener.startTime
      val endTime = appListener.endTime
      val info = ApplicationHistoryInfo(appName, startTime, endTime, lastUpdated, logPath, ui)

      // If the UI already exists, terminate it and replace it
      appIdToInfo.remove(appId).foreach { info => detachUI(info.ui) }
      appIdToInfo(appId) = info

      // Use mnemonic original app name rather than app ID
      val originalAppName = "%s (history)".format(appName)
      ui.setAppName(originalAppName)
    }
  }

}

object HistoryServer {
  val STATIC_RESOURCE_DIR = SparkUI.STATIC_RESOURCE_DIR

  def main(argStrings: Array[String]) {
    val conf = new SparkConf
    val args = new HistoryServerArguments(argStrings, conf)
    val server = new HistoryServer(args.logDir, args.port, conf)
    server.bind()

    // Wait until the end of the world... or if the HistoryServer process is manually stopped
    while(true) { Thread.sleep(Int.MaxValue) }
  }
}

private[spark] case class ApplicationHistoryInfo(
    name: String,
    startTime: Long,
    endTime: Long,
    lastUpdated: Long,
    logPath: String,
    ui: SparkUI) {
  def started = startTime != -1
  def finished = endTime != -1
}
