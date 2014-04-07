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

import javax.servlet.http.HttpServletRequest

import scala.collection.mutable
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global

import org.apache.hadoop.fs.{FileStatus, Path}
import org.eclipse.jetty.servlet.ServletContextHandler

import org.apache.spark.{Logging, SecurityManager, SparkConf}
import org.apache.spark.deploy.SparkUIContainer
import org.apache.spark.ui.SparkUI
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.util.Utils
import org.apache.spark.scheduler.{ApplicationEventListener, ReplayListenerBus}

/**
 * A web server that renders SparkUIs of finished applications.
 *
 * For the standalone mode, MasterWebUI already achieves this functionality. Thus, the
 * main use case of the HistoryServer is in other deploy modes (e.g. Yarn or Mesos).
 *
 * The logging directory structure is as follows: Within the given base directory, each
 * application's event logs are maintained in the application's own sub-directory. This
 * is the same structure as maintained in the event log write code path in
 * EventLoggingListener.
 *
 * @param baseLogDir The base directory in which event logs are found
 * @param requestedPort The requested port to which this server is to be bound
 */
class HistoryServer(val baseLogDir: String, requestedPort: Int)
  extends SparkUIContainer("History Server") with Logging {

  private val fileSystem = Utils.getHadoopFileSystem(baseLogDir)
  private val bindHost = Utils.localHostName()
  private val publicHost = Option(System.getenv("SPARK_PUBLIC_DNS")).getOrElse(bindHost)
  private val port = requestedPort
  private val conf = new SparkConf
  private val securityManager = new SecurityManager(conf)
  private val indexPage = new IndexPage(this)

  // A timestamp of when the disk was last accessed to check for log updates
  private var lastLogCheck = -1L

  private val handlers = Seq[ServletContextHandler](
    createStaticHandler(HistoryServer.STATIC_RESOURCE_DIR, "/static"),
    createServletHandler("/",
      (request: HttpServletRequest) => indexPage.render(request), securityMgr = securityManager)
  )

  val appIdToInfo = mutable.HashMap[String, ApplicationHistoryInfo]()

  /** Bind to the HTTP server behind this web interface */
  override def bind() {
    try {
      serverInfo = Some(startJettyServer(bindHost, port, handlers, conf))
      logInfo("Started HistoryServer at http://%s:%d".format(publicHost, boundPort))
    } catch {
      case e: Exception =>
        logError("Failed to bind HistoryServer", e)
        System.exit(1)
    }
    checkForLogs()
  }

  /**
   * Asynchronously check for any updates to event logs in the base directory.
   *
   * If a new finished application is found, the server renders the associated SparkUI
   * from the application's event logs, attaches this UI to itself, and stores metadata
   * information for this application.
   *
   * If the logs for an existing finished application are no longer found, the server
   * removes all associated information and detaches the SparkUI.
   */
  def checkForLogs() {
    if (logCheckReady) {
      lastLogCheck = System.currentTimeMillis
      val asyncCheck = future {
        val logStatus = fileSystem.listStatus(new Path(baseLogDir))
        val logDirs = if (logStatus != null) logStatus.filter(_.isDir).toSeq else Seq[FileStatus]()

        // Render SparkUI for any new completed applications
        logDirs.foreach { dir =>
          val path = dir.getPath.toString
          val appId = getAppId(path)
          val lastUpdated = getModificationTime(dir)
          if (!appIdToInfo.contains(appId)) {
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
      asyncCheck.onFailure { case t =>
        logError("Unable to synchronize HistoryServer with files on disk: ", t)
      }
    }
  }

  /**
   * Render a new SparkUI from the event logs if the associated application is finished.
   *
   * HistoryServer looks for a special file that indicates application completion in the given
   * directory. If this file exists, the associated application is regarded to be complete, in
   * which case the server proceeds to render the SparkUI. Otherwise, the server does nothing.
   */
  private def maybeRenderUI(appId: String, logPath: String, lastUpdated: Long) {
    val replayBus = new ReplayListenerBus(logPath, fileSystem)
    replayBus.start()

    // If the application completion file is found
    if (replayBus.isApplicationComplete) {
      val ui = new SparkUI(replayBus, appId, "/history/%s".format(appId))
      val appListener = new ApplicationEventListener
      replayBus.addListener(appListener)

      // Do not call ui.bind() to avoid creating a new server for each application
      ui.start()
      val success = replayBus.replay()
      if (success) {
        attachUI(ui)
        val appName = if (appListener.applicationStarted) appListener.appName else appId
        ui.setAppName("%s (finished)".format(appName))
        val startTime = appListener.startTime
        val endTime = appListener.endTime
        val info = ApplicationHistoryInfo(appName, startTime, endTime, lastUpdated, logPath, ui)
        appIdToInfo(appId) = info
      }
    } else {
      logWarning("Skipping incomplete application: %s".format(logPath))
    }
  }

  /** Stop the server and close the file system. */
  override def stop() {
    super.stop()
    fileSystem.close()
  }

  /** Parse app ID from the given log path. */
  def getAppId(logPath: String): String = logPath.split("/").last

  /** Return the address of this server. */
  def getAddress = "http://" + publicHost + ":" + boundPort

  /** Return when this directory was last modified. */
  private def getModificationTime(dir: FileStatus): Long = {
    val logFiles = fileSystem.listStatus(dir.getPath)
    if (logFiles != null) {
      logFiles.map(_.getModificationTime).max
    } else {
      dir.getModificationTime
    }
  }

  /** Return whether the last log check has happened sufficiently long ago. */
  private def logCheckReady: Boolean = {
    System.currentTimeMillis - lastLogCheck > HistoryServer.UPDATE_INTERVAL_SECONDS * 1000
  }
}

/**
 * The recommended way of starting and stopping a HistoryServer is through the scripts
 * start-history-server.sh and stop-history-server.sh. The path to a base log directory
 * is must be specified, while the requested UI port is optional. For example:
 *
 *   ./sbin/spark-history-server.sh /tmp/spark-events 18080
 *   ./sbin/spark-history-server.sh hdfs://1.2.3.4:9000/spark-events
 *
 * This launches the HistoryServer as a Spark daemon.
 */
object HistoryServer {
  val STATIC_RESOURCE_DIR = SparkUI.STATIC_RESOURCE_DIR

  // Minimum interval between each check for logs, which requires a disk access
  val UPDATE_INTERVAL_SECONDS = 5

  def main(argStrings: Array[String]) {
    val args = new HistoryServerArguments(argStrings)
    val server = new HistoryServer(args.logDir, args.port)
    server.bind()

    // Wait until the end of the world... or if the HistoryServer process is manually stopped
    while(true) { Thread.sleep(Int.MaxValue) }
    server.stop()
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
