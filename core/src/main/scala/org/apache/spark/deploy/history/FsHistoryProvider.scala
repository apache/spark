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

import java.io.FileNotFoundException

import scala.collection.mutable

import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.{Logging, SecurityManager, SparkConf}
import org.apache.spark.scheduler._
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.Utils

private[history] class FsHistoryProvider(conf: SparkConf) extends ApplicationHistoryProvider
  with Logging {

  // Interval between each check for event log updates
  private val UPDATE_INTERVAL_MS = conf.getInt("spark.history.fs.updateInterval",
    conf.getInt("spark.history.updateInterval", 10)) * 1000

  private val logDir = conf.get("spark.history.fs.logDirectory", null)
  private val resolvedLogDir = Option(logDir)
    .map { d => Utils.resolveURI(d) }
    .getOrElse { throw new IllegalArgumentException("Logging directory must be specified.") }

  private val fs = Utils.getHadoopFileSystem(resolvedLogDir)

  // A timestamp of when the disk was last accessed to check for log updates
  private var lastLogCheckTimeMs = -1L

  // List of applications, in order from newest to oldest.
  @volatile private var appList: Seq[ApplicationHistoryInfo] = Nil

  /**
   * A background thread that periodically checks for event log updates on disk.
   *
   * If a log check is invoked manually in the middle of a period, this thread re-adjusts the
   * time at which it performs the next log check to maintain the same period as before.
   *
   * TODO: Add a mechanism to update manually.
   */
  private val logCheckingThread = new Thread("LogCheckingThread") {
    override def run() = Utils.logUncaughtExceptions {
      while (true) {
        val now = getMonotonicTimeMs()
        if (now - lastLogCheckTimeMs > UPDATE_INTERVAL_MS) {
          Thread.sleep(UPDATE_INTERVAL_MS)
        } else {
          // If the user has manually checked for logs recently, wait until
          // UPDATE_INTERVAL_MS after the last check time
          Thread.sleep(lastLogCheckTimeMs + UPDATE_INTERVAL_MS - now)
        }
        checkForLogs()
      }
    }
  }

  initialize()

  private def initialize() {
    // Validate the log directory.
    val path = new Path(resolvedLogDir)
    if (!fs.exists(path)) {
      throw new IllegalArgumentException(
        "Logging directory specified does not exist: %s".format(resolvedLogDir))
    }
    if (!fs.getFileStatus(path).isDir) {
      throw new IllegalArgumentException(
        "Logging directory specified is not a directory: %s".format(resolvedLogDir))
    }

    checkForLogs()
    logCheckingThread.setDaemon(true)
    logCheckingThread.start()
  }

  override def getListing() = appList

  override def getAppUI(appId: String): SparkUI = {
    try {
      val appLogDir = fs.getFileStatus(new Path(resolvedLogDir.toString, appId))
      val (_, ui) = loadAppInfo(appLogDir, renderUI = true)
      ui
    } catch {
      case e: FileNotFoundException => null
    }
  }

  override def getConfig(): Map[String, String] =
    Map("Event Log Location" -> resolvedLogDir.toString)

  /**
   * Builds the application list based on the current contents of the log directory.
   * Tries to reuse as much of the data already in memory as possible, by not reading
   * applications that haven't been updated since last time the logs were checked.
   */
  private def checkForLogs() = {
    lastLogCheckTimeMs = getMonotonicTimeMs()
    logDebug("Checking for logs. Time is now %d.".format(lastLogCheckTimeMs))
    try {
      val logStatus = fs.listStatus(new Path(resolvedLogDir))
      val logDirs = if (logStatus != null) logStatus.filter(_.isDir).toSeq else Seq[FileStatus]()
      val logInfos = logDirs.filter { dir =>
        fs.isFile(new Path(dir.getPath, EventLoggingListener.APPLICATION_COMPLETE))
      }

      val currentApps = Map[String, ApplicationHistoryInfo](
        appList.map(app => app.id -> app):_*)

      // For any application that either (i) is not listed or (ii) has changed since the last time
      // the listing was created (defined by the log dir's modification time), load the app's info.
      // Otherwise just reuse what's already in memory.
      val newApps = new mutable.ArrayBuffer[ApplicationHistoryInfo](logInfos.size)
      for (dir <- logInfos) {
        val curr = currentApps.getOrElse(dir.getPath().getName(), null)
        if (curr == null || curr.lastUpdated < getModificationTime(dir)) {
          try {
            val (app, _) = loadAppInfo(dir, renderUI = false)
            newApps += app
          } catch {
            case e: Exception => logError(s"Failed to load app info from directory $dir.")
          }
        } else {
          newApps += curr
        }
      }

      appList = newApps.sortBy { info => -info.endTime }
    } catch {
      case t: Throwable => logError("Exception in checking for event log updates", t)
    }
  }

  /**
   * Parse the application's logs to find out the information we need to build the
   * listing page.
   *
   * When creating the listing of available apps, there is no need to load the whole UI for the
   * application. The UI is requested by the HistoryServer (by calling getAppInfo()) when the user
   * clicks on a specific application.
   *
   * @param logDir Directory with application's log files.
   * @param renderUI Whether to create the SparkUI for the application.
   * @return A 2-tuple `(app info, ui)`. `ui` will be null if `renderUI` is false.
   */
  private def loadAppInfo(logDir: FileStatus, renderUI: Boolean) = {
    val path = logDir.getPath
    val appId = path.getName
    val elogInfo = EventLoggingListener.parseLoggingInfo(path, fs)
    val replayBus = new ReplayListenerBus(elogInfo.logPaths, fs, elogInfo.compressionCodec)
    val appListener = new ApplicationEventListener
    replayBus.addListener(appListener)

    val ui: SparkUI = if (renderUI) {
        val conf = this.conf.clone()
        val appSecManager = new SecurityManager(conf)
        new SparkUI(conf, appSecManager, replayBus, appId,
          HistoryServer.UI_PATH_PREFIX + s"/$appId")
        // Do not call ui.bind() to avoid creating a new server for each application
      } else {
        null
      }

    replayBus.replay()
    val appInfo = ApplicationHistoryInfo(
      appId,
      appListener.appName,
      appListener.startTime,
      appListener.endTime,
      getModificationTime(logDir),
      appListener.sparkUser)

    if (ui != null) {
      val uiAclsEnabled = conf.getBoolean("spark.history.ui.acls.enable", false)
      ui.getSecurityManager.setUIAcls(uiAclsEnabled)
      ui.getSecurityManager.setViewAcls(appListener.sparkUser, appListener.viewAcls)
    }
    (appInfo, ui)
  }

  /** Return when this directory was last modified. */
  private def getModificationTime(dir: FileStatus): Long = {
    try {
      val logFiles = fs.listStatus(dir.getPath)
      if (logFiles != null && !logFiles.isEmpty) {
        logFiles.map(_.getModificationTime).max
      } else {
        dir.getModificationTime
      }
    } catch {
      case t: Throwable =>
        logError("Exception in accessing modification time of %s".format(dir.getPath), t)
        -1L
    }
  }

  /** Returns the system's mononotically increasing time. */
  private def getMonotonicTimeMs() = System.nanoTime() / (1000 * 1000)

}
