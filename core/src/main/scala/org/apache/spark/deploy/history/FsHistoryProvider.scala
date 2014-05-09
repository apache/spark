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
import java.util.concurrent.atomic.AtomicReference

import scala.collection.mutable

import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.{Logging, SecurityManager, SparkConf}
import org.apache.spark.scheduler._
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.Utils

class FsHistoryProvider(conf: SparkConf) extends ApplicationHistoryProvider
  with Logging {

  // Interval between each check for event log updates
  private val UPDATE_INTERVAL_MS = conf.getInt("spark.history.fs.updateInterval", 10) * 1000

  private val logDir = conf.get("spark.history.fs.logDirectory")
  private val fs = Utils.getHadoopFileSystem(logDir)

  // A timestamp of when the disk was last accessed to check for log updates
  private var lastLogCheckTime = -1L

  // List of applications, in order from newest to oldest.
  private val appList = new AtomicReference[Seq[ApplicationHistoryInfo]](Nil)

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
        val now = System.currentTimeMillis
        if (now - lastLogCheckTime > UPDATE_INTERVAL_MS) {
          Thread.sleep(UPDATE_INTERVAL_MS)
        } else {
          // If the user has manually checked for logs recently, wait until
          // UPDATE_INTERVAL_MS after the last check time
          Thread.sleep(lastLogCheckTime + UPDATE_INTERVAL_MS - now)
        }
        checkForLogs()
      }
    }
  }

  initialize()

  private def initialize() {
    // Validate the log directory.
    val path = new Path(logDir)
    if (!fs.exists(path)) {
      throw new IllegalArgumentException(
        "Logging directory specified does not exist: %s".format(logDir))
    }
    if (!fs.getFileStatus(path).isDir) {
      throw new IllegalArgumentException(
        "Logging directory specified is not a directory: %s".format(logDir))
    }

    checkForLogs()
    logCheckingThread.setDaemon(true)
    logCheckingThread.start()
  }

  override def getListing(offset: Int, count: Int) = {
    val list = appList.get()
    val theOffset = if (offset < list.size) offset else 0
    (list.slice(theOffset, Math.min(theOffset + count, list.size)), theOffset, list.size)
  }

  override def getAppInfo(appId: String): ApplicationHistoryInfo = {
    try {
      val appLogDir = fs.getFileStatus(new Path(logDir, appId))
      loadAppInfo(appLogDir, true)
    } catch {
      case e: FileNotFoundException => null
    }
  }

  /**
   * Check for any updates to event logs in the base directory. This is only effective once
   * the server has been bound.
   *
   * If a new completed application is found, the server renders the associated SparkUI
   * from the application's event logs, attaches this UI to itself, and stores metadata
   * information for this application.
   *
   * If the logs for an existing completed application are no longer found, the server
   * removes all associated information and detaches the SparkUI.
   */
  def checkForLogs() = synchronized {
    lastLogCheckTime = System.currentTimeMillis
    logDebug("Checking for logs. Time is now %d.".format(lastLogCheckTime))
    try {
      val logStatus = fs.listStatus(new Path(logDir))
      val logDirs = if (logStatus != null) logStatus.filter(_.isDir).toSeq else Seq[FileStatus]()
      val logInfos = logDirs
        .sortBy { dir => getModificationTime(dir) }
        .filter {
            dir => fs.isFile(new Path(dir.getPath(), EventLoggingListener.APPLICATION_COMPLETE))
          }

      var currentApps = Map[String, ApplicationHistoryInfo](
        appList.get().map(app => (app.id -> app)):_*)

      // For any application that either (i) is not listed or (ii) has changed since the last time
      // the listing was created (defined by the log dir's modification time), load the app's info.
      // Otherwise just reuse what's already in memory.
      val newApps = new mutable.ListBuffer[ApplicationHistoryInfo]
      for (dir <- logInfos) {
        val curr = currentApps.getOrElse(dir.getPath().getName(), null)
        if (curr == null || curr.lastUpdated < getModificationTime(dir)) {
          try {
            newApps += loadAppInfo(dir, false)
          } catch {
            case e: Exception => logError(s"Failed to load app info from directory $dir.")
          }
        } else {
          newApps += curr
        }
      }

      appList.set(newApps.sortBy { info => -info.lastUpdated })
    } catch {
      case t: Throwable => logError("Exception in checking for event log updates", t)
    }
  }

  /**
   * Parse the application's logs to find out the information we need to build the
   * listing page.
   */
  private def loadAppInfo(logDir: FileStatus, renderUI: Boolean): ApplicationHistoryInfo = {
    val elogInfo = EventLoggingListener.parseLoggingInfo(logDir.getPath(), fs)
    val path = logDir.getPath
    val appId = path.getName
    val replayBus = new ReplayListenerBus(elogInfo.logPaths, fs, elogInfo.compressionCodec)
    val appListener = new ApplicationEventListener
    replayBus.addListener(appListener)

    val ui: SparkUI = if (renderUI) {
        val conf = this.conf.clone()
        val appSecManager = new SecurityManager(conf)
        new SparkUI(conf, appSecManager, replayBus, appId, "/history/" + appId)
        // Do not call ui.bind() to avoid creating a new server for each application
      } else {
        null
      }

    replayBus.replay()
    val appName = appListener.appName
    val sparkUser = appListener.sparkUser
    val startTime = appListener.startTime
    val endTime = appListener.endTime
    val lastUpdated = getModificationTime(logDir)
    ApplicationHistoryInfo(appId,
      appListener.appName,
      appListener.startTime,
      appListener.endTime,
      getModificationTime(logDir),
      appListener.sparkUser,
      if (renderUI) appListener.viewAcls else null,
      ui)
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

}
