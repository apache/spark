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

import java.io.{BufferedInputStream, FileNotFoundException, InputStream}

import scala.collection.mutable

import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.{Logging, SecurityManager, SparkConf}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.io.CompressionCodec
import org.apache.spark.scheduler._
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.Utils

private[history] class FsHistoryProvider(conf: SparkConf) extends ApplicationHistoryProvider
  with Logging {

  private val NOT_STARTED = "<Not Started>"

  // Interval between each check for event log updates
  private val UPDATE_INTERVAL_MS = conf.getInt("spark.history.fs.updateInterval",
    conf.getInt("spark.history.updateInterval", 10)) * 1000

  private val logDir = conf.get("spark.history.fs.logDirectory", null)
  private val resolvedLogDir = Option(logDir)
    .map { d => Utils.resolveURI(d) }
    .getOrElse { throw new IllegalArgumentException("Logging directory must be specified.") }

  private val fs = Utils.getHadoopFileSystem(resolvedLogDir,
    SparkHadoopUtil.get.newConfiguration(conf))

  // A timestamp of when the disk was last accessed to check for log updates
  private var lastLogCheckTimeMs = -1L

  // The modification time of the newest log detected during the last scan. This is used
  // to ignore logs that are older during subsequent scans, to avoid processing data that
  // is already known.
  private var lastModifiedTime = -1L

  // Mapping of application IDs to their metadata, in descending end time order. Apps are inserted
  // into the map in order, so the LinkedHashMap maintains the correct ordering.
  @volatile private var applications: mutable.LinkedHashMap[String, FsApplicationHistoryInfo]
    = new mutable.LinkedHashMap()

  // Constants used to parse Spark 1.0.0 log directories.
  private[history] val LOG_PREFIX = "EVENT_LOG_"
  private[history] val SPARK_VERSION_PREFIX = "SPARK_VERSION_"
  private[history] val COMPRESSION_CODEC_PREFIX = "COMPRESSION_CODEC_"
  private[history] val APPLICATION_COMPLETE = "APPLICATION_COMPLETE"

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

    // Disable the background thread during tests.
    if (!conf.contains("spark.testing")) {
      logCheckingThread.setDaemon(true)
      logCheckingThread.start()
    }
  }

  override def getListing() = applications.values

  override def getAppUI(appId: String): Option[SparkUI] = {
    try {
      applications.get(appId).map { info =>
        val replayBus = new ReplayListenerBus()
        val ui = {
          val conf = this.conf.clone()
          val appSecManager = new SecurityManager(conf)
          new SparkUI(conf, appSecManager, replayBus, appId,
            s"${HistoryServer.UI_PATH_PREFIX}/$appId")
          // Do not call ui.bind() to avoid creating a new server for each application
        }

        val appListener = new ApplicationEventListener()
        replayBus.addListener(appListener)
        val appInfo = replay(fs.getFileStatus(new Path(logDir, info.logPath)), replayBus)

        ui.setAppName(s"${appInfo.name} ($appId)")

        val uiAclsEnabled = conf.getBoolean("spark.history.ui.acls.enable", false)
        ui.getSecurityManager.setAcls(uiAclsEnabled)
        // make sure to set admin acls before view acls so they are properly picked up
        ui.getSecurityManager.setAdminAcls(appListener.adminAcls.getOrElse(""))
        ui.getSecurityManager.setViewAcls(appInfo.sparkUser,
          appListener.viewAcls.getOrElse(""))
        ui
      }
    } catch {
      case e: FileNotFoundException => None
    }
  }

  override def getConfig(): Map[String, String] =
    Map("Event Log Location" -> resolvedLogDir.toString)

  /**
   * Builds the application list based on the current contents of the log directory.
   * Tries to reuse as much of the data already in memory as possible, by not reading
   * applications that haven't been updated since last time the logs were checked.
   */
  private[history] def checkForLogs() = {
    lastLogCheckTimeMs = getMonotonicTimeMs()
    logDebug("Checking for logs. Time is now %d.".format(lastLogCheckTimeMs))

    try {
      var newLastModifiedTime = lastModifiedTime
      val logInfos = fs.listStatus(new Path(logDir))
        .filter { entry =>
          val isFinishedApplication =
            if (isLegacyLogDirectory(entry)) {
              fs.exists(new Path(entry.getPath(), APPLICATION_COMPLETE))
            } else {
              !entry.getPath().getName().endsWith(EventLoggingListener.IN_PROGRESS)
            }

          if (isFinishedApplication) {
            val modTime = getModificationTime(entry)
            newLastModifiedTime = math.max(newLastModifiedTime, modTime)
            modTime >= lastModifiedTime
          } else {
            false
          }
        }
        .flatMap { entry =>
          try {
            Some(replay(entry, new ReplayListenerBus()))
          } catch {
            case e: Exception =>
              logInfo(s"Failed to load application log data from $entry.", e)
              None
          }
        }
        .sortBy { info => -info.endTime }

      lastModifiedTime = newLastModifiedTime

      // When there are new logs, merge the new list with the existing one, maintaining
      // the expected ordering (descending end time). Maintaining the order is important
      // to avoid having to sort the list every time there is a request for the log list.
      if (!logInfos.isEmpty) {
        val newApps = new mutable.LinkedHashMap[String, FsApplicationHistoryInfo]()
        def addIfAbsent(info: FsApplicationHistoryInfo) = {
          if (!newApps.contains(info.id)) {
            newApps += (info.id -> info)
          }
        }

        val newIterator = logInfos.iterator.buffered
        val oldIterator = applications.values.iterator.buffered
        while (newIterator.hasNext && oldIterator.hasNext) {
          if (newIterator.head.endTime > oldIterator.head.endTime) {
            addIfAbsent(newIterator.next)
          } else {
            addIfAbsent(oldIterator.next)
          }
        }
        newIterator.foreach(addIfAbsent)
        oldIterator.foreach(addIfAbsent)

        applications = newApps
      }
    } catch {
      case t: Throwable => logError("Exception in checking for event log updates", t)
    }
  }

  /**
   * Replays the event data in the given log, and returns the application information.
   */
  private def replay(logPath: FileStatus, bus: ReplayListenerBus): FsApplicationHistoryInfo = {
    val (logInput, sparkVersion) =
      if (isLegacyLogDirectory(logPath)) {
        openOldLog(logPath.getPath())
      } else {
        EventLoggingListener.openEventLog(logPath.getPath(), fs)
      }
    try {
      val appListener = new ApplicationEventListener
      bus.addListener(appListener)
      bus.replay(logInput, sparkVersion)
      new FsApplicationHistoryInfo(
        logPath.getPath().getName(),
        appListener.appId.getOrElse(logPath.getPath().getName()),
        appListener.appName.getOrElse(NOT_STARTED),
        appListener.startTime.getOrElse(-1L),
        appListener.endTime.getOrElse(-1L),
        getModificationTime(logPath),
        appListener.sparkUser.getOrElse(NOT_STARTED))
    } finally {
      logInput.close()
    }
  }

  /**
   * Load the a legacy log directory. This assumes that the log directory contains a single event
   * log file, which is the case for directories generated by the code in previous releases.
   */
  private[history] def openOldLog(dir: Path): (InputStream, String) = {
    val children = fs.listStatus(dir)
    var eventLogPath: Path = null
    var codecName: String = null
    var sparkVersion: String = null

    children.foreach { child =>
      child.getPath().getName() match {
        case name if name.startsWith(LOG_PREFIX) =>
          eventLogPath = child.getPath()

        case codec if codec.startsWith(COMPRESSION_CODEC_PREFIX) =>
          codecName = codec.substring(COMPRESSION_CODEC_PREFIX.length())

        case version if version.startsWith(SPARK_VERSION_PREFIX) =>
          sparkVersion = version.substring(SPARK_VERSION_PREFIX.length())

        case _ =>
      }
    }

    val codec = try {
        if (codecName != null) {
          Some(CompressionCodec.createCodec(conf, codecName))
        } else None
      } catch {
        case e: Exception =>
          throw new IllegalArgumentException(s"Unknown compression codec $codecName.")
      }

    if (eventLogPath == null || sparkVersion == null) {
      throw new IllegalArgumentException(s"$dir is not a Spark application log directory.")
    }

    val in = new BufferedInputStream(fs.open(eventLogPath))
    (codec.map(_.compressedInputStream(in)).getOrElse(in), sparkVersion)
  }

  private def isLegacyLogDirectory(entry: FileStatus) = entry.isDir()

  private def getModificationTime(fsEntry: FileStatus) = {
    if (fsEntry.isDir) {
      fs.listStatus(fsEntry.getPath).map(_.getModificationTime()).max
    } else {
      fsEntry.getModificationTime()
    }
  }

  /** Returns the system's mononotically increasing time. */
  private def getMonotonicTimeMs() = System.nanoTime() / (1000 * 1000)

}

private class FsApplicationHistoryInfo(
    val logPath: String,
    id: String,
    name: String,
    startTime: Long,
    endTime: Long,
    lastUpdated: Long,
    sparkUser: String)
  extends ApplicationHistoryInfo(id, name, startTime, endTime, lastUpdated, sparkUser)
