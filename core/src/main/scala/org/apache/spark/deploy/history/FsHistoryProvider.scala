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

import java.io.{IOException, BufferedInputStream, FileNotFoundException, InputStream}
import java.util.concurrent.{Executors, TimeUnit}

import scala.collection.mutable
import scala.concurrent.duration.Duration

import com.google.common.util.concurrent.ThreadFactoryBuilder

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.fs.permission.AccessControlException

import org.apache.spark.{Logging, SecurityManager, SparkConf}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.io.CompressionCodec
import org.apache.spark.scheduler._
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.Utils

/**
 * A class that provides application history from event logs stored in the file system.
 * This provider checks for new finished applications in the background periodically and
 * renders the history application UI by parsing the associated event logs.
 */
private[history] class FsHistoryProvider(conf: SparkConf) extends ApplicationHistoryProvider
  with Logging {

  import FsHistoryProvider._

  private val NOT_STARTED = "<Not Started>"

  // Interval between each check for event log updates
  private val UPDATE_INTERVAL_MS = conf.getOption("spark.history.fs.update.interval.seconds")
    .orElse(conf.getOption("spark.history.fs.updateInterval"))
    .orElse(conf.getOption("spark.history.updateInterval"))
    .map(_.toInt)
    .getOrElse(10) * 1000

  // Interval between each cleaner checks for event logs to delete
  private val CLEAN_INTERVAL_MS = conf.getLong("spark.history.fs.cleaner.interval.seconds",
    DEFAULT_SPARK_HISTORY_FS_CLEANER_INTERVAL_S) * 1000

  private val logDir = conf.getOption("spark.history.fs.logDirectory")
    .map { d => Utils.resolveURI(d).toString }
    .getOrElse(DEFAULT_LOG_DIR)

  private val fs = Utils.getHadoopFileSystem(logDir, SparkHadoopUtil.get.newConfiguration(conf))

  // Used by check event thread and clean log thread.
  // Scheduled thread pool size must be one, otherwise it will have concurrent issues about fs
  // and applications between check task and clean task.
  private val pool = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder()
    .setNameFormat("spark-history-task-%d").setDaemon(true).build())

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
   * Return a runnable that performs the given operation on the event logs.
   * This operation is expected to be executed periodically.
   */
  private def getRunner(operateFun: () => Unit): Runnable = {
    new Runnable() {
      override def run() = Utils.logUncaughtExceptions {
        operateFun()
      }
    }
  }

  initialize()

  private def initialize(): Unit = {
    // Validate the log directory.
    val path = new Path(logDir)
    if (!fs.exists(path)) {
      var msg = s"Log directory specified does not exist: $logDir."
      if (logDir == DEFAULT_LOG_DIR) {
        msg += " Did you configure the correct one through spark.fs.history.logDirectory?"
      }
      throw new IllegalArgumentException(msg)
    }
    if (!fs.getFileStatus(path).isDir) {
      throw new IllegalArgumentException(
        "Logging directory specified is not a directory: %s".format(logDir))
    }

    // Disable the background thread during tests.
    if (!conf.contains("spark.testing")) {
      // A task that periodically checks for event log updates on disk.
      pool.scheduleAtFixedRate(getRunner(checkForLogs), 0, UPDATE_INTERVAL_MS,
        TimeUnit.MILLISECONDS)

      if (conf.getBoolean("spark.history.fs.cleaner.enabled", false)) {
        // A task that periodically cleans event logs on disk.
        pool.scheduleAtFixedRate(getRunner(cleanLogs), 0, CLEAN_INTERVAL_MS,
          TimeUnit.MILLISECONDS)
      }
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
          SparkUI.createHistoryUI(conf, replayBus, appSecManager, appId,
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

  override def getConfig(): Map[String, String] = Map("Event log directory" -> logDir.toString)

  /**
   * Builds the application list based on the current contents of the log directory.
   * Tries to reuse as much of the data already in memory as possible, by not reading
   * applications that haven't been updated since last time the logs were checked.
   */
  private[history] def checkForLogs(): Unit = {
    try {
      var newLastModifiedTime = lastModifiedTime
      val statusList = Option(fs.listStatus(new Path(logDir))).map(_.toSeq)
        .getOrElse(Seq[FileStatus]())
      val logInfos = statusList
        .filter { entry =>
          try {
            getModificationTime(entry).map { time =>
              newLastModifiedTime = math.max(newLastModifiedTime, time)
              time >= lastModifiedTime
            }.getOrElse(false)
          } catch {
            case e: AccessControlException =>
              // Do not use "logInfo" since these messages can get pretty noisy if printed on
              // every poll.
              logDebug(s"No permission to read $entry, ignoring.")
              false
          }
        }
        .flatMap { entry =>
          try {
            Some(replay(entry, new ReplayListenerBus()))
          } catch {
            case e: Exception =>
              logError(s"Failed to load application log data from $entry.", e)
              None
          }
        }
        .sortWith(compareAppInfo)

      lastModifiedTime = newLastModifiedTime

      // When there are new logs, merge the new list with the existing one, maintaining
      // the expected ordering (descending end time). Maintaining the order is important
      // to avoid having to sort the list every time there is a request for the log list.
      if (!logInfos.isEmpty) {
        val newApps = new mutable.LinkedHashMap[String, FsApplicationHistoryInfo]()
        def addIfAbsent(info: FsApplicationHistoryInfo) = {
          if (!newApps.contains(info.id) ||
              newApps(info.id).logPath.endsWith(EventLoggingListener.IN_PROGRESS) &&
              !info.logPath.endsWith(EventLoggingListener.IN_PROGRESS)) {
            newApps += (info.id -> info)
          }
        }

        val newIterator = logInfos.iterator.buffered
        val oldIterator = applications.values.iterator.buffered
        while (newIterator.hasNext && oldIterator.hasNext) {
          if (compareAppInfo(newIterator.head, oldIterator.head)) {
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
      case e: Exception => logError("Exception in checking for event log updates", e)
    }
  }

  /**
   * Delete event logs from the log directory according to the clean policy defined by the user.
   */
  private def cleanLogs(): Unit = {
    try {
      val statusList = Option(fs.listStatus(new Path(logDir))).map(_.toSeq)
        .getOrElse(Seq[FileStatus]())
      val maxAge = conf.getLong("spark.history.fs.cleaner.maxAge.seconds",
        DEFAULT_SPARK_HISTORY_FS_MAXAGE_S) * 1000

      val now = System.currentTimeMillis()
      val appsToRetain = new mutable.LinkedHashMap[String, FsApplicationHistoryInfo]()

      applications.values.foreach { info =>
        if (now - info.lastUpdated <= maxAge) {
          appsToRetain += (info.id -> info)
        }
      }

      applications = appsToRetain

      // Scan all logs from the log directory.
      // Only directories older than the specified max age will be deleted
      statusList.foreach { dir =>
        try {
          if (now - dir.getModificationTime() > maxAge) {
            // if path is a directory and set to  true,
            // the directory is deleted else throws an exception
            fs.delete(dir.getPath, true)
          }
        } catch {
          case t: IOException => logError(s"IOException in cleaning logs of $dir", t)
        }
      }
    } catch {
      case t: Exception => logError("Exception in cleaning logs", t)
    }
  }

  /**
   * Comparison function that defines the sort order for the application listing.
   *
   * @return Whether `i1` should precede `i2`.
   */
  private def compareAppInfo(
      i1: FsApplicationHistoryInfo,
      i2: FsApplicationHistoryInfo): Boolean = {
    if (i1.endTime != i2.endTime) i1.endTime >= i2.endTime else i1.startTime >= i2.startTime
  }

  /**
   * Replays the events in the specified log file and returns information about the associated
   * application.
   */
  private def replay(eventLog: FileStatus, bus: ReplayListenerBus): FsApplicationHistoryInfo = {
    val logPath = eventLog.getPath()
    logInfo(s"Replaying log path: $logPath")
    val (logInput, sparkVersion) =
      if (isLegacyLogDirectory(eventLog)) {
        openLegacyEventLog(logPath)
      } else {
        EventLoggingListener.openEventLog(logPath, fs)
      }
    try {
      val appListener = new ApplicationEventListener
      bus.addListener(appListener)
      bus.replay(logInput, sparkVersion, logPath.toString)
      new FsApplicationHistoryInfo(
        logPath.getName(),
        appListener.appId.getOrElse(logPath.getName()),
        appListener.appName.getOrElse(NOT_STARTED),
        appListener.startTime.getOrElse(-1L),
        appListener.endTime.getOrElse(-1L),
        getModificationTime(eventLog).get,
        appListener.sparkUser.getOrElse(NOT_STARTED),
        isApplicationCompleted(eventLog))
    } finally {
      logInput.close()
    }
  }

  /**
   * Loads a legacy log directory. This assumes that the log directory contains a single event
   * log file (along with other metadata files), which is the case for directories generated by
   * the code in previous releases.
   *
   * @return 2-tuple of (input stream of the events, version of Spark which wrote the log)
   */
  private[history] def openLegacyEventLog(dir: Path): (InputStream, String) = {
    val children = fs.listStatus(dir)
    var eventLogPath: Path = null
    var codecName: Option[String] = None
    var sparkVersion: String = null

    children.foreach { child =>
      child.getPath().getName() match {
        case name if name.startsWith(LOG_PREFIX) =>
          eventLogPath = child.getPath()

        case codec if codec.startsWith(COMPRESSION_CODEC_PREFIX) =>
          codecName = Some(codec.substring(COMPRESSION_CODEC_PREFIX.length()))

        case version if version.startsWith(SPARK_VERSION_PREFIX) =>
          sparkVersion = version.substring(SPARK_VERSION_PREFIX.length())

        case _ =>
      }
    }

    if (eventLogPath == null || sparkVersion == null) {
      throw new IllegalArgumentException(s"$dir is not a Spark application log directory.")
    }

    val codec = try {
        codecName.map { c => CompressionCodec.createCodec(conf, c) }
      } catch {
        case e: Exception =>
          throw new IllegalArgumentException(s"Unknown compression codec $codecName.")
      }

    val in = new BufferedInputStream(fs.open(eventLogPath))
    (codec.map(_.compressedInputStream(in)).getOrElse(in), sparkVersion)
  }

  /**
   * Return whether the specified event log path contains a old directory-based event log.
   * Previously, the event log of an application comprises of multiple files in a directory.
   * As of Spark 1.3, these files are consolidated into a single one that replaces the directory.
   * See SPARK-2261 for more detail.
   */
  private def isLegacyLogDirectory(entry: FileStatus): Boolean = entry.isDir()

  /**
   * Returns the modification time of the given event log. If the status points at an empty
   * directory, `None` is returned, indicating that there isn't an event log at that location.
   */
  private def getModificationTime(fsEntry: FileStatus): Option[Long] = {
    if (isLegacyLogDirectory(fsEntry)) {
      val statusList = fs.listStatus(fsEntry.getPath)
      if (!statusList.isEmpty) Some(statusList.map(_.getModificationTime()).max) else None
    } else {
      Some(fsEntry.getModificationTime())
    }
  }

  /**
   * Return true when the application has completed.
   */
  private def isApplicationCompleted(entry: FileStatus): Boolean = {
    if (isLegacyLogDirectory(entry)) {
      fs.exists(new Path(entry.getPath(), APPLICATION_COMPLETE))
    } else {
      !entry.getPath().getName().endsWith(EventLoggingListener.IN_PROGRESS)
    }
  }

}

private object FsHistoryProvider {
  val DEFAULT_LOG_DIR = "file:/tmp/spark-events"

  // One day
  val DEFAULT_SPARK_HISTORY_FS_CLEANER_INTERVAL_S = Duration(1, TimeUnit.DAYS).toSeconds

  // One week
  val DEFAULT_SPARK_HISTORY_FS_MAXAGE_S = Duration(7, TimeUnit.DAYS).toSeconds
}

private class FsApplicationHistoryInfo(
    val logPath: String,
    id: String,
    name: String,
    startTime: Long,
    endTime: Long,
    lastUpdated: Long,
    sparkUser: String,
    completed: Boolean = true)
  extends ApplicationHistoryInfo(id, name, startTime, endTime, lastUpdated, sparkUser, completed)
