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

import java.io.{BufferedInputStream, FileNotFoundException, IOException, InputStream}
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

import scala.collection.mutable

import com.google.common.util.concurrent.{MoreExecutors, ThreadFactoryBuilder}
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.fs.permission.AccessControlException

import org.apache.spark.{Logging, SecurityManager, SparkConf}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.io.CompressionCodec
import org.apache.spark.scheduler._
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.{Clock, SystemClock, ThreadUtils, Utils}

/**
 * A class that provides application history from event logs stored in the file system.
 * This provider checks for new finished applications in the background periodically and
 * renders the history application UI by parsing the associated event logs.
 */
private[history] class FsHistoryProvider(conf: SparkConf, clock: Clock)
  extends ApplicationHistoryProvider with Logging {

  def this(conf: SparkConf) = {
    this(conf, new SystemClock())
  }

  import FsHistoryProvider._

  private val NOT_STARTED = "<Not Started>"

  // Interval between each check for event log updates
  private val UPDATE_INTERVAL_S = conf.getTimeAsSeconds("spark.history.fs.update.interval", "10s")

  // Interval between each cleaner checks for event logs to delete
  private val CLEAN_INTERVAL_S = conf.getTimeAsSeconds("spark.history.fs.cleaner.interval", "1d")

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

  // List of application logs to be deleted by event log cleaner.
  private var attemptsToClean = new mutable.ListBuffer[FsApplicationAttemptInfo]

  /**
   * Return a runnable that performs the given operation on the event logs.
   * This operation is expected to be executed periodically.
   */
  private def getRunner(operateFun: () => Unit): Runnable = {
    new Runnable() {
      override def run(): Unit = Utils.tryOrExit {
        operateFun()
      }
    }
  }

  /**
   * An Executor to fetch and parse log files.
   */
  private val replayExecutor: ExecutorService = {
    if (!conf.contains("spark.testing")) {
      ThreadUtils.newDaemonSingleThreadExecutor("log-replay-executor")
    } else {
      MoreExecutors.sameThreadExecutor()
    }
  }

  initialize()

  private def initialize(): Unit = {
    // Validate the log directory.
    val path = new Path(logDir)
    if (!fs.exists(path)) {
      var msg = s"Log directory specified does not exist: $logDir."
      if (logDir == DEFAULT_LOG_DIR) {
        msg += " Did you configure the correct one through spark.history.fs.logDirectory?"
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
      pool.scheduleAtFixedRate(getRunner(checkForLogs), 0, UPDATE_INTERVAL_S, TimeUnit.SECONDS)

      if (conf.getBoolean("spark.history.fs.cleaner.enabled", false)) {
        // A task that periodically cleans event logs on disk.
        pool.scheduleAtFixedRate(getRunner(cleanLogs), 0, CLEAN_INTERVAL_S, TimeUnit.SECONDS)
      }
    }
  }

  override def getListing(): Iterable[FsApplicationHistoryInfo] = applications.values

  override def getAppUI(appId: String, attemptId: Option[String]): Option[SparkUI] = {
    try {
      applications.get(appId).flatMap { appInfo =>
        appInfo.attempts.find(_.attemptId == attemptId).flatMap { attempt =>
          val replayBus = new ReplayListenerBus()
          val ui = {
            val conf = this.conf.clone()
            val appSecManager = new SecurityManager(conf)
            SparkUI.createHistoryUI(conf, replayBus, appSecManager, appId,
              HistoryServer.getAttemptURI(appId, attempt.attemptId), attempt.startTime)
            // Do not call ui.bind() to avoid creating a new server for each application
          }
          val appListener = new ApplicationEventListener()
          replayBus.addListener(appListener)
          val appInfo = replay(fs.getFileStatus(new Path(logDir, attempt.logPath)), replayBus)
          appInfo.map { info =>
            ui.setAppName(s"${info.name} ($appId)")

            val uiAclsEnabled = conf.getBoolean("spark.history.ui.acls.enable", false)
            ui.getSecurityManager.setAcls(uiAclsEnabled)
            // make sure to set admin acls before view acls so they are properly picked up
            ui.getSecurityManager.setAdminAcls(appListener.adminAcls.getOrElse(""))
            ui.getSecurityManager.setViewAcls(attempt.sparkUser,
              appListener.viewAcls.getOrElse(""))
            ui
          }
        }
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
      val statusList = Option(fs.listStatus(new Path(logDir))).map(_.toSeq)
        .getOrElse(Seq[FileStatus]())
      var newLastModifiedTime = lastModifiedTime
      val logInfos: Seq[FileStatus] = statusList
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
        .flatMap { entry => Some(entry) }
        .sortWith { case (entry1, entry2) =>
          val mod1 = getModificationTime(entry1).getOrElse(-1L)
          val mod2 = getModificationTime(entry2).getOrElse(-1L)
          mod1 >= mod2
      }

      logInfos.sliding(20, 20).foreach { batch =>
        replayExecutor.submit(new Runnable {
          override def run(): Unit = mergeApplicationListing(batch)
        })
      }

      lastModifiedTime = newLastModifiedTime
    } catch {
      case e: Exception => logError("Exception in checking for event log updates", e)
    }
  }

  /**
   * Replay the log files in the list and merge the list of old applications with new ones
   */
  private def mergeApplicationListing(logs: Seq[FileStatus]): Unit = {
    val bus = new ReplayListenerBus()
    val newAttempts = logs.flatMap { fileStatus =>
      try {
        val res = replay(fileStatus, bus)
        res match {
          case Some(r) => logDebug(s"Application log ${r.logPath} loaded successfully.")
          case None => logWarning(s"Failed to load application log ${fileStatus.getPath}. " +
            "The application may have not started.")
        }
        res
      } catch {
        case e: Exception =>
          logError(
            s"Exception encountered when attempting to load application log ${fileStatus.getPath}",
            e)
          None
      }
    }

    if (newAttempts.isEmpty) {
      return
    }

    // Build a map containing all apps that contain new attempts. The app information in this map
    // contains both the new app attempt, and those that were already loaded in the existing apps
    // map. If an attempt has been updated, it replaces the old attempt in the list.
    val newAppMap = new mutable.HashMap[String, FsApplicationHistoryInfo]()
    newAttempts.foreach { attempt =>
      val appInfo = newAppMap.get(attempt.appId)
        .orElse(applications.get(attempt.appId))
        .map { app =>
          val attempts =
            app.attempts.filter(_.attemptId != attempt.attemptId).toList ++ List(attempt)
          new FsApplicationHistoryInfo(attempt.appId, attempt.name,
            attempts.sortWith(compareAttemptInfo))
        }
        .getOrElse(new FsApplicationHistoryInfo(attempt.appId, attempt.name, List(attempt)))
      newAppMap(attempt.appId) = appInfo
    }

    // Merge the new app list with the existing one, maintaining the expected ordering (descending
    // end time). Maintaining the order is important to avoid having to sort the list every time
    // there is a request for the log list.
    val newApps = newAppMap.values.toSeq.sortWith(compareAppInfo)
    val mergedApps = new mutable.LinkedHashMap[String, FsApplicationHistoryInfo]()
    def addIfAbsent(info: FsApplicationHistoryInfo): Unit = {
      if (!mergedApps.contains(info.id)) {
        mergedApps += (info.id -> info)
      }
    }

    val newIterator = newApps.iterator.buffered
    val oldIterator = applications.values.iterator.buffered
    while (newIterator.hasNext && oldIterator.hasNext) {
      if (newAppMap.contains(oldIterator.head.id)) {
        oldIterator.next()
      } else if (compareAppInfo(newIterator.head, oldIterator.head)) {
        addIfAbsent(newIterator.next())
      } else {
        addIfAbsent(oldIterator.next())
      }
    }
    newIterator.foreach(addIfAbsent)
    oldIterator.foreach(addIfAbsent)

    applications = mergedApps
  }

  /**
   * Delete event logs from the log directory according to the clean policy defined by the user.
   */
  private[history] def cleanLogs(): Unit = {
    try {
      val maxAge = conf.getTimeAsSeconds("spark.history.fs.cleaner.maxAge", "7d") * 1000

      val now = clock.getTimeMillis()
      val appsToRetain = new mutable.LinkedHashMap[String, FsApplicationHistoryInfo]()

      def shouldClean(attempt: FsApplicationAttemptInfo): Boolean = {
        now - attempt.lastUpdated > maxAge && attempt.completed
      }

      // Scan all logs from the log directory.
      // Only completed applications older than the specified max age will be deleted.
      applications.values.foreach { app =>
        val (toClean, toRetain) = app.attempts.partition(shouldClean)
        attemptsToClean ++= toClean

        if (toClean.isEmpty) {
          appsToRetain += (app.id -> app)
        } else if (toRetain.nonEmpty) {
          appsToRetain += (app.id ->
            new FsApplicationHistoryInfo(app.id, app.name, toRetain.toList))
        }
      }

      applications = appsToRetain

      val leftToClean = new mutable.ListBuffer[FsApplicationAttemptInfo]
      attemptsToClean.foreach { attempt =>
        try {
          val path = new Path(logDir, attempt.logPath)
          if (fs.exists(path)) {
            fs.delete(path, true)
          }
        } catch {
          case e: AccessControlException =>
            logInfo(s"No permission to delete ${attempt.logPath}, ignoring.")
          case t: IOException =>
            logError(s"IOException in cleaning ${attempt.logPath}", t)
            leftToClean += attempt
        }
      }

      attemptsToClean = leftToClean
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
    val a1 = i1.attempts.head
    val a2 = i2.attempts.head
    if (a1.endTime != a2.endTime) a1.endTime >= a2.endTime else a1.startTime >= a2.startTime
  }

  /**
   * Comparison function that defines the sort order for application attempts within the same
   * application. Order is: attempts are sorted by descending start time.
   * Most recent attempt state matches with current state of the app.
   *
   * Normally applications should have a single running attempt; but failure to call sc.stop()
   * may cause multiple running attempts to show up.
   *
   * @return Whether `a1` should precede `a2`.
   */
  private def compareAttemptInfo(
      a1: FsApplicationAttemptInfo,
      a2: FsApplicationAttemptInfo): Boolean = {
    a1.startTime >= a2.startTime
  }

  /**
   * Replays the events in the specified log file and returns information about the associated
   * application. Return `None` if the application ID cannot be located.
   */
  private def replay(
      eventLog: FileStatus,
      bus: ReplayListenerBus): Option[FsApplicationAttemptInfo] = {
    val logPath = eventLog.getPath()
    logInfo(s"Replaying log path: $logPath")
    val logInput =
      if (isLegacyLogDirectory(eventLog)) {
        openLegacyEventLog(logPath)
      } else {
        EventLoggingListener.openEventLog(logPath, fs)
      }
    try {
      val appListener = new ApplicationEventListener
      val appCompleted = isApplicationCompleted(eventLog)
      bus.addListener(appListener)
      bus.replay(logInput, logPath.toString, !appCompleted)

      // Without an app ID, new logs will render incorrectly in the listing page, so do not list or
      // try to show their UI. Some old versions of Spark generate logs without an app ID, so let
      // logs generated by those versions go through.
      if (appListener.appId.isDefined || !sparkVersionHasAppId(eventLog)) {
        Some(new FsApplicationAttemptInfo(
          logPath.getName(),
          appListener.appName.getOrElse(NOT_STARTED),
          appListener.appId.getOrElse(logPath.getName()),
          appListener.appAttemptId,
          appListener.startTime.getOrElse(-1L),
          appListener.endTime.getOrElse(-1L),
          getModificationTime(eventLog).get,
          appListener.sparkUser.getOrElse(NOT_STARTED),
          appCompleted))
      } else {
        None
      }
    } finally {
      logInput.close()
    }
  }

  /**
   * Loads a legacy log directory. This assumes that the log directory contains a single event
   * log file (along with other metadata files), which is the case for directories generated by
   * the code in previous releases.
   *
   * @return input stream that holds one JSON record per line.
   */
  private[history] def openLegacyEventLog(dir: Path): InputStream = {
    val children = fs.listStatus(dir)
    var eventLogPath: Path = null
    var codecName: Option[String] = None

    children.foreach { child =>
      child.getPath().getName() match {
        case name if name.startsWith(LOG_PREFIX) =>
          eventLogPath = child.getPath()
        case codec if codec.startsWith(COMPRESSION_CODEC_PREFIX) =>
          codecName = Some(codec.substring(COMPRESSION_CODEC_PREFIX.length()))
        case _ =>
      }
    }

    if (eventLogPath == null) {
      throw new IllegalArgumentException(s"$dir is not a Spark application log directory.")
    }

    val codec = try {
        codecName.map { c => CompressionCodec.createCodec(conf, c) }
      } catch {
        case e: Exception =>
          throw new IllegalArgumentException(s"Unknown compression codec $codecName.")
      }

    val in = new BufferedInputStream(fs.open(eventLogPath))
    codec.map(_.compressedInputStream(in)).getOrElse(in)
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

  /**
   * Returns whether the version of Spark that generated logs records app IDs. App IDs were added
   * in Spark 1.1.
   */
  private def sparkVersionHasAppId(entry: FileStatus): Boolean = {
    if (isLegacyLogDirectory(entry)) {
      fs.listStatus(entry.getPath())
        .find { status => status.getPath().getName().startsWith(SPARK_VERSION_PREFIX) }
        .map { status =>
          val version = status.getPath().getName().substring(SPARK_VERSION_PREFIX.length())
          version != "1.0" && version != "1.1"
        }
        .getOrElse(true)
    } else {
      true
    }
  }

}

private[history] object FsHistoryProvider {
  val DEFAULT_LOG_DIR = "file:/tmp/spark-events"

  // Constants used to parse Spark 1.0.0 log directories.
  val LOG_PREFIX = "EVENT_LOG_"
  val SPARK_VERSION_PREFIX = EventLoggingListener.SPARK_VERSION_KEY + "_"
  val COMPRESSION_CODEC_PREFIX = EventLoggingListener.COMPRESSION_CODEC_KEY + "_"
  val APPLICATION_COMPLETE = "APPLICATION_COMPLETE"
}

private class FsApplicationAttemptInfo(
    val logPath: String,
    val name: String,
    val appId: String,
    attemptId: Option[String],
    startTime: Long,
    endTime: Long,
    lastUpdated: Long,
    sparkUser: String,
    completed: Boolean = true)
  extends ApplicationAttemptInfo(
      attemptId, startTime, endTime, lastUpdated, sparkUser, completed)

private class FsApplicationHistoryInfo(
    id: String,
    override val name: String,
    override val attempts: List[FsApplicationAttemptInfo])
  extends ApplicationHistoryInfo(id, name, attempts)
