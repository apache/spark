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

import java.io.{FileNotFoundException, IOException, OutputStream}
import java.util.UUID
import java.util.concurrent.{Executors, ExecutorService, TimeUnit}
import java.util.zip.{ZipEntry, ZipOutputStream}

import scala.collection.mutable

import com.google.common.io.ByteStreams
import com.google.common.util.concurrent.{MoreExecutors, ThreadFactoryBuilder}
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.hdfs.protocol.HdfsConstants
import org.apache.hadoop.security.AccessControlException

import org.apache.spark.{SecurityManager, SparkConf, SparkException}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.{Clock, SystemClock, ThreadUtils, Utils}

/**
 * A class that provides application history from event logs stored in the file system.
 * This provider checks for new finished applications in the background periodically and
 * renders the history application UI by parsing the associated event logs.
 *
 * == How new and updated attempts are detected ==
 *
 * - New attempts are detected in [[checkForLogs]]: the log dir is scanned, and any
 * entries in the log dir whose modification time is greater than the last scan time
 * are considered new or updated. These are replayed to create a new [[FsApplicationAttemptInfo]]
 * entry and update or create a matching [[FsApplicationHistoryInfo]] element in the list
 * of applications.
 * - Updated attempts are also found in [[checkForLogs]] -- if the attempt's log file has grown, the
 * [[FsApplicationAttemptInfo]] is replaced by another one with a larger log size.
 * - When [[updateProbe()]] is invoked to check if a loaded [[SparkUI]]
 * instance is out of date, the log size of the cached instance is checked against the app last
 * loaded by [[checkForLogs]].
 *
 * The use of log size, rather than simply relying on modification times, is needed to
 * address the following issues
 * - some filesystems do not appear to update the `modtime` value whenever data is flushed to
 * an open file output stream. Changes to the history may not be picked up.
 * - the granularity of the `modtime` field may be 2+ seconds. Rapid changes to the FS can be
 * missed.
 *
 * Tracking filesize works given the following invariant: the logs get bigger
 * as new events are added. If a format was used in which this did not hold, the mechanism would
 * break. Simple streaming of JSON-formatted events, as is implemented today, implicitly
 * maintains this invariant.
 */
private[history] class FsHistoryProvider(conf: SparkConf, clock: Clock)
  extends ApplicationHistoryProvider with Logging {

  def this(conf: SparkConf) = {
    this(conf, new SystemClock())
  }

  import FsHistoryProvider._

  private val NOT_STARTED = "<Not Started>"

  private val SPARK_HISTORY_FS_NUM_REPLAY_THREADS = "spark.history.fs.numReplayThreads"

  // Interval between safemode checks.
  private val SAFEMODE_CHECK_INTERVAL_S = conf.getTimeAsSeconds(
    "spark.history.fs.safemodeCheck.interval", "5s")

  // Interval between each check for event log updates
  private val UPDATE_INTERVAL_S = conf.getTimeAsSeconds("spark.history.fs.update.interval", "10s")

  // Interval between each cleaner checks for event logs to delete
  private val CLEAN_INTERVAL_S = conf.getTimeAsSeconds("spark.history.fs.cleaner.interval", "1d")

  // Number of threads used to replay event logs.
  private val NUM_PROCESSING_THREADS = conf.getInt(SPARK_HISTORY_FS_NUM_REPLAY_THREADS,
    Math.ceil(Runtime.getRuntime.availableProcessors() / 4f).toInt)

  private val logDir = conf.getOption("spark.history.fs.logDirectory")
    .map { d => Utils.resolveURI(d).toString }
    .getOrElse(DEFAULT_LOG_DIR)

  private val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
  private val fs = Utils.getHadoopFileSystem(logDir, hadoopConf)

  // Used by check event thread and clean log thread.
  // Scheduled thread pool size must be one, otherwise it will have concurrent issues about fs
  // and applications between check task and clean task.
  private val pool = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder()
    .setNameFormat("spark-history-task-%d").setDaemon(true).build())

  // The modification time of the newest log detected during the last scan.   Currently only
  // used for logging msgs (logs are re-scanned based on file size, rather than modtime)
  private var lastScanTime = -1L

  // Mapping of application IDs to their metadata, in descending end time order. Apps are inserted
  // into the map in order, so the LinkedHashMap maintains the correct ordering.
  @volatile private var applications: mutable.LinkedHashMap[String, FsApplicationHistoryInfo]
    = new mutable.LinkedHashMap()

  val fileToAppInfo = new mutable.HashMap[Path, FsApplicationAttemptInfo]()

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
   * Fixed size thread pool to fetch and parse log files.
   */
  private val replayExecutor: ExecutorService = {
    if (!conf.contains("spark.testing")) {
      ThreadUtils.newDaemonFixedThreadPool(NUM_PROCESSING_THREADS, "log-replay-executor")
    } else {
      MoreExecutors.sameThreadExecutor()
    }
  }

  // Conf option used for testing the initialization code.
  val initThread = initialize()

  private[history] def initialize(): Thread = {
    if (!isFsInSafeMode()) {
      startPolling()
      null
    } else {
      startSafeModeCheckThread(None)
    }
  }

  private[history] def startSafeModeCheckThread(
      errorHandler: Option[Thread.UncaughtExceptionHandler]): Thread = {
    // Cannot probe anything while the FS is in safe mode, so spawn a new thread that will wait
    // for the FS to leave safe mode before enabling polling. This allows the main history server
    // UI to be shown (so that the user can see the HDFS status).
    val initThread = new Thread(new Runnable() {
      override def run(): Unit = {
        try {
          while (isFsInSafeMode()) {
            logInfo("HDFS is still in safe mode. Waiting...")
            val deadline = clock.getTimeMillis() +
              TimeUnit.SECONDS.toMillis(SAFEMODE_CHECK_INTERVAL_S)
            clock.waitTillTime(deadline)
          }
          startPolling()
        } catch {
          case _: InterruptedException =>
        }
      }
    })
    initThread.setDaemon(true)
    initThread.setName(s"${getClass().getSimpleName()}-init")
    initThread.setUncaughtExceptionHandler(errorHandler.getOrElse(
      new Thread.UncaughtExceptionHandler() {
        override def uncaughtException(t: Thread, e: Throwable): Unit = {
          logError("Error initializing FsHistoryProvider.", e)
          System.exit(1)
        }
      }))
    initThread.start()
    initThread
  }

  private def startPolling(): Unit = {
    // Validate the log directory.
    val path = new Path(logDir)
    try {
      if (!fs.getFileStatus(path).isDirectory) {
        throw new IllegalArgumentException(
          "Logging directory specified is not a directory: %s".format(logDir))
      }
    } catch {
      case f: FileNotFoundException =>
        var msg = s"Log directory specified does not exist: $logDir"
        if (logDir == DEFAULT_LOG_DIR) {
          msg += " Did you configure the correct one through spark.history.fs.logDirectory?"
        }
        throw new FileNotFoundException(msg).initCause(f)
    }

    // Disable the background thread during tests.
    if (!conf.contains("spark.testing")) {
      // A task that periodically checks for event log updates on disk.
      logDebug(s"Scheduling update thread every $UPDATE_INTERVAL_S seconds")
      pool.scheduleWithFixedDelay(getRunner(checkForLogs), 0, UPDATE_INTERVAL_S, TimeUnit.SECONDS)

      if (conf.getBoolean("spark.history.fs.cleaner.enabled", false)) {
        // A task that periodically cleans event logs on disk.
        pool.scheduleWithFixedDelay(getRunner(cleanLogs), 0, CLEAN_INTERVAL_S, TimeUnit.SECONDS)
      }
    } else {
      logDebug("Background update thread disabled for testing")
    }
  }

  override def getListing(): Iterable[FsApplicationHistoryInfo] = applications.values

  override def getApplicationInfo(appId: String): Option[FsApplicationHistoryInfo] = {
    applications.get(appId)
  }

  override def getAppUI(appId: String, attemptId: Option[String]): Option[LoadedAppUI] = {
    try {
      applications.get(appId).flatMap { appInfo =>
        appInfo.attempts.find(_.attemptId == attemptId).flatMap { attempt =>
          val replayBus = new ReplayListenerBus()
          val ui = {
            val conf = this.conf.clone()
            val appSecManager = new SecurityManager(conf)
            SparkUI.createHistoryUI(conf, replayBus, appSecManager, appInfo.name,
              HistoryServer.getAttemptURI(appId, attempt.attemptId), attempt.startTime)
            // Do not call ui.bind() to avoid creating a new server for each application
          }
          val appListener = new ApplicationEventListener()
          replayBus.addListener(appListener)
          val appAttemptInfo = replay(fs.getFileStatus(new Path(logDir, attempt.logPath)),
            replayBus)
          appAttemptInfo.map { info =>
            val uiAclsEnabled = conf.getBoolean("spark.history.ui.acls.enable", false)
            ui.getSecurityManager.setAcls(uiAclsEnabled)
            // make sure to set admin acls before view acls so they are properly picked up
            ui.getSecurityManager.setAdminAcls(appListener.adminAcls.getOrElse(""))
            ui.getSecurityManager.setViewAcls(attempt.sparkUser,
              appListener.viewAcls.getOrElse(""))
            ui.getSecurityManager.setAdminAclsGroups(appListener.adminAclsGroups.getOrElse(""))
            ui.getSecurityManager.setViewAclsGroups(appListener.viewAclsGroups.getOrElse(""))
            LoadedAppUI(ui, updateProbe(appId, attemptId, attempt.fileSize))
          }
        }
      }
    } catch {
      case e: FileNotFoundException => None
    }
  }

  override def getConfig(): Map[String, String] = {
    val safeMode = if (isFsInSafeMode()) {
      Map("HDFS State" -> "In safe mode, application logs not available.")
    } else {
      Map()
    }
    Map("Event log directory" -> logDir.toString) ++ safeMode
  }

  override def stop(): Unit = {
    if (initThread != null && initThread.isAlive()) {
      initThread.interrupt()
      initThread.join()
    }
  }

  /**
   * Builds the application list based on the current contents of the log directory.
   * Tries to reuse as much of the data already in memory as possible, by not reading
   * applications that haven't been updated since last time the logs were checked.
   */
  private[history] def checkForLogs(): Unit = {
    try {
      val newLastScanTime = getNewLastScanTime()
      logDebug(s"Scanning $logDir with lastScanTime==$lastScanTime")
      val statusList = Option(fs.listStatus(new Path(logDir))).map(_.toSeq)
        .getOrElse(Seq[FileStatus]())
      // scan for modified applications, replay and merge them
      val logInfos: Seq[FileStatus] = statusList
        .filter { entry =>
          try {
            val prevFileSize = fileToAppInfo.get(entry.getPath()).map{_.fileSize}.getOrElse(0L)
            !entry.isDirectory() && prevFileSize < entry.getLen()
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
          entry1.getModificationTime() >= entry2.getModificationTime()
      }

      if (logInfos.nonEmpty) {
        logDebug(s"New/updated attempts found: ${logInfos.size} ${logInfos.map(_.getPath)}")
      }
      logInfos.map { file =>
          replayExecutor.submit(new Runnable {
            override def run(): Unit = mergeApplicationListing(file)
          })
        }
        .foreach { task =>
          try {
            // Wait for all tasks to finish. This makes sure that checkForLogs
            // is not scheduled again while some tasks are already running in
            // the replayExecutor.
            task.get()
          } catch {
            case e: InterruptedException =>
              throw e
            case e: Exception =>
              logError("Exception while merging application listings", e)
          }
        }

      lastScanTime = newLastScanTime
    } catch {
      case e: Exception => logError("Exception in checking for event log updates", e)
    }
  }

  private def getNewLastScanTime(): Long = {
    val fileName = "." + UUID.randomUUID().toString
    val path = new Path(logDir, fileName)
    val fos = fs.create(path)

    try {
      fos.close()
      fs.getFileStatus(path).getModificationTime
    } catch {
      case e: Exception =>
        logError("Exception encountered when attempting to update last scan time", e)
        lastScanTime
    } finally {
      if (!fs.delete(path, true)) {
        logWarning(s"Error deleting ${path}")
      }
    }
  }

  override def writeEventLogs(
      appId: String,
      attemptId: Option[String],
      zipStream: ZipOutputStream): Unit = {

    /**
     * This method compresses the files passed in, and writes the compressed data out into the
     * [[OutputStream]] passed in. Each file is written as a new [[ZipEntry]] with its name being
     * the name of the file being compressed.
     */
    def zipFileToStream(file: Path, entryName: String, outputStream: ZipOutputStream): Unit = {
      val fs = file.getFileSystem(hadoopConf)
      val inputStream = fs.open(file, 1 * 1024 * 1024) // 1MB Buffer
      try {
        outputStream.putNextEntry(new ZipEntry(entryName))
        ByteStreams.copy(inputStream, outputStream)
        outputStream.closeEntry()
      } finally {
        inputStream.close()
      }
    }

    applications.get(appId) match {
      case Some(appInfo) =>
        try {
          // If no attempt is specified, or there is no attemptId for attempts, return all attempts
          appInfo.attempts.filter { attempt =>
            attempt.attemptId.isEmpty || attemptId.isEmpty || attempt.attemptId.get == attemptId.get
          }.foreach { attempt =>
            val logPath = new Path(logDir, attempt.logPath)
            zipFileToStream(logPath, attempt.logPath, zipStream)
          }
        } finally {
          zipStream.close()
        }
      case None => throw new SparkException(s"Logs for $appId not found.")
    }
  }


  /**
   * Replay the log files in the list and merge the list of old applications with new ones
   */
  private def mergeApplicationListing(fileStatus: FileStatus): Unit = {
    val newAttempts = try {
        val bus = new ReplayListenerBus()
        val res = replay(fileStatus, bus)
        res match {
          case Some(r) => logDebug(s"Application log ${r.logPath} loaded successfully: $r")
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

    if (newAttempts.isEmpty) {
      return
    }

    // Build a map containing all apps that contain new attempts. The app information in this map
    // contains both the new app attempt, and those that were already loaded in the existing apps
    // map. If an attempt has been updated, it replaces the old attempt in the list.
    val newAppMap = new mutable.HashMap[String, FsApplicationHistoryInfo]()

    applications.synchronized {
      newAttempts.foreach { attempt =>
        val appInfo = newAppMap.get(attempt.appId)
          .orElse(applications.get(attempt.appId))
          .map { app =>
            val attempts =
              app.attempts.filter(_.attemptId != attempt.attemptId) ++ List(attempt)
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
          fs.delete(new Path(logDir, attempt.logPath), true)
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
    // Note that the eventLog may have *increased* in size since when we grabbed the filestatus,
    // and when we read the file here.  That is OK -- it may result in an unnecessary refresh
    // when there is no update, but will not result in missing an update.  We *must* prevent
    // an error the other way -- if we report a size bigger (ie later) than the file that is
    // actually read, we may never refresh the app.  FileStatus is guaranteed to be static
    // after it's created, so we get a file size that is no bigger than what is actually read.
    val logInput = EventLoggingListener.openEventLog(logPath, fs)
    try {
      val appListener = new ApplicationEventListener
      val appCompleted = isApplicationCompleted(eventLog)
      bus.addListener(appListener)
      bus.replay(logInput, logPath.toString, !appCompleted)

      // Without an app ID, new logs will render incorrectly in the listing page, so do not list or
      // try to show their UI.
      if (appListener.appId.isDefined) {
        val attemptInfo = new FsApplicationAttemptInfo(
          logPath.getName(),
          appListener.appName.getOrElse(NOT_STARTED),
          appListener.appId.getOrElse(logPath.getName()),
          appListener.appAttemptId,
          appListener.startTime.getOrElse(-1L),
          appListener.endTime.getOrElse(-1L),
          eventLog.getModificationTime(),
          appListener.sparkUser.getOrElse(NOT_STARTED),
          appCompleted,
          eventLog.getLen()
        )
        fileToAppInfo(logPath) = attemptInfo
        Some(attemptInfo)
      } else {
        None
      }
    } finally {
      logInput.close()
    }
  }

  /**
   * Return true when the application has completed.
   */
  private def isApplicationCompleted(entry: FileStatus): Boolean = {
    !entry.getPath().getName().endsWith(EventLoggingListener.IN_PROGRESS)
  }

  /**
   * Checks whether HDFS is in safe mode.
   *
   * Note that DistributedFileSystem is a `@LimitedPrivate` class, which for all practical reasons
   * makes it more public than not.
   */
  private[history] def isFsInSafeMode(): Boolean = fs match {
    case dfs: DistributedFileSystem =>
      isFsInSafeMode(dfs)
    case _ =>
      false
  }

  // For testing.
  private[history] def isFsInSafeMode(dfs: DistributedFileSystem): Boolean = {
    dfs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_GET)
  }

  /**
   * String description for diagnostics
   * @return a summary of the component state
   */
  override def toString: String = {
    val header = s"""
      | FsHistoryProvider: logdir=$logDir,
      | last scan time=$lastScanTime
      | Cached application count =${applications.size}}
    """.stripMargin
    val sb = new StringBuilder(header)
    applications.foreach(entry => sb.append(entry._2).append("\n"))
    sb.toString
  }

  /**
   * Look up an application attempt
   * @param appId application ID
   * @param attemptId Attempt ID, if set
   * @return the matching attempt, if found
   */
  def lookup(appId: String, attemptId: Option[String]): Option[FsApplicationAttemptInfo] = {
    applications.get(appId).flatMap { appInfo =>
      appInfo.attempts.find(_.attemptId == attemptId)
    }
  }

  /**
   * Return true iff a newer version of the UI is available.  The check is based on whether the
   * fileSize for the currently loaded UI is smaller than the file size the last time
   * the logs were loaded.
   *
   * This is a very cheap operation -- the work of loading the new attempt was already done
   * by [[checkForLogs]].
   * @param appId application to probe
   * @param attemptId attempt to probe
   * @param prevFileSize the file size of the logs for the currently displayed UI
   */
  private def updateProbe(
      appId: String,
      attemptId: Option[String],
      prevFileSize: Long)(): Boolean = {
    lookup(appId, attemptId) match {
      case None =>
        logDebug(s"Application Attempt $appId/$attemptId not found")
        false
      case Some(latest) =>
        prevFileSize < latest.fileSize
    }
  }
}

private[history] object FsHistoryProvider {
  val DEFAULT_LOG_DIR = "file:/tmp/spark-events"
}

/**
 * Application attempt information.
 *
 * @param logPath path to the log file, or, for a legacy log, its directory
 * @param name application name
 * @param appId application ID
 * @param attemptId optional attempt ID
 * @param startTime start time (from playback)
 * @param endTime end time (from playback). -1 if the application is incomplete.
 * @param lastUpdated the modification time of the log file when this entry was built by replaying
 *                    the history.
 * @param sparkUser user running the application
 * @param completed flag to indicate whether or not the application has completed.
 * @param fileSize the size of the log file the last time the file was scanned for changes
 */
private class FsApplicationAttemptInfo(
    val logPath: String,
    val name: String,
    val appId: String,
    attemptId: Option[String],
    startTime: Long,
    endTime: Long,
    lastUpdated: Long,
    sparkUser: String,
    completed: Boolean,
    val fileSize: Long)
  extends ApplicationAttemptInfo(
      attemptId, startTime, endTime, lastUpdated, sparkUser, completed) {

  /** extend the superclass string value with the extra attributes of this class */
  override def toString: String = {
    s"FsApplicationAttemptInfo($name, $appId," +
      s" ${super.toString}, source=$logPath, size=$fileSize"
  }
}

/**
 * Application history information
 * @param id application ID
 * @param name application name
 * @param attempts list of attempts, most recent first.
 */
private class FsApplicationHistoryInfo(
    id: String,
    override val name: String,
    override val attempts: List[FsApplicationAttemptInfo])
  extends ApplicationHistoryInfo(id, name, attempts)
