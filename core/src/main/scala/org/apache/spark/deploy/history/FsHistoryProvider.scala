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

import java.io.{File, FileNotFoundException, IOException}
import java.lang.{Long => JLong}
import java.util.{Date, NoSuchElementException, ServiceLoader}
import java.util.concurrent.{ConcurrentHashMap, ExecutorService, TimeUnit}
import java.util.zip.ZipOutputStream

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.{Codec, Source}
import scala.util.control.NonFatal
import scala.xml.Node

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.hdfs.protocol.HdfsConstants
import org.apache.hadoop.security.AccessControlException

import org.apache.spark.{SecurityManager, SparkConf, SparkException}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.History._
import org.apache.spark.internal.config.Status._
import org.apache.spark.internal.config.Tests.IS_TESTING
import org.apache.spark.internal.config.UI._
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.ReplayListenerBus._
import org.apache.spark.status._
import org.apache.spark.status.KVUtils._
import org.apache.spark.status.api.v1.{ApplicationAttemptInfo, ApplicationInfo}
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.{Clock, SystemClock, ThreadUtils, Utils}
import org.apache.spark.util.kvstore._

/**
 * A class that provides application history from event logs stored in the file system.
 * This provider checks for new finished applications in the background periodically and
 * renders the history application UI by parsing the associated event logs.
 *
 * == How new and updated attempts are detected ==
 *
 * - New attempts are detected in [[checkForLogs]]: the log dir is scanned, and any entries in the
 * log dir whose size changed since the last scan time are considered new or updated. These are
 * replayed to create a new attempt info entry and update or create a matching application info
 * element in the list of applications.
 * - Updated attempts are also found in [[checkForLogs]] -- if the attempt's log file has grown, the
 * attempt is replaced by another one with a larger log size.
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

  // Interval between safemode checks.
  private val SAFEMODE_CHECK_INTERVAL_S = conf.get(History.SAFEMODE_CHECK_INTERVAL_S)

  // Interval between each check for event log updates
  private val UPDATE_INTERVAL_S = conf.get(History.UPDATE_INTERVAL_S)

  // Interval between each cleaner checks for event logs to delete
  private val CLEAN_INTERVAL_S = conf.get(History.CLEANER_INTERVAL_S)

  // Number of threads used to replay event logs.
  private val NUM_PROCESSING_THREADS = conf.get(History.NUM_REPLAY_THREADS)

  private val logDir = conf.get(History.HISTORY_LOG_DIR)

  private val historyUiAclsEnable = conf.get(History.HISTORY_SERVER_UI_ACLS_ENABLE)
  private val historyUiAdminAcls = conf.get(History.HISTORY_SERVER_UI_ADMIN_ACLS)
  private val historyUiAdminAclsGroups = conf.get(History.HISTORY_SERVER_UI_ADMIN_ACLS_GROUPS)
  logInfo(s"History server ui acls " + (if (historyUiAclsEnable) "enabled" else "disabled") +
    "; users with admin permissions: " + historyUiAdminAcls.mkString(",") +
    "; groups with admin permissions: " + historyUiAdminAclsGroups.mkString(","))

  private val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
  // Visible for testing
  private[history] val fs: FileSystem = new Path(logDir).getFileSystem(hadoopConf)

  // Used by check event thread and clean log thread.
  // Scheduled thread pool size must be one, otherwise it will have concurrent issues about fs
  // and applications between check task and clean task.
  private val pool = ThreadUtils.newDaemonSingleThreadScheduledExecutor("spark-history-task-%d")

  // The modification time of the newest log detected during the last scan.   Currently only
  // used for logging msgs (logs are re-scanned based on file size, rather than modtime)
  private val lastScanTime = new java.util.concurrent.atomic.AtomicLong(-1)

  private val pendingReplayTasksCount = new java.util.concurrent.atomic.AtomicInteger(0)

  private val storePath = conf.get(LOCAL_STORE_DIR).map(new File(_))
  private val fastInProgressParsing = conf.get(FAST_IN_PROGRESS_PARSING)

  private val hybridStoreEnabled = conf.get(History.HYBRID_STORE_ENABLED)
  private val hybridStoreDiskBackend =
    HybridStoreDiskBackend.withName(conf.get(History.HYBRID_STORE_DISK_BACKEND))

  // Visible for testing.
  private[history] val listing: KVStore = {
    KVUtils.createKVStore(storePath, live = false, conf)
  }

  private val diskManager = storePath.map { path =>
    new HistoryServerDiskManager(conf, path, listing, clock)
  }

  private var memoryManager: HistoryServerMemoryManager = null
  if (hybridStoreEnabled) {
    memoryManager = new HistoryServerMemoryManager(conf)
  }

  private val fileCompactor = new EventLogFileCompactor(conf, hadoopConf, fs,
    conf.get(EVENT_LOG_ROLLING_MAX_FILES_TO_RETAIN), conf.get(EVENT_LOG_COMPACTION_SCORE_THRESHOLD))

  // Used to store the paths, which are being processed. This enable the replay log tasks execute
  // asynchronously and make sure that checkForLogs would not process a path repeatedly.
  private val processing = ConcurrentHashMap.newKeySet[String]

  private def isProcessing(path: Path): Boolean = {
    processing.contains(path.getName)
  }

  private def isProcessing(info: LogInfo): Boolean = {
    processing.contains(info.logPath.split("/").last)
  }

  private def processing(path: Path): Unit = {
    processing.add(path.getName)
  }

  private def endProcessing(path: Path): Unit = {
    processing.remove(path.getName)
  }

  private val inaccessibleList = new ConcurrentHashMap[String, Long]

  // Visible for testing
  private[history] def isAccessible(path: Path): Boolean = {
    !inaccessibleList.containsKey(path.getName)
  }

  private def markInaccessible(path: Path): Unit = {
    inaccessibleList.put(path.getName, clock.getTimeMillis())
  }

  /**
   * Removes expired entries in the inaccessibleList, according to the provided
   * `expireTimeInSeconds`.
   */
  private def clearInaccessibleList(expireTimeInSeconds: Long): Unit = {
    val expiredThreshold = clock.getTimeMillis() - expireTimeInSeconds * 1000
    inaccessibleList.asScala.retain((_, creationTime) => creationTime >= expiredThreshold)
  }

  private val activeUIs = new mutable.HashMap[(String, Option[String]), LoadedAppUI]()

  /**
   * Return a runnable that performs the given operation on the event logs.
   * This operation is expected to be executed periodically.
   */
  private def getRunner(operateFun: () => Unit): Runnable =
    () => Utils.tryOrExit { operateFun() }

  /**
   * Fixed size thread pool to fetch and parse log files.
   */
  private val replayExecutor: ExecutorService = {
    if (!Utils.isTesting) {
      ThreadUtils.newDaemonFixedThreadPool(NUM_PROCESSING_THREADS, "log-replay-executor")
    } else {
      ThreadUtils.sameThreadExecutorService
    }
  }

  var initThread: Thread = null

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
    val initThread = new Thread(() => {
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
    })
    initThread.setDaemon(true)
    initThread.setName(s"${getClass().getSimpleName()}-init")
    initThread.setUncaughtExceptionHandler(errorHandler.getOrElse(
      (_: Thread, e: Throwable) => {
        logError("Error initializing FsHistoryProvider.", e)
        System.exit(1)
      }))
    initThread.start()
    initThread
  }

  private def startPolling(): Unit = {
    diskManager.foreach(_.initialize())
    if (memoryManager != null) {
      memoryManager.initialize()
    }

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
    if (!conf.contains(IS_TESTING)) {
      // A task that periodically checks for event log updates on disk.
      logDebug(s"Scheduling update thread every $UPDATE_INTERVAL_S seconds")
      pool.scheduleWithFixedDelay(
        getRunner(() => checkForLogs()), 0, UPDATE_INTERVAL_S, TimeUnit.SECONDS)

      if (conf.get(CLEANER_ENABLED)) {
        // A task that periodically cleans event logs on disk.
        pool.scheduleWithFixedDelay(
          getRunner(() => cleanLogs()), 0, CLEAN_INTERVAL_S, TimeUnit.SECONDS)
      }

      if (conf.contains(DRIVER_LOG_DFS_DIR) && conf.get(DRIVER_LOG_CLEANER_ENABLED)) {
        pool.scheduleWithFixedDelay(getRunner(() => cleanDriverLogs()),
          0,
          conf.get(DRIVER_LOG_CLEANER_INTERVAL),
          TimeUnit.SECONDS)
      }
    } else {
      logDebug("Background update thread disabled for testing")
    }
  }

  override def getListing(): Iterator[ApplicationInfo] = {
    // Return the listing in end time descending order.
    KVUtils.mapToSeq(listing.view(classOf[ApplicationInfoWrapper])
      .index("endTime").reverse())(_.toApplicationInfo()).iterator
  }

  override def getApplicationInfo(appId: String): Option[ApplicationInfo] = {
    try {
      Some(load(appId).toApplicationInfo())
    } catch {
      case _: NoSuchElementException =>
        None
    }
  }

  override def getEventLogsUnderProcess(): Int = pendingReplayTasksCount.get()

  override def getLastUpdatedTime(): Long = lastScanTime.get()

  /**
   * Split a comma separated String, filter out any empty items, and return a Sequence of strings
   */
  private def stringToSeq(list: String): Seq[String] = {
    list.split(',').map(_.trim).filter(!_.isEmpty)
  }

  override def getAppUI(appId: String, attemptId: Option[String]): Option[LoadedAppUI] = {
    val app = try {
      load(appId)
     } catch {
      case _: NoSuchElementException =>
        return None
    }

    val attempt = app.attempts.find(_.info.attemptId == attemptId).orNull
    if (attempt == null) {
      return None
    }

    val conf = this.conf.clone()
    val secManager = createSecurityManager(conf, attempt)

    val kvstore = try {
      diskManager match {
        case Some(sm) =>
          loadDiskStore(sm, appId, attempt)

        case _ =>
          createInMemoryStore(attempt)
      }
    } catch {
      case _: FileNotFoundException =>
        return None
    }

    val ui = SparkUI.create(None, new HistoryAppStatusStore(conf, kvstore), conf, secManager,
      app.info.name, HistoryServer.getAttemptURI(appId, attempt.info.attemptId),
      attempt.info.startTime.getTime(), attempt.info.appSparkVersion)

    // place the tab in UI based on the display order
    loadPlugins().toSeq.sortBy(_.displayOrder).foreach(_.setupUI(ui))

    val loadedUI = LoadedAppUI(ui)
    synchronized {
      activeUIs((appId, attemptId)) = loadedUI
    }

    Some(loadedUI)
  }

  override def getEmptyListingHtml(): Seq[Node] = {
    <p>
      Did you specify the correct logging directory? Please verify your setting of
      <span style="font-style:italic">spark.history.fs.logDirectory</span>
      listed above and whether you have the permissions to access it.
      <br/>
      It is also possible that your application did not run to
      completion or did not stop the SparkContext.
    </p>
  }

  override def getConfig(): Map[String, String] = {
    val safeMode = if (isFsInSafeMode()) {
      Map("HDFS State" -> "In safe mode, application logs not available.")
    } else {
      Map()
    }
    Map("Event log directory" -> logDir) ++ safeMode
  }

  override def start(): Unit = {
    initThread = initialize()
  }

  override def stop(): Unit = {
    try {
      if (initThread != null && initThread.isAlive()) {
        initThread.interrupt()
        initThread.join()
      }
      Seq(pool, replayExecutor).foreach { executor =>
        executor.shutdown()
        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
          executor.shutdownNow()
        }
      }
    } finally {
      activeUIs.foreach { case (_, loadedUI) => loadedUI.ui.store.close() }
      activeUIs.clear()
      listing.close()
    }
  }

  override def onUIDetached(appId: String, attemptId: Option[String], ui: SparkUI): Unit = {
    val uiOption = synchronized {
      activeUIs.remove((appId, attemptId))
    }
    uiOption.foreach { loadedUI =>
      loadedUI.lock.writeLock().lock()
      try {
        loadedUI.ui.store.close()
      } finally {
        loadedUI.lock.writeLock().unlock()
      }

      diskManager.foreach { dm =>
        // If the UI is not valid, delete its files from disk, if any. This relies on the fact that
        // ApplicationCache will never call this method concurrently with getAppUI() for the same
        // appId / attemptId.
        dm.release(appId, attemptId, delete = !loadedUI.valid)
      }
    }
  }

  override def checkUIViewPermissions(appId: String, attemptId: Option[String],
      user: String): Boolean = {
    val app = load(appId)
    val attempt = app.attempts.find(_.info.attemptId == attemptId).orNull
    if (attempt == null) {
      throw new NoSuchElementException()
    }
    val secManager = createSecurityManager(this.conf.clone(), attempt)
    secManager.checkUIViewPermissions(user)
  }

  /**
   * Builds the application list based on the current contents of the log directory.
   * Tries to reuse as much of the data already in memory as possible, by not reading
   * applications that haven't been updated since last time the logs were checked.
   * Only a max of UPDATE_BATCHSIZE jobs are processed in each cycle, to prevent the process
   * from running for too long which blocks updating newly appeared eventlog files.
   */
  private[history] def checkForLogs(): Unit = {
    var count: Int = 0
    try {
      val newLastScanTime = clock.getTimeMillis()
      logDebug(s"Scanning $logDir with lastScanTime==$lastScanTime")

      // Mark entries that are processing as not stale. Such entries do not have a chance to be
      // updated with the new 'lastProcessed' time and thus any entity that completes processing
      // right after this check and before the check for stale entities will be identified as stale
      // and will be deleted from the UI until the next 'checkForLogs' run.
      val notStale = mutable.HashSet[String]()
      val updated = Option(fs.listStatus(new Path(logDir))).map(_.toSeq).getOrElse(Nil)
        .filter { entry => isAccessible(entry.getPath) }
        .filter { entry =>
          if (isProcessing(entry.getPath)) {
            notStale.add(entry.getPath.toString())
            false
          } else {
            true
          }
        }
        .flatMap { entry => EventLogFileReader(fs, entry) }
        .filter { reader =>
          try {
            reader.modificationTime
            true
          } catch {
            case e: IllegalArgumentException =>
              logInfo("Exception in getting modificationTime of "
                + reader.rootPath.getName + ". " + e.toString)
              false
          }
        }
        .sortWith { case (entry1, entry2) =>
          entry1.modificationTime > entry2.modificationTime
        }
        .filter { reader =>
          try {
            val info = listing.read(classOf[LogInfo], reader.rootPath.toString())

            if (info.appId.isDefined) {
              // If the SHS view has a valid application, update the time the file was last seen so
              // that the entry is not deleted from the SHS listing. Also update the file size, in
              // case the code below decides we don't need to parse the log.
              listing.write(info.copy(lastProcessed = newLastScanTime,
                fileSize = reader.fileSizeForLastIndex,
                lastIndex = reader.lastIndex,
                isComplete = reader.completed))
            }

            if (shouldReloadLog(info, reader)) {
              // ignore fastInProgressParsing when rolling event log is enabled on the log path,
              // to ensure proceeding compaction even fastInProgressParsing is turned on.
              if (info.appId.isDefined && reader.lastIndex.isEmpty && fastInProgressParsing) {
                // When fast in-progress parsing is on, we don't need to re-parse when the
                // size changes, but we do need to invalidate any existing UIs.
                // Also, we need to update the `lastUpdated time` to display the updated time in
                // the HistoryUI and to avoid cleaning the inprogress app while running.
                val appInfo = listing.read(classOf[ApplicationInfoWrapper], info.appId.get)

                val attemptList = appInfo.attempts.map { attempt =>
                  if (attempt.info.attemptId == info.attemptId) {
                    new AttemptInfoWrapper(
                      attempt.info.copy(lastUpdated = new Date(newLastScanTime)),
                      attempt.logPath,
                      attempt.fileSize,
                      attempt.lastIndex,
                      attempt.adminAcls,
                      attempt.viewAcls,
                      attempt.adminAclsGroups,
                      attempt.viewAclsGroups)
                  } else {
                    attempt
                  }
                }

                val updatedAppInfo = new ApplicationInfoWrapper(appInfo.info, attemptList)
                listing.write(updatedAppInfo)

                invalidateUI(info.appId.get, info.attemptId)
                false
              } else {
                true
              }
            } else {
              false
            }
          } catch {
            case _: NoSuchElementException =>
              // If the file is currently not being tracked by the SHS, add an entry for it and try
              // to parse it. This will allow the cleaner code to detect the file as stale later on
              // if it was not possible to parse it.
              try {
                if (count < conf.get(UPDATE_BATCHSIZE)) {
                  listing.write(LogInfo(reader.rootPath.toString(), newLastScanTime,
                    LogType.EventLogs, None, None, reader.fileSizeForLastIndex, reader.lastIndex,
                    None, reader.completed))
                  count = count + 1
                  reader.fileSizeForLastIndex > 0
                } else {
                  false
                }
              } catch {
                case _: FileNotFoundException => false
                case NonFatal(e) =>
                  logWarning(s"Error while reading new log ${reader.rootPath}", e)
                  false
              }

            case NonFatal(e) =>
              logWarning(s"Error while filtering log ${reader.rootPath}", e)
              false
          }
        }

      if (updated.nonEmpty) {
        logDebug(s"New/updated attempts found: ${updated.size} ${updated.map(_.rootPath)}")
      }

      updated.foreach { entry =>
        submitLogProcessTask(entry.rootPath) { () =>
          mergeApplicationListing(entry, newLastScanTime, true)
        }
      }

      // Delete all information about applications whose log files disappeared from storage.
      // This is done by identifying the event logs which were not touched by the current
      // directory scan.
      //
      // Only entries with valid applications are cleaned up here. Cleaning up invalid log
      // files is done by the periodic cleaner task.
      val stale = listing.synchronized {
        KVUtils.viewToSeq(listing.view(classOf[LogInfo])
          .index("lastProcessed")
          .last(newLastScanTime - 1))
      }
      stale.filterNot(isProcessing)
        .filterNot(info => notStale.contains(info.logPath))
        .foreach { log =>
          log.appId.foreach { appId =>
            cleanAppData(appId, log.attemptId, log.logPath)
            listing.delete(classOf[LogInfo], log.logPath)
          }
        }

      lastScanTime.set(newLastScanTime)
    } catch {
      case e: Exception => logError("Exception in checking for event log updates", e)
    }
  }

  private[history] def shouldReloadLog(info: LogInfo, reader: EventLogFileReader): Boolean = {
    if (info.isComplete != reader.completed) {
      true
    } else {
      var result = if (info.lastIndex.isDefined) {
        require(reader.lastIndex.isDefined)
        info.lastIndex.get < reader.lastIndex.get || info.fileSize < reader.fileSizeForLastIndex
      } else {
        info.fileSize < reader.fileSizeForLastIndex
      }
      if (!result && !reader.completed) {
        try {
          result = reader.fileSizeForLastIndexForDFS.exists(info.fileSize < _)
        } catch {
          case e: Exception =>
            logDebug(s"Failed to check the length for the file : ${info.logPath}", e)
        }
      }
      result
    }
  }

  private def cleanAppData(appId: String, attemptId: Option[String], logPath: String): Unit = {
    try {
      var isStale = false
      listing.synchronized {
        val app = load(appId)
        val (attempt, others) = app.attempts.partition(_.info.attemptId == attemptId)

        assert(attempt.isEmpty || attempt.size == 1)
        isStale = attempt.headOption.exists { a =>
          if (a.logPath != new Path(logPath).getName()) {
            // If the log file name does not match, then probably the old log file was from an
            // in progress application. Just return that the app should be left alone.
            false
          } else {
            if (others.nonEmpty) {
              val newAppInfo = new ApplicationInfoWrapper(app.info, others)
              listing.write(newAppInfo)
            } else {
              listing.delete(classOf[ApplicationInfoWrapper], appId)
            }
            true
          }
        }
      }

      if (isStale) {
        val maybeUI = synchronized {
          activeUIs.remove(appId -> attemptId)
        }
        maybeUI.foreach { ui =>
          ui.invalidate()
          ui.ui.store.close()
        }
        diskManager.foreach(_.release(appId, attemptId, delete = true))
      }
    } catch {
      case _: NoSuchElementException =>
    }
  }

  override def writeEventLogs(
      appId: String,
      attemptId: Option[String],
      zipStream: ZipOutputStream): Unit = {

    val app = try {
      load(appId)
    } catch {
      case _: NoSuchElementException =>
        throw new SparkException(s"Logs for $appId not found.")
    }

    try {
      // If no attempt is specified, or there is no attemptId for attempts, return all attempts
      attemptId
        .map { id => app.attempts.filter(_.info.attemptId == Some(id)) }
        .getOrElse(app.attempts)
        .foreach { attempt =>
          val reader = EventLogFileReader(fs, new Path(logDir, attempt.logPath),
            attempt.lastIndex)
          reader.zipEventLogFiles(zipStream)
        }
    } finally {
      zipStream.close()
    }
  }

  private def mergeApplicationListing(
      reader: EventLogFileReader,
      scanTime: Long,
      enableOptimizations: Boolean): Unit = {
    val rootPath = reader.rootPath
    var succeeded = false
    try {
      val lastEvaluatedForCompaction: Option[Long] = try {
        listing.read(classOf[LogInfo], rootPath.toString).lastEvaluatedForCompaction
      } catch {
        case _: NoSuchElementException => None
      }

      pendingReplayTasksCount.incrementAndGet()
      doMergeApplicationListing(reader, scanTime, enableOptimizations, lastEvaluatedForCompaction)
      if (conf.get(CLEANER_ENABLED)) {
        checkAndCleanLog(rootPath.toString)
      }

      succeeded = true
    } catch {
      case e: InterruptedException =>
        throw e
      case e: AccessControlException =>
        // We don't have read permissions on the log file
        logWarning(s"Unable to read log $rootPath", e)
        markInaccessible(rootPath)
        // SPARK-28157 We should remove this inaccessible entry from the KVStore
        // to handle permission-only changes with the same file sizes later.
        listing.synchronized {
          listing.delete(classOf[LogInfo], rootPath.toString)
        }
      case _: FileNotFoundException
          if reader.rootPath.getName.endsWith(EventLogFileWriter.IN_PROGRESS) =>
        val finalFileName = reader.rootPath.getName.stripSuffix(EventLogFileWriter.IN_PROGRESS)
        val finalFilePath = new Path(reader.rootPath.getParent, finalFileName)
        if (fs.exists(finalFilePath)) {
          // Do nothing, the application completed during processing, the final event log file
          // will be processed by next around.
        } else {
          logWarning(s"In-progress event log file does not exist: ${reader.rootPath}, " +
            s"neither does the final event log file: $finalFilePath.")
        }
      case e: Exception =>
        logError("Exception while merging application listings", e)
    } finally {
      endProcessing(rootPath)
      pendingReplayTasksCount.decrementAndGet()

      // triggering another task for compaction task only if it succeeds
      if (succeeded) {
        submitLogProcessTask(rootPath) { () => compact(reader) }
      }
    }
  }

  /**
   * Replay the given log file, saving the application in the listing db.
   * Visible for testing
   */
  private[history] def doMergeApplicationListing(
      reader: EventLogFileReader,
      scanTime: Long,
      enableOptimizations: Boolean,
      lastEvaluatedForCompaction: Option[Long]): Unit = doMergeApplicationListingInternal(
    reader, scanTime, enableOptimizations, lastEvaluatedForCompaction)

  @scala.annotation.tailrec
  private def doMergeApplicationListingInternal(
      reader: EventLogFileReader,
      scanTime: Long,
      enableOptimizations: Boolean,
      lastEvaluatedForCompaction: Option[Long]): Unit = {
    val eventsFilter: ReplayEventsFilter = { eventString =>
      eventString.startsWith(APPL_START_EVENT_PREFIX) ||
        eventString.startsWith(APPL_END_EVENT_PREFIX) ||
        eventString.startsWith(LOG_START_EVENT_PREFIX) ||
        eventString.startsWith(ENV_UPDATE_EVENT_PREFIX)
    }

    val logPath = reader.rootPath
    val appCompleted = reader.completed
    val reparseChunkSize = conf.get(END_EVENT_REPARSE_CHUNK_SIZE)

    // Enable halt support in listener if:
    // - app in progress && fast parsing enabled
    // - skipping to end event is enabled (regardless of in-progress state)
    val shouldHalt = enableOptimizations &&
      ((!appCompleted && fastInProgressParsing) || reparseChunkSize > 0)

    val bus = new ReplayListenerBus()
    val listener = new AppListingListener(reader, clock, shouldHalt)
    bus.addListener(listener)

    logInfo(s"Parsing $logPath for listing data...")
    val logFiles = reader.listEventLogFiles
    parseAppEventLogs(logFiles, bus, !appCompleted, eventsFilter)

    // If enabled above, the listing listener will halt parsing when there's enough information to
    // create a listing entry. When the app is completed, or fast parsing is disabled, we still need
    // to replay until the end of the log file to try to find the app end event. Instead of reading
    // and parsing line by line, this code skips bytes from the underlying stream so that it is
    // positioned somewhere close to the end of the log file.
    //
    // Because the application end event is written while some Spark subsystems such as the
    // scheduler are still active, there is no guarantee that the end event will be the last
    // in the log. So, to be safe, the code uses a configurable chunk to be re-parsed at
    // the end of the file, and retries parsing the whole log later if the needed data is
    // still not found.
    //
    // Note that skipping bytes in compressed files is still not cheap, but there are still some
    // minor gains over the normal log parsing done by the replay bus.
    //
    // This code re-opens the file so that it knows where it's skipping to. This isn't as cheap as
    // just skipping from the current position, but there isn't a a good way to detect what the
    // current position is, since the replay listener bus buffers data internally.
    val lookForEndEvent = shouldHalt && (appCompleted || !fastInProgressParsing)
    if (lookForEndEvent && listener.applicationInfo.isDefined) {
      val lastFile = logFiles.last
      Utils.tryWithResource(EventLogFileReader.openEventLog(lastFile.getPath, fs)) { in =>
        val target = lastFile.getLen - reparseChunkSize
        if (target > 0) {
          logInfo(s"Looking for end event; skipping $target bytes from $logPath...")
          var skipped = 0L
          while (skipped < target) {
            skipped += in.skip(target - skipped)
          }
        }

        val source = Source.fromInputStream(in)(Codec.UTF8).getLines()

        // Because skipping may leave the stream in the middle of a line, read the next line
        // before replaying.
        if (target > 0) {
          source.next()
        }

        bus.replay(source, lastFile.getPath.toString, !appCompleted, eventsFilter)
      }
    }

    logInfo(s"Finished parsing $logPath")

    listener.applicationInfo match {
      case Some(app) if !lookForEndEvent || app.attempts.head.info.completed =>
        // In this case, we either didn't care about the end event, or we found it. So the
        // listing data is good.
        invalidateUI(app.info.id, app.attempts.head.info.attemptId)
        addListing(app)
        listing.write(LogInfo(logPath.toString(), scanTime, LogType.EventLogs, Some(app.info.id),
          app.attempts.head.info.attemptId, reader.fileSizeForLastIndex, reader.lastIndex,
          lastEvaluatedForCompaction, reader.completed))

        // For a finished log, remove the corresponding "in progress" entry from the listing DB if
        // the file is really gone.
        // The logic is only valid for single event log, as root path doesn't change for
        // rolled event logs.
        if (appCompleted && reader.lastIndex.isEmpty) {
          val inProgressLog = logPath.toString() + EventLogFileWriter.IN_PROGRESS
          try {
            // Fetch the entry first to avoid an RPC when it's already removed.
            listing.read(classOf[LogInfo], inProgressLog)
            if (!fs.isFile(new Path(inProgressLog))) {
              listing.synchronized {
                listing.delete(classOf[LogInfo], inProgressLog)
              }
            }
          } catch {
            case _: NoSuchElementException =>
          }
        }

      case Some(_) =>
        // In this case, the attempt is still not marked as finished but was expected to. This can
        // mean the end event is before the configured threshold, so call the method again to
        // re-parse the whole log.
        logInfo(s"Reparsing $logPath since end event was not found.")
        doMergeApplicationListingInternal(reader, scanTime, enableOptimizations = false,
          lastEvaluatedForCompaction)

      case _ =>
        // If the app hasn't written down its app ID to the logs, still record the entry in the
        // listing db, with an empty ID. This will make the log eligible for deletion if the app
        // does not make progress after the configured max log age.
        listing.write(
          LogInfo(logPath.toString(), scanTime, LogType.EventLogs, None, None,
            reader.fileSizeForLastIndex, reader.lastIndex, lastEvaluatedForCompaction,
            reader.completed))
    }
  }

  private def compact(reader: EventLogFileReader): Unit = {
    val rootPath = reader.rootPath
    try {
      reader.lastIndex match {
        case Some(lastIndex) =>
          try {
            val info = listing.read(classOf[LogInfo], reader.rootPath.toString)
            if (info.lastEvaluatedForCompaction.isEmpty ||
                info.lastEvaluatedForCompaction.get < lastIndex) {
              // haven't tried compaction for this index, do compaction
              fileCompactor.compact(reader.listEventLogFiles)
              listing.write(info.copy(lastEvaluatedForCompaction = Some(lastIndex)))
            }
          } catch {
            case _: NoSuchElementException =>
            // this should exist, but ignoring doesn't hurt much
          }

        case None => // This is not applied to single event log file.
      }
    } catch {
      case e: InterruptedException =>
        throw e
      case e: AccessControlException =>
        logWarning(s"Insufficient permission while compacting log for $rootPath", e)
      case e: Exception =>
        logError(s"Exception while compacting log for $rootPath", e)
    } finally {
      endProcessing(rootPath)
    }
  }

  /**
   * Invalidate an existing UI for a given app attempt. See LoadedAppUI for a discussion on the
   * UI lifecycle.
   */
  private def invalidateUI(appId: String, attemptId: Option[String]): Unit = {
    synchronized {
      activeUIs.get((appId, attemptId)).foreach { ui =>
        ui.invalidate()
        ui.ui.store.close()
      }
    }
  }

  /**
   * Check and delete specified event log according to the max log age defined by the user.
   */
  private[history] def checkAndCleanLog(logPath: String): Unit = Utils.tryLog {
    val maxTime = clock.getTimeMillis() - conf.get(MAX_LOG_AGE_S) * 1000
    val log = listing.read(classOf[LogInfo], logPath)

    if (log.lastProcessed <= maxTime && log.appId.isEmpty) {
      logInfo(s"Deleting invalid / corrupt event log ${log.logPath}")
      deleteLog(fs, new Path(log.logPath))
      listing.delete(classOf[LogInfo], log.logPath)
    }

    log.appId.foreach { appId =>
      val app = listing.read(classOf[ApplicationInfoWrapper], appId)
      if (app.oldestAttempt() <= maxTime) {
        val (remaining, toDelete) = app.attempts.partition { attempt =>
          attempt.info.lastUpdated.getTime() >= maxTime
        }
        deleteAttemptLogs(app, remaining, toDelete)
      }
    }
  }

  /**
   * Delete event logs from the log directory according to the clean policy defined by the user.
   */
  private[history] def cleanLogs(): Unit = Utils.tryLog {
    val maxTime = clock.getTimeMillis() - conf.get(MAX_LOG_AGE_S) * 1000
    val maxNum = conf.get(MAX_LOG_NUM)

    val expired = KVUtils.viewToSeq(listing.view(classOf[ApplicationInfoWrapper])
      .index("oldestAttempt")
      .reverse()
      .first(maxTime))
    expired.foreach { app =>
      // Applications may have multiple attempts, some of which may not need to be deleted yet.
      val (remaining, toDelete) = app.attempts.partition { attempt =>
        attempt.info.lastUpdated.getTime() >= maxTime
      }
      deleteAttemptLogs(app, remaining, toDelete)
    }

    // Delete log files that don't have a valid application and exceed the configured max age.
    val stale = KVUtils.viewToSeq(listing.view(classOf[LogInfo])
      .index("lastProcessed")
      .reverse()
      .first(maxTime), Int.MaxValue) { l => l.logType == null || l.logType == LogType.EventLogs }
    stale.filterNot(isProcessing).foreach { log =>
      if (log.appId.isEmpty) {
        logInfo(s"Deleting invalid / corrupt event log ${log.logPath}")
        deleteLog(fs, new Path(log.logPath))
        listing.delete(classOf[LogInfo], log.logPath)
      }
    }

    // If the number of files is bigger than MAX_LOG_NUM,
    // clean up all completed attempts per application one by one.
    val num = KVUtils.size(listing.view(classOf[LogInfo]).index("lastProcessed"))
    var count = num - maxNum
    if (count > 0) {
      logInfo(s"Try to delete $count old event logs to keep $maxNum logs in total.")
      KVUtils.foreach(listing.view(classOf[ApplicationInfoWrapper]).index("oldestAttempt")) { app =>
        if (count > 0) {
          // Applications may have multiple attempts, some of which may not be completed yet.
          val (toDelete, remaining) = app.attempts.partition(_.info.completed)
          count -= deleteAttemptLogs(app, remaining, toDelete)
        }
      }
      if (count > 0) {
        logWarning(s"Fail to clean up according to MAX_LOG_NUM policy ($maxNum).")
      }
    }

    // Clean the inaccessibleList from the expired entries.
    clearInaccessibleList(CLEAN_INTERVAL_S)
  }

  private def deleteAttemptLogs(
      app: ApplicationInfoWrapper,
      remaining: List[AttemptInfoWrapper],
      toDelete: List[AttemptInfoWrapper]): Int = {
    if (remaining.nonEmpty) {
      val newApp = new ApplicationInfoWrapper(app.info, remaining)
      listing.write(newApp)
    }

    var countDeleted = 0
    toDelete.foreach { attempt =>
      logInfo(s"Deleting expired event log for ${attempt.logPath}")
      val logPath = new Path(logDir, attempt.logPath)
      listing.delete(classOf[LogInfo], logPath.toString())
      cleanAppData(app.id, attempt.info.attemptId, logPath.toString())
      if (deleteLog(fs, logPath)) {
        countDeleted += 1
      }
    }

    if (remaining.isEmpty) {
      listing.delete(app.getClass(), app.id)
    }

    countDeleted
  }

  /**
   * Delete driver logs from the configured spark dfs dir that exceed the configured max age
   */
  private[history] def cleanDriverLogs(): Unit = Utils.tryLog {
    val driverLogDir = conf.get(DRIVER_LOG_DFS_DIR).get
    val driverLogFs = new Path(driverLogDir).getFileSystem(hadoopConf)
    val currentTime = clock.getTimeMillis()
    val maxTime = currentTime - conf.get(MAX_DRIVER_LOG_AGE_S) * 1000
    val logFiles = driverLogFs.listLocatedStatus(new Path(driverLogDir))
    while (logFiles.hasNext()) {
      val f = logFiles.next()
      // Do not rely on 'modtime' as it is not updated for all filesystems when files are written to
      val deleteFile =
        try {
          val info = listing.read(classOf[LogInfo], f.getPath().toString())
          // Update the lastprocessedtime of file if it's length or modification time has changed
          if (info.fileSize < f.getLen() || info.lastProcessed < f.getModificationTime()) {
            listing.write(
              info.copy(lastProcessed = currentTime, fileSize = f.getLen()))
            false
          } else if (info.lastProcessed > maxTime) {
            false
          } else {
            true
          }
        } catch {
          case e: NoSuchElementException =>
            // For every new driver log file discovered, create a new entry in listing
            listing.write(LogInfo(f.getPath().toString(), currentTime, LogType.DriverLogs, None,
              None, f.getLen(), None, None, false))
          false
        }
      if (deleteFile) {
        logInfo(s"Deleting expired driver log for: ${f.getPath().getName()}")
        listing.delete(classOf[LogInfo], f.getPath().toString())
        deleteLog(driverLogFs, f.getPath())
      }
    }

    // Delete driver log file entries that exceed the configured max age and
    // may have been deleted on filesystem externally.
    val stale = KVUtils.viewToSeq(listing.view(classOf[LogInfo])
      .index("lastProcessed")
      .reverse()
      .first(maxTime), Int.MaxValue) { l => l.logType != null && l.logType == LogType.DriverLogs }
    stale.filterNot(isProcessing).foreach { log =>
      logInfo(s"Deleting invalid driver log ${log.logPath}")
      listing.delete(classOf[LogInfo], log.logPath)
      deleteLog(driverLogFs, new Path(log.logPath))
    }
  }

  /**
   * Rebuilds the application state store from its event log. Exposed for testing.
   */
  private[spark] def rebuildAppStore(
      store: KVStore,
      reader: EventLogFileReader,
      lastUpdated: Long): Unit = {
    // Disable async updates, since they cause higher memory usage, and it's ok to take longer
    // to parse the event logs in the SHS.
    val replayConf = conf.clone().set(ASYNC_TRACKING_ENABLED, false)
    val trackingStore = new ElementTrackingStore(store, replayConf)
    val replayBus = new ReplayListenerBus()
    val listener = new AppStatusListener(trackingStore, replayConf, false,
      lastUpdateTime = Some(lastUpdated))
    replayBus.addListener(listener)

    for {
      plugin <- loadPlugins()
      listener <- plugin.createListeners(conf, trackingStore)
    } replayBus.addListener(listener)

    try {
      val eventLogFiles = reader.listEventLogFiles
      logInfo(s"Parsing ${reader.rootPath} to re-build UI...")
      parseAppEventLogs(eventLogFiles, replayBus, !reader.completed)
      trackingStore.close(false)
      logInfo(s"Finished parsing ${reader.rootPath}")
    } catch {
      case e: Exception =>
        Utils.tryLogNonFatalError {
          trackingStore.close()
        }
        throw e
    }
  }

  private def parseAppEventLogs(
      logFiles: Seq[FileStatus],
      replayBus: ReplayListenerBus,
      maybeTruncated: Boolean,
      eventsFilter: ReplayEventsFilter = SELECT_ALL_FILTER): Unit = {
    // stop replaying next log files if ReplayListenerBus indicates some error or halt
    var continueReplay = true
    logFiles.foreach { file =>
      if (continueReplay) {
        Utils.tryWithResource(EventLogFileReader.openEventLog(file.getPath, fs)) { in =>
          continueReplay = replayBus.replay(in, file.getPath.toString,
            maybeTruncated = maybeTruncated, eventsFilter = eventsFilter)
        }
      }
    }
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

  private[history] def isFsInSafeMode(dfs: DistributedFileSystem): Boolean = {
    /* true to check only for Active NNs status */
    dfs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_GET, true)
  }

  /**
   * String description for diagnostics
   * @return a summary of the component state
   */
  override def toString: String = {
    val count = listing.count(classOf[ApplicationInfoWrapper])
    s"""|FsHistoryProvider{logdir=$logDir,
        |  storedir=$storePath,
        |  last scan time=$lastScanTime
        |  application count=$count}""".stripMargin
  }

  private def load(appId: String): ApplicationInfoWrapper = {
    listing.read(classOf[ApplicationInfoWrapper], appId)
  }

  /**
   * Write the app's information to the given store. Serialized to avoid the (notedly rare) case
   * where two threads are processing separate attempts of the same application.
   */
  private def addListing(app: ApplicationInfoWrapper): Unit = listing.synchronized {
    val attempt = app.attempts.head

    val oldApp = try {
      load(app.id)
    } catch {
      case _: NoSuchElementException =>
        app
    }

    def compareAttemptInfo(a1: AttemptInfoWrapper, a2: AttemptInfoWrapper): Boolean = {
      a1.info.startTime.getTime() > a2.info.startTime.getTime()
    }

    val attempts = oldApp.attempts.filter(_.info.attemptId != attempt.info.attemptId) ++
      List(attempt)

    val newAppInfo = new ApplicationInfoWrapper(
      app.info,
      attempts.sortWith(compareAttemptInfo))
    listing.write(newAppInfo)
  }

  private def loadDiskStore(
      dm: HistoryServerDiskManager,
      appId: String,
      attempt: AttemptInfoWrapper): KVStore = {
    val metadata = new AppStatusStoreMetadata(AppStatusStore.CURRENT_VERSION)

    // First check if the store already exists and try to open it. If that fails, then get rid of
    // the existing data.
    dm.openStore(appId, attempt.info.attemptId).foreach { path =>
      try {
        return KVUtils.open(path, metadata, conf)
      } catch {
        case e: Exception =>
          logInfo(s"Failed to open existing store for $appId/${attempt.info.attemptId}.", e)
          dm.release(appId, attempt.info.attemptId, delete = true)
      }
    }

    // At this point the disk data either does not exist or was deleted because it failed to
    // load, so the event log needs to be replayed.

    // If the hybrid store is enabled, try it first and fail back to leveldb store.
    if (hybridStoreEnabled) {
      try {
        return createHybridStore(dm, appId, attempt, metadata)
      } catch {
        case e: Exception =>
          logInfo(s"Failed to create HybridStore for $appId/${attempt.info.attemptId}." +
            s" Using $hybridStoreDiskBackend.", e)
      }
    }

    createDiskStore(dm, appId, attempt, metadata)
  }

  private def createHybridStore(
      dm: HistoryServerDiskManager,
      appId: String,
      attempt: AttemptInfoWrapper,
      metadata: AppStatusStoreMetadata): KVStore = {
    var retried = false
    var hybridStore: HybridStore = null
    val reader = EventLogFileReader(fs, new Path(logDir, attempt.logPath),
      attempt.lastIndex)

    // Use InMemoryStore to rebuild app store
    while (hybridStore == null) {
      // A RuntimeException will be thrown if the heap memory is not sufficient
      memoryManager.lease(appId, attempt.info.attemptId, reader.totalSize,
        reader.compressionCodec)
      var store: HybridStore = null
      try {
        store = new HybridStore()
        rebuildAppStore(store, reader, attempt.info.lastUpdated.getTime())
        hybridStore = store
      } catch {
        case _: IOException if !retried =>
          // compaction may touch the file(s) which app rebuild wants to read
          // compaction wouldn't run in short interval, so try again...
          logWarning(s"Exception occurred while rebuilding log path ${attempt.logPath} - " +
            "trying again...")
          store.close()
          memoryManager.release(appId, attempt.info.attemptId)
          retried = true
        case e: Exception =>
          store.close()
          memoryManager.release(appId, attempt.info.attemptId)
          throw e
      }
    }

    // Create a disk-base KVStore and start a background thread to dump data to it
    var lease: dm.Lease = null
    try {
      logInfo(s"Leasing disk manager space for app $appId / ${attempt.info.attemptId}...")
      lease = dm.lease(reader.totalSize, reader.compressionCodec.isDefined)
      val diskStore = KVUtils.open(lease.tmpPath, metadata, conf)
      hybridStore.setDiskStore(diskStore)
      hybridStore.switchToDiskStore(new HybridStore.SwitchToDiskStoreListener {
        override def onSwitchToDiskStoreSuccess: Unit = {
          logInfo(s"Completely switched to diskStore for app $appId / ${attempt.info.attemptId}.")
          diskStore.close()
          val newStorePath = lease.commit(appId, attempt.info.attemptId)
          hybridStore.setDiskStore(KVUtils.open(newStorePath, metadata, conf))
          memoryManager.release(appId, attempt.info.attemptId)
        }
        override def onSwitchToDiskStoreFail(e: Exception): Unit = {
          logWarning(s"Failed to switch to diskStore for app $appId / ${attempt.info.attemptId}", e)
          diskStore.close()
          lease.rollback()
        }
      }, appId, attempt.info.attemptId)
    } catch {
      case e: Exception =>
        hybridStore.close()
        memoryManager.release(appId, attempt.info.attemptId)
        if (lease != null) {
          lease.rollback()
        }
        throw e
    }

    hybridStore
  }

  private def createDiskStore(
      dm: HistoryServerDiskManager,
      appId: String,
      attempt: AttemptInfoWrapper,
      metadata: AppStatusStoreMetadata): KVStore = {
    var retried = false
    var newStorePath: File = null
    while (newStorePath == null) {
      val reader = EventLogFileReader(fs, new Path(logDir, attempt.logPath),
        attempt.lastIndex)
      val isCompressed = reader.compressionCodec.isDefined
      logInfo(s"Leasing disk manager space for app $appId / ${attempt.info.attemptId}...")
      val lease = dm.lease(reader.totalSize, isCompressed)
      try {
        Utils.tryWithResource(KVUtils.open(lease.tmpPath, metadata, conf)) { store =>
          rebuildAppStore(store, reader, attempt.info.lastUpdated.getTime())
        }
        newStorePath = lease.commit(appId, attempt.info.attemptId)
      } catch {
        case _: IOException if !retried =>
          // compaction may touch the file(s) which app rebuild wants to read
          // compaction wouldn't run in short interval, so try again...
          logWarning(s"Exception occurred while rebuilding app $appId - trying again...")
          lease.rollback()
          retried = true

        case e: Exception =>
          lease.rollback()
          throw e
      }
    }

    KVUtils.open(newStorePath, metadata, conf)
  }

  private def createInMemoryStore(attempt: AttemptInfoWrapper): KVStore = {
    var retried = false
    var store: KVStore = null
    while (store == null) {
      try {
        val s = new InMemoryStore()
        val reader = EventLogFileReader(fs, new Path(logDir, attempt.logPath),
          attempt.lastIndex)
        rebuildAppStore(s, reader, attempt.info.lastUpdated.getTime())
        store = s
      } catch {
        case _: IOException if !retried =>
          // compaction may touch the file(s) which app rebuild wants to read
          // compaction wouldn't run in short interval, so try again...
          logWarning(s"Exception occurred while rebuilding log path ${attempt.logPath} - " +
            "trying again...")
          retried = true

        case e: Exception =>
          throw e
      }
    }

    store
  }

  private def loadPlugins(): Iterable[AppHistoryServerPlugin] = {
    ServiceLoader.load(classOf[AppHistoryServerPlugin], Utils.getContextOrSparkClassLoader).asScala
  }

  /** For testing. Returns internal data about a single attempt. */
  private[history] def getAttempt(appId: String, attemptId: Option[String]): AttemptInfoWrapper = {
    load(appId).attempts.find(_.info.attemptId == attemptId).getOrElse(
      throw new NoSuchElementException(s"Cannot find attempt $attemptId of $appId."))
  }

  private def deleteLog(fs: FileSystem, log: Path): Boolean = {
    var deleted = false
    if (!isAccessible(log)) {
      logDebug(s"Skipping deleting $log as we don't have permissions on it.")
    } else {
      try {
        deleted = fs.delete(log, true)
      } catch {
        case _: AccessControlException =>
          logInfo(s"No permission to delete $log, ignoring.")
        case ioe: IOException =>
          logError(s"IOException in cleaning $log", ioe)
      }
    }
    deleted
  }

  /** NOTE: 'task' should ensure it executes 'endProcessing' at the end */
  private def submitLogProcessTask(rootPath: Path)(task: Runnable): Unit = {
    try {
      processing(rootPath)
      replayExecutor.submit(task)
    } catch {
      // let the iteration over the updated entries break, since an exception on
      // replayExecutor.submit (..) indicates the ExecutorService is unable
      // to take any more submissions at this time
      case e: Exception =>
        logError(s"Exception while submitting task", e)
        endProcessing(rootPath)
    }
  }

  private def createSecurityManager(conf: SparkConf,
      attempt: AttemptInfoWrapper): SecurityManager = {
    val secManager = new SecurityManager(conf)
    secManager.setAcls(historyUiAclsEnable)
    // make sure to set admin acls before view acls so they are properly picked up
    secManager.setAdminAcls(historyUiAdminAcls ++ stringToSeq(attempt.adminAcls.getOrElse("")))
    secManager.setViewAcls(attempt.info.sparkUser, stringToSeq(attempt.viewAcls.getOrElse("")))
    secManager.setAdminAclsGroups(historyUiAdminAclsGroups ++
      stringToSeq(attempt.adminAclsGroups.getOrElse("")))
    secManager.setViewAclsGroups(stringToSeq(attempt.viewAclsGroups.getOrElse("")))
    secManager
  }
}

private[spark] object FsHistoryProvider {

  private val APPL_START_EVENT_PREFIX = "{\"Event\":\"SparkListenerApplicationStart\""

  private val APPL_END_EVENT_PREFIX = "{\"Event\":\"SparkListenerApplicationEnd\""

  private val LOG_START_EVENT_PREFIX = "{\"Event\":\"SparkListenerLogStart\""

  private val ENV_UPDATE_EVENT_PREFIX = "{\"Event\":\"SparkListenerEnvironmentUpdate\","

  /**
   * Current version of the data written to the listing database. When opening an existing
   * db, if the version does not match this value, the FsHistoryProvider will throw away
   * all data and re-generate the listing data from the event logs.
   */
  val CURRENT_LISTING_VERSION = 1L
}

private[spark] case class FsHistoryProviderMetadata(
    version: Long,
    uiVersion: Long,
    logDir: String)

private[history] object LogType extends Enumeration {
  val DriverLogs, EventLogs = Value
}

/**
 * Tracking info for event logs detected in the configured log directory. Tracks both valid and
 * invalid logs (e.g. unparseable logs, recorded as logs with no app ID) so that the cleaner
 * can know what log files are safe to delete.
 */
private[history] case class LogInfo(
    @KVIndexParam logPath: String,
    @KVIndexParam("lastProcessed") lastProcessed: Long,
    logType: LogType.Value,
    appId: Option[String],
    attemptId: Option[String],
    fileSize: Long,
    @JsonDeserialize(contentAs = classOf[JLong])
    lastIndex: Option[Long],
    @JsonDeserialize(contentAs = classOf[JLong])
    lastEvaluatedForCompaction: Option[Long],
    isComplete: Boolean)

private[history] class AttemptInfoWrapper(
    val info: ApplicationAttemptInfo,
    val logPath: String,
    val fileSize: Long,
    @JsonDeserialize(contentAs = classOf[JLong])
    val lastIndex: Option[Long],
    val adminAcls: Option[String],
    val viewAcls: Option[String],
    val adminAclsGroups: Option[String],
    val viewAclsGroups: Option[String])

private[history] class ApplicationInfoWrapper(
    val info: ApplicationInfo,
    val attempts: List[AttemptInfoWrapper]) {

  @JsonIgnore @KVIndexParam
  def id: String = info.id

  @JsonIgnore @KVIndexParam("endTime")
  def endTime(): Long = attempts.head.info.endTime.getTime()

  @JsonIgnore @KVIndexParam("oldestAttempt")
  def oldestAttempt(): Long = attempts.map(_.info.lastUpdated.getTime()).min

  def toApplicationInfo(): ApplicationInfo = info.copy(attempts = attempts.map(_.info))

}

private[history] class AppListingListener(
    reader: EventLogFileReader,
    clock: Clock,
    haltEnabled: Boolean) extends SparkListener {

  private val app = new MutableApplicationInfo()
  private val attempt = new MutableAttemptInfo(reader.rootPath.getName(),
    reader.fileSizeForLastIndex, reader.lastIndex)

  private var gotEnvUpdate = false
  private var halted = false

  override def onApplicationStart(event: SparkListenerApplicationStart): Unit = {
    app.id = event.appId.orNull
    app.name = event.appName

    attempt.attemptId = event.appAttemptId
    attempt.startTime = new Date(event.time)
    attempt.lastUpdated = new Date(clock.getTimeMillis())
    attempt.sparkUser = event.sparkUser

    checkProgress()
  }

  override def onApplicationEnd(event: SparkListenerApplicationEnd): Unit = {
    attempt.endTime = new Date(event.time)
    attempt.lastUpdated = new Date(reader.modificationTime)
    attempt.duration = event.time - attempt.startTime.getTime()
    attempt.completed = true
  }

  override def onEnvironmentUpdate(event: SparkListenerEnvironmentUpdate): Unit = {
    // Only parse the first env update, since any future changes don't have any effect on
    // the ACLs set for the UI.
    if (!gotEnvUpdate) {
      def emptyStringToNone(strOption: Option[String]): Option[String] = strOption match {
        case Some("") => None
        case _ => strOption
      }

      val allProperties = event.environmentDetails("Spark Properties").toMap
      attempt.viewAcls = emptyStringToNone(allProperties.get(UI_VIEW_ACLS.key))
      attempt.adminAcls = emptyStringToNone(allProperties.get(ADMIN_ACLS.key))
      attempt.viewAclsGroups = emptyStringToNone(allProperties.get(UI_VIEW_ACLS_GROUPS.key))
      attempt.adminAclsGroups = emptyStringToNone(allProperties.get(ADMIN_ACLS_GROUPS.key))

      gotEnvUpdate = true
      checkProgress()
    }
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case SparkListenerLogStart(sparkVersion) =>
      attempt.appSparkVersion = sparkVersion
    case _ =>
  }

  def applicationInfo: Option[ApplicationInfoWrapper] = {
    if (app.id != null) {
      Some(app.toView())
    } else {
      None
    }
  }

  /**
   * Throws a halt exception to stop replay if enough data to create the app listing has been
   * read.
   */
  private def checkProgress(): Unit = {
    if (haltEnabled && !halted && app.id != null && gotEnvUpdate) {
      halted = true
      throw new HaltReplayException()
    }
  }

  private class MutableApplicationInfo {
    var id: String = null
    var name: String = null

    def toView(): ApplicationInfoWrapper = {
      val apiInfo = ApplicationInfo(id, name, None, None, None, None, Nil)
      new ApplicationInfoWrapper(apiInfo, List(attempt.toView()))
    }

  }

  private class MutableAttemptInfo(logPath: String, fileSize: Long, lastIndex: Option[Long]) {
    var attemptId: Option[String] = None
    var startTime = new Date(-1)
    var endTime = new Date(-1)
    var lastUpdated = new Date(-1)
    var duration = 0L
    var sparkUser: String = null
    var completed = false
    var appSparkVersion = ""

    var adminAcls: Option[String] = None
    var viewAcls: Option[String] = None
    var adminAclsGroups: Option[String] = None
    var viewAclsGroups: Option[String] = None

    def toView(): AttemptInfoWrapper = {
      val apiInfo = ApplicationAttemptInfo(
        attemptId,
        startTime,
        endTime,
        lastUpdated,
        duration,
        sparkUser,
        completed,
        appSparkVersion)
      new AttemptInfoWrapper(
        apiInfo,
        logPath,
        fileSize,
        lastIndex,
        adminAcls,
        viewAcls,
        adminAclsGroups,
        viewAclsGroups)
    }

  }

}
