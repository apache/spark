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

package org.apache.spark.sql.execution.streaming.state

import java.io.File
import java.util.Locale
import javax.annotation.concurrent.GuardedBy

import scala.collection.{mutable, Map}
import scala.collection.JavaConverters._
import scala.ref.WeakReference
import scala.util.Try

import org.apache.hadoop.conf.Configuration
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.rocksdb.{RocksDB => NativeRocksDB, _}
import org.rocksdb.TickerType._

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.util.{NextIterator, Utils}

/**
 * Class representing a RocksDB instance that checkpoints version of data to DFS.
 * After a set of updates, a new version can be committed by calling `commit()`.
 * Any past version can be loaded by calling `load(version)`.
 *
 * @note This class is not thread-safe, so use it only from one thread.
 * @see [[RocksDBFileManager]] to see how the files are laid out in local disk and DFS.
 * @param dfsRootDir  Remote directory where checkpoints are going to be written
 * @param conf         Configuration for RocksDB
 * @param localRootDir Root directory in local disk that is used to working and checkpointing dirs
 * @param hadoopConf   Hadoop configuration for talking to the remote file system
 * @param loggingId    Id that will be prepended in logs for isolating concurrent RocksDBs
 */
class RocksDB(
    dfsRootDir: String,
    val conf: RocksDBConf,
    localRootDir: File = Utils.createTempDir(),
    hadoopConf: Configuration = new Configuration,
    loggingId: String = "") extends Logging {

  RocksDBLoader.loadLibrary()

  // Java wrapper objects linking to native RocksDB objects
  private val readOptions = new ReadOptions()  // used for gets
  private val writeOptions = new WriteOptions().setSync(true)  // wait for batched write to complete
  private val flushOptions = new FlushOptions().setWaitForFlush(true)  // wait for flush to complete
  private val writeBatch = new WriteBatchWithIndex(true)  // overwrite multiple updates to a key

  private val bloomFilter = new BloomFilter()
  private val tableFormatConfig = new BlockBasedTableConfig()
  tableFormatConfig.setBlockSize(conf.blockSizeKB * 1024)
  tableFormatConfig.setBlockCache(new LRUCache(conf.blockCacheSizeMB * 1024 * 1024))
  tableFormatConfig.setFilterPolicy(bloomFilter)
  tableFormatConfig.setFormatVersion(conf.formatVersion)

  private val dbOptions = new Options() // options to open the RocksDB
  dbOptions.setCreateIfMissing(true)
  dbOptions.setTableFormatConfig(tableFormatConfig)
  private val dbLogger = createLogger() // for forwarding RocksDB native logs to log4j
  dbOptions.setStatistics(new Statistics())
  private val nativeStats = dbOptions.statistics()

  private val workingDir = createTempDir("workingDir")
  private val fileManager = new RocksDBFileManager(
    dfsRootDir, createTempDir("fileManager"), hadoopConf, loggingId = loggingId)
  private val byteArrayPair = new ByteArrayPair()
  private val commitLatencyMs = new mutable.HashMap[String, Long]()
  private val acquireLock = new Object

  @volatile private var db: NativeRocksDB = _
  @volatile private var loadedVersion = -1L   // -1 = nothing valid is loaded
  @volatile private var numKeysOnLoadedVersion = 0L
  @volatile private var numKeysOnWritingVersion = 0L
  @volatile private var fileManagerMetrics = RocksDBFileManagerMetrics.EMPTY_METRICS

  @GuardedBy("acquireLock")
  @volatile private var acquiredThreadInfo: AcquiredThreadInfo = _

  private val prefixScanReuseIter =
    new java.util.concurrent.ConcurrentHashMap[Long, RocksIterator]()

  /**
   * Load the given version of data in a native RocksDB instance.
   * Note that this will copy all the necessary file from DFS to local disk as needed,
   * and possibly restart the native RocksDB instance.
   */
  def load(version: Long): RocksDB = {
    assert(version >= 0)
    acquire()
    logInfo(s"Loading $version")
    try {
      if (loadedVersion != version) {
        closeDB()
        val metadata = fileManager.loadCheckpointFromDfs(version, workingDir)
        openDB()

        val numKeys = if (!conf.trackTotalNumberOfRows) {
          // we don't track the total number of rows - discard the number being track
          -1L
        } else if (metadata.numKeys < 0) {
          // we track the total number of rows, but the snapshot doesn't have tracking number
          // need to count keys now
          countKeys()
        } else {
          metadata.numKeys
        }
        numKeysOnWritingVersion = numKeys
        numKeysOnLoadedVersion = numKeys

        loadedVersion = version
        fileManagerMetrics = fileManager.latestLoadCheckpointMetrics
      }
      if (conf.resetStatsOnLoad) {
        nativeStats.reset
      }
      // reset resources to prevent side-effects from previous loaded version
      closePrefixScanIterators()
      writeBatch.clear()
      logInfo(s"Loaded $version")
    } catch {
      case t: Throwable =>
        loadedVersion = -1  // invalidate loaded data
        throw t
    }
    this
  }

  /**
   * Get the value for the given key if present, or null.
   * @note This will return the last written value even if it was uncommitted.
   */
  def get(key: Array[Byte]): Array[Byte] = {
    writeBatch.getFromBatchAndDB(db, readOptions, key)
  }

  /**
   * Put the given value for the given key.
   * @note This update is not committed to disk until commit() is called.
   */
  def put(key: Array[Byte], value: Array[Byte]): Unit = {
    if (conf.trackTotalNumberOfRows) {
      val oldValue = writeBatch.getFromBatchAndDB(db, readOptions, key)
      if (oldValue == null) {
        numKeysOnWritingVersion += 1
      }
    }
    writeBatch.put(key, value)
  }

  /**
   * Remove the key if present.
   * @note This update is not committed to disk until commit() is called.
   */
  def remove(key: Array[Byte]): Unit = {
    if (conf.trackTotalNumberOfRows) {
      val value = writeBatch.getFromBatchAndDB(db, readOptions, key)
      if (value != null) {
        numKeysOnWritingVersion -= 1
      }
    }
    writeBatch.remove(key)
  }

  /**
   * Get an iterator of all committed and uncommitted key-value pairs.
   */
  def iterator(): Iterator[ByteArrayPair] = {
    val iter = writeBatch.newIteratorWithBase(db.newIterator())
    logInfo(s"Getting iterator from version $loadedVersion")
    iter.seekToFirst()

    // Attempt to close this iterator if there is a task failure, or a task interruption.
    // This is a hack because it assumes that the RocksDB is running inside a task.
    Option(TaskContext.get()).foreach { tc =>
      tc.addTaskCompletionListener[Unit] { _ => iter.close() }
    }

    new NextIterator[ByteArrayPair] {
      override protected def getNext(): ByteArrayPair = {
        if (iter.isValid) {
          byteArrayPair.set(iter.key, iter.value)
          iter.next()
          byteArrayPair
        } else {
          finished = true
          iter.close()
          null
        }
      }
      override protected def close(): Unit = { iter.close() }
    }
  }

  private def countKeys(): Long = {
    // This is being called when opening DB, so doesn't need to deal with writeBatch.
    val iter = db.newIterator()
    try {
      logInfo(s"Counting keys - getting iterator from version $loadedVersion")

      iter.seekToFirst()

      var keys = 0L
      while (iter.isValid) {
        keys += 1
        iter.next()
      }

      keys
    } finally {
      iter.close()
    }
  }

  def prefixScan(prefix: Array[Byte]): Iterator[ByteArrayPair] = {
    val threadId = Thread.currentThread().getId
    val iter = prefixScanReuseIter.computeIfAbsent(threadId, tid => {
      val it = writeBatch.newIteratorWithBase(db.newIterator())
      logInfo(s"Getting iterator from version $loadedVersion for prefix scan on " +
        s"thread ID $tid")
      it
    })

    iter.seek(prefix)

    new NextIterator[ByteArrayPair] {
      override protected def getNext(): ByteArrayPair = {
        if (iter.isValid && iter.key().take(prefix.length).sameElements(prefix)) {
          byteArrayPair.set(iter.key, iter.value)
          iter.next()
          byteArrayPair
        } else {
          finished = true
          null
        }
      }

      override protected def close(): Unit = {}
    }
  }

  /**
   * Commit all the updates made as a version to DFS. The steps it needs to do to commits are:
   * - Write all the updates to the native RocksDB
   * - Flush all changes to disk
   * - Create a RocksDB checkpoint in a new local dir
   * - Sync the checkpoint dir files to DFS
   */
  def commit(): Long = {
    val newVersion = loadedVersion + 1
    val checkpointDir = createTempDir("checkpoint")
    try {
      // Make sure the directory does not exist. Native RocksDB fails if the directory to
      // checkpoint exists.
      Utils.deleteRecursively(checkpointDir)

      logInfo(s"Writing updates for $newVersion")
      val writeTimeMs = timeTakenMs { db.write(writeOptions, writeBatch) }

      logInfo(s"Flushing updates for $newVersion")
      val flushTimeMs = timeTakenMs { db.flush(flushOptions) }

      val compactTimeMs = if (conf.compactOnCommit) {
        logInfo("Compacting")
        timeTakenMs { db.compactRange() }
      } else 0

      logInfo("Pausing background work")
      val pauseTimeMs = timeTakenMs {
        db.pauseBackgroundWork() // To avoid files being changed while committing
      }

      logInfo(s"Creating checkpoint for $newVersion in $checkpointDir")
      val checkpointTimeMs = timeTakenMs {
        val cp = Checkpoint.create(db)
        cp.createCheckpoint(checkpointDir.toString)
      }

      logInfo(s"Syncing checkpoint for $newVersion to DFS")
      val fileSyncTimeMs = timeTakenMs {
        fileManager.saveCheckpointToDfs(checkpointDir, newVersion, numKeysOnWritingVersion)
      }
      numKeysOnLoadedVersion = numKeysOnWritingVersion
      loadedVersion = newVersion
      fileManagerMetrics = fileManager.latestSaveCheckpointMetrics
      commitLatencyMs ++= Map(
        "writeBatch" -> writeTimeMs,
        "flush" -> flushTimeMs,
        "compact" -> compactTimeMs,
        "pause" -> pauseTimeMs,
        "checkpoint" -> checkpointTimeMs,
        "fileSync" -> fileSyncTimeMs
      )
      logInfo(s"Committed $newVersion, stats = ${metrics.json}")
      loadedVersion
    } catch {
      case t: Throwable =>
        loadedVersion = -1  // invalidate loaded version
        throw t
    } finally {
      db.continueBackgroundWork()
      silentDeleteRecursively(checkpointDir, s"committing $newVersion")
      release()
    }
  }

  /**
   * Drop uncommitted changes, and roll back to previous version.
   */
  def rollback(): Unit = {
    closePrefixScanIterators()
    writeBatch.clear()
    numKeysOnWritingVersion = numKeysOnLoadedVersion
    release()
    logInfo(s"Rolled back to $loadedVersion")
  }

  def cleanup(): Unit = {
    val cleanupTime = timeTakenMs {
      fileManager.deleteOldVersions(conf.minVersionsToRetain)
    }
    logInfo(s"Cleaned old data, time taken: $cleanupTime ms")
  }

  /** Release all resources */
  def close(): Unit = {
    closePrefixScanIterators()
    try {
      closeDB()

      // Release all resources related to native RockDB objects
      writeBatch.clear()
      writeBatch.close()
      readOptions.close()
      writeOptions.close()
      flushOptions.close()
      dbOptions.close()
      dbLogger.close()
      silentDeleteRecursively(localRootDir, "closing RocksDB")
    } catch {
      case e: Exception =>
        logWarning("Error closing RocksDB", e)
    }
  }

  /** Get the latest version available in the DFS */
  def getLatestVersion(): Long = fileManager.getLatestVersion()

  /** Get current instantaneous statistics */
  def metrics: RocksDBMetrics = {
    import HistogramType._
    val totalSSTFilesBytes = getDBProperty("rocksdb.total-sst-files-size")
    val readerMemUsage = getDBProperty("rocksdb.estimate-table-readers-mem")
    val memTableMemUsage = getDBProperty("rocksdb.size-all-mem-tables")
    val blockCacheUsage = getDBProperty("rocksdb.block-cache-usage")
    // Get the approximate memory usage of this writeBatchWithIndex
    val writeBatchMemUsage = writeBatch.getWriteBatch.getDataSize
    val nativeOpsHistograms = Seq(
      "get" -> DB_GET,
      "put" -> DB_WRITE,
      "compaction" -> COMPACTION_TIME
    ).toMap
    val nativeOpsLatencyMicros = nativeOpsHistograms.mapValues { typ =>
      RocksDBNativeHistogram(nativeStats.getHistogramData(typ))
    }
    val nativeOpsMetricTickers = Seq(
      /** Number of cache misses that required reading from local disk */
      "readBlockCacheMissCount" -> BLOCK_CACHE_MISS,
      /** Number of cache hits that read data from RocksDB block cache avoiding local disk read */
      "readBlockCacheHitCount" -> BLOCK_CACHE_HIT,
      /** Number of uncompressed bytes read (from memtables/cache/sst) from DB::Get() */
      "totalBytesRead" -> BYTES_READ,
      /** Number of uncompressed bytes issued by DB::{Put(), Delete(), Merge(), Write()} */
      "totalBytesWritten" -> BYTES_WRITTEN,
      /** The number of uncompressed bytes read from an iterator. */
      "totalBytesReadThroughIterator" -> ITER_BYTES_READ,
      /** Duration of writer requiring to wait for compaction or flush to finish. */
      "writerStallDuration" -> STALL_MICROS,
      /** Number of bytes read during compaction */
      "totalBytesReadByCompaction" -> COMPACT_READ_BYTES,
      /** Number of bytes written during compaction */
      "totalBytesWrittenByCompaction" -> COMPACT_WRITE_BYTES
    ).toMap
    val nativeOpsMetrics = nativeOpsMetricTickers.mapValues { typ =>
      nativeStats.getTickerCount(typ)
    }

    RocksDBMetrics(
      numKeysOnLoadedVersion,
      numKeysOnWritingVersion,
      readerMemUsage + memTableMemUsage + blockCacheUsage + writeBatchMemUsage,
      writeBatchMemUsage,
      totalSSTFilesBytes,
      nativeOpsLatencyMicros.toMap,
      commitLatencyMs,
      bytesCopied = fileManagerMetrics.bytesCopied,
      filesCopied = fileManagerMetrics.filesCopied,
      filesReused = fileManagerMetrics.filesReused,
      zipFileBytesUncompressed = fileManagerMetrics.zipFileBytesUncompressed,
      nativeOpsMetrics = nativeOpsMetrics.toMap)
  }

  private def acquire(): Unit = acquireLock.synchronized {
    val newAcquiredThreadInfo = AcquiredThreadInfo()
    val waitStartTime = System.currentTimeMillis
    def timeWaitedMs = System.currentTimeMillis - waitStartTime
    def isAcquiredByDifferentThread = acquiredThreadInfo != null &&
      acquiredThreadInfo.threadRef.get.isDefined &&
      newAcquiredThreadInfo.threadRef.get.get.getId != acquiredThreadInfo.threadRef.get.get.getId

    while (isAcquiredByDifferentThread && timeWaitedMs < conf.lockAcquireTimeoutMs) {
      acquireLock.wait(10)
    }
    if (isAcquiredByDifferentThread) {
      val stackTraceOutput = acquiredThreadInfo.threadRef.get.get.getStackTrace.mkString("\n")
      val msg = s"RocksDB instance could not be acquired by $newAcquiredThreadInfo as it " +
        s"was not released by $acquiredThreadInfo after $timeWaitedMs ms.\n" +
        s"Thread holding the lock has trace: $stackTraceOutput"
      logError(msg)
      throw new IllegalStateException(s"$loggingId: $msg")
    } else {
      acquiredThreadInfo = newAcquiredThreadInfo
      // Add a listener to always release the lock when the task (if active) completes
      Option(TaskContext.get).foreach(_.addTaskCompletionListener[Unit] { _ => this.release() })
      logInfo(s"RocksDB instance was acquired by $acquiredThreadInfo")
    }
  }

  private def release(): Unit = acquireLock.synchronized {
    acquiredThreadInfo = null
    acquireLock.notifyAll()
  }

  private def closePrefixScanIterators(): Unit = {
    prefixScanReuseIter.entrySet().asScala.foreach(_.getValue.close())
    prefixScanReuseIter.clear()
  }

  private def getDBProperty(property: String): Long = {
    db.getProperty(property).toLong
  }

  private def openDB(): Unit = {
    assert(db == null)
    db = NativeRocksDB.open(dbOptions, workingDir.toString)
    logInfo(s"Opened DB with conf ${conf}")
  }

  private def closeDB(): Unit = {
    if (db != null) {
      db.close()
      db = null
    }
  }

  /** Create a native RocksDB logger that forwards native logs to log4j with correct log levels. */
  private def createLogger(): Logger = {
    val dbLogger = new Logger(dbOptions) {
      override def log(infoLogLevel: InfoLogLevel, logMsg: String) = {
        // Map DB log level to log4j levels
        // Warn is mapped to info because RocksDB warn is too verbose
        // (e.g. dumps non-warning stuff like stats)
        val loggingFunc: ( => String) => Unit = infoLogLevel match {
          case InfoLogLevel.FATAL_LEVEL | InfoLogLevel.ERROR_LEVEL => logError(_)
          case InfoLogLevel.WARN_LEVEL | InfoLogLevel.INFO_LEVEL => logInfo(_)
          case InfoLogLevel.DEBUG_LEVEL => logDebug(_)
          case _ => logTrace(_)
        }
        loggingFunc(s"[NativeRocksDB-${infoLogLevel.getValue}] $logMsg")
      }
    }

    var dbLogLevel = InfoLogLevel.ERROR_LEVEL
    if (log.isWarnEnabled) dbLogLevel = InfoLogLevel.WARN_LEVEL
    if (log.isInfoEnabled) dbLogLevel = InfoLogLevel.INFO_LEVEL
    if (log.isDebugEnabled) dbLogLevel = InfoLogLevel.DEBUG_LEVEL
    dbOptions.setLogger(dbLogger)
    dbOptions.setInfoLogLevel(dbLogLevel)
    logInfo(s"Set RocksDB native logging level to $dbLogLevel")
    dbLogger
  }

  /** Create a temp directory inside the local root directory */
  private def createTempDir(prefix: String): File = {
    Utils.createDirectory(localRootDir.getAbsolutePath, prefix)
  }

  /** Attempt to delete recursively, and log the error if any */
  private def silentDeleteRecursively(file: File, msg: String): Unit = {
    try {
      Utils.deleteRecursively(file)
    } catch {
      case e: Exception =>
        logWarning(s"Error recursively deleting local dir $file while $msg", e)
    }
  }

  /** Records the duration of running `body` for the next query progress update. */
  protected def timeTakenMs(body: => Unit): Long = Utils.timeTakenMs(body)._2

  override protected def logName: String = s"${super.logName} $loggingId"
}


/** Mutable and reusable pair of byte arrays */
class ByteArrayPair(var key: Array[Byte] = null, var value: Array[Byte] = null) {
  def set(key: Array[Byte], value: Array[Byte]): ByteArrayPair = {
    this.key = key
    this.value = value
    this
  }
}


/**
 * Configurations for optimizing RocksDB
 * @param compactOnCommit Whether to compact RocksDB data before commit / checkpointing
 */
case class RocksDBConf(
    minVersionsToRetain: Int,
    compactOnCommit: Boolean,
    blockSizeKB: Long,
    blockCacheSizeMB: Long,
    lockAcquireTimeoutMs: Long,
    resetStatsOnLoad : Boolean,
    formatVersion: Int,
    trackTotalNumberOfRows: Boolean)

object RocksDBConf {
  /** Common prefix of all confs in SQLConf that affects RocksDB */
  val ROCKSDB_CONF_NAME_PREFIX = "spark.sql.streaming.stateStore.rocksdb"

  private case class ConfEntry(name: String, default: String) {
    def fullName: String = s"$ROCKSDB_CONF_NAME_PREFIX.${name}".toLowerCase(Locale.ROOT)
  }

  // Configuration that specifies whether to compact the RocksDB data every time data is committed
  private val COMPACT_ON_COMMIT_CONF = ConfEntry("compactOnCommit", "false")
  private val BLOCK_SIZE_KB_CONF = ConfEntry("blockSizeKB", "4")
  private val BLOCK_CACHE_SIZE_MB_CONF = ConfEntry("blockCacheSizeMB", "8")
  private val LOCK_ACQUIRE_TIMEOUT_MS_CONF = ConfEntry("lockAcquireTimeoutMs", "60000")
  private val RESET_STATS_ON_LOAD = ConfEntry("resetStatsOnLoad", "true")
  // Configuration to set the RocksDB format version. When upgrading the RocksDB version in Spark,
  // it may introduce a new table format version that can not be supported by an old RocksDB version
  // used by an old Spark version. Hence, we store the table format version in the checkpoint when
  // a query starts and use it in the entire lifetime of the query. This will ensure the user can
  // still rollback their Spark version for an existing query when RocksDB changes its default
  // table format in a new version. See
  // https://github.com/facebook/rocksdb/wiki/RocksDB-Compatibility-Between-Different-Releases
  // for the RocksDB compatibility guarantee.
  //
  // Note: this is also defined in `SQLConf.STATE_STORE_ROCKSDB_FORMAT_VERSION`. These two
  // places should be updated together.
  private val FORMAT_VERSION = ConfEntry("formatVersion", "5")

  // Flag to enable/disable tracking the total number of rows.
  // When this is enabled, this class does additional lookup on write operations (put/delete) to
  // track the changes of total number of rows, which would help observability on state store.
  // The additional lookups bring non-trivial overhead on write-heavy workloads - if your query
  // does lots of writes on state, it would be encouraged to turn off the config and turn on
  // again when you really need the know the number for observability/debuggability.
  private val TRACK_TOTAL_NUMBER_OF_ROWS = ConfEntry("trackTotalNumberOfRows", "true")

  def apply(storeConf: StateStoreConf): RocksDBConf = {
    val confs = CaseInsensitiveMap[String](storeConf.confs)

    def getBooleanConf(conf: ConfEntry): Boolean = {
      Try { confs.getOrElse(conf.fullName, conf.default).toBoolean } getOrElse {
        throw new IllegalArgumentException(s"Invalid value for '${conf.fullName}', must be boolean")
      }
    }

    def getPositiveLongConf(conf: ConfEntry): Long = {
      Try { confs.getOrElse(conf.fullName, conf.default).toLong } filter { _ >= 0 } getOrElse {
        throw new IllegalArgumentException(
          s"Invalid value for '${conf.fullName}', must be a positive integer")
      }
    }

    def getPositiveIntConf(conf: ConfEntry): Int = {
      Try { confs.getOrElse(conf.fullName, conf.default).toInt } filter { _ >= 0 } getOrElse {
        throw new IllegalArgumentException(
          s"Invalid value for '${conf.fullName}', must be a positive integer")
      }
    }

    RocksDBConf(
      storeConf.minVersionsToRetain,
      getBooleanConf(COMPACT_ON_COMMIT_CONF),
      getPositiveLongConf(BLOCK_SIZE_KB_CONF),
      getPositiveLongConf(BLOCK_CACHE_SIZE_MB_CONF),
      getPositiveLongConf(LOCK_ACQUIRE_TIMEOUT_MS_CONF),
      getBooleanConf(RESET_STATS_ON_LOAD),
      getPositiveIntConf(FORMAT_VERSION),
      getBooleanConf(TRACK_TOTAL_NUMBER_OF_ROWS))
  }

  def apply(): RocksDBConf = apply(new StateStoreConf())
}

/** Class to represent stats from each commit. */
case class RocksDBMetrics(
    numCommittedKeys: Long,
    numUncommittedKeys: Long,
    totalMemUsageBytes: Long,
    writeBatchMemUsageBytes: Long,
    totalSSTFilesBytes: Long,
    nativeOpsHistograms: Map[String, RocksDBNativeHistogram],
    lastCommitLatencyMs: Map[String, Long],
    filesCopied: Long,
    bytesCopied: Long,
    filesReused: Long,
    zipFileBytesUncompressed: Option[Long],
    nativeOpsMetrics: Map[String, Long]) {
  def json: String = Serialization.write(this)(RocksDBMetrics.format)
}

object RocksDBMetrics {
  val format = Serialization.formats(NoTypeHints)
}

/** Class to wrap RocksDB's native histogram */
case class RocksDBNativeHistogram(
    sum: Long, avg: Double, stddev: Double, median: Double, p95: Double, p99: Double, count: Long) {
  def json: String = Serialization.write(this)(RocksDBMetrics.format)
}

object RocksDBNativeHistogram {
  def apply(nativeHist: HistogramData): RocksDBNativeHistogram = {
    RocksDBNativeHistogram(
      nativeHist.getSum,
      nativeHist.getAverage,
      nativeHist.getStandardDeviation,
      nativeHist.getMedian,
      nativeHist.getPercentile95,
      nativeHist.getPercentile99,
      nativeHist.getCount)
  }
}

case class AcquiredThreadInfo() {
  val threadRef: WeakReference[Thread] = new WeakReference[Thread](Thread.currentThread())
  val tc: TaskContext = TaskContext.get()

  override def toString(): String = {
    val taskStr = if (tc != null) {
      val taskDetails =
        s"${tc.partitionId}.${tc.attemptNumber} in stage ${tc.stageId}, TID ${tc.taskAttemptId}"
      s", task: $taskDetails"
    } else ""

    s"[ThreadId: ${threadRef.get.map(_.getId)}$taskStr]"
  }
}

