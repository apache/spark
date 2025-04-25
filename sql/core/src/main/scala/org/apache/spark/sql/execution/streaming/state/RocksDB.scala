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
import java.util.Set
import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}
import javax.annotation.concurrent.GuardedBy

import scala.collection.{mutable, Map}
import scala.jdk.CollectionConverters.ConcurrentMapHasAsScala
import scala.ref.WeakReference
import scala.util.Try

import org.apache.hadoop.conf.Configuration
import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization
import org.rocksdb.{RocksDB => NativeRocksDB, _}
import org.rocksdb.CompressionType._
import org.rocksdb.TickerType._

import org.apache.spark.TaskContext
import org.apache.spark.internal.{LogEntry, Logging, LogKeys, MDC}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.util.{NextIterator, Utils}

// RocksDB operations that could acquire/release the instance lock
sealed abstract class RocksDBOpType(name: String) {
  override def toString: String = name
}
case object LoadStore extends RocksDBOpType("load_store")
case object RollbackStore extends RocksDBOpType("rollback_store")
case object CloseStore extends RocksDBOpType("close_store")
case object ReportStoreMetrics extends RocksDBOpType("report_store_metrics")
case object StoreTaskCompletionListener extends RocksDBOpType("store_task_completion_listener")

/**
 * Class representing a RocksDB instance that checkpoints version of data to DFS.
 * After a set of updates, a new version can be committed by calling `commit()`.
 * Any past version can be loaded by calling `load(version)`.
 *
 * @note This class is not thread-safe, so use it only from one thread.
 * @see [[RocksDBFileManager]] to see how the files are laid out in local disk and DFS.
 * @param conf         Configuration for RocksDB
 * @param stateStoreId StateStoreId for the state store
 * @param localRootDir Root directory in local disk that is used to working and checkpointing dirs
 * @param hadoopConf   Hadoop configuration for talking to the remote file system
 * @param eventForwarder The RocksDBEventForwarder object for reporting events to the coordinator
 */
class RocksDB(
    dfsRootDir: String,
    val conf: RocksDBConf,
    localRootDir: File = Utils.createTempDir(),
    hadoopConf: Configuration = new Configuration,
    loggingId: String = "",
    useColumnFamilies: Boolean = false,
    enableStateStoreCheckpointIds: Boolean = false,
    partitionId: Int = 0,
    eventForwarder: Option[RocksDBEventForwarder] = None) extends Logging {

  import RocksDB._

  @volatile private var lastSnapshotVersion = 0L

  RocksDBLoader.loadLibrary()

  // Java wrapper objects linking to native RocksDB objects
  private val readOptions = new ReadOptions()  // used for gets
  // disable WAL since we flush explicitly on commit
  private val writeOptions = new WriteOptions().setDisableWAL(true)
  private val flushOptions = new FlushOptions().setWaitForFlush(true)  // wait for flush to complete

  private val bloomFilter = new BloomFilter()
  private val tableFormatConfig = new BlockBasedTableConfig()

  private val (writeBufferManager, lruCache) = RocksDBMemoryManager
    .getOrCreateRocksDBMemoryManagerAndCache(conf)

  tableFormatConfig.setBlockSize(conf.blockSizeKB * 1024)
  tableFormatConfig.setBlockCache(lruCache)
  tableFormatConfig.setFilterPolicy(bloomFilter)
  tableFormatConfig.setFormatVersion(conf.formatVersion)

  if (conf.boundedMemoryUsage) {
    tableFormatConfig.setCacheIndexAndFilterBlocks(true)
    tableFormatConfig.setCacheIndexAndFilterBlocksWithHighPriority(true)
    tableFormatConfig.setPinL0FilterAndIndexBlocksInCache(true)
  }

  private[state] val rocksDbOptions = new Options() // options to open the RocksDB

  rocksDbOptions.setCreateIfMissing(true)

  // Set RocksDB options around MemTable memory usage. By default, we let RocksDB
  // use its internal default values for these settings.
  if (conf.writeBufferSizeMB > 0L) {
    rocksDbOptions.setWriteBufferSize(conf.writeBufferSizeMB * 1024 * 1024)
  }

  if (conf.maxWriteBufferNumber > 0L) {
    rocksDbOptions.setMaxWriteBufferNumber(conf.maxWriteBufferNumber)
  }

  rocksDbOptions.setCompressionType(getCompressionType(conf.compression))

  rocksDbOptions.setTableFormatConfig(tableFormatConfig)
  rocksDbOptions.setMaxOpenFiles(conf.maxOpenFiles)
  rocksDbOptions.setAllowFAllocate(conf.allowFAllocate)
  rocksDbOptions.setAvoidFlushDuringShutdown(true)
  rocksDbOptions.setMergeOperator(new StringAppendOperator())

  if (conf.boundedMemoryUsage) {
    rocksDbOptions.setWriteBufferManager(writeBufferManager)
  }

  private val dbLogger = createLogger() // for forwarding RocksDB native logs to log4j
  rocksDbOptions.setStatistics(new Statistics())
  private val nativeStats = rocksDbOptions.statistics()

  private val workingDir = createTempDir("workingDir")

  protected def createFileManager(
      dfsRootDir: String,
      localTempDir: File,
      hadoopConf: Configuration,
      codecName: String,
      loggingId: String): RocksDBFileManager = {
    new RocksDBFileManager(
      dfsRootDir,
      localTempDir,
      hadoopConf,
      codecName,
      loggingId = loggingId
    )
  }

  private[spark] val fileManager = createFileManager(dfsRootDir, createTempDir("fileManager"),
    hadoopConf, conf.compressionCodec, loggingId = loggingId)
  private val byteArrayPair = new ByteArrayPair()
  private val commitLatencyMs = new mutable.HashMap[String, Long]()

  private val acquireLock = new Object

  @volatile private var db: NativeRocksDB = _
  @volatile private var changelogWriter: Option[StateStoreChangelogWriter] = None
  private val enableChangelogCheckpointing: Boolean = conf.enableChangelogCheckpointing
  @volatile protected var loadedVersion: Long = -1L   // -1 = nothing valid is loaded

  // Can be updated by whichever thread uploaded a snapshot, which could be either task,
  // maintenance, or both. -1 represents no version has ever been uploaded.
  protected val lastUploadedSnapshotVersion: AtomicLong = new AtomicLong(-1L)

  // variables to manage checkpoint ID. Once a checkpointing finishes, it needs to return
  // `lastCommittedStateStoreCkptId` as the committed checkpointID, as well as
  // `lastCommitBasedStateStoreCkptId` as the checkpontID of the previous version that is based on.
  // `loadedStateStoreCkptId` is the checkpointID for the current live DB. After the batch finishes
  // and checkpoint finishes, it will turn into `lastCommitBasedStateStoreCkptId`.
  // `sessionStateStoreCkptId` store an ID to be used for future checkpoints. It will be used as
  // `lastCommittedStateStoreCkptId` after the checkpoint is committed. It will be reused until
  // we have to use a new one. We have to update `sessionStateStoreCkptId` if we reload a previous
  // batch version, as we would have to use a new checkpointID for re-committing a version.
  // The reusing is to help debugging but is not required for the algorithm to work.
  protected var lastCommitBasedStateStoreCkptId: Option[String] = None
  protected var lastCommittedStateStoreCkptId: Option[String] = None
  protected var loadedStateStoreCkptId: Option[String] = None
  protected var sessionStateStoreCkptId: Option[String] = None
  protected[sql] val lineageManager: RocksDBLineageManager = new RocksDBLineageManager

  @volatile private var numKeysOnLoadedVersion = 0L
  @volatile private var numKeysOnWritingVersion = 0L

  @volatile private var numInternalKeysOnLoadedVersion = 0L
  @volatile private var numInternalKeysOnWritingVersion = 0L

  @volatile private var fileManagerMetrics = RocksDBFileManagerMetrics.EMPTY_METRICS

  // SPARK-46249 - Keep track of recorded metrics per version which can be used for querying later
  // Updates and access to recordedMetrics are protected by the DB instance lock
  @GuardedBy("acquireLock")
  @volatile private var recordedMetrics: Option[RocksDBMetrics] = None

  @GuardedBy("acquireLock")
  @volatile private var acquiredThreadInfo: AcquiredThreadInfo = _

  // This is accessed and updated only between load and commit
  // which means it is implicitly guarded by acquireLock
  @GuardedBy("acquireLock")
  private val colFamilyNameToInfoMap = new ConcurrentHashMap[String, ColumnFamilyInfo]()

  @GuardedBy("acquireLock")
  private val colFamilyIdToNameMap = new ConcurrentHashMap[Short, String]()

  @GuardedBy("acquireLock")
  private val maxColumnFamilyId: AtomicInteger = new AtomicInteger(-1)

  @GuardedBy("acquireLock")
  private val shouldForceSnapshot: AtomicBoolean = new AtomicBoolean(false)

  private def getColumnFamilyInfo(cfName: String): ColumnFamilyInfo = {
    colFamilyNameToInfoMap.get(cfName)
  }

  private def getColumnFamilyNameForId(cfId: Short): String = {
    colFamilyIdToNameMap.get(cfId)
  }

  private def addToColFamilyMaps(cfName: String, cfId: Short, isInternal: Boolean): Unit = {
    colFamilyNameToInfoMap.putIfAbsent(cfName, ColumnFamilyInfo(cfId, isInternal))
    colFamilyIdToNameMap.putIfAbsent(cfId, cfName)
  }

  private def removeFromColFamilyMaps(cfName: String): Unit = {
    val colFamilyInfo = colFamilyNameToInfoMap.get(cfName)
    if (colFamilyInfo != null) {
      colFamilyNameToInfoMap.remove(cfName)
      colFamilyIdToNameMap.remove(colFamilyInfo.cfId)
    }
  }

  private def clearColFamilyMaps(): Unit = {
    colFamilyNameToInfoMap.clear()
    colFamilyIdToNameMap.clear()
  }

  /**
   * Check if the column family exists with given name and create one if it doesn't. Users can
   * create external column families storing user facing data as well as internal column families
   * such as secondary indexes. Metrics for both of these types are tracked separately.
   *
   * @param colFamilyName - column family name
   * @param isInternal - whether the column family is for internal use or not
   * @return - virtual column family id
   */
  def createColFamilyIfAbsent(colFamilyName: String, isInternal: Boolean): Short = {
    if (!checkColFamilyExists(colFamilyName)) {
      val newColumnFamilyId = maxColumnFamilyId.incrementAndGet().toShort
      addToColFamilyMaps(colFamilyName, newColumnFamilyId, isInternal)
      shouldForceSnapshot.set(true)
      newColumnFamilyId
    } else {
      colFamilyNameToInfoMap.get(colFamilyName).cfId
    }
  }

  /**
   * Remove RocksDB column family, if exists
   * @return columnFamilyId if it exists, else None
   */
  def removeColFamilyIfExists(colFamilyName: String): Boolean = {
    if (checkColFamilyExists(colFamilyName)) {
      shouldForceSnapshot.set(true)
      iterator(colFamilyName).foreach { kv =>
        remove(kv.key, colFamilyName)
      }
      removeFromColFamilyMaps(colFamilyName)
      true
    } else {
      false
    }
  }

  /**
   * Function to check if the column family exists in the state store instance.
   *
   * @param colFamilyName - name of the column family
   * @return - true if the column family exists, false otherwise
   */
  def checkColFamilyExists(colFamilyName: String): Boolean = {
    colFamilyNameToInfoMap.containsKey(colFamilyName)
  }

  // This method sets the internal column family metadata to
  // the default values it should be set to on load
  private def setInitialCFInfo(): Unit = {
    clearColFamilyMaps()
    shouldForceSnapshot.set(false)
    maxColumnFamilyId.set(0)
  }

  def getColFamilyCount(isInternal: Boolean): Long = {
    colFamilyNameToInfoMap.asScala.values.toSeq.count(_.isInternal == isInternal)
  }

  // Mapping of local SST files to DFS files for file reuse.
  // This mapping should only be updated using the Task thread - at version load and commit time.
  // If same mapping instance is updated from different threads,
  // it will result in undefined behavior (and most likely incorrect mapping state).
  @GuardedBy("acquireLock")
  private val rocksDBFileMapping: RocksDBFileMapping = new RocksDBFileMapping()

  // We send snapshots that needs to be uploaded by the maintenance thread to this queue
  private val snapshotsToUploadQueue = new ConcurrentLinkedQueue[RocksDBSnapshot]()

  /**
   * Read the lineage from the changelog files. It first get the changelog reader
   * of the correct changelog version and then read the lineage information from the file.
   * The changelog file is named as version_stateStoreCkptId.changelog
   * @param version version of the changelog file, used to load changelog file.
   * @param stateStoreCkptId uniqueId of the changelog file, used to load changelog file.
   * @return the lineage stored in the changelog file
   */
  private def getLineageFromChangelogFile(
      version: Long,
      stateStoreCkptId: Option[String]): Array[LineageItem] = {
    var changelogReader: StateStoreChangelogReader = null
    var currLineage: Array[LineageItem] = Array.empty
    try {
      changelogReader = fileManager.getChangelogReader(version, stateStoreCkptId)
      currLineage = changelogReader.lineage
      logInfo(log"Loading lineage: " +
        log"${MDC(LogKeys.LINEAGE, lineageManager)} from " +
        log"changelog version: ${MDC(LogKeys.VERSION_NUM, version)} " +
        log"uniqueId: ${MDC(LogKeys.UUID, stateStoreCkptId.getOrElse(""))}.")
    } finally {
      if (changelogReader != null) {
        changelogReader.closeIfNeeded()
      }
    }
    currLineage
  }


  /**
   * Load the given version of data in a native RocksDB instance.
   * Note that this will copy all the necessary file from DFS to local disk as needed,
   * and possibly restart the native RocksDB instance.
   */
  private def loadWithCheckpointId(
      version: Long,
      stateStoreCkptId: Option[String],
      readOnly: Boolean = false): RocksDB = {
    // An array contains lineage information from [snapShotVersion, version]
    // (inclusive in both ends)
    var currVersionLineage: Array[LineageItem] = lineageManager.getLineageForCurrVersion()
    try {
      if (loadedVersion != version || (loadedStateStoreCkptId.isEmpty ||
          stateStoreCkptId.get != loadedStateStoreCkptId.get)) {
        closeDB(ignoreException = false)

        val (latestSnapshotVersion, latestSnapshotUniqueId) = {
          // Special handling when version is 0.
          // When loading the very first version (0), stateStoreCkptId does not need to be defined
          // because there won't be 0.changelog / 0.zip file created in RocksDB under v2.
          if (version == 0) {
            assert(stateStoreCkptId.isEmpty,
              "stateStoreCkptId should be empty when version is zero")
            (0L, None)
          // When there is a snapshot file, it is the ground truth, we can skip
          // reconstructing the lineage from changelog file.
          } else if (fileManager.existsSnapshotFile(version, stateStoreCkptId)) {
            currVersionLineage = Array(LineageItem(version, stateStoreCkptId.get))
            (version, stateStoreCkptId)
          } else {
            currVersionLineage = getLineageFromChangelogFile(version, stateStoreCkptId) :+
              LineageItem(version, stateStoreCkptId.get)
            currVersionLineage = currVersionLineage.sortBy(_.version)

            val latestSnapshotVersionsAndUniqueId =
              fileManager.getLatestSnapshotVersionAndUniqueIdFromLineage(currVersionLineage)
            latestSnapshotVersionsAndUniqueId match {
              case Some(pair) => (pair._1, Option(pair._2))
              case None if currVersionLineage.head.version == 1L =>
                logDebug(log"Cannot find latest snapshot based on lineage but first version " +
                  log"is 1, use 0 as default. Lineage: ${MDC(LogKeys.LINEAGE, lineageManager)}")
                (0L, None)
              case _ =>
                throw QueryExecutionErrors.cannotFindBaseSnapshotCheckpoint(
                  printLineageItems(currVersionLineage))
            }
          }
        }

        logInfo(log"Loaded latestSnapshotVersion: ${
          MDC(LogKeys.SNAPSHOT_VERSION, latestSnapshotVersion)}, latestSnapshotUniqueId: ${
          MDC(LogKeys.UUID, latestSnapshotUniqueId)}")

        val metadata = fileManager.loadCheckpointFromDfs(latestSnapshotVersion,
          workingDir, rocksDBFileMapping, latestSnapshotUniqueId)

        loadedVersion = latestSnapshotVersion

        // reset the last snapshot version to the latest available snapshot version
        lastSnapshotVersion = latestSnapshotVersion
        lineageManager.resetLineage(currVersionLineage)

        // Initialize maxVersion upon successful load from DFS
        fileManager.setMaxSeenVersion(version)

        // Report this snapshot version to the coordinator
        reportSnapshotUploadToCoordinator(latestSnapshotVersion)

        openLocalRocksDB(metadata)

        if (loadedVersion != version) {
          val versionsAndUniqueIds = currVersionLineage.collect {
              case i if i.version > loadedVersion && i.version <= version =>
                (i.version, Option(i.checkpointUniqueId))
            }
          replayChangelog(versionsAndUniqueIds)
          loadedVersion = version
          lineageManager.resetLineage(currVersionLineage)
        }
        // After changelog replay the numKeysOnWritingVersion will be updated to
        // the correct number of keys in the loaded version.
        numKeysOnLoadedVersion = numKeysOnWritingVersion
        numInternalKeysOnLoadedVersion = numInternalKeysOnWritingVersion
        fileManagerMetrics = fileManager.latestLoadCheckpointMetrics
      }

      lastCommitBasedStateStoreCkptId = None
      loadedStateStoreCkptId = stateStoreCkptId
      sessionStateStoreCkptId = Some(java.util.UUID.randomUUID.toString)
      lastCommittedStateStoreCkptId = None

      if (conf.resetStatsOnLoad) {
        nativeStats.reset
      }

      logInfo(log"Loaded ${MDC(LogKeys.VERSION_NUM, version)} " +
        log"with uniqueId ${MDC(LogKeys.UUID, stateStoreCkptId)}")
    } catch {
      case t: Throwable =>
        loadedVersion = -1  // invalidate loaded data
        lastCommitBasedStateStoreCkptId = None
        lastCommittedStateStoreCkptId = None
        loadedStateStoreCkptId = None
        sessionStateStoreCkptId = None
        lineageManager.clear()
        throw t
    }
    if (enableChangelogCheckpointing && !readOnly) {
      // Make sure we don't leak resource.
      changelogWriter.foreach(_.abort())
      // Initialize the changelog writer with lineage info
      // The lineage stored in changelog files should normally start with
      // the version of a snapshot, except for the first few versions.
      // Because they are solely loaded from changelog file.
      // (e.g. with default minDeltasForSnapshot, there is only 1_uuid1.changelog, no 1_uuid1.zip)
      // It should end with exactly one version before the change log's version.
      changelogWriter = Some(fileManager.getChangeLogWriter(
        version + 1,
        useColumnFamilies,
        sessionStateStoreCkptId,
        Some(currVersionLineage)))
    }
    this
  }

  private def loadWithoutCheckpointId(
      version: Long,
      readOnly: Boolean = false): RocksDB = {
    try {
      if (loadedVersion != version) {
        closeDB(ignoreException = false)
        val latestSnapshotVersion = fileManager.getLatestSnapshotVersion(version)
        val metadata = fileManager.loadCheckpointFromDfs(
          latestSnapshotVersion,
          workingDir,
          rocksDBFileMapping)

        loadedVersion = latestSnapshotVersion

        // reset the last snapshot version to the latest available snapshot version
        lastSnapshotVersion = latestSnapshotVersion

        // Initialize maxVersion upon successful load from DFS
        fileManager.setMaxSeenVersion(version)

        // Report this snapshot version to the coordinator
        reportSnapshotUploadToCoordinator(latestSnapshotVersion)

        openLocalRocksDB(metadata)

        if (loadedVersion != version) {
          val versionsAndUniqueIds: Array[(Long, Option[String])] =
            (loadedVersion + 1 to version).map((_, None)).toArray
          replayChangelog(versionsAndUniqueIds)
          loadedVersion = version
        }
        // After changelog replay the numKeysOnWritingVersion will be updated to
        // the correct number of keys in the loaded version.
        numKeysOnLoadedVersion = numKeysOnWritingVersion
        numInternalKeysOnLoadedVersion = numInternalKeysOnWritingVersion
        fileManagerMetrics = fileManager.latestLoadCheckpointMetrics
      }
      if (conf.resetStatsOnLoad) {
        nativeStats.reset
      }
      logInfo(log"Loaded ${MDC(LogKeys.VERSION_NUM, version)}")
    } catch {
      case t: Throwable =>
        loadedVersion = -1  // invalidate loaded data
        throw t
    }
    if (enableChangelogCheckpointing && !readOnly) {
      // Make sure we don't leak resource.
      changelogWriter.foreach(_.abort())
      changelogWriter = Some(fileManager.getChangeLogWriter(version + 1, useColumnFamilies))
    }
    this
  }

  /**
   * Function to check if col family is internal or not based on information recorded in
   * checkpoint metadata.
   * @param cfName - column family name
   * @param metadata - checkpoint metadata
   * @return - type of column family (internal or otherwise)
   */
  private def isInternalColFamily(
      cfName: String,
      metadata: RocksDBCheckpointMetadata): Boolean = {
    if (metadata.columnFamilyTypeMap.isEmpty) {
      false
    } else {
      metadata.columnFamilyTypeMap.get.get(cfName) match {
        case Some(cfType) =>
          cfType
        case None =>
          false
      }
    }
  }

  /**
   * Initialize key metrics based on the metadata loaded from DFS and open local RocksDB.
   */
  private def openLocalRocksDB(metadata: RocksDBCheckpointMetadata): Unit = {
    setInitialCFInfo()
    metadata.columnFamilyMapping.foreach { mapping =>
      mapping.foreach { case (colFamilyName, cfId) =>
        addToColFamilyMaps(colFamilyName, cfId, isInternalColFamily(colFamilyName, metadata))
      }
    }

    metadata.maxColumnFamilyId.foreach { maxId =>
      maxColumnFamilyId.set(maxId)
    }

    openDB()
    // Call this after opening the DB to ensure that forcing snapshot is not triggered
    // unnecessarily.
    if (useColumnFamilies) {
      createColFamilyIfAbsent(StateStore.DEFAULT_COL_FAMILY_NAME, isInternal = false)
    }

    val (numKeys, numInternalKeys) = {
      if (!conf.trackTotalNumberOfRows) {
        // we don't track the total number of rows - discard the number being track
        (-1L, -1L)
      } else if (metadata.numKeys < 0) {
        // we track the total number of rows, but the snapshot doesn't have tracking number
        // need to count keys now
        countKeys()
      } else {
        (metadata.numKeys, metadata.numInternalKeys)
      }
    }
    numKeysOnWritingVersion = numKeys
    numInternalKeysOnWritingVersion = numInternalKeys
  }

  def load(
      version: Long,
      stateStoreCkptId: Option[String] = None,
      readOnly: Boolean = false): RocksDB = {
    assert(version >= 0)
    acquire(LoadStore)
    recordedMetrics = None
    logInfo(log"Loading ${MDC(LogKeys.VERSION_NUM, version)} with stateStoreCkptId: ${
      MDC(LogKeys.UUID, stateStoreCkptId.getOrElse(""))}")
    if (stateStoreCkptId.isDefined || enableStateStoreCheckpointIds && version == 0) {
      loadWithCheckpointId(version, stateStoreCkptId, readOnly)
    } else {
      loadWithoutCheckpointId(version, readOnly)
    }
    this
  }

  /**
   * Load from the start snapshot version and apply all the changelog records to reach the
   * end version. Note that this will copy all the necessary files from DFS to local disk as needed,
   * and possibly restart the native RocksDB instance.
   *
   * @param snapshotVersion version of the snapshot to start with
   * @param endVersion end version
   * @return A RocksDB instance loaded with the state endVersion replayed from snapshotVersion.
   *         Note that the instance will be read-only since this method is only used in State Data
   *         Source.
   */
  def loadFromSnapshot(snapshotVersion: Long, endVersion: Long): RocksDB = {
    assert(snapshotVersion >= 0 && endVersion >= snapshotVersion)
    acquire(LoadStore)
    recordedMetrics = None
    logInfo(
      log"Loading snapshot at version ${MDC(LogKeys.VERSION_NUM, snapshotVersion)} and apply " +
      log"changelog files to version ${MDC(LogKeys.VERSION_NUM, endVersion)}.")
    try {
      replayFromCheckpoint(snapshotVersion, endVersion)

      logInfo(
        log"Loaded snapshot at version ${MDC(LogKeys.VERSION_NUM, snapshotVersion)} and apply " +
        log"changelog files to version ${MDC(LogKeys.VERSION_NUM, endVersion)}.")
    } catch {
      case t: Throwable =>
        loadedVersion = -1  // invalidate loaded data
        throw t
    }
    // Report this snapshot version to the coordinator
    reportSnapshotUploadToCoordinator(snapshotVersion)
    this
  }

  /**
   * Load from the start checkpoint version and apply all the changelog records to reach the
   * end version.
   * If the start version does not exist, it will throw an exception.
   *
   * @param snapshotVersion start checkpoint version
   * @param endVersion end version
   */
  private def replayFromCheckpoint(snapshotVersion: Long, endVersion: Long): Any = {
    closeDB()
    val metadata = fileManager.loadCheckpointFromDfs(snapshotVersion,
      workingDir, rocksDBFileMapping)
    loadedVersion = snapshotVersion
    lastSnapshotVersion = snapshotVersion
    openDB()

    val (numKeys, numInternalKeys) = if (!conf.trackTotalNumberOfRows) {
      // we don't track the total number of rows - discard the number being track
      (-1L, -1L)
    } else if (metadata.numKeys < 0) {
      // we track the total number of rows, but the snapshot doesn't have tracking number
      // need to count keys now
      countKeys()
    } else {
      (metadata.numKeys, metadata.numInternalKeys)
    }
    numKeysOnWritingVersion = numKeys
    numInternalKeysOnWritingVersion = numInternalKeys

    if (loadedVersion != endVersion) {
      val versionsAndUniqueIds: Array[(Long, Option[String])] =
        (loadedVersion + 1 to endVersion).map((_, None)).toArray
      replayChangelog(versionsAndUniqueIds)
      loadedVersion = endVersion
    }
    // After changelog replay the numKeysOnWritingVersion will be updated to
    // the correct number of keys in the loaded version.
    numKeysOnLoadedVersion = numKeysOnWritingVersion
    numInternalKeysOnLoadedVersion = numInternalKeysOnWritingVersion
    fileManagerMetrics = fileManager.latestLoadCheckpointMetrics

    if (conf.resetStatsOnLoad) {
      nativeStats.reset
    }
  }

  /**
   * Replay change log from the loaded version to the target version.
   */
  private def replayChangelog(versionsAndUniqueIds: Array[(Long, Option[String])]): Unit = {
    assert(!versionsAndUniqueIds.isEmpty && versionsAndUniqueIds.head._1 == loadedVersion + 1,
      s"Replay changelog should start from one version after loadedVersion: $loadedVersion," +
        s" but it is not."
    )

    logInfo(log"Replaying changelog from version " +
      log"${MDC(LogKeys.LOADED_VERSION, loadedVersion)} -> " +
      log"${MDC(LogKeys.END_VERSION, versionsAndUniqueIds.lastOption.map(_._1))}")

    versionsAndUniqueIds.foreach { case (v, uniqueId) =>
      logInfo(log"replaying changelog from version ${MDC(LogKeys.VERSION_NUM, v)} with " +
        log"unique Id: ${MDC(LogKeys.UUID, uniqueId)}")

      var changelogReader: StateStoreChangelogReader = null
      try {
        changelogReader = fileManager.getChangelogReader(v, uniqueId)

        if (useColumnFamilies) {
          changelogReader.foreach { case (recordType, key, value) =>
            recordType match {
              case RecordType.PUT_RECORD =>
                put(key, value, includesPrefix = true)

              case RecordType.DELETE_RECORD =>
                remove(key, includesPrefix = true)

              case RecordType.MERGE_RECORD =>
                merge(key, value, includesPrefix = true)
            }
          }
        } else {
          changelogReader.foreach { case (recordType, key, value) =>
            recordType match {
              case RecordType.PUT_RECORD =>
                put(key, value)

              case RecordType.DELETE_RECORD =>
                remove(key)

              case RecordType.MERGE_RECORD =>
                merge(key, value)
            }
          }
        }
      } finally {
        if (changelogReader != null) changelogReader.closeIfNeeded()
      }
    }
  }

  /**
   * Function to encode state row with virtual col family id prefix
   * @param data - passed byte array to be stored in state store
   * @param cfName - name of column family
   * @return - encoded byte array with virtual column family id prefix
   */
  private def encodeStateRowWithPrefix(
      data: Array[Byte],
      cfName: String): Array[Byte] = {
    val cfInfo = getColumnFamilyInfo(cfName)
    RocksDBStateStoreProvider.encodeStateRowWithPrefix(data, cfInfo.cfId)
  }

  /**
   * Function to decode state row with virtual col family id prefix
   * @param data - passed byte array retrieved from state store
   * @return - pair of decoded byte array without virtual column family id prefix
   *           and name of column family
   */
  private def decodeStateRowWithPrefix(data: Array[Byte]): (Array[Byte], String) = {
    val cfId = RocksDBStateStoreProvider.getColumnFamilyBytesAsId(data)
    val cfName = getColumnFamilyNameForId(cfId)
    val key = RocksDBStateStoreProvider.decodeStateRowWithPrefix(data)
    (key, cfName)
  }

  /**
   * Get the value for the given key if present, or null.
   * @note This will return the last written value even if it was uncommitted.
   */
  def get(
      key: Array[Byte],
      cfName: String = StateStore.DEFAULT_COL_FAMILY_NAME): Array[Byte] = {
    val keyWithPrefix = if (useColumnFamilies) {
      encodeStateRowWithPrefix(key, cfName)
    } else {
      key
    }

    db.get(readOptions, keyWithPrefix)
  }

  /**
   * Function to check if value exists for a key or not depending on the operation type.
   * @param oldValue - old value for the key
   * @param isPutOrMerge - flag to indicate if the operation is put or merge
   * @return - true if the value doesn't exist for putAndMerge operation and vice versa for remove
   */
  private def checkExistingEntry(
    oldValue: Array[Byte],
    isPutOrMerge: Boolean): Boolean = {
    if (isPutOrMerge) {
      oldValue == null
    } else {
      oldValue != null
    }
  }

  /**
   * Function to keep track of metrics updates around the number of keys in the store.
   * @param keyWithPrefix - key with prefix
   * @param cfName - column family name
   * @param isPutOrMerge - flag to indicate if the operation is put or merge
   */
  private def handleMetricsUpdate(
      keyWithPrefix: Array[Byte],
      cfName: String,
      isPutOrMerge: Boolean): Unit = {
    val updateCount = if (isPutOrMerge) 1L else -1L
    if (useColumnFamilies) {
      if (conf.trackTotalNumberOfRows) {
        val oldValue = db.get(readOptions, keyWithPrefix)
        if (checkExistingEntry(oldValue, isPutOrMerge)) {
          val cfInfo = getColumnFamilyInfo(cfName)
          if (cfInfo.isInternal) {
            numInternalKeysOnWritingVersion += updateCount
          } else {
            numKeysOnWritingVersion += updateCount
          }
        }
      }
    } else {
      if (conf.trackTotalNumberOfRows) {
        val oldValue = db.get(readOptions, keyWithPrefix)
        if (checkExistingEntry(oldValue, isPutOrMerge)) {
          numKeysOnWritingVersion += updateCount
        }
      }
    }
  }

  /**
   * Put the given value for the given key.
   * @note This update is not committed to disk until commit() is called.
   */
  def put(
      key: Array[Byte],
      value: Array[Byte],
      cfName: String = StateStore.DEFAULT_COL_FAMILY_NAME,
      includesPrefix: Boolean = false): Unit = {
    val keyWithPrefix = if (useColumnFamilies && !includesPrefix) {
      encodeStateRowWithPrefix(key, cfName)
    } else {
      key
    }

    handleMetricsUpdate(keyWithPrefix, cfName, isPutOrMerge = true)
    db.put(writeOptions, keyWithPrefix, value)
    changelogWriter.foreach(_.put(keyWithPrefix, value))
  }

  /**
   * Merge the given value for the given key. This is equivalent to the Atomic
   * Read-Modify-Write operation in RocksDB, known as the "Merge" operation. The
   * modification is appending the provided value to current list of values for
   * the given key.
   *
   * @note This operation requires that the encoder used can decode multiple values for
   * a key from the values byte array.
   *
   * @note This update is not committed to disk until commit() is called.
   */
  def merge(
      key: Array[Byte],
      value: Array[Byte],
      cfName: String = StateStore.DEFAULT_COL_FAMILY_NAME,
      includesPrefix: Boolean = false): Unit = {
    val keyWithPrefix = if (useColumnFamilies && !includesPrefix) {
      encodeStateRowWithPrefix(key, cfName)
    } else {
      key
    }

    handleMetricsUpdate(keyWithPrefix, cfName, isPutOrMerge = true)
    db.merge(writeOptions, keyWithPrefix, value)
    changelogWriter.foreach(_.merge(keyWithPrefix, value))
  }

  /**
   * Remove the key if present.
   * @note This update is not committed to disk until commit() is called.
   */
  def remove(
      key: Array[Byte],
      cfName: String = StateStore.DEFAULT_COL_FAMILY_NAME,
      includesPrefix: Boolean = false): Unit = {
    val keyWithPrefix = if (useColumnFamilies && !includesPrefix) {
      encodeStateRowWithPrefix(key, cfName)
    } else {
      key
    }

    handleMetricsUpdate(keyWithPrefix, cfName, isPutOrMerge = false)
    db.delete(writeOptions, keyWithPrefix)
    changelogWriter.foreach(_.delete(keyWithPrefix))
  }

  /**
   * Get an iterator of all committed and uncommitted key-value pairs.
   */
  def iterator(): Iterator[ByteArrayPair] = {
    val iter = db.newIterator()
    logInfo(log"Getting iterator from version ${MDC(LogKeys.LOADED_VERSION, loadedVersion)}")
    iter.seekToFirst()

    // Attempt to close this iterator if there is a task failure, or a task interruption.
    // This is a hack because it assumes that the RocksDB is running inside a task.
    Option(TaskContext.get()).foreach { tc =>
      tc.addTaskCompletionListener[Unit] { _ => iter.close() }
    }

    new NextIterator[ByteArrayPair] {
      override protected def getNext(): ByteArrayPair = {
        if (iter.isValid) {
          val key = if (useColumnFamilies) {
            decodeStateRowWithPrefix(iter.key)._1
          } else {
            iter.key
          }

          byteArrayPair.set(key, iter.value)
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

  /**
   * Get an iterator of all committed and uncommitted key-value pairs for the given column family.
   */
  def iterator(cfName: String): Iterator[ByteArrayPair] = {
    if (!useColumnFamilies) {
      iterator()
    } else {
      prefixScan(Array.empty[Byte], cfName)
    }
  }

  private def countKeys(): (Long, Long) = {
    val iter = db.newIterator()

    try {
      logInfo(log"Counting keys - getting iterator from version " +
        log"${MDC(LogKeys.LOADED_VERSION, loadedVersion)}")

      iter.seekToFirst()

      var keys = 0L
      var internalKeys = 0L

      if (!useColumnFamilies) {
        while (iter.isValid) {
          keys += 1
          iter.next()
        }
      } else {
        var currCfInfoOpt: Option[(String, ColumnFamilyInfo)] = None
        while (iter.isValid) {
          val (_, cfName) = decodeStateRowWithPrefix(iter.key)
          if (currCfInfoOpt.isEmpty || currCfInfoOpt.get._1 != cfName) {
            currCfInfoOpt = Some((cfName, getColumnFamilyInfo(cfName)))
          }
          if (currCfInfoOpt.get._2.isInternal) {
            internalKeys += 1
          } else {
            keys += 1
          }
          iter.next()
        }
      }

      (keys, internalKeys)
    } finally {
      iter.close()
    }
  }

  def prefixScan(
      prefix: Array[Byte],
      cfName: String = StateStore.DEFAULT_COL_FAMILY_NAME): Iterator[ByteArrayPair] = {
    val iter = db.newIterator()
    val updatedPrefix = if (useColumnFamilies) {
      encodeStateRowWithPrefix(prefix, cfName)
    } else {
      prefix
    }

    iter.seek(updatedPrefix)

    // Attempt to close this iterator if there is a task failure, or a task interruption.
    Option(TaskContext.get()).foreach { tc =>
      tc.addTaskCompletionListener[Unit] { _ => iter.close() }
    }

    new NextIterator[ByteArrayPair] {
      override protected def getNext(): ByteArrayPair = {
        if (iter.isValid && iter.key().take(updatedPrefix.length).sameElements(updatedPrefix)) {
          val key = if (useColumnFamilies) {
            decodeStateRowWithPrefix(iter.key)._1
          } else {
            iter.key
          }

          byteArrayPair.set(key, iter.value)
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

  /**
   * Commit all the updates made as a version to DFS. The steps it needs to do to commits are:
   * - Flush all changes to disk
   * - Create a RocksDB checkpoint in a new local dir
   * - Sync the checkpoint dir files to DFS
   */
  def commit(): (Long, StateStoreCheckpointInfo) = {
    val newVersion = loadedVersion + 1
    try {
      logInfo(log"Flushing updates for ${MDC(LogKeys.VERSION_NUM, newVersion)}")

      var snapshot: Option[RocksDBSnapshot] = None
      if (shouldCreateSnapshot() || shouldForceSnapshot.get()) {
        val (newSnapshot, snapshotLatency) = createSnapshot(newVersion, sessionStateStoreCkptId)
        snapshot = newSnapshot
        commitLatencyMs ++= snapshotLatency
      }

      logInfo(log"Syncing checkpoint for ${MDC(LogKeys.VERSION_NUM, newVersion)} to DFS")
      val fileSyncTimeMs = timeTakenMs {
        if (enableChangelogCheckpointing) {
          // If we have changed the columnFamilyId mapping, we have set a new
          // snapshot and need to upload this to the DFS even if changelog checkpointing
          // is enabled.
          var isUploaded = false
          if (shouldForceSnapshot.get()) {
            assert(snapshot.isDefined)
            uploadSnapshot(snapshot.get)
            isUploaded = true
            shouldForceSnapshot.set(false)
          }

          // ensure that changelog files are always written
          try {
            assert(changelogWriter.isDefined)
            changelogWriter.foreach(_.commit())
            if (!isUploaded) {
              snapshot.foreach(snapshotsToUploadQueue.offer)
            }
          } finally {
            changelogWriter = None
          }
        } else {
          assert(changelogWriter.isEmpty)
          assert(snapshot.isDefined)
          uploadSnapshot(snapshot.get)
        }
      }

      if (enableStateStoreCheckpointIds) {
        lastCommitBasedStateStoreCkptId = loadedStateStoreCkptId
        lastCommittedStateStoreCkptId = sessionStateStoreCkptId
        loadedStateStoreCkptId = sessionStateStoreCkptId
        lineageManager.appendLineageItem(LineageItem(newVersion, sessionStateStoreCkptId.get))
        logInfo(log"Update checkpoint IDs and lineage: ${MDC(
          LogKeys.LOADED_CHECKPOINT_ID, loadedStateStoreCkptId)}," +
          log" ${MDC(LogKeys.LAST_COMMITTED_CHECKPOINT_ID, lastCommittedStateStoreCkptId)}," +
          log" ${MDC(LogKeys.LAST_COMMIT_BASED_CHECKPOINT_ID, lastCommitBasedStateStoreCkptId)}," +
          log" ${MDC(LogKeys.LINEAGE, lineageManager)}")
      }

      // Set maxVersion when checkpoint files are synced to DFS successfully
      // We need to handle this explicitly in RocksDB as we could use different
      // changeLogWriter instances in fileManager instance when committing
      fileManager.setMaxSeenVersion(newVersion)

      numKeysOnLoadedVersion = numKeysOnWritingVersion
      numInternalKeysOnLoadedVersion = numInternalKeysOnWritingVersion
      loadedVersion = newVersion
      commitLatencyMs ++= Map(
        "fileSync" -> fileSyncTimeMs
      )
      recordedMetrics = Some(metrics)
      logInfo(log"Committed ${MDC(LogKeys.VERSION_NUM, newVersion)}, " +
        log"stats = ${MDC(LogKeys.METRICS_JSON, recordedMetrics.get.json)}")
      (loadedVersion, getLatestCheckpointInfo)
    } catch {
      case t: Throwable =>
        loadedVersion = -1  // invalidate loaded version
        throw t
    } finally {
      // reset resources as either 1) we already pushed the changes and it has been committed or
      // 2) commit has failed and the current version is "invalidated".
      release(LoadStore)
    }
  }

  private def shouldCreateSnapshot(): Boolean = {
    if (enableChangelogCheckpointing) {
      assert(changelogWriter.isDefined)
      val newVersion = loadedVersion + 1
      newVersion - lastSnapshotVersion >= conf.minDeltasForSnapshot
    } else true
  }

  private def createSnapshot(
      version: Long,
      checkpointUniqueId: Option[String]): (Option[RocksDBSnapshot], Map[String, Long]) = {
    // Need to flush the change to disk before creating a checkpoint
    // because rocksdb wal is disabled.
    logInfo(log"Flushing updates for ${MDC(LogKeys.VERSION_NUM, version)}")
    val flushTimeMs = timeTakenMs {
      db.flush(flushOptions)
    }
    val compactTimeMs = if (conf.compactOnCommit) {
      logInfo(log"Compacting")
      timeTakenMs { db.compactRange() }
    } else 0L

    var snapshot: Option[RocksDBSnapshot] = None

    val checkpointTimeMs = timeTakenMs {
      val checkpointDir = createTempDir("checkpoint")
      logInfo(log"Creating checkpoint for ${MDC(LogKeys.VERSION_NUM, version)} in " +
        log"${MDC(LogKeys.CHECKPOINT_PATH, checkpointDir)}")
      // Make sure the directory does not exist. Native RocksDB fails if the directory to
      // checkpoint exists.
      Utils.deleteRecursively(checkpointDir)
      // We no longer pause background operation before creating a RocksDB checkpoint because
      // it is unnecessary. The captured snapshot will still be consistent with ongoing
      // background operations.
      val cp = Checkpoint.create(db)
      cp.createCheckpoint(checkpointDir.toString)

      val (dfsFileSuffix, immutableFileMapping) = rocksDBFileMapping.createSnapshotFileMapping(
        fileManager, checkpointDir, version)
      logInfo(log"RocksDB file mapping after creating snapshot file mapping for version " +
        log"${MDC(LogKeys.VERSION_NUM, version)}:\n" +
        log"${MDC(LogKeys.ROCKS_DB_FILE_MAPPING, rocksDBFileMapping)}")

      val newSnapshot = Some(RocksDBSnapshot(
        checkpointDir,
        version,
        numKeysOnWritingVersion,
        numInternalKeysOnWritingVersion,
        colFamilyNameToInfoMap.asScala.toMap,
        maxColumnFamilyId.get().toShort,
        dfsFileSuffix,
        immutableFileMapping,
        checkpointUniqueId))

        snapshot = newSnapshot
        lastSnapshotVersion = version
      }

    (snapshot,
      Map(
        "flush" -> flushTimeMs,
        "compact" -> compactTimeMs,
        "checkpoint" -> checkpointTimeMs
      )
    )
  }

  private[sql] def uploadSnapshot(snapshot: RocksDBSnapshot): Unit = {
    fileManagerMetrics = uploadSnapshot(
      snapshot,
      fileManager,
      rocksDBFileMapping.snapshotsPendingUpload,
      loggingId
    )
  }

  /**
   * Drop uncommitted changes, and roll back to previous version.
   */
  def rollback(): Unit = {
    acquire(RollbackStore)
    try {
      numKeysOnWritingVersion = numKeysOnLoadedVersion
      numInternalKeysOnWritingVersion = numInternalKeysOnLoadedVersion
      loadedVersion = -1L
      lastCommitBasedStateStoreCkptId = None
      lastCommittedStateStoreCkptId = None
      loadedStateStoreCkptId = None
      sessionStateStoreCkptId = None
      lineageManager.clear()
      changelogWriter.foreach(_.abort())
      // Make sure changelogWriter gets recreated next time.
      changelogWriter = None
      logInfo(log"Rolled back to ${MDC(LogKeys.VERSION_NUM, loadedVersion)}")
    } finally {
      release(RollbackStore)
    }
  }

  def doMaintenance(): Unit = {
    if (enableChangelogCheckpointing) {

      var mostRecentSnapshot: Option[RocksDBSnapshot] = None
      var snapshot = snapshotsToUploadQueue.poll()

      // We only want to upload the most recent snapshot and skip the previous ones.
      while (snapshot != null) {
        logDebug(s"RocksDB Maintenance - polled snapshot ${snapshot.version}")
        mostRecentSnapshot.foreach(_.close())
        mostRecentSnapshot = Some(snapshot)
        snapshot = snapshotsToUploadQueue.poll()
      }

      if (mostRecentSnapshot.isDefined) {
        uploadSnapshot(mostRecentSnapshot.get)
      }
    }
    val cleanupTime = timeTakenMs {
      fileManager.deleteOldVersions(
        numVersionsToRetain = conf.minVersionsToRetain,
        minVersionsToDelete = conf.minVersionsToDelete)
    }
    logInfo(log"Cleaned old data, time taken: ${MDC(LogKeys.TIME_UNITS, cleanupTime)} ms")
  }

  /** Release all resources */
  def close(): Unit = {
    // Acquire DB instance lock and release at the end to allow for synchronized access
    acquire(CloseStore)
    try {
      closeDB()

      readOptions.close()
      writeOptions.close()
      flushOptions.close()
      rocksDbOptions.close()
      dbLogger.close()

      var snapshot = snapshotsToUploadQueue.poll()
      while (snapshot != null) {
        snapshot.close()
        snapshot = snapshotsToUploadQueue.poll()
      }

      silentDeleteRecursively(localRootDir, "closing RocksDB")
      // Clear internal maps to reset the state
      clearColFamilyMaps()
    } catch {
      case e: Exception =>
        logWarning("Error closing RocksDB", e)
    } finally {
      release(CloseStore)
    }
  }

  /** Get the latest version available in the DFS */
  def getLatestVersion(): Long = fileManager.getLatestVersion()

  /** Get the write buffer manager and cache */
  def getWriteBufferManagerAndCache(): (WriteBufferManager, Cache) = (writeBufferManager, lruCache)

  /**
   * Called by commit() to retrieve the checkpoint information to be
   * passed back to the stateful operator. It will return the information for the latest
   * state store checkpointing.
   */
  private def getLatestCheckpointInfo: StateStoreCheckpointInfo = {
    StateStoreCheckpointInfo(
      partitionId,
      loadedVersion,
      lastCommittedStateStoreCkptId,
      lastCommitBasedStateStoreCkptId)
  }

  /** Get current instantaneous statistics */
  private def metrics: RocksDBMetrics = {
    import HistogramType._
    val totalSSTFilesBytes = getDBProperty("rocksdb.total-sst-files-size")
    val readerMemUsage = getDBProperty("rocksdb.estimate-table-readers-mem")
    val memTableMemUsage = getDBProperty("rocksdb.size-all-mem-tables")
    val blockCacheUsage = getDBProperty("rocksdb.block-cache-usage")
    val pinnedBlocksMemUsage = getDBProperty("rocksdb.block-cache-pinned-usage")
    val nativeOpsHistograms = Seq(
      "get" -> DB_GET,
      "put" -> DB_WRITE,
      "compaction" -> COMPACTION_TIME
    ).toMap
    val nativeOpsLatencyMicros = nativeOpsHistograms.transform { (_, typ) =>
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
      "totalBytesWrittenByCompaction" -> COMPACT_WRITE_BYTES,
      /** Number of bytes written during flush */
      "totalBytesWrittenByFlush" -> FLUSH_WRITE_BYTES
    ).toMap
    val nativeOpsMetrics = nativeOpsMetricTickers.transform { (_, typ) =>
      nativeStats.getTickerCount(typ)
    }

    // if bounded memory usage is enabled, we share the block cache across all state providers
    // running on the same node and account the usage to this single cache. In this case, its not
    // possible to provide partition level or query level memory usage.
    val memoryUsage = if (conf.boundedMemoryUsage) {
      0L
    } else {
      readerMemUsage + memTableMemUsage + blockCacheUsage
    }

    RocksDBMetrics(
      numKeysOnLoadedVersion,
      numKeysOnWritingVersion,
      numInternalKeysOnWritingVersion,
      memoryUsage,
      pinnedBlocksMemUsage,
      totalSSTFilesBytes,
      nativeOpsLatencyMicros,
      commitLatencyMs,
      bytesCopied = fileManagerMetrics.bytesCopied,
      filesCopied = fileManagerMetrics.filesCopied,
      filesReused = fileManagerMetrics.filesReused,
      lastUploadedSnapshotVersion = lastUploadedSnapshotVersion.get(),
      zipFileBytesUncompressed = fileManagerMetrics.zipFileBytesUncompressed,
      nativeOpsMetrics = nativeOpsMetrics)
  }

  /**
   * Function to return RocksDB metrics if the recorded metrics are available and the operator
   * has reached the commit stage for this state store instance and version. If not, we return None
   * @return - Return RocksDBMetrics if available and None otherwise
   */
  def metricsOpt: Option[RocksDBMetrics] = {
    var rocksDBMetricsOpt: Option[RocksDBMetrics] = None
    acquire(ReportStoreMetrics)
    try {
      rocksDBMetricsOpt = recordedMetrics
    } catch {
      case ex: Exception =>
        logInfo(log"Failed to acquire metrics with exception=${MDC(LogKeys.ERROR, ex)}")
    } finally {
      release(ReportStoreMetrics)
    }
    rocksDBMetricsOpt
  }

  /**
   * Function to acquire RocksDB instance lock that allows for synchronized access to the state
   * store instance
   *
   * @param opType - operation type requesting the lock
   */
  private def acquire(opType: RocksDBOpType): Unit = acquireLock.synchronized {
    val newAcquiredThreadInfo = AcquiredThreadInfo()
    val waitStartTime = System.nanoTime()
    def timeWaitedMs = {
      val elapsedNanos = System.nanoTime() - waitStartTime
      TimeUnit.MILLISECONDS.convert(elapsedNanos, TimeUnit.NANOSECONDS)
    }
    def isAcquiredByDifferentThread = acquiredThreadInfo != null &&
      acquiredThreadInfo.threadRef.get.isDefined &&
      newAcquiredThreadInfo.threadRef.get.get.getId != acquiredThreadInfo.threadRef.get.get.getId

    while (isAcquiredByDifferentThread && timeWaitedMs < conf.lockAcquireTimeoutMs) {
      acquireLock.wait(10)
    }
    if (isAcquiredByDifferentThread) {
      val stackTraceOutput = acquiredThreadInfo.threadRef.get.get.getStackTrace.mkString("\n")
      throw QueryExecutionErrors.unreleasedThreadError(loggingId, opType.toString,
        newAcquiredThreadInfo.toString(), acquiredThreadInfo.toString(), timeWaitedMs,
        stackTraceOutput)
    } else {
      acquiredThreadInfo = newAcquiredThreadInfo
      // Add a listener to always release the lock when the task (if active) completes
      Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit] {
        _ => this.release(StoreTaskCompletionListener, Some(newAcquiredThreadInfo))
      })
      logInfo(log"RocksDB instance was acquired by " +
        log"ownerThread=${MDC(LogKeys.THREAD, acquiredThreadInfo)} " +
        log"for opType=${MDC(LogKeys.OP_TYPE, opType.toString)}")
    }
  }

  /**
   * Function to release RocksDB instance lock that allows for synchronized access to the state
   * store instance. Optionally provide a thread to check against, and release only if provided
   * thread is the one that acquired the lock.
   *
   * @param opType - operation type releasing the lock
   * @param releaseForThreadOpt - optional thread to check against acquired thread
   */
  private def release(
      opType: RocksDBOpType,
      releaseForThreadOpt: Option[AcquiredThreadInfo] = None): Unit = acquireLock.synchronized {
    if (acquiredThreadInfo != null) {
      val release = releaseForThreadOpt match {
        case Some(releaseForThread) if releaseForThread.threadRef.get.isEmpty =>
          logInfo(log"Thread reference is empty when attempting to release for " +
            log"opType=${MDC(LogKeys.OP_TYPE, opType.toString)}, ignoring release. " +
            log"Lock is held by ownerThread=${MDC(LogKeys.THREAD, acquiredThreadInfo)}")
          false
        // NOTE: we compare the entire acquiredThreadInfo object to ensure that we are
        // releasing not only for the right thread but the right task as well. This is
        // inconsistent with the logic for acquire which uses only the thread ID, consider
        // updating this in future.
        case Some(releaseForThread) if acquiredThreadInfo != releaseForThread =>
          logInfo(log"Thread info for " +
            log"releaseThread=${MDC(LogKeys.THREAD, releaseForThreadOpt.get)} " +
            log"does not match the acquired thread when attempting to " +
            log"release for opType=${MDC(LogKeys.OP_TYPE, opType.toString)}, ignoring release. " +
            log"Lock is held by ownerThread=${MDC(LogKeys.THREAD, acquiredThreadInfo)}")
          false
        case _ => true
      }

      if (release) {
        logInfo(log"RocksDB instance was released by " +
          log"releaseThread=${MDC(LogKeys.THREAD, AcquiredThreadInfo())} " +
          log"with ownerThread=${MDC(LogKeys.THREAD, acquiredThreadInfo)} " +
          log"for opType=${MDC(LogKeys.OP_TYPE, opType.toString)}")
        acquiredThreadInfo = null
        acquireLock.notifyAll()
      }
    }
  }

  private def getDBProperty(property: String): Long = db.getProperty(property).toLong

  private def openDB(): Unit = {
    assert(db == null)
    db = NativeRocksDB.open(rocksDbOptions, workingDir.toString)
    logInfo(log"Opened DB with conf ${MDC(LogKeys.CONFIG, conf)}")
  }

  private def closeDB(ignoreException: Boolean = true): Unit = {
    if (db != null) {
      // Cancel and wait until all background work finishes
      db.cancelAllBackgroundWork(true)
      if (ignoreException) {
        // Close the DB instance
        db.close()
      } else {
        // Close the DB instance and throw the exception if any
        db.closeE()
      }
      db = null
    }
  }

  private[state] def getAcquiredThreadInfo(): Option[AcquiredThreadInfo] =
      acquireLock.synchronized {
    Option(acquiredThreadInfo).map(_.copy())
  }

  /** Upload the snapshot to DFS and remove it from snapshots pending */
  private def uploadSnapshot(
      snapshot: RocksDBSnapshot,
      fileManager: RocksDBFileManager,
      snapshotsPendingUpload: Set[RocksDBVersionSnapshotInfo],
      loggingId: String): RocksDBFileManagerMetrics = {
    var fileManagerMetrics: RocksDBFileManagerMetrics = null
    try {
      val uploadTime = timeTakenMs {
        fileManager.saveCheckpointToDfs(
          snapshot.checkpointDir,
          snapshot.version,
          snapshot.numKeys,
          snapshot.numInternalKeys,
          snapshot.fileMapping,
          Some(snapshot.columnFamilyMapping),
          Some(snapshot.maxColumnFamilyId),
          snapshot.uniqueId
        )
        fileManagerMetrics = fileManager.latestSaveCheckpointMetrics

        val snapshotInfo = RocksDBVersionSnapshotInfo(snapshot.version, snapshot.dfsFileSuffix)
        // We are only removing the uploaded snapshot info from the pending set,
        // to let the file mapping (i.e. query threads) know that the snapshot (i.e. and its files)
        // have been uploaded to DFS. We don't touch the file mapping here to avoid corrupting it.
        snapshotsPendingUpload.remove(snapshotInfo)
      }
      // This is relative aggressive because that even if the uploading succeeds,
      // it is not necessarily the one written to the commit log. But we can always load lineage
      // from commit log so it is fine.
      lineageManager.truncateFromVersion(snapshot.version)
      logInfo(log"${MDC(LogKeys.LOG_ID, loggingId)}: " +
        log"Upload snapshot of version ${MDC(LogKeys.VERSION_NUM, snapshot.version)}, " +
        log"with uniqueId: ${MDC(LogKeys.UUID, snapshot.uniqueId)} " +
        log"time taken: ${MDC(LogKeys.TIME_UNITS, uploadTime)} ms. " +
        log"Current lineage: ${MDC(LogKeys.LINEAGE, lineageManager)}")
      // Compare and update with the version that was just uploaded.
      lastUploadedSnapshotVersion.updateAndGet(v => Math.max(snapshot.version, v))
      // Report snapshot upload event to the coordinator.
      reportSnapshotUploadToCoordinator(snapshot.version)
    } finally {
      snapshot.close()
    }

    fileManagerMetrics
  }

  /** Reports to the coordinator with the event listener that a snapshot finished uploading */
  private def reportSnapshotUploadToCoordinator(version: Long): Unit = {
    if (conf.reportSnapshotUploadLag) {
      // Note that we still report snapshot versions even when changelog checkpointing is disabled.
      // The coordinator needs a way to determine whether upload messages are disabled or not,
      // which would be different between RocksDB and HDFS stores due to changelog checkpointing.
      eventForwarder.foreach(_.reportSnapshotUploaded(version))
    }
  }

  /** Create a native RocksDB logger that forwards native logs to log4j with correct log levels. */
  private def createLogger(): Logger = {
    val dbLogger = new Logger(rocksDbOptions.infoLogLevel()) {
      override def log(infoLogLevel: InfoLogLevel, logMsg: String) = {
        // Map DB log level to log4j levels
        // Warn is mapped to info because RocksDB warn is too verbose
        // Info is mapped to debug because RocksDB info is too verbose
        // (e.g. dumps non-warning stuff like stats)
        val loggingFunc: ( => LogEntry) => Unit = infoLogLevel match {
          case InfoLogLevel.FATAL_LEVEL | InfoLogLevel.ERROR_LEVEL => logError(_)
          case InfoLogLevel.WARN_LEVEL => logInfo(_)
          case InfoLogLevel.INFO_LEVEL | InfoLogLevel.DEBUG_LEVEL => logDebug(_)
          case _ => logTrace(_)
        }
        loggingFunc(log"[NativeRocksDB-${MDC(LogKeys.ROCKS_DB_LOG_LEVEL, infoLogLevel.getValue)}]" +
          log" ${MDC(LogKeys.ROCKS_DB_LOG_MESSAGE, logMsg)}")
      }
    }

    var dbLogLevel = InfoLogLevel.ERROR_LEVEL
    if (log.isWarnEnabled) dbLogLevel = InfoLogLevel.WARN_LEVEL
    if (log.isInfoEnabled) dbLogLevel = InfoLogLevel.INFO_LEVEL
    if (log.isDebugEnabled) dbLogLevel = InfoLogLevel.DEBUG_LEVEL
    dbLogger.setInfoLogLevel(dbLogLevel)
    // The log level set in dbLogger is effective and the one to dbOptions isn't applied to
    // customized logger. We still set it as it might show up in RocksDB config file or logging.
    rocksDbOptions.setInfoLogLevel(dbLogLevel)
    rocksDbOptions.setLogger(dbLogger)
    logInfo(log"Set RocksDB native logging level to ${MDC(LogKeys.ROCKS_DB_LOG_LEVEL, dbLogLevel)}")
    dbLogger
  }

  /** Create a temp directory inside the local root directory */
  private def createTempDir(prefix: String): File = {
    Utils.createDirectory(localRootDir.getAbsolutePath, prefix)
  }

  override protected def logName: String = s"${super.logName} $loggingId"
}

object RocksDB extends Logging {
  case class RocksDBSnapshot(
      checkpointDir: File,
      version: Long,
      numKeys: Long,
      numInternalKeys: Long,
      columnFamilyMapping: Map[String, ColumnFamilyInfo],
      maxColumnFamilyId: Short,
      dfsFileSuffix: String,
      fileMapping: Map[String, RocksDBSnapshotFile],
      uniqueId: Option[String] = None) {
    def close(): Unit = {
      silentDeleteRecursively(checkpointDir, s"Free up local checkpoint of snapshot $version")
    }
  }

  /** Attempt to delete recursively, and log the error if any */
  private def silentDeleteRecursively(file: File, msg: String): Unit = {
    try {
      Utils.deleteRecursively(file)
    } catch {
      case e: Exception =>
        logWarning(log"Error recursively deleting local dir ${MDC(LogKeys.PATH, file)} " +
          log"while ${MDC(LogKeys.ERROR, msg)}", e)
    }
  }

  private def printLineageItems(lineage: Array[LineageItem]): String = lineage.map {
    case LineageItem(l, optStr) => s"$l:$optStr"
  }.mkString(" ")

  /** Records the duration of running `body` for the next query progress update. */
  private def timeTakenMs(body: => Unit): Long = Utils.timeTakenMs(body)._2
}

// uniquely identifies a Snapshot. Multiple snapshots created for same version will
// use a different dfsFilesUUID, and hence will have different RocksDBVersionSnapshotInfo
case class RocksDBVersionSnapshotInfo(version: Long, dfsFilesUUID: String)

// Encapsulates a RocksDB immutable file, and the information whether it has been previously
// uploaded to DFS. Already uploaded files can be skipped during SST file upload.
case class RocksDBSnapshotFile(immutableFile: RocksDBImmutableFile, isUploaded: Boolean)

// Encapsulates the mapping of local SST files to DFS files. This mapping prevents
// re-uploading the same SST file multiple times to DFS, saving I/O and reducing snapshot
// upload time. During version load, if a DFS file is already present on local file system,
// it will be reused.
// This mapping should only be updated using the Task thread - at version load and commit time.
// If same mapping instance is updated from different threads, it will result in undefined behavior
// (and most likely incorrect mapping state).
class RocksDBFileMapping {

  // Maps a local SST file to the DFS version and DFS file.
  private val localFileMappings: mutable.Map[String, (Long, RocksDBImmutableFile)] =
    mutable.HashMap[String, (Long, RocksDBImmutableFile)]()

  // Keeps track of all snapshots which have not been uploaded yet. This prevents Spark
  // from reusing SST files which have not been yet persisted to DFS,
  val snapshotsPendingUpload: Set[RocksDBVersionSnapshotInfo] = ConcurrentHashMap.newKeySet()

  /**
   * Clear everything stored in the file mapping.
   */
  def clear(): Unit = {
    localFileMappings.clear()
    snapshotsPendingUpload.clear()
  }

  override def toString: String = localFileMappings.toString()

  /**
   * Get the mapped DFS file for the given local file for a DFS load operation.
   * If the currently mapped DFS file was mapped in the same or newer version as the version we
   * want to load (or was generated in a version which has not been uploaded to DFS yet),
   * the mapped DFS file is ignored. In this scenario, the local mapping to this DFS file
   * will be cleared, and function will return None.
   *
   * @note For same version number, this is because we want to make sure we are using
   *       the latest files in DFS, in case the previous zip file has been overwritten in DFS.
   *
   * @return - Option with the DFS file or None
   */
  def getDfsFileForLoad(
      fileManager: RocksDBFileManager,
      localFileName: String,
      versionToLoad: Long): Option[RocksDBImmutableFile] = {
    getDfsFileWithIncompatibilityCheck(
      fileManager,
      localFileName,
      // We can't reuse the current local file since it was added in the same or newer version
      // as the version we want to load
      (fileVersion, _) => fileVersion >= versionToLoad
    )
  }

  /**
   * Get the mapped DFS file for the given local file for a DFS save (i.e. checkpoint) operation.
   * If the currently mapped DFS file was mapped in the same or newer version as the version we
   * want to save (or was generated in a version which has not been uploaded to DFS yet)
   * or the mapped dfs file isn't the same size as the local file,
   * then the mapped DFS file is ignored. In this scenario, the local mapping to this DFS file
   * will be cleared, and function will return None.
   *
   * @note If the file was added in current version (i.e. versionToSave - 1), we can reuse it.
   *       e.g. we load(v1) -> save(v2), the loaded SST files from version 1 can be reused
   *       in version 2 upload.
   *
   * @return - Option with the DFS file or None
   */
  private def getDfsFileForSave(
      fileManager: RocksDBFileManager,
      localFile: File,
      versionToSave: Long): Option[RocksDBImmutableFile] = {
    getDfsFileWithIncompatibilityCheck(
      fileManager,
      localFile.getName,
      (dfsFileVersion, dfsFile) =>
        // The DFS file is not the same as the file we want to save, either if
        // the DFS file was added in the same or higher version, or the file size is different
        dfsFileVersion >= versionToSave || dfsFile.sizeBytes != localFile.length()
    )
  }

  private def getDfsFileWithIncompatibilityCheck(
      fileManager: RocksDBFileManager,
      localFileName: String,
      isIncompatible: (Long, RocksDBImmutableFile) => Boolean): Option[RocksDBImmutableFile] = {
    localFileMappings.get(localFileName).map { case (dfsFileMappedVersion, dfsFile) =>
      val dfsFileSuffix = fileManager.dfsFileSuffix(dfsFile)
      val versionSnapshotInfo = RocksDBVersionSnapshotInfo(dfsFileMappedVersion, dfsFileSuffix)
      if (isIncompatible(dfsFileMappedVersion, dfsFile) ||
        snapshotsPendingUpload.contains(versionSnapshotInfo)) {
        // the mapped dfs file cannot be used, delete from mapping
        remove(localFileName)
        None
      } else {
        Some(dfsFile)
      }
    }.getOrElse(None)
  }

  def mapToDfsFile(
      localFileName: String,
      dfsFile: RocksDBImmutableFile,
      version: Long): Unit = {
    localFileMappings.put(localFileName, (version, dfsFile))
  }

  def remove(localFileName: String): Unit = {
    localFileMappings.remove(localFileName)
  }

  private def syncWithLocalState(localFiles: Seq[File]): Unit = {
    val localFileNames = localFiles.map(_.getName).toSet
    val deletedFiles = localFileMappings.keys.filterNot(localFileNames.contains)

    deletedFiles.foreach(localFileMappings.remove)
  }

  // Generates the DFS file names for local Immutable files in checkpoint directory, and
  // returns the mapping from local fileName in checkpoint directory to generated DFS file.
  // If the DFS file has been previously uploaded - the snapshot file isUploaded flag is set
  // to true.
  def createSnapshotFileMapping(
      fileManager: RocksDBFileManager,
      checkpointDir: File,
      version: Long): (String, Map[String, RocksDBSnapshotFile]) = {
    val (localImmutableFiles, _) = fileManager.listRocksDBFiles(checkpointDir)
    // UUID used to prefix files uploaded to DFS as part of commit
    val dfsFilesSuffix = UUID.randomUUID().toString
    val snapshotFileMapping = localImmutableFiles.map { f =>
      val localFileName = f.getName
      val existingDfsFile = getDfsFileForSave(fileManager, f, version)
      val dfsFile = existingDfsFile.getOrElse {
        val newDfsFileName = fileManager.newDFSFileName(localFileName, dfsFilesSuffix)
        val newDfsFile = RocksDBImmutableFile(localFileName, newDfsFileName, sizeBytes = f.length())
        mapToDfsFile(localFileName, newDfsFile, version)
        newDfsFile
      }
      localFileName -> RocksDBSnapshotFile(dfsFile, existingDfsFile.isDefined)
    }.toMap

    syncWithLocalState(localImmutableFiles)

    val rocksDBSnapshotInfo = RocksDBVersionSnapshotInfo(version, dfsFilesSuffix)
    snapshotsPendingUpload.add(rocksDBSnapshotInfo)
    (dfsFilesSuffix, snapshotFileMapping)
  }
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
    minVersionsToDelete: Long,
    minDeltasForSnapshot: Int,
    compactOnCommit: Boolean,
    enableChangelogCheckpointing: Boolean,
    blockSizeKB: Long,
    blockCacheSizeMB: Long,
    lockAcquireTimeoutMs: Long,
    resetStatsOnLoad : Boolean,
    formatVersion: Int,
    trackTotalNumberOfRows: Boolean,
    maxOpenFiles: Int,
    writeBufferSizeMB: Long,
    maxWriteBufferNumber: Int,
    boundedMemoryUsage: Boolean,
    totalMemoryUsageMB: Long,
    writeBufferCacheRatio: Double,
    highPriorityPoolRatio: Double,
    compressionCodec: String,
    allowFAllocate: Boolean,
    compression: String,
    reportSnapshotUploadLag: Boolean)

object RocksDBConf {
  /** Common prefix of all confs in SQLConf that affects RocksDB */
  val ROCKSDB_SQL_CONF_NAME_PREFIX = "spark.sql.streaming.stateStore.rocksdb"

  private abstract class ConfEntry(name: String, val default: String) {
    def fullName: String = name.toLowerCase(Locale.ROOT)
  }

  private case class SQLConfEntry(name: String, override val default: String)
    extends ConfEntry(name, default) {

    override def fullName: String =
      s"$ROCKSDB_SQL_CONF_NAME_PREFIX.${name}".toLowerCase(Locale.ROOT)
  }

  private case class ExtraConfEntry(name: String, override val default: String)
    extends ConfEntry(name, default)

  // Configuration that specifies whether to compact the RocksDB data every time data is committed
  private val COMPACT_ON_COMMIT_CONF = SQLConfEntry("compactOnCommit", "false")
  private val ENABLE_CHANGELOG_CHECKPOINTING_CONF = SQLConfEntry(
    "changelogCheckpointing.enabled", "false")
  private val BLOCK_SIZE_KB_CONF = SQLConfEntry("blockSizeKB", "4")
  private val BLOCK_CACHE_SIZE_MB_CONF = SQLConfEntry("blockCacheSizeMB", "8")
  // See SPARK-42794 for details.
  private val LOCK_ACQUIRE_TIMEOUT_MS_CONF = SQLConfEntry("lockAcquireTimeoutMs", "120000")
  private val RESET_STATS_ON_LOAD = SQLConfEntry("resetStatsOnLoad", "true")
  // Config to specify the number of open files that can be used by the DB. Value of -1 means
  // that files opened are always kept open.
  private val MAX_OPEN_FILES_CONF = SQLConfEntry("maxOpenFiles", "-1")
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
  private val FORMAT_VERSION = SQLConfEntry("formatVersion", "5")

  // Flag to enable/disable tracking the total number of rows.
  // When this is enabled, this class does additional lookup on write operations (put/delete) to
  // track the changes of total number of rows, which would help observability on state store.
  // The additional lookups bring non-trivial overhead on write-heavy workloads - if your query
  // does lots of writes on state, it would be encouraged to turn off the config and turn on
  // again when you really need the know the number for observability/debuggability.
  private val TRACK_TOTAL_NUMBER_OF_ROWS = SQLConfEntry("trackTotalNumberOfRows", "true")

  // RocksDB Memory Management Related Configurations
  // Configuration to control maximum size of MemTable in RocksDB
  val WRITE_BUFFER_SIZE_MB_CONF_KEY = "writeBufferSizeMB"
  private val WRITE_BUFFER_SIZE_MB_CONF = SQLConfEntry(WRITE_BUFFER_SIZE_MB_CONF_KEY, "-1")

  // Configuration to set maximum number of MemTables in RocksDB, both active and immutable.
  // If the active MemTable fills up and the total number of MemTables is larger than
  // maxWriteBufferNumber, then RocksDB will stall further writes.
  // This may happen if the flush process is slower than the write rate.
  val MAX_WRITE_BUFFER_NUMBER_CONF_KEY = "maxWriteBufferNumber"
  private val MAX_WRITE_BUFFER_NUMBER_CONF = SQLConfEntry(MAX_WRITE_BUFFER_NUMBER_CONF_KEY, "-1")

  // Config to determine whether RocksDB memory usage will be bounded for a given executor
  val BOUNDED_MEMORY_USAGE_CONF_KEY = "boundedMemoryUsage"
  private val BOUNDED_MEMORY_USAGE_CONF = SQLConfEntry(BOUNDED_MEMORY_USAGE_CONF_KEY, "false")

  // Total memory to be occupied by all RocksDB state store instances on this executor
  val MAX_MEMORY_USAGE_MB_CONF_KEY = "maxMemoryUsageMB"
  private val MAX_MEMORY_USAGE_MB_CONF = SQLConfEntry("maxMemoryUsageMB", "500")

  // Memory to be occupied by memtables as part of the cache
  val WRITE_BUFFER_CACHE_RATIO_CONF_KEY = "writeBufferCacheRatio"
  private val WRITE_BUFFER_CACHE_RATIO_CONF = SQLConfEntry(WRITE_BUFFER_CACHE_RATIO_CONF_KEY,
    "0.5")

  // Memory to be occupied by index and filter blocks as part of the cache
  val HIGH_PRIORITY_POOL_RATIO_CONF_KEY = "highPriorityPoolRatio"
  private val HIGH_PRIORITY_POOL_RATIO_CONF = SQLConfEntry(HIGH_PRIORITY_POOL_RATIO_CONF_KEY,
    "0.1")

  // Allow files to be pre-allocated on disk using fallocate
  // Disabling may slow writes, but can solve an issue where
  // significant quantities of disk are wasted if there are
  // many smaller concurrent state-stores running with the
  // spark context
  val ALLOW_FALLOCATE_CONF_KEY = "allowFAllocate"
  private val ALLOW_FALLOCATE_CONF = SQLConfEntry(ALLOW_FALLOCATE_CONF_KEY, "true")

  // Pass as compression type to RocksDB.
  val COMPRESSION_KEY = "compression"
  private val COMPRESSION_CONF = SQLConfEntry(COMPRESSION_KEY, "lz4")

  def apply(storeConf: StateStoreConf): RocksDBConf = {
    val sqlConfs = CaseInsensitiveMap[String](storeConf.sqlConfs)
    val extraConfs = CaseInsensitiveMap[String](storeConf.extraOptions)

    def getConfigMap(conf: ConfEntry): CaseInsensitiveMap[String] = {
      conf match {
        case _: SQLConfEntry => sqlConfs
        case _: ExtraConfEntry => extraConfs
      }
    }

    def getBooleanConf(conf: ConfEntry): Boolean = {
      Try { getConfigMap(conf).getOrElse(conf.fullName, conf.default).toBoolean } getOrElse {
        throw new IllegalArgumentException(s"Invalid value for '${conf.fullName}', must be boolean")
      }
    }

    def getIntConf(conf: ConfEntry): Int = {
      Try { getConfigMap(conf).getOrElse(conf.fullName, conf.default).toInt } getOrElse {
        throw new IllegalArgumentException(s"Invalid value for '${conf.fullName}', " +
          "must be an integer")
      }
    }

    def getRatioConf(conf: ConfEntry): Double = {
      Try {
        getConfigMap(conf).getOrElse(conf.fullName, conf.default).toDouble
      } filter { config => config >= 0.0 && config <= 1.0 } getOrElse {
        throw new IllegalArgumentException(s"Invalid value for '${conf.fullName}', " +
          "must be a ratio between 0.0 and 1.0")
      }
    }

    def getLongConf(conf: ConfEntry): Long = {
      Try { getConfigMap(conf).getOrElse(conf.fullName, conf.default).toLong } getOrElse {
        throw new IllegalArgumentException(
          s"Invalid value for '${conf.fullName}', must be a long"
        )
      }
    }

    def getPositiveLongConf(conf: ConfEntry): Long = {
      Try {
        getConfigMap(conf).getOrElse(conf.fullName, conf.default).toLong
      } filter { _ >= 0 } getOrElse {
        throw new IllegalArgumentException(
          s"Invalid value for '${conf.fullName}', must be a positive integer")
      }
    }

    def getPositiveIntConf(conf: ConfEntry): Int = {
      Try {
        getConfigMap(conf).getOrElse(conf.fullName, conf.default).toInt
      } filter { _ >= 0 } getOrElse {
        throw new IllegalArgumentException(
          s"Invalid value for '${conf.fullName}', must be a positive integer")
      }
    }

    def getStringConf(conf: ConfEntry): String = {
      Try { getConfigMap(conf).getOrElse(conf.fullName, conf.default) } getOrElse {
        throw new IllegalArgumentException(
          s"Invalid value for '${conf.fullName}', must be a string"
        )
      }
    }

    RocksDBConf(
      storeConf.minVersionsToRetain,
      storeConf.minVersionsToDelete,
      storeConf.minDeltasForSnapshot,
      getBooleanConf(COMPACT_ON_COMMIT_CONF),
      getBooleanConf(ENABLE_CHANGELOG_CHECKPOINTING_CONF),
      getPositiveLongConf(BLOCK_SIZE_KB_CONF),
      getPositiveLongConf(BLOCK_CACHE_SIZE_MB_CONF),
      getPositiveLongConf(LOCK_ACQUIRE_TIMEOUT_MS_CONF),
      getBooleanConf(RESET_STATS_ON_LOAD),
      getPositiveIntConf(FORMAT_VERSION),
      getBooleanConf(TRACK_TOTAL_NUMBER_OF_ROWS),
      getIntConf(MAX_OPEN_FILES_CONF),
      getLongConf(WRITE_BUFFER_SIZE_MB_CONF),
      getIntConf(MAX_WRITE_BUFFER_NUMBER_CONF),
      getBooleanConf(BOUNDED_MEMORY_USAGE_CONF),
      getLongConf(MAX_MEMORY_USAGE_MB_CONF),
      getRatioConf(WRITE_BUFFER_CACHE_RATIO_CONF),
      getRatioConf(HIGH_PRIORITY_POOL_RATIO_CONF),
      storeConf.compressionCodec,
      getBooleanConf(ALLOW_FALLOCATE_CONF),
      getStringConf(COMPRESSION_CONF),
      storeConf.reportSnapshotUploadLag)
  }

  def apply(): RocksDBConf = apply(new StateStoreConf())
}

/** Class to represent stats from each commit. */
case class RocksDBMetrics(
    numCommittedKeys: Long,
    numUncommittedKeys: Long,
    numInternalKeys: Long,
    totalMemUsageBytes: Long,
    pinnedBlocksMemUsage: Long,
    totalSSTFilesBytes: Long,
    nativeOpsHistograms: Map[String, RocksDBNativeHistogram],
    lastCommitLatencyMs: Map[String, Long],
    filesCopied: Long,
    bytesCopied: Long,
    filesReused: Long,
    zipFileBytesUncompressed: Option[Long],
    nativeOpsMetrics: Map[String, Long],
    lastUploadedSnapshotVersion: Long) {
  def json: String = Serialization.write(this)(RocksDBMetrics.format)
}

object RocksDBMetrics {
  val format: Formats = Serialization.formats(NoTypeHints)
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

case class AcquiredThreadInfo(
    threadRef: WeakReference[Thread] = new WeakReference[Thread](Thread.currentThread()),
    tc: TaskContext = TaskContext.get()) {
  override def toString(): String = {
    val taskStr = if (tc != null) {
      val taskDetails =
        s"partition ${tc.partitionId()}.${tc.attemptNumber()} in stage " +
          s"${tc.stageId()}.${tc.stageAttemptNumber()}, TID ${tc.taskAttemptId()}"
      s", task: $taskDetails"
    } else ""

    s"[ThreadId: ${threadRef.get.map(_.getId)}$taskStr]"
  }
}

/**
 * A helper class to manage the lineage information when checkpoint unique id is enabled.
 * "lineage" is an array of LineageItem (version, uniqueId) pair.
 *
 * The first item of "lineage" should normally be the version of a snapshot, except
 * for the first few versions. Because they are solely loaded from changelog file.
 * (i.e. with default minDeltasForSnapshot, there is only 1_uuid1.changelog, no 1_uuid1.zip)
 *
 * The last item of "lineage" corresponds to one version before the to-be-committed version.
 */
private[sql] class RocksDBLineageManager {
  @volatile private var lineage: Array[LineageItem] = Array.empty

  override def toString: String = synchronized {
    lineage.map {
      case LineageItem(version, uuid) => s"$version: $uuid"
    }.mkString(" ")
  }

  def appendLineageItem(item: LineageItem): Unit = synchronized {
    lineage = lineage :+ item
  }

  def truncateFromVersion(versionToKeep: Long): Unit = synchronized {
    resetLineage(getLineageForCurrVersion().filter(i => i.version >= versionToKeep))
  }

  def resetLineage(newLineage: Array[LineageItem]): Unit = synchronized {
    lineage = newLineage
  }

  def getLineageForCurrVersion(): Array[LineageItem] = synchronized {
    lineage.clone()
  }

  def contains(item: LineageItem): Boolean = synchronized {
    lineage.contains(item)
  }

  def clear(): Unit = synchronized {
    lineage = Array.empty
  }
}
