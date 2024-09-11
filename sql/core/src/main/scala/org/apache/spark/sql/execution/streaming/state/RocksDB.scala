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
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import javax.annotation.concurrent.GuardedBy

import scala.collection.{mutable, Map}
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.{ConcurrentMapHasAsScala, MapHasAsJava}
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
case object StoreMaintenance extends RocksDBOpType("store_maintenance")

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
    loggingId: String = "",
    useColumnFamilies: Boolean = false,
    checkpointFormatVersion: Int = 1) extends Logging {

  // scalastyle:off
  case class RocksDBSnapshot(
      checkpointDir: File,
      version: Long,
      numKeys: Long,
      capturedFileMappings: RocksDBFileMappings,
      columnFamilyMapping: Map[String, Short],
      maxColumnFamilyId: Short,
      uniqueId: Option[String] = None) {
    def close(): Unit = {
      silentDeleteRecursively(checkpointDir, s"Free up local checkpoint of snapshot $version")
    }
  }

  @volatile private var latestSnapshot: Option[RocksDBSnapshot] = None
  @volatile private var lastSnapshotVersion = 0L
  private val oldSnapshots = new ListBuffer[RocksDBSnapshot]

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

  private[state] val columnFamilyOptions = new ColumnFamilyOptions()

  // Set RocksDB options around MemTable memory usage. By default, we let RocksDB
  // use its internal default values for these settings.
  if (conf.writeBufferSizeMB > 0L) {
    columnFamilyOptions.setWriteBufferSize(conf.writeBufferSizeMB * 1024 * 1024)
  }

  if (conf.maxWriteBufferNumber > 0L) {
    columnFamilyOptions.setMaxWriteBufferNumber(conf.maxWriteBufferNumber)
  }

  columnFamilyOptions.setCompressionType(getCompressionType(conf.compression))
  columnFamilyOptions.setMergeOperator(new StringAppendOperator())

  private val dbOptions =
    new Options(new DBOptions(), columnFamilyOptions) // options to open the RocksDB

  dbOptions.setCreateIfMissing(true)
  dbOptions.setTableFormatConfig(tableFormatConfig)
  dbOptions.setMaxOpenFiles(conf.maxOpenFiles)
  dbOptions.setAllowFAllocate(conf.allowFAllocate)
  dbOptions.setMergeOperator(new StringAppendOperator())

  if (conf.boundedMemoryUsage) {
    dbOptions.setWriteBufferManager(writeBufferManager)
  }

  private val dbLogger = createLogger() // for forwarding RocksDB native logs to log4j
  dbOptions.setStatistics(new Statistics())
  private val nativeStats = dbOptions.statistics()

  private val workingDir = createTempDir("workingDir")
  private val fileManager = new RocksDBFileManager(dfsRootDir, createTempDir("fileManager"),
    hadoopConf, conf.compressionCodec, loggingId = loggingId,
    checkpointFormatVersion = checkpointFormatVersion)
  private val byteArrayPair = new ByteArrayPair()
  private val commitLatencyMs = new mutable.HashMap[String, Long]()
  private val acquireLock = new Object

  @volatile private var db: NativeRocksDB = _
  @volatile private var changelogWriter: Option[StateStoreChangelogWriter] = None
  private val enableChangelogCheckpointing: Boolean = conf.enableChangelogCheckpointing
  @volatile private var loadedVersion = -1L   // -1 = nothing valid is loaded

  // variables to manage checkpoint ID. Once a checkpoint finishes, it needs to return
  // the `lastCommittedCheckpointId` as the committed checkpointID, as well as
  // `LastCommitBasedCheckpointId` as the checkpoint id of the previous version that is based on.
  // `loadedCheckpointId` is the checkpointID for the current live DB. After the batch finishes
  // and checkpoint finishes, it will turn into `LastCommitBasedCheckpointId`.
  // `sessionCheckpointId` store an ID to be used for future checkpoints. It is kept being used
  // until we have to use a new one. We don't need to reuse any uniqueID, but reusing when possible
  // can help debug problems.
  @volatile private var LastCommitBasedCheckpointId: Option[String] = None
  @volatile private var lastCommittedCheckpointId: Option[String] = None
  @volatile private var loadedCheckpointId: Option[String] = None
  @volatile private var sessionCheckpointId: Option[String] = None

  @volatile private var numKeysOnLoadedVersion = 0L
  @volatile private var numKeysOnWritingVersion = 0L
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
  private val colFamilyNameToIdMap = new ConcurrentHashMap[String, Short]()

  @GuardedBy("acquireLock")
  private val maxColumnFamilyId: AtomicInteger = new AtomicInteger(-1)

  @GuardedBy("acquireLock")
  private val shouldForceSnapshot: AtomicBoolean = new AtomicBoolean(false)

  /**
   * Check whether the column family name is for internal column families.
   *
   * @param cfName - column family name
   * @return - true if the column family is for internal use, false otherwise
   */
  private def checkInternalColumnFamilies(cfName: String): Boolean = cfName.charAt(0) == '_'

  // Methods to fetch column family mapping for this State Store version
  def getColumnFamilyMapping: Map[String, Short] = {
    colFamilyNameToIdMap.asScala
  }

  def getColumnFamilyId(cfName: String): Short = {
    colFamilyNameToIdMap.get(cfName)
  }

  /**
   * Create RocksDB column family, if not created already
   */
  def createColFamilyIfAbsent(colFamilyName: String): Short = {
    if (!checkColFamilyExists(colFamilyName)) {
      val newColumnFamilyId = maxColumnFamilyId.incrementAndGet().toShort
      colFamilyNameToIdMap.putIfAbsent(colFamilyName, newColumnFamilyId)
      shouldForceSnapshot.set(true)
      newColumnFamilyId
    } else {
      colFamilyNameToIdMap.get(colFamilyName)
    }
  }

  /**
   * Remove RocksDB column family, if exists
   * @return columnFamilyId if it exists, else None
   */
  def removeColFamilyIfExists(colFamilyName: String): Option[Short] = {
    if (checkColFamilyExists(colFamilyName)) {
      shouldForceSnapshot.set(true)
      Some(colFamilyNameToIdMap.remove(colFamilyName))
    } else {
      None
    }
  }

  /**
   * Function to check if the column family exists in the state store instance.
   *
   * @param colFamilyName - name of the column family
   * @return - true if the column family exists, false otherwise
   */
  def checkColFamilyExists(colFamilyName: String): Boolean = {
    colFamilyNameToIdMap.containsKey(colFamilyName)
  }

  // This method sets the internal column family metadata to
  // the default values it should be set to on load
  private def setInitialCFInfo(): Unit = {
    colFamilyNameToIdMap.clear()
    shouldForceSnapshot.set(false)
    maxColumnFamilyId.set(0)
  }

  def getColFamilyCount(isInternal: Boolean): Long = {
    if (isInternal) {
      colFamilyNameToIdMap.asScala.keys.toSeq.count(checkInternalColumnFamilies)
    } else {
      colFamilyNameToIdMap.asScala.keys.toSeq.count(!checkInternalColumnFamilies(_))
    }
  }

/**
 * In the case of checkpointFormatVersion 2, if we find multiple latest snapshot files of
 * the same version but they have different uniqueIds, we need to find the correct one based on
 * the lineage we have.
 *
 * When this happens, the stored file on the DFS must be a changelog file, because if not, the
 * latest snapshot is uniquely identified as the (version, checkpointUniqueId) pair.
 *
 * This method achieves traversing back the lineage and find the correct latest snapshot file
 * by creating a changelog reader and compare with the lineages stored there.
 **/
  private def getLatestSnapshotVersionAndUniqueIdFromLineage(
      currLineage: Array[(Long, Option[String])],
      latestSnapshotVersionsAndUniqueIds: Array[(Long, Option[String])]):
      (Long, Option[String]) = {
    currLineage.foreach {
      case (version, uniqueId) =>
        if (latestSnapshotVersionsAndUniqueIds.contains((version, uniqueId))) {
          return (version, uniqueId)
        }
    }
    // TODO: This error is not precise
    throw QueryExecutionErrors.cannotReadCheckpoint(1.toString, "v2")
  }

  /**
   * Load the given version of data in a native RocksDB instance.
   * Note that this will copy all the necessary file from DFS to local disk as needed,
   * and possibly restart the native RocksDB instance.
   */
  def load(
      version: Long,
      checkpointUniqueId: Option[String] = None,
      readOnly: Boolean = false): RocksDB = {
    assert(version >= 0)
    acquire(LoadStore)
    recordedMetrics = None
    logInfo(log"Loading ${MDC(LogKeys.VERSION_NUM, version)}")
    var currLineage: Option[Array[(Long, Option[String])]] = None
    try {
      if (loadedVersion != version ||
        (checkpointFormatVersion >= 2 && checkpointUniqueId.isDefined &&
        (!loadedCheckpointId.isDefined || checkpointUniqueId.get != loadedCheckpointId.get))) {
        closeDB(ignoreException = false)
        // deep copy is needed to avoid race condition
        // between maintenance and task threads
        fileManager.copyFileMapping()
        val latestSnapshotVersionsAndUniqueIds = {
          // checkpointUniqueId is wrong? It should be the uniqueId of the previous version
          // so you use the lineage to get version-1_uniqueId.zip
          fileManager.getLatestSnapshotVersionAndUniqueId(version, checkpointUniqueId)
        }

        if (checkpointFormatVersion >= 2 && enableChangelogCheckpointing && version != 0) {
          // TODO: changelog not always enabled?
          val changelogReader = fileManager.getChangelogReader(
            version, useColumnFamilies, checkpointUniqueId)
          // currLineage contains the version -> uniqueId mapping from the previous snapshot file
          // to current version's changelog file
          currLineage = Some(changelogReader.lineage.get.map(x => (x._1, Option(x._2))))
          println("wei== currLineage in load(): ")
          currLineage.foreach(x => println("wei== " + x.mkString(", ")))
        }

        val (latestSnapshotVersion, latestSnapshotUniqueId) = {
          // When loading from version 0
          if (latestSnapshotVersionsAndUniqueIds.length == 0) {
            (0L, None)
          } else if (latestSnapshotVersionsAndUniqueIds.length == 1) {
            latestSnapshotVersionsAndUniqueIds.head
          } else {
            logInfo("Multiple snapshots found for the version. " +
              "Loading the latest snapshot from lineage.")
            println(s"wei== version: $version, latestSnapshotVersionsAndUniqueIds: " + latestSnapshotVersionsAndUniqueIds.mkString(", "))
            println("wei== multiple snapshots found for the version. Loading the latest snapshot from lineage.")
            latestSnapshotVersionsAndUniqueIds.foreach(println)
            assert(enableChangelogCheckpointing, s"When loading version $version, multiple " +
              s"previous snapshots found for version ${latestSnapshotVersionsAndUniqueIds(0)._1}" +
              "This should happen only when changelog checkpointing is enabled, but it is not.")
            getLatestSnapshotVersionAndUniqueIdFromLineage(
              currLineage.get, latestSnapshotVersionsAndUniqueIds)
          }
        }
        val metadata = fileManager.loadCheckpointFromDfs(
          latestSnapshotVersion, workingDir, latestSnapshotUniqueId)
        loadedVersion = latestSnapshotVersion
        // Initialize maxVersion upon successful load from DFS
        fileManager.setMaxSeenVersion(version)

        setInitialCFInfo()
        metadata.columnFamilyMapping.foreach { mapping =>
          colFamilyNameToIdMap.putAll(mapping.asJava)
        }

        metadata.maxColumnFamilyId.foreach { maxId =>
          maxColumnFamilyId.set(maxId)
        }
        // TODO: update loadedUniqueId?
        // reset last snapshot version
        if (lastSnapshotVersion > latestSnapshotVersion) {
          // discard any newer snapshots
          synchronized {
            if (latestSnapshot.isDefined) {
              oldSnapshots += latestSnapshot.get
              latestSnapshot = None
            }
          }
        }

        // reset the last snapshot version to the latest available snapshot version
        lastSnapshotVersion = latestSnapshotVersion
        openDB()

        numKeysOnWritingVersion = if (!conf.trackTotalNumberOfRows) {
            // we don't track the total number of rows - discard the number being track
            -1L
          } else if (metadata.numKeys < 0) {
            // we track the total number of rows, but the snapshot doesn't have tracking number
            // need to count keys now
            countKeys()
          } else {
            metadata.numKeys
          }
        if (loadedVersion != version) {
          replayChangelog(version, currLineage) // TODO: this code is not backward compatible
        }
        // After changelog replay the numKeysOnWritingVersion will be updated to
        // the correct number of keys in the loaded version.
        numKeysOnLoadedVersion = numKeysOnWritingVersion
        fileManagerMetrics = fileManager.latestLoadCheckpointMetrics
      }
      if (checkpointFormatVersion >= 2) {
        LastCommitBasedCheckpointId = None
        lastCommittedCheckpointId = None
        loadedCheckpointId = checkpointUniqueId
        sessionCheckpointId = Some(java.util.UUID.randomUUID.toString)
      }
      lastCommittedCheckpointId = None
      if (conf.resetStatsOnLoad) {
        nativeStats.reset
      }
      logInfo(log"Loaded ${MDC(LogKeys.VERSION_NUM, version)} " +
        log"with uniqueId ${MDC(LogKeys.UUID, checkpointUniqueId)}")
    } catch {
      case t: Throwable =>
        loadedVersion = -1  // invalidate loaded data
        LastCommitBasedCheckpointId = None
        lastCommittedCheckpointId = None
        loadedCheckpointId = None
        sessionCheckpointId = None
        throw t
    }
    if (enableChangelogCheckpointing && !readOnly) {
      // Make sure we don't leak resource.
      changelogWriter.foreach(_.abort())
      // loadedVersion set to version in replayChangelog
      changelogWriter = Some(fileManager.getChangeLogWriter(
        version + 1, useColumnFamilies, sessionCheckpointId))

      // currLineage is only constructed when version is a change log version.
      // When loading from snapshot, currLineage is None because snapshot files
      // (version_uniqueId.zip) are source of truth.
      // They don't need to store lineage up to the previous version.
      if (checkpointFormatVersion >= 2) {
        val lineage = (currLineage.getOrElse(Array()) ++ Array((version + 1, sessionCheckpointId)))
          .map(x => (x._1, x._2.get))

        changelogWriter.get.writeLineage(lineage)
      }
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
    val metadata = fileManager.loadCheckpointFromDfs(snapshotVersion, workingDir)
    loadedVersion = snapshotVersion

    // reset last snapshot version
    if (lastSnapshotVersion > snapshotVersion) {
      // discard any newer snapshots
      lastSnapshotVersion = 0L
      latestSnapshot = None
    }
    openDB()

    numKeysOnWritingVersion = if (!conf.trackTotalNumberOfRows) {
      // we don't track the total number of rows - discard the number being track
      -1L
    } else if (metadata.numKeys < 0) {
      // we track the total number of rows, but the snapshot doesn't have tracking number
      // need to count keys now
      countKeys()
    } else {
      metadata.numKeys
    }
    if (loadedVersion != endVersion) replayChangelog(endVersion)
    // After changelog replay the numKeysOnWritingVersion will be updated to
    // the correct number of keys in the loaded version.
    numKeysOnLoadedVersion = numKeysOnWritingVersion
    fileManagerMetrics = fileManager.latestLoadCheckpointMetrics

    if (conf.resetStatsOnLoad) {
      nativeStats.reset
    }
  }

  /**
   * Replay change log from the loaded version to the target version.
   */
<<<<<<< HEAD
  private def replayChangelog(
      endVersion: Long,
      checkpointUniqueIdLineage: Option[Array[(Long, Option[String])]] = None): Unit = {

    println("wei== replayChangelog(), endVersion: " + endVersion + ", checkpointUniqueIdLineage: " + checkpointUniqueIdLineage.mkString(", "))
    checkpointUniqueIdLineage.foreach(x => println("wei== checkpointUniqueIdLineage: " + x.mkString(", ")))

    val versionsAndUniqueIds = checkpointUniqueIdLineage match {
      // First entry of lineage corresponds to loadedVersion
      case Some(lineage) => lineage.drop(1) // TODO: assert lineage.head == loadedVersion
      case None => (loadedVersion + 1 to endVersion).map((_, None)).toArray
    }

    logInfo(log"Replaying changelog from version " +
      log"${MDC(LogKeys.LOADED_VERSION, loadedVersion)} -> " +
      log"${MDC(LogKeys.END_VERSION, endVersion)}")

    versionsAndUniqueIds.foreach { case (v, uniqueId) =>
      logInfo(log"replaying changelog from version ${MDC(LogKeys.VERSION_NUM, v)} with " +
        log"unique Id: ${MDC(LogKeys.UUID, uniqueId.getOrElse(""))}")

      var changelogReader: StateStoreChangelogReader = null
      try {
        changelogReader = fileManager.getChangelogReader(v, useColumnFamilies, uniqueId)
        // TODO: verification? verify lineage is valid
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
      } finally {
        if (changelogReader != null) changelogReader.closeIfNeeded()
      }
    }
    loadedVersion = endVersion
  }

  /**
   * Get the value for the given key if present, or null.
   * @note This will return the last written value even if it was uncommitted.
   */
  def get(key: Array[Byte]): Array[Byte] = {
    db.get(readOptions, key)
  }

  /**
   * Put the given value for the given key.
   * @note This update is not committed to disk until commit() is called.
   */
  def put(key: Array[Byte], value: Array[Byte]): Unit = {
    if (conf.trackTotalNumberOfRows) {
      val oldValue = db.get(readOptions, key)
      if (oldValue == null) {
        numKeysOnWritingVersion += 1
      }
    }

    db.put(writeOptions, key, value)
    changelogWriter.foreach(_.put(key, value))
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
  def merge(key: Array[Byte], value: Array[Byte]): Unit = {

    if (conf.trackTotalNumberOfRows) {
      val oldValue = db.get(readOptions, key)
      if (oldValue == null) {
        numKeysOnWritingVersion += 1
      }
    }
    db.merge(writeOptions, key, value)

    changelogWriter.foreach(_.merge(key, value))
  }

  /**
   * Remove the key if present.
   * @note This update is not committed to disk until commit() is called.
   */
  def remove(key: Array[Byte]): Unit = {
    if (conf.trackTotalNumberOfRows) {
      val value = db.get(readOptions, key)
      if (value != null) {
        numKeysOnWritingVersion -= 1
      }
    }
    db.delete(writeOptions, key)
    changelogWriter.foreach(_.delete(key))
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
    val iter = db.newIterator()

    try {
      logInfo(log"Counting keys - getting iterator from version " +
        log"${MDC(LogKeys.LOADED_VERSION, loadedVersion)}")

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
    val iter = db.newIterator()
    iter.seek(prefix)

    // Attempt to close this iterator if there is a task failure, or a task interruption.
    Option(TaskContext.get()).foreach { tc =>
      tc.addTaskCompletionListener[Unit] { _ => iter.close() }
    }

    new NextIterator[ByteArrayPair] {
      override protected def getNext(): ByteArrayPair = {
        if (iter.isValid && iter.key().take(prefix.length).sameElements(prefix)) {
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

  /**
   * Commit all the updates made as a version to DFS. The steps it needs to do to commits are:
   * - Flush all changes to disk
   * - Create a RocksDB checkpoint in a new local dir
   * - Sync the checkpoint dir files to DFS
   */
  def commit(): Long = {
    val newVersion = loadedVersion + 1
    try {

      logInfo(log"Flushing updates for ${MDC(LogKeys.VERSION_NUM, newVersion)}")

      var compactTimeMs = 0L
      var flushTimeMs = 0L
      var checkpointTimeMs = 0L
      if (shouldCreateSnapshot() || shouldForceSnapshot.get()) {
        // Need to flush the change to disk before creating a checkpoint
        // because rocksdb wal is disabled.
        logInfo(log"Flushing updates for ${MDC(LogKeys.VERSION_NUM, newVersion)}")
        flushTimeMs = timeTakenMs {
          db.flush(flushOptions)
        }

        if (conf.compactOnCommit) {
          logInfo("Compacting")
          compactTimeMs = timeTakenMs {
            db.compactRange()
          }
        }

        checkpointTimeMs = timeTakenMs {
          val checkpointDir = createTempDir("checkpoint")
          logInfo(log"Creating checkpoint for ${MDC(LogKeys.VERSION_NUM, newVersion)} " +
            log"in ${MDC(LogKeys.PATH, checkpointDir)}")
          // Make sure the directory does not exist. Native RocksDB fails if the directory to
          // checkpoint exists.
          Utils.deleteRecursively(checkpointDir)
          // We no longer pause background operation before creating a RocksDB checkpoint because
          // it is unnecessary. The captured snapshot will still be consistent with ongoing
          // background operations.
          val cp = Checkpoint.create(db)
          cp.createCheckpoint(checkpointDir.toString)
          // if changelog checkpointing is disabled, the snapshot is uploaded synchronously
          // inside the uploadSnapshot() called below.
          // If changelog checkpointing is enabled, snapshot will be uploaded asynchronously
          // during state store maintenance.
          synchronized {
            if (latestSnapshot.isDefined) {
              oldSnapshots += latestSnapshot.get
            }
            latestSnapshot = Some(
              RocksDBSnapshot(checkpointDir,
                newVersion,
                numKeysOnWritingVersion,
                fileManager.captureFileMapReference(),
                colFamilyNameToIdMap.asScala.toMap,
                maxColumnFamilyId.get().toShort,
                sessionCheckpointId))
            lastSnapshotVersion = newVersion
          }
        }
      }

      if (checkpointFormatVersion >= 2) {
        LastCommitBasedCheckpointId = loadedCheckpointId
        lastCommittedCheckpointId = sessionCheckpointId
        loadedCheckpointId = sessionCheckpointId
      }

      logInfo(log"Syncing checkpoint for ${MDC(LogKeys.VERSION_NUM, newVersion)} to DFS")
      val fileSyncTimeMs = timeTakenMs {
        if (enableChangelogCheckpointing) {
          // If we have changed the columnFamilyId mapping, we have set a new
          // snapshot and need to upload this to the DFS even if changelog checkpointing
          // is enabled.
          if (shouldForceSnapshot.get()) {
            uploadSnapshot()
            changelogWriter = None
            changelogWriter.foreach(_.abort())
          } else {
            try {
              assert(changelogWriter.isDefined)
              changelogWriter.foreach(_.commit())
            } finally {
              changelogWriter = None
            }
          }
        } else {
          assert(changelogWriter.isEmpty)
          uploadSnapshot()
        }
      }

      // Set maxVersion when checkpoint files are synced to DFS successfully
      // We need to handle this explicitly in RocksDB as we could use different
      // changeLogWriter instances in fileManager instance when committing
      fileManager.setMaxSeenVersion(newVersion)

      numKeysOnLoadedVersion = numKeysOnWritingVersion
      loadedVersion = newVersion
      commitLatencyMs ++= Map(
        "flush" -> flushTimeMs,
        "compact" -> compactTimeMs,
        "checkpoint" -> checkpointTimeMs,
        "fileSync" -> fileSyncTimeMs
      )
      recordedMetrics = Some(metrics)
      logInfo(log"Committed ${MDC(LogKeys.VERSION_NUM, newVersion)}, " +
        log"stats = ${MDC(LogKeys.METRICS_JSON, recordedMetrics.get.json)}")
      loadedVersion
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

  private def uploadSnapshot(): Unit = {
    var oldSnapshotsImmutable: List[RocksDBSnapshot] = Nil
    val localCheckpoint = synchronized {
      val checkpoint = latestSnapshot
      latestSnapshot = None

      // Convert mutable list buffer to immutable to prevent
      // race condition with commit where old snapshot is added
      oldSnapshotsImmutable = oldSnapshots.toList
      oldSnapshots.clear()

      checkpoint
    }
    localCheckpoint match {
      case Some(
        // TODO: this snapshot should store the lineage
        // and you construct a new one with loadedCheckpointUniqueId
        RocksDBSnapshot(
          localDir,
          version,
          numKeys,
          capturedFileMappings,
          columnFamilyMapping,
          maxColumnFamilyId,
          uniqueId)) =>
        try {
          val uploadTime = timeTakenMs {
            fileManager.saveCheckpointToDfs(
              localDir,
              version,
              numKeys,
              capturedFileMappings,
              Some(columnFamilyMapping.toMap),
              Some(maxColumnFamilyId),
              uniqueId
            )
            fileManagerMetrics = fileManager.latestSaveCheckpointMetrics
          }
          logInfo(log"${MDC(LogKeys.LOG_ID, loggingId)}: Upload snapshot of version " +
            log"${MDC(LogKeys.VERSION_NUM, version)}," +
            log" time taken: ${MDC(LogKeys.TIME_UNITS, uploadTime)} ms")
        } finally {
          localCheckpoint.foreach(_.close())

          // Clean up old latestSnapshots
          for (snapshot <- oldSnapshotsImmutable) {
            snapshot.close()
          }

        }
      case _ =>
    }
  }

  /**
   * Drop uncommitted changes, and roll back to previous version.
   */
  def rollback(): Unit = {
    acquire(RollbackStore)
    numKeysOnWritingVersion = numKeysOnLoadedVersion
    loadedVersion = -1L
    changelogWriter.foreach(_.abort())
    // Make sure changelogWriter gets recreated next time.
    changelogWriter = None
    release(RollbackStore)
    // TODO: roll back id?
    logInfo(log"Rolled back to ${MDC(LogKeys.VERSION_NUM, loadedVersion)}")
  }

  def doMaintenance(): Unit = {
    if (enableChangelogCheckpointing) {
      // TODO: here need to correctly setup loadedId
      uploadSnapshot()
    }
    val cleanupTime = timeTakenMs {
      fileManager.deleteOldVersions(conf.minVersionsToRetain, conf.minVersionsToDelete)
    }
    logInfo(log"Cleaned old data, time taken: ${MDC(LogKeys.TIME_UNITS, cleanupTime)} ms")
  }

  /** Release all resources */
  def close(): Unit = {
    try {
      // Acquire DB instance lock and release at the end to allow for synchronized access
      acquire(CloseStore)
      closeDB()

      readOptions.close()
      writeOptions.close()
      flushOptions.close()
      dbOptions.close()
      dbLogger.close()
      synchronized {
        latestSnapshot.foreach(_.close())
        latestSnapshot = None
      }
      silentDeleteRecursively(localRootDir, "closing RocksDB")
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

  def getLatestCheckpointInfo(partitionId: Int): StateStoreCheckpointInfo = {
    StateStoreCheckpointInfo(
      partitionId,
      loadedVersion,
      lastCommittedCheckpointId,
      LastCommitBasedCheckpointId)
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
      memoryUsage,
      pinnedBlocksMemUsage,
      totalSSTFilesBytes,
      nativeOpsLatencyMicros,
      commitLatencyMs,
      bytesCopied = fileManagerMetrics.bytesCopied,
      filesCopied = fileManagerMetrics.filesCopied,
      filesReused = fileManagerMetrics.filesReused,
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
    try {
      acquire(ReportStoreMetrics)
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
        _ => this.release(StoreTaskCompletionListener)
      })
      logInfo(log"RocksDB instance was acquired by ${MDC(LogKeys.THREAD, acquiredThreadInfo)} " +
        log"for opType=${MDC(LogKeys.OP_TYPE, opType.toString)}")
    }
  }

  /**
   * Function to release RocksDB instance lock that allows for synchronized access to the state
   * store instance
   *
   * @param opType - operation type releasing the lock
   */
  private def release(opType: RocksDBOpType): Unit = acquireLock.synchronized {
    if (acquiredThreadInfo != null) {
      logInfo(log"RocksDB instance was released by ${MDC(LogKeys.THREAD,
        acquiredThreadInfo)} " + log"for opType=${MDC(LogKeys.OP_TYPE, opType.toString)}")
      acquiredThreadInfo = null
      acquireLock.notifyAll()
    }
  }

  private def getDBProperty(property: String): Long = db.getProperty(property).toLong

  private def openDB(): Unit = {
    assert(db == null)
    db = NativeRocksDB.open(dbOptions, workingDir.toString)
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

  /** Create a native RocksDB logger that forwards native logs to log4j with correct log levels. */
  private def createLogger(): Logger = {
    val dbLogger = new Logger(dbOptions.infoLogLevel()) {
      override def log(infoLogLevel: InfoLogLevel, logMsg: String) = {
        // Map DB log level to log4j levels
        // Warn is mapped to info because RocksDB warn is too verbose
        // (e.g. dumps non-warning stuff like stats)
        val loggingFunc: ( => LogEntry) => Unit = infoLogLevel match {
          case InfoLogLevel.FATAL_LEVEL | InfoLogLevel.ERROR_LEVEL => logError(_)
          case InfoLogLevel.WARN_LEVEL | InfoLogLevel.INFO_LEVEL => logInfo(_)
          case InfoLogLevel.DEBUG_LEVEL => logDebug(_)
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
    dbOptions.setInfoLogLevel(dbLogLevel)
    dbOptions.setLogger(dbLogger)
    logInfo(log"Set RocksDB native logging level to ${MDC(LogKeys.ROCKS_DB_LOG_LEVEL, dbLogLevel)}")
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
        logWarning(log"Error recursively deleting local dir ${MDC(LogKeys.PATH, file)} " +
          log"while ${MDC(LogKeys.ERROR, msg)}", e)
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
    compression: String)

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
      getStringConf(COMPRESSION_CONF))
  }

  def apply(): RocksDBConf = apply(new StateStoreConf())
}

/** Class to represent stats from each commit. */
case class RocksDBMetrics(
    numCommittedKeys: Long,
    numUncommittedKeys: Long,
    totalMemUsageBytes: Long,
    pinnedBlocksMemUsage: Long,
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

case class AcquiredThreadInfo() {
  val threadRef: WeakReference[Thread] = new WeakReference[Thread](Thread.currentThread())
  val tc: TaskContext = TaskContext.get()

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
// scalastyle:on
