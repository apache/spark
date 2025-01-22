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

import java.io._
import java.util
import java.util.Locale
import java.util.concurrent.atomic.LongAdder

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import com.google.common.io.ByteStreams
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

import org.apache.spark.{SparkConf, SparkEnv, SparkException}
import org.apache.spark.internal.{Logging, LogKeys, MDC, MessageWithContext}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.streaming.CheckpointFileManager
import org.apache.spark.sql.execution.streaming.CheckpointFileManager.CancellableFSDataOutputStream
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.{SizeEstimator, Utils}
import org.apache.spark.util.ArrayImplicits._


/**
 * An implementation of [[StateStoreProvider]] and [[StateStore]] in which all the data is backed
 * by files in an HDFS-compatible file system. All updates to the store has to be done in sets
 * transactionally, and each set of updates increments the store's version. These versions can
 * be used to re-execute the updates (by retries in RDD operations) on the correct version of
 * the store, and regenerate the store version.
 *
 * Usage:
 * To update the data in the state store, the following order of operations are needed.
 *
 * // get the right store
 * - val store = StateStore.get(
 *      StateStoreId(checkpointLocation, operatorId, partitionId), ..., version, ...)
 * - store.put(...)
 * - store.remove(...)
 * - store.commit()    // commits all the updates to made; the new version will be returned
 * - store.iterator()  // key-value data after last commit as an iterator
 *
 * Fault-tolerance model:
 * - Every set of updates is written to a delta file before committing.
 * - The state store is responsible for managing, collapsing and cleaning up of delta files.
 * - Multiple attempts to commit the same version of updates may overwrite each other.
 *   Consistency guarantees depend on whether multiple attempts have the same updates and
 *   the overwrite semantics of underlying file system.
 * - Background maintenance of files ensures that last versions of the store is always recoverable
 * to ensure re-executed RDD operations re-apply updates on the correct past version of the
 * store.
 */
private[sql] class HDFSBackedStateStoreProvider extends StateStoreProvider with Logging
  with SupportsFineGrainedReplay {

  private val providerName = "HDFSBackedStateStoreProvider"

  class HDFSBackedReadStateStore(val version: Long, map: HDFSBackedStateStoreMap)
    extends ReadStateStore {

    override def id: StateStoreId = HDFSBackedStateStoreProvider.this.stateStoreId

    override def get(key: UnsafeRow, colFamilyName: String): UnsafeRow = map.get(key)

    override def iterator(colFamilyName: String): Iterator[UnsafeRowPair] = {
      map.iterator()
    }

    override def abort(): Unit = {}

    override def toString(): String = {
      s"HDFSReadStateStore[id=(op=${id.operatorId},part=${id.partitionId}),dir=$baseDir]"
    }

    override def prefixScan(prefixKey: UnsafeRow, colFamilyName: String):
      Iterator[UnsafeRowPair] = {
      map.prefixScan(prefixKey)
    }

    override def valuesIterator(key: UnsafeRow, colFamilyName: String): Iterator[UnsafeRow] = {
      throw StateStoreErrors.unsupportedOperationException("multipleValuesPerKey", "HDFSStateStore")
    }
  }

  /** Implementation of [[StateStore]] API which is backed by an HDFS-compatible file system */
  class HDFSBackedStateStore(val version: Long, mapToUpdate: HDFSBackedStateStoreMap)
    extends StateStore {

    /** Trait and classes representing the internal state of the store */
    trait STATE
    case object UPDATING extends STATE
    case object COMMITTED extends STATE
    case object ABORTED extends STATE

    private val newVersion = version + 1
    @volatile private var state: STATE = UPDATING
    private val finalDeltaFile: Path = deltaFile(newVersion)
    private lazy val deltaFileStream = fm.createAtomic(finalDeltaFile, overwriteIfPossible = true)
    private lazy val compressedStream = compressStream(deltaFileStream)

    override def id: StateStoreId = HDFSBackedStateStoreProvider.this.stateStoreId

    override def createColFamilyIfAbsent(
        colFamilyName: String,
        keySchema: StructType,
        valueSchema: StructType,
        keyStateEncoderSpec: KeyStateEncoderSpec,
        useMultipleValuesPerKey: Boolean = false,
        isInternal: Boolean = false): Unit = {
      throw StateStoreErrors.multipleColumnFamiliesNotSupported(providerName)
    }

    // Multiple col families are not supported with HDFSBackedStateStoreProvider. Throw an exception
    // if the user tries to use a non-default col family.
    private def assertUseOfDefaultColFamily(colFamilyName: String): Unit = {
      if (colFamilyName != StateStore.DEFAULT_COL_FAMILY_NAME) {

        throw StateStoreErrors.multipleColumnFamiliesNotSupported(providerName)
      }
    }

    override def get(key: UnsafeRow, colFamilyName: String): UnsafeRow = {
      assertUseOfDefaultColFamily(colFamilyName)
      mapToUpdate.get(key)
    }

    override def put(key: UnsafeRow, value: UnsafeRow, colFamilyName: String): Unit = {
      assertUseOfDefaultColFamily(colFamilyName)
      require(value != null, "Cannot put a null value")
      verify(state == UPDATING, "Cannot put after already committed or aborted")
      val keyCopy = key.copy()
      val valueCopy = value.copy()
      mapToUpdate.put(keyCopy, valueCopy)
      writeUpdateToDeltaFile(compressedStream, keyCopy, valueCopy)
    }

    override def remove(key: UnsafeRow, colFamilyName: String): Unit = {
      assertUseOfDefaultColFamily(colFamilyName)
      verify(state == UPDATING, "Cannot remove after already committed or aborted")
      val prevValue = mapToUpdate.remove(key)
      if (prevValue != null) {
        writeRemoveToDeltaFile(compressedStream, key)
      }
    }

    /** Commit all the updates that have been made to the store, and return the new version. */
    override def commit(): Long = {
      try {
        verify(state == UPDATING, "Cannot commit after already committed or aborted")
        commitUpdates(newVersion, mapToUpdate, compressedStream)
        state = COMMITTED
        logInfo(log"Committed version ${MDC(LogKeys.COMMITTED_VERSION, newVersion)} " +
          log"for ${MDC(LogKeys.STATE_STORE_PROVIDER, this)} to file " +
          log"${MDC(LogKeys.FILE_NAME, finalDeltaFile)}")
        newVersion
      } catch {
        case e: Throwable =>
          throw QueryExecutionErrors.failedToCommitStateFileError(this.toString(), e)
      }
    }

    /** Abort all the updates made on this store. This store will not be usable any more. */
    override def abort(): Unit = {
      // This if statement is to ensure that files are deleted only once: if either commit or abort
      // is called before, it will be no-op.
      if (state == UPDATING) {
        state = ABORTED
        cancelDeltaFile(compressedStream, deltaFileStream)
      } else {
        state = ABORTED
      }
      logInfo(log"Aborted version ${MDC(LogKeys.STATE_STORE_VERSION, newVersion)} " +
        log"for ${MDC(LogKeys.STATE_STORE_PROVIDER, this)}")
    }

    /**
     * Get an iterator of all the store data.
     * This can be called only after committing all the updates made in the current thread.
     */
    override def iterator(colFamilyName: String): Iterator[UnsafeRowPair] = {
      assertUseOfDefaultColFamily(colFamilyName)
      mapToUpdate.iterator()
    }

    override def prefixScan(prefixKey: UnsafeRow, colFamilyName: String):
      Iterator[UnsafeRowPair] = {
      assertUseOfDefaultColFamily(colFamilyName)
      mapToUpdate.prefixScan(prefixKey)
    }

    override def metrics: StateStoreMetrics = {
      // NOTE: we provide estimation of cache size as "memoryUsedBytes", and size of state for
      // current version as "stateOnCurrentVersionSizeBytes"
      val metricsFromProvider: Map[String, Long] = getMetricsForProvider()

      val customMetrics = metricsFromProvider.flatMap { case (name, value) =>
        // just allow searching from list cause the list is small enough
        supportedCustomMetrics.find(_.name == name).map(_ -> value)
      } + (metricStateOnCurrentVersionSizeBytes -> SizeEstimator.estimate(mapToUpdate))

      StateStoreMetrics(mapToUpdate.size(), metricsFromProvider("memoryUsedBytes"), customMetrics)
    }

    override def getStateStoreCheckpointInfo(): StateStoreCheckpointInfo = {
      StateStoreCheckpointInfo(id.partitionId, newVersion, None, None)
    }

    /**
     * Whether all updates have been committed
     */
    override def hasCommitted: Boolean = {
      state == COMMITTED
    }

    override def toString(): String = {
      s"HDFSStateStore[id=(op=${id.operatorId},part=${id.partitionId}),dir=$baseDir]"
    }

    override def removeColFamilyIfExists(colFamilyName: String): Boolean = {
      throw StateStoreErrors.removingColumnFamiliesNotSupported(providerName)
    }

    override def valuesIterator(key: UnsafeRow, colFamilyName: String): Iterator[UnsafeRow] = {
      throw StateStoreErrors.unsupportedOperationException("multipleValuesPerKey", providerName)
    }

    override def merge(key: UnsafeRow,
        value: UnsafeRow,
        colFamilyName: String): Unit = {
      throw StateStoreErrors.unsupportedOperationException("merge", providerName)
    }
  }

  def getMetricsForProvider(): Map[String, Long] = synchronized {
    Map("memoryUsedBytes" -> SizeEstimator.estimate(loadedMaps),
      metricLoadedMapCacheHit.name -> loadedMapCacheHitCount.sum(),
      metricLoadedMapCacheMiss.name -> loadedMapCacheMissCount.sum())
  }

  /** Get the state store for making updates to create a new `version` of the store. */
  override def getStore(version: Long, uniqueId: Option[String] = None): StateStore = {
    if (uniqueId.isDefined) {
      throw QueryExecutionErrors.cannotLoadStore(new SparkException(
        "HDFSBackedStateStoreProvider does not support checkpointFormatVersion > 1 " +
        "but a state store checkpointID is passed in"))
    }
    val newMap = getLoadedMapForStore(version)
    logInfo(log"Retrieved version ${MDC(LogKeys.STATE_STORE_VERSION, version)} " +
      log"of ${MDC(LogKeys.STATE_STORE_PROVIDER, HDFSBackedStateStoreProvider.this)} for update")
    new HDFSBackedStateStore(version, newMap)
  }

  /** Get the state store for reading to specific `version` of the store. */
  override def getReadStore(version: Long, stateStoreCkptId: Option[String]): ReadStateStore = {
    val newMap = getLoadedMapForStore(version)
    logInfo(log"Retrieved version ${MDC(LogKeys.STATE_STORE_VERSION, version)} of " +
      log"${MDC(LogKeys.STATE_STORE_PROVIDER, HDFSBackedStateStoreProvider.this)} for readonly")
    new HDFSBackedReadStateStore(version, newMap)
  }

  private def getLoadedMapForStore(version: Long): HDFSBackedStateStoreMap = synchronized {
    try {
      if (version < 0) {
        throw QueryExecutionErrors.unexpectedStateStoreVersion(version)
      }
      val newMap = HDFSBackedStateStoreMap.create(keySchema, numColsPrefixKey)
      if (version > 0) {
        newMap.putAll(loadMap(version))
      }
      newMap
    }
    catch {
      case e: SparkException
        if Option(e.getCondition).exists(_.contains("CANNOT_LOAD_STATE_STORE")) =>
        throw e
      case e: OutOfMemoryError =>
        throw QueryExecutionErrors.notEnoughMemoryToLoadStore(
          stateStoreId.toString,
          "HDFS_STORE_PROVIDER",
          e)
      case e: Throwable => throw QueryExecutionErrors.cannotLoadStore(e)
    }
  }

  // Run bunch of validations specific to HDFSBackedStateStoreProvider
  private def runValidation(
      useColumnFamilies: Boolean,
      useMultipleValuesPerKey: Boolean,
      keyStateEncoderSpec: KeyStateEncoderSpec): Unit = {
    // TODO: add support for multiple col families with HDFSBackedStateStoreProvider
    if (useColumnFamilies) {
      throw StateStoreErrors.multipleColumnFamiliesNotSupported(providerName)
    }

    if (useMultipleValuesPerKey) {
      throw StateStoreErrors.unsupportedOperationException("multipleValuesPerKey", providerName)
    }

    if (keyStateEncoderSpec.isInstanceOf[RangeKeyScanStateEncoderSpec]) {
      throw StateStoreErrors.unsupportedOperationException("Range scan", providerName)
    }
  }

  private def getNumColsPrefixKey(keyStateEncoderSpec: KeyStateEncoderSpec): Int = {
    keyStateEncoderSpec match {
      case NoPrefixKeyStateEncoderSpec(_) => 0
      case PrefixKeyScanStateEncoderSpec(_, numColsPrefixKey) => numColsPrefixKey
      case _ => throw StateStoreErrors.unsupportedOperationException("Invalid key state encoder",
        providerName)
    }
  }

  override def init(
      stateStoreId: StateStoreId,
      keySchema: StructType,
      valueSchema: StructType,
      keyStateEncoderSpec: KeyStateEncoderSpec,
      useColumnFamilies: Boolean,
      storeConf: StateStoreConf,
      hadoopConf: Configuration,
      useMultipleValuesPerKey: Boolean = false,
      stateSchemaProvider: Option[StateSchemaProvider] = None): Unit = {
    assert(
      !storeConf.enableStateStoreCheckpointIds,
      "HDFS State Store Provider doesn't support checkpointFormatVersion >= 2 " +
        s"checkpointFormatVersion ${storeConf.sqlConf.stateStoreCheckpointFormatVersion}")

    this.stateStoreId_ = stateStoreId
    this.keySchema = keySchema
    this.valueSchema = valueSchema
    this.storeConf = storeConf
    this.hadoopConf = hadoopConf
    this.numberOfVersionsToRetainInMemory = storeConf.maxVersionsToRetainInMemory

    // run a bunch of validation checks for this state store provider
    runValidation(useColumnFamilies, useMultipleValuesPerKey, keyStateEncoderSpec)

    this.numColsPrefixKey = getNumColsPrefixKey(keyStateEncoderSpec)

    fm.mkdirs(baseDir)
  }

  override def stateStoreId: StateStoreId = stateStoreId_

  /** Do maintenance backing data files, including creating snapshots and cleaning up old files */
  override def doMaintenance(): Unit = {
    try {
      doSnapshot("maintenance")
      cleanup()
    } catch {
      case NonFatal(e) =>
        logWarning(log"Error performing snapshot and cleaning up " + toMessageWithContext)
    }
  }

  override def close(): Unit = {
    // Clearing the map resets the TreeMap.root to null, and therefore entries inside the
    // `loadedMaps` will be de-referenced and GCed automatically when their reference
    // counts become 0.
    synchronized { loadedMaps.clear() }
  }

  override def supportedCustomMetrics: Seq[StateStoreCustomMetric] = {
    metricStateOnCurrentVersionSizeBytes :: metricLoadedMapCacheHit :: metricLoadedMapCacheMiss ::
      Nil
  }

  private def toMessageWithContext: MessageWithContext = {
    log"HDFSStateStoreProvider[id = (op=${MDC(LogKeys.OP_ID, stateStoreId.operatorId)}," +
      log"part=${MDC(LogKeys.PARTITION_ID, stateStoreId.partitionId)})," +
      log"dir = ${MDC(LogKeys.PATH, baseDir)}]"
  }

  override def toString(): String = {
    toMessageWithContext.message
  }

  /* Internal fields and methods */

  @volatile private var stateStoreId_ : StateStoreId = _
  @volatile private var keySchema: StructType = _
  @volatile private var valueSchema: StructType = _
  @volatile private var storeConf: StateStoreConf = _
  @volatile private var hadoopConf: Configuration = _
  @volatile private var numberOfVersionsToRetainInMemory: Int = _
  @volatile private var numColsPrefixKey: Int = 0

  // TODO: The validation should be moved to a higher level so that it works for all state store
  // implementations
  @volatile private var isValidated = false

  private lazy val loadedMaps = new util.TreeMap[Long, HDFSBackedStateStoreMap](
    Ordering[Long].reverse)
  private lazy val baseDir = stateStoreId.storeCheckpointLocation()
  private lazy val fm = CheckpointFileManager.create(baseDir, hadoopConf)
  private lazy val sparkConf = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf)

  private val loadedMapCacheHitCount: LongAdder = new LongAdder
  private val loadedMapCacheMissCount: LongAdder = new LongAdder

  private lazy val metricStateOnCurrentVersionSizeBytes: StateStoreCustomSizeMetric =
    StateStoreCustomSizeMetric("stateOnCurrentVersionSizeBytes",
      "estimated size of state only on current version")

  private lazy val metricLoadedMapCacheHit: StateStoreCustomMetric =
    StateStoreCustomSumMetric("loadedMapCacheHitCount",
      "count of cache hit on states cache in provider")

  private lazy val metricLoadedMapCacheMiss: StateStoreCustomMetric =
    StateStoreCustomSumMetric("loadedMapCacheMissCount",
      "count of cache miss on states cache in provider")

  private case class StoreFile(version: Long, path: Path, isSnapshot: Boolean)

  private def commitUpdates(
      newVersion: Long,
      map: HDFSBackedStateStoreMap,
      output: DataOutputStream): Unit = {
    synchronized {
      finalizeDeltaFile(output)
      putStateIntoStateCacheMap(newVersion, map)
    }
  }

  /**
   * Get iterator of all the data of the latest version of the store.
   * Note that this will look up the files to determined the latest known version.
   */
  private[state] def latestIterator(): Iterator[UnsafeRowPair] = synchronized {
    val versionsInFiles = fetchFiles().map(_.version).toSet
    val versionsLoaded = loadedMaps.keySet.asScala
    val allKnownVersions = versionsInFiles ++ versionsLoaded
    if (allKnownVersions.nonEmpty) {
      loadMap(allKnownVersions.max).iterator()
    } else Iterator.empty
  }

  /** This method is intended to be only used for unit test(s). DO NOT TOUCH ELEMENTS IN MAP! */
  private[state] def getLoadedMaps(): util.SortedMap[Long, HDFSBackedStateStoreMap] = synchronized {
    // shallow copy as a minimal guard
    loadedMaps.clone().asInstanceOf[util.SortedMap[Long, HDFSBackedStateStoreMap]]
  }

  private def putStateIntoStateCacheMap(
      newVersion: Long,
      map: HDFSBackedStateStoreMap): Unit = synchronized {
    val loadedEntries = loadedMaps.size()
    val earliestLoadedVersion: Option[Long] = if (loadedEntries > 0) {
      Some(loadedMaps.lastKey())
    } else {
      None
    }

    if (earliestLoadedVersion.isDefined) {
      logInfo(log"Trying to add version=${MDC(LogKeys.STATE_STORE_VERSION, newVersion)} to state " +
        log"cache map with current_size=${MDC(LogKeys.NUM_LOADED_ENTRIES, loadedEntries)} and " +
        log"earliest_loaded_version=" +
        log"${MDC(LogKeys.EARLIEST_LOADED_VERSION, earliestLoadedVersion.get)}} " +
        log"and max_versions_to_retain_in_memory=" +
        log"${MDC(LogKeys.NUM_VERSIONS_RETAIN, numberOfVersionsToRetainInMemory)}")
    } else {
      logInfo(log"Trying to add version=${MDC(LogKeys.STATE_STORE_VERSION, newVersion)} to state " +
        log"cache map with current_size=${MDC(LogKeys.NUM_LOADED_ENTRIES, loadedEntries)} and " +
        log"max_versions_to_retain_in_memory=" +
        log"${MDC(LogKeys.NUM_VERSIONS_RETAIN, numberOfVersionsToRetainInMemory)}")
    }

    if (numberOfVersionsToRetainInMemory <= 0) {
      if (loadedMaps.size() > 0) loadedMaps.clear()
      return
    }

    while (loadedMaps.size() > numberOfVersionsToRetainInMemory) {
      loadedMaps.remove(loadedMaps.lastKey())
    }

    val size = loadedMaps.size()
    if (size == numberOfVersionsToRetainInMemory) {
      val versionIdForLastKey = loadedMaps.lastKey()
      if (versionIdForLastKey > newVersion) {
        // this is the only case which we can avoid putting, because new version will be placed to
        // the last key and it should be evicted right away
        return
      } else if (versionIdForLastKey < newVersion) {
        // this case needs removal of the last key before putting new one
        loadedMaps.remove(versionIdForLastKey)
      }
    }

    loadedMaps.put(newVersion, map)
  }

  /** Load the required version of the map data from the backing files */
  private def loadMap(version: Long): HDFSBackedStateStoreMap = {

    // Shortcut if the map for this version is already there to avoid a redundant put.
    val loadedCurrentVersionMap = synchronized { Option(loadedMaps.get(version)) }
    if (loadedCurrentVersionMap.isDefined) {
      loadedMapCacheHitCount.increment()
      return loadedCurrentVersionMap.get
    }

    logWarning(log"The state for version ${MDC(LogKeys.FILE_VERSION, version)} doesn't exist in " +
      log"loadedMaps. Reading snapshot file and delta files if needed..." +
      log"Note that this is normal for the first batch of starting query.")

    loadedMapCacheMissCount.increment()

    val (result, elapsedMs) = Utils.timeTakenMs {
      val snapshotCurrentVersionMap = readSnapshotFile(version)
      if (snapshotCurrentVersionMap.isDefined) {
        synchronized { putStateIntoStateCacheMap(version, snapshotCurrentVersionMap.get) }
        return snapshotCurrentVersionMap.get
      }

      // Find the most recent map before this version that we can.
      // [SPARK-22305] This must be done iteratively to avoid stack overflow.
      var lastAvailableVersion = version
      var lastAvailableMap: Option[HDFSBackedStateStoreMap] = None
      while (lastAvailableMap.isEmpty) {
        lastAvailableVersion -= 1

        if (lastAvailableVersion <= 0) {
          // Use an empty map for versions 0 or less.
          lastAvailableMap = Some(HDFSBackedStateStoreMap.create(keySchema, numColsPrefixKey))
        } else {
          lastAvailableMap =
            synchronized { Option(loadedMaps.get(lastAvailableVersion)) }
              .orElse(readSnapshotFile(lastAvailableVersion))
        }
      }

      // Load all the deltas from the version after the last available one up to the target version.
      // The last available version is the one with a full snapshot, so it doesn't need deltas.
      val resultMap = HDFSBackedStateStoreMap.create(keySchema, numColsPrefixKey)
      resultMap.putAll(lastAvailableMap.get)
      for (deltaVersion <- lastAvailableVersion + 1 to version) {
        updateFromDeltaFile(deltaVersion, resultMap)
      }

      synchronized { putStateIntoStateCacheMap(version, resultMap) }
      resultMap
    }

    logDebug(s"Loading state for $version takes $elapsedMs ms.")

    result
  }

  private def writeUpdateToDeltaFile(
      output: DataOutputStream,
      key: UnsafeRow,
      value: UnsafeRow): Unit = {
    val keyBytes = key.getBytes()
    val valueBytes = value.getBytes()
    output.writeInt(keyBytes.size)
    output.write(keyBytes)
    output.writeInt(valueBytes.size)
    output.write(valueBytes)
  }

  private def writeRemoveToDeltaFile(output: DataOutputStream, key: UnsafeRow): Unit = {
    val keyBytes = key.getBytes()
    output.writeInt(keyBytes.size)
    output.write(keyBytes)
    output.writeInt(-1)
  }

  private def finalizeDeltaFile(output: DataOutputStream): Unit = {
    output.writeInt(-1)  // Write this magic number to signify end of file
    output.close()
  }

  private def updateFromDeltaFile(version: Long, map: HDFSBackedStateStoreMap): Unit = {
    val fileToRead = deltaFile(version)
    var input: DataInputStream = null
    val sourceStream = try {
      fm.open(fileToRead)
    } catch {
      case f: FileNotFoundException =>
        throw QueryExecutionErrors.failedToReadDeltaFileNotExistsError(fileToRead, toString(), f)
    }
    try {
      input = decompressStream(sourceStream)
      var eof = false

      while (!eof) {
        val keySize = input.readInt()
        if (keySize == -1) {
          eof = true
        } else if (keySize < 0) {
          throw QueryExecutionErrors.failedToReadDeltaFileKeySizeError(
            fileToRead, toString(), keySize)
        } else {
          val keyRowBuffer = new Array[Byte](keySize)
          ByteStreams.readFully(input, keyRowBuffer, 0, keySize)

          val keyRow = new UnsafeRow(keySchema.fields.length)
          keyRow.pointTo(keyRowBuffer, keySize)

          val valueSize = input.readInt()
          if (valueSize < 0) {
            map.remove(keyRow)
          } else {
            val valueRowBuffer = new Array[Byte](valueSize)
            ByteStreams.readFully(input, valueRowBuffer, 0, valueSize)
            val valueRow = new UnsafeRow(valueSchema.fields.length)
            // If valueSize in existing file is not multiple of 8, floor it to multiple of 8.
            // This is a workaround for the following:
            // Prior to Spark 2.3 mistakenly append 4 bytes to the value row in
            // `RowBasedKeyValueBatch`, which gets persisted into the checkpoint data
            valueRow.pointTo(valueRowBuffer, (valueSize / 8) * 8)
            if (!isValidated) {
              StateStoreProvider.validateStateRowFormat(
                keyRow, keySchema, valueRow, valueSchema, storeConf)
              isValidated = true
            }
            map.put(keyRow, valueRow)
          }
        }
      }
    } finally {
      if (input != null) input.close()
    }
    logInfo(log"Read delta file for version ${MDC(LogKeys.FILE_VERSION, version)} " +
      log"of ${MDC(LogKeys.STATE_STORE_PROVIDER, this)} from ${MDC(LogKeys.FILE_NAME, fileToRead)}")
  }

  private def writeSnapshotFile(
      version: Long,
      map: HDFSBackedStateStoreMap,
      opType: String): Unit = {
    val targetFile = snapshotFile(version)
    var rawOutput: CancellableFSDataOutputStream = null
    var output: DataOutputStream = null
    try {
      rawOutput = fm.createAtomic(targetFile, overwriteIfPossible = true)
      output = compressStream(rawOutput)
      val iter = map.iterator()
      while (iter.hasNext) {
        val entry = iter.next()
        val keyBytes = entry.key.getBytes()
        val valueBytes = entry.value.getBytes()
        output.writeInt(keyBytes.size)
        output.write(keyBytes)
        output.writeInt(valueBytes.size)
        output.write(valueBytes)
      }
      output.writeInt(-1)
      output.close()
    } catch {
      case e: Throwable =>
        cancelDeltaFile(compressedStream = output, rawStream = rawOutput)
        throw e
    }
    logInfo(log"Written snapshot file for version ${MDC(LogKeys.FILE_VERSION, version)} of " +
      log"${MDC(LogKeys.STATE_STORE_PROVIDER, this)} at ${MDC(LogKeys.FILE_NAME, targetFile)} " +
      log"for ${MDC(LogKeys.OP_TYPE, opType)}")
  }

  /**
   * Try to cancel the underlying stream and safely close the compressed stream.
   *
   * @param compressedStream the compressed stream.
   * @param rawStream the underlying stream which needs to be cancelled.
   */
  private def cancelDeltaFile(
      compressedStream: DataOutputStream,
      rawStream: CancellableFSDataOutputStream): Unit = {
    try {
      if (rawStream != null) rawStream.cancel()
      IOUtils.closeQuietly(compressedStream)
    } catch {
      // Closing the compressedStream causes the stream to write/flush flush data into the
      // rawStream. Since the rawStream is already closed, there may be errors.
      // Usually its an IOException. However, Hadoop's RawLocalFileSystem wraps
      // IOException into FSError.
      case e: FSError if e.getCause.isInstanceOf[IOException] =>

      // SPARK-42668 - Catch and log any other exception thrown while trying to cancel
      // raw stream or close compressed stream.
      case NonFatal(ex) =>
        logInfo(log"Failed to cancel delta file for " +
          log"provider=${MDC(LogKeys.STATE_STORE_ID, stateStoreId)} " +
          log"with exception=${MDC(LogKeys.ERROR, ex)}")
    }
  }

  /**
   * Try to read the snapshot file. If the snapshot file is not available, return [[None]].
   *
   * @param version the version of the snapshot file
  */
  private def readSnapshotFile(version: Long): Option[HDFSBackedStateStoreMap] = {
    val fileToRead = snapshotFile(version)
    val map = HDFSBackedStateStoreMap.create(keySchema, numColsPrefixKey)
    var input: DataInputStream = null

    try {
      input = decompressStream(fm.open(fileToRead))
      var eof = false

      while (!eof) {
        val keySize = input.readInt()
        if (keySize == -1) {
          eof = true
        } else if (keySize < 0) {
          throw QueryExecutionErrors.failedToReadSnapshotFileKeySizeError(
            fileToRead, toString(), keySize)
        } else {
          val keyRowBuffer = new Array[Byte](keySize)
          ByteStreams.readFully(input, keyRowBuffer, 0, keySize)

          val keyRow = new UnsafeRow(keySchema.fields.length)
          keyRow.pointTo(keyRowBuffer, keySize)

          val valueSize = input.readInt()
          if (valueSize < 0) {
            throw QueryExecutionErrors.failedToReadSnapshotFileValueSizeError(
              fileToRead, toString(), valueSize)
          } else {
            val valueRowBuffer = new Array[Byte](valueSize)
            ByteStreams.readFully(input, valueRowBuffer, 0, valueSize)
            val valueRow = new UnsafeRow(valueSchema.fields.length)
            // If valueSize in existing file is not multiple of 8, floor it to multiple of 8.
            // This is a workaround for the following:
            // Prior to Spark 2.3 mistakenly append 4 bytes to the value row in
            // `RowBasedKeyValueBatch`, which gets persisted into the checkpoint data
            valueRow.pointTo(valueRowBuffer, (valueSize / 8) * 8)
            if (!isValidated) {
              StateStoreProvider.validateStateRowFormat(
                keyRow, keySchema, valueRow, valueSchema, storeConf)
              isValidated = true
            }
            map.put(keyRow, valueRow)
          }
        }
      }
      logInfo(log"Read snapshot file for version ${MDC(LogKeys.SNAPSHOT_VERSION, version)} of " +
        log"${MDC(LogKeys.STATE_STORE_PROVIDER, this)} from ${MDC(LogKeys.FILE_NAME, fileToRead)}")
      Some(map)
    } catch {
      case _: FileNotFoundException =>
        None
    } finally {
      if (input != null) input.close()
    }
  }


  /** Perform a snapshot of the store to allow delta files to be consolidated */
  private def doSnapshot(opType: String): Unit = {
    try {
      val (files, e1) = Utils.timeTakenMs(fetchFiles())
      logDebug(s"fetchFiles() took $e1 ms.")

      if (files.nonEmpty) {
        val lastVersion = files.last.version
        val deltaFilesForLastVersion =
          filesForVersion(files, lastVersion).filter(_.isSnapshot == false)
        synchronized { Option(loadedMaps.get(lastVersion)) } match {
          case Some(map) =>
            if (deltaFilesForLastVersion.size > storeConf.minDeltasForSnapshot) {
              val (_, e2) = Utils.timeTakenMs(writeSnapshotFile(lastVersion, map, opType))
              logDebug(s"writeSnapshotFile() took $e2 ms.")
            }
          case None =>
            // The last map is not loaded, probably some other instance is in charge
        }
      }
    } catch {
      case NonFatal(e) =>
        logWarning(log"Error doing snapshots for " + toMessageWithContext, e)
    }
  }

  /**
   * Clean up old snapshots and delta files that are not needed any more. It ensures that last
   * few versions of the store can be recovered from the files, so re-executed RDD operations
   * can re-apply updates on the past versions of the store.
   */
  private[state] def cleanup(): Unit = {
    try {
      val (files, e1) = Utils.timeTakenMs(fetchFiles())
      logDebug(s"fetchFiles() took $e1 ms.")

      if (files.nonEmpty) {
        val earliestVersionToRetain = files.last.version - storeConf.minVersionsToRetain
        if (earliestVersionToRetain > 0) {
          val earliestFileToRetain = filesForVersion(files, earliestVersionToRetain).head
          val filesToDelete = files.filter(_.version < earliestFileToRetain.version)
          val (_, e2) = Utils.timeTakenMs {
            filesToDelete.foreach { f =>
              fm.delete(f.path)
            }
          }
          logDebug(s"deleting files took $e2 ms.")
          logInfo(log"Deleted files older than " +
            log"${MDC(LogKeys.FILE_VERSION, earliestFileToRetain.version)} for " +
            log"${MDC(LogKeys.STATE_STORE_PROVIDER, this)}: " +
            log"${MDC(LogKeys.FILE_NAME, filesToDelete.mkString(", "))}")
        }
      }
    } catch {
      case NonFatal(e) =>
        logWarning(log"Error cleaning up files for " + toMessageWithContext, e)
    }
  }

  /** Files needed to recover the given version of the store */
  private def filesForVersion(allFiles: Seq[StoreFile], version: Long): Seq[StoreFile] = {
    require(version >= 0)
    require(allFiles.exists(_.version == version))

    val latestSnapshotFileBeforeVersion = allFiles
      .filter(_.isSnapshot)
      .takeWhile(_.version <= version)
      .lastOption
    val deltaBatchFiles = latestSnapshotFileBeforeVersion match {
      case Some(snapshotFile) =>

        val deltaFiles = allFiles.filter { file =>
          file.version > snapshotFile.version && file.version <= version
        }.toList
        verify(
          deltaFiles.size == version - snapshotFile.version,
          s"Unexpected list of delta files for version $version for $this: $deltaFiles"
        )
        deltaFiles

      case None =>
        allFiles.takeWhile(_.version <= version)
    }
    latestSnapshotFileBeforeVersion.toSeq ++ deltaBatchFiles
  }

  /** Fetch all the files that back the store */
  private def fetchFiles(): Seq[StoreFile] = {
    val files: Seq[FileStatus] = try {
      fm.list(baseDir).toImmutableArraySeq
    } catch {
      case _: java.io.FileNotFoundException =>
        Seq.empty
    }
    val versionToFiles = new mutable.HashMap[Long, StoreFile]
    files.foreach { status =>
      val path = status.getPath
      val nameParts = path.getName.split("\\.")
      if (nameParts.size == 2) {
        val version = nameParts(0).toLong
        nameParts(1).toLowerCase(Locale.ROOT) match {
          case "delta" =>
            // ignore the file otherwise, snapshot file already exists for that batch id
            if (!versionToFiles.contains(version)) {
              versionToFiles.put(version, StoreFile(version, path, isSnapshot = false))
            }
          case "snapshot" =>
            versionToFiles.put(version, StoreFile(version, path, isSnapshot = true))
          case _ => logWarning(
            log"Could not identify file ${MDC(LogKeys.PATH, path)} for " + toMessageWithContext)
        }
      }
    }
    val storeFiles = versionToFiles.values.toSeq.sortBy(_.version)
    logDebug(s"Current set of files for $this: ${storeFiles.mkString(", ")}")
    storeFiles
  }

  private def compressStream(outputStream: DataOutputStream): DataOutputStream = {
    val compressed = CompressionCodec.createCodec(sparkConf, storeConf.compressionCodec)
      .compressedOutputStream(outputStream)
    new DataOutputStream(compressed)
  }

  private def decompressStream(inputStream: DataInputStream): DataInputStream = {
    val compressed = CompressionCodec.createCodec(sparkConf, storeConf.compressionCodec)
      .compressedInputStream(inputStream)
    new DataInputStream(compressed)
  }

  private def deltaFile(version: Long): Path = {
    new Path(baseDir, s"$version.delta")
  }

  private def snapshotFile(version: Long): Path = {
    new Path(baseDir, s"$version.snapshot")
  }

  private def verify(condition: => Boolean, msg: String): Unit = {
    if (!condition) {
      throw new IllegalStateException(msg)
    }
  }

  /**
   * Get the state store of endVersion by applying delta files on the snapshot of snapshotVersion.
   * If snapshot for snapshotVersion does not exist, an error will be thrown.
   *
   * @param snapshotVersion checkpoint version of the snapshot to start with
   * @param endVersion   checkpoint version to end with
   * @return [[HDFSBackedStateStore]]
   */
  override def replayStateFromSnapshot(snapshotVersion: Long, endVersion: Long): StateStore = {
    val newMap = replayLoadedMapFromSnapshot(snapshotVersion, endVersion)
    logInfo(log"Retrieved snapshot at version " +
      log"${MDC(LogKeys.STATE_STORE_VERSION, snapshotVersion)} and apply delta files to version " +
      log"${MDC(LogKeys.STATE_STORE_VERSION, endVersion)} of " +
      log"${MDC(LogKeys.STATE_STORE_PROVIDER, HDFSBackedStateStoreProvider.this)} for update")
    new HDFSBackedStateStore(endVersion, newMap)
  }

  /**
   * Get the state store of endVersion for reading by applying delta files on the snapshot of
   * snapshotVersion. If snapshot for snapshotVersion does not exist, an error will be thrown.
   *
   * @param snapshotVersion checkpoint version of the snapshot to start with
   * @param endVersion   checkpoint version to end with
   * @return [[HDFSBackedReadStateStore]]
   */
  override def replayReadStateFromSnapshot(snapshotVersion: Long, endVersion: Long):
    ReadStateStore = {
    val newMap = replayLoadedMapFromSnapshot(snapshotVersion, endVersion)
    logInfo(log"Retrieved snapshot at version " +
      log"${MDC(LogKeys.STATE_STORE_VERSION, snapshotVersion)} and apply delta files to version " +
      log"${MDC(LogKeys.STATE_STORE_VERSION, endVersion)} of " +
      log"${MDC(LogKeys.STATE_STORE_PROVIDER, HDFSBackedStateStoreProvider.this)} for read-only")
    new HDFSBackedReadStateStore(endVersion, newMap)
  }

  /**
   * Construct the state map at endVersion from snapshot of version snapshotVersion.
   * Returns a new [[HDFSBackedStateStoreMap]]
   * @param snapshotVersion checkpoint version of the snapshot to start with
   * @param endVersion   checkpoint version to end with
   */
  private def replayLoadedMapFromSnapshot(snapshotVersion: Long, endVersion: Long):
  HDFSBackedStateStoreMap = synchronized {
    try {
      if (snapshotVersion < 1) {
        throw QueryExecutionErrors.unexpectedStateStoreVersion(snapshotVersion)
      }
      if (endVersion < snapshotVersion) {
        throw QueryExecutionErrors.unexpectedStateStoreVersion(endVersion)
      }

      val newMap = HDFSBackedStateStoreMap.create(keySchema, numColsPrefixKey)
      newMap.putAll(constructMapFromSnapshot(snapshotVersion, endVersion))

      newMap
    }
    catch {
      case e: OutOfMemoryError =>
        throw QueryExecutionErrors.notEnoughMemoryToLoadStore(
          stateStoreId.toString,
          "HDFS_STORE_PROVIDER",
          e)
      case e: Throwable => throw QueryExecutionErrors.cannotLoadStore(e)
    }
  }

  private def constructMapFromSnapshot(snapshotVersion: Long, endVersion: Long):
  HDFSBackedStateStoreMap = {
    val (result, elapsedMs) = Utils.timeTakenMs {
      val startVersionMap = synchronized { Option(loadedMaps.get(snapshotVersion)) } match {
        case Some(value) => Option(value)
        case None => readSnapshotFile(snapshotVersion)
      }
      if (startVersionMap.isEmpty) {
        throw StateStoreErrors.stateStoreSnapshotFileNotFound(
          snapshotFile(snapshotVersion).toString, toString())
      }

      // Load all the deltas from the version after the start version up to the end version.
      val resultMap = HDFSBackedStateStoreMap.create(keySchema, numColsPrefixKey)
      resultMap.putAll(startVersionMap.get)
      for (deltaVersion <- snapshotVersion + 1 to endVersion) {
        updateFromDeltaFile(deltaVersion, resultMap)
      }

      resultMap
    }

    logDebug(s"Loading snapshot at version $snapshotVersion and apply delta files to version " +
      s"$endVersion takes $elapsedMs ms.")

    result
  }

  override def getStateStoreChangeDataReader(
      startVersion: Long,
      endVersion: Long,
      colFamilyNameOpt: Option[String] = None):
    StateStoreChangeDataReader = {
    // Multiple column families are not supported with HDFSBackedStateStoreProvider
    if (colFamilyNameOpt.isDefined) {
      throw StateStoreErrors.multipleColumnFamiliesNotSupported(providerName)
    }

    new HDFSBackedStateStoreChangeDataReader(fm, baseDir, startVersion, endVersion,
      CompressionCodec.createCodec(sparkConf, storeConf.compressionCodec),
      keySchema, valueSchema)
  }
}

/** [[StateStoreChangeDataReader]] implementation for [[HDFSBackedStateStoreProvider]] */
class HDFSBackedStateStoreChangeDataReader(
    fm: CheckpointFileManager,
    stateLocation: Path,
    startVersion: Long,
    endVersion: Long,
    compressionCodec: CompressionCodec,
    keySchema: StructType,
    valueSchema: StructType)
  extends StateStoreChangeDataReader(
    fm, stateLocation, startVersion, endVersion, compressionCodec) {

  override protected var changelogSuffix: String = "delta"

  override def getNext(): (RecordType.Value, UnsafeRow, UnsafeRow, Long) = {
    val reader = currentChangelogReader()
    if (reader == null) {
      return null
    }
    val (recordType, keyArray, valueArray) = reader.next()
    val keyRow = new UnsafeRow(keySchema.fields.length)
    keyRow.pointTo(keyArray, keyArray.length)
    if (valueArray == null) {
      (recordType, keyRow, null, currentChangelogVersion - 1)
    } else {
      val valueRow = new UnsafeRow(valueSchema.fields.length)
      // If valueSize in existing file is not multiple of 8, floor it to multiple of 8.
      // This is a workaround for the following:
      // Prior to Spark 2.3 mistakenly append 4 bytes to the value row in
      // `RowBasedKeyValueBatch`, which gets persisted into the checkpoint data
      valueRow.pointTo(valueArray, (valueArray.length / 8) * 8)
      (recordType, keyRow, valueRow, currentChangelogVersion - 1)
    }
  }
}
