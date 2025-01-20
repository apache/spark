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
import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkConf, SparkEnv, SparkException}
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.io.CompressionCodec
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.streaming.{CheckpointFileManager, StreamExecution}
import org.apache.spark.sql.execution.streaming.state.StateStoreEncoding.Avro
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.{NonFateSharingCache, Utils}

private[sql] class RocksDBStateStoreProvider
  extends StateStoreProvider with Logging with Closeable
  with SupportsFineGrainedReplay {
  import RocksDBStateStoreProvider._

  class RocksDBStateStore(lastVersion: Long) extends StateStore {
    /** Trait and classes representing the internal state of the store */
    trait STATE
    case object UPDATING extends STATE
    case object COMMITTED extends STATE
    case object ABORTED extends STATE

    @volatile private var state: STATE = UPDATING
    @volatile private var isValidated = false

    override def id: StateStoreId = RocksDBStateStoreProvider.this.stateStoreId

    override def version: Long = lastVersion

    // Test-visible methods to fetch column family mapping for this State Store version
    // Because column families are only enabled for RocksDBStateStore, these methods
    // are no-ops everywhere else.
    private[sql] def getColumnFamilyMapping: Map[String, Short] = {
      rocksDB.getColumnFamilyMapping.toMap
    }

    private[sql] def getColumnFamilyId(cfName: String): Short = {
      rocksDB.getColumnFamilyId(cfName)
    }

    override def createColFamilyIfAbsent(
        colFamilyName: String,
        keySchema: StructType,
        valueSchema: StructType,
        keyStateEncoderSpec: KeyStateEncoderSpec,
        useMultipleValuesPerKey: Boolean = false,
        isInternal: Boolean = false): Unit = {
      verifyColFamilyCreationOrDeletion("create_col_family", colFamilyName, isInternal)
      val newColFamilyId = rocksDB.createColFamilyIfAbsent(colFamilyName)
      val dataEncoderCacheKey = StateRowEncoderCacheKey(
        queryRunId = getRunId(hadoopConf),
        operatorId = stateStoreId.operatorId,
        partitionId = stateStoreId.partitionId,
        stateStoreName = stateStoreId.storeName,
        colFamilyName = colFamilyName)

      val columnFamilyInfo = Some(ColumnFamilyInfo(colFamilyName, newColFamilyId))

      // For unit tests only: TestStateSchemaProvider allows dynamically adding schemas
      // during unit test execution to verify schema compatibility checks and evolution logic.
      // This provider is only used in isolated unit tests where we directly instantiate
      // state store components, not in streaming query execution or e2e tests.
      stateSchemaProvider match {
        case Some(t: TestStateSchemaProvider) =>
          t.captureSchema(colFamilyName, keySchema, valueSchema)
        case _ =>
      }

      val dataEncoder = getDataEncoder(
        stateStoreEncoding,
        dataEncoderCacheKey,
        keyStateEncoderSpec,
        valueSchema,
        stateSchemaProvider,
        columnFamilyInfo
      )

      val keyEncoder = RocksDBStateEncoder.getKeyEncoder(
        dataEncoder,
        keyStateEncoderSpec,
        useColumnFamilies,
        columnFamilyInfo
      )
      val valueEncoder = RocksDBStateEncoder.getValueEncoder(
        dataEncoder,
        valueSchema,
        useMultipleValuesPerKey
      )
      keyValueEncoderMap.putIfAbsent(colFamilyName, (keyEncoder, valueEncoder))
    }

    override def get(key: UnsafeRow, colFamilyName: String): UnsafeRow = {
      verify(key != null, "Key cannot be null")
      verifyColFamilyOperations("get", colFamilyName)

      val kvEncoder = keyValueEncoderMap.get(colFamilyName)
      val value =
        kvEncoder._2.decodeValue(rocksDB.get(kvEncoder._1.encodeKey(key)))

      if (!isValidated && value != null && !useColumnFamilies) {
        StateStoreProvider.validateStateRowFormat(
          key, keySchema, value, valueSchema, storeConf)
        isValidated = true
      }
      value
    }

    /**
     * Provides an iterator containing all values of a non-null key.
     *
     * Inside RocksDB, the values are merged together and stored as a byte Array.
     * This operation relies on state store value encoder to be able to split the
     * single array into multiple values.
     *
     * Also see [[MultiValuedStateEncoder]] which supports encoding/decoding multiple
     * values per key.
     */
    override def valuesIterator(key: UnsafeRow, colFamilyName: String): Iterator[UnsafeRow] = {
      verify(key != null, "Key cannot be null")
      verifyColFamilyOperations("valuesIterator", colFamilyName)

      val kvEncoder = keyValueEncoderMap.get(colFamilyName)
      val valueEncoder = kvEncoder._2
      val keyEncoder = kvEncoder._1

      verify(valueEncoder.supportsMultipleValuesPerKey, "valuesIterator requires a encoder " +
      "that supports multiple values for a single key.")

      val encodedValues = rocksDB.get(keyEncoder.encodeKey(key))
      valueEncoder.decodeValues(encodedValues)
    }

    override def merge(key: UnsafeRow, value: UnsafeRow,
        colFamilyName: String = StateStore.DEFAULT_COL_FAMILY_NAME): Unit = {
      verify(state == UPDATING, "Cannot merge after already committed or aborted")
      verifyColFamilyOperations("merge", colFamilyName)

      val kvEncoder = keyValueEncoderMap.get(colFamilyName)
      val keyEncoder = kvEncoder._1
      val valueEncoder = kvEncoder._2
      verify(valueEncoder.supportsMultipleValuesPerKey, "Merge operation requires an encoder" +
        " which supports multiple values for a single key")
      verify(key != null, "Key cannot be null")
      require(value != null, "Cannot merge a null value")

      rocksDB.merge(keyEncoder.encodeKey(key), valueEncoder.encodeValue(value))
    }

    override def put(key: UnsafeRow, value: UnsafeRow, colFamilyName: String): Unit = {
      verify(state == UPDATING, "Cannot put after already committed or aborted")
      verify(key != null, "Key cannot be null")
      require(value != null, "Cannot put a null value")
      verifyColFamilyOperations("put", colFamilyName)

      val kvEncoder = keyValueEncoderMap.get(colFamilyName)
      rocksDB.put(kvEncoder._1.encodeKey(key), kvEncoder._2.encodeValue(value))
    }

    override def remove(key: UnsafeRow, colFamilyName: String): Unit = {
      verify(state == UPDATING, "Cannot remove after already committed or aborted")
      verify(key != null, "Key cannot be null")
      verifyColFamilyOperations("remove", colFamilyName)

      val kvEncoder = keyValueEncoderMap.get(colFamilyName)
      rocksDB.remove(kvEncoder._1.encodeKey(key))
    }

    override def iterator(colFamilyName: String): Iterator[UnsafeRowPair] = {
      // Note this verify function only verify on the colFamilyName being valid,
      // we are actually doing prefix when useColumnFamilies,
      // but pass "iterator" to throw correct error message
      verifyColFamilyOperations("iterator", colFamilyName)
      val kvEncoder = keyValueEncoderMap.get(colFamilyName)
      val rowPair = new UnsafeRowPair()

      // As Virtual Column Family attaches a column family prefix to the key row,
      // we'll need to do prefixScan on the default column family with the same column
      // family id prefix to get all rows stored in a given virtual column family
      if (useColumnFamilies) {
        rocksDB.prefixScan(kvEncoder._1.getColumnFamilyIdBytes()).map { kv =>
          rowPair.withRows(kvEncoder._1.decodeKey(kv.key),
            kvEncoder._2.decodeValue(kv.value))
          if (!isValidated && rowPair.value != null && !useColumnFamilies) {
            StateStoreProvider.validateStateRowFormat(
              rowPair.key, keySchema, rowPair.value, valueSchema, storeConf)
            isValidated = true
          }
          rowPair
        }
      } else {
        rocksDB.iterator().map { kv =>
          rowPair.withRows(kvEncoder._1.decodeKey(kv.key),
            kvEncoder._2.decodeValue(kv.value))
          if (!isValidated && rowPair.value != null && !useColumnFamilies) {
            StateStoreProvider.validateStateRowFormat(
              rowPair.key, keySchema, rowPair.value, valueSchema, storeConf)
            isValidated = true
          }
          rowPair
        }
      }
    }

    override def prefixScan(prefixKey: UnsafeRow, colFamilyName: String):
      Iterator[UnsafeRowPair] = {
      verifyColFamilyOperations("prefixScan", colFamilyName)

      val kvEncoder = keyValueEncoderMap.get(colFamilyName)
      require(kvEncoder._1.supportPrefixKeyScan,
        "Prefix scan requires setting prefix key!")

      val rowPair = new UnsafeRowPair()
      val prefix = kvEncoder._1.encodePrefixKey(prefixKey)
      rocksDB.prefixScan(prefix).map { kv =>
        rowPair.withRows(kvEncoder._1.decodeKey(kv.key),
          kvEncoder._2.decodeValue(kv.value))
        rowPair
      }
    }

    override def commit(): Long = synchronized {
      try {
        verify(state == UPDATING, "Cannot commit after already committed or aborted")
        val newVersion = rocksDB.commit()
        state = COMMITTED
        logInfo(log"Committed ${MDC(VERSION_NUM, newVersion)} " +
          log"for ${MDC(STATE_STORE_ID, id)}")
        newVersion
      } catch {
        case e: Throwable =>
          throw QueryExecutionErrors.failedToCommitStateFileError(this.toString(), e)
      }
    }

    override def abort(): Unit = {
      verify(state == UPDATING || state == ABORTED, "Cannot abort after already committed")
      logInfo(log"Aborting ${MDC(VERSION_NUM, version + 1)} " +
        log"for ${MDC(STATE_STORE_ID, id)}")
      rocksDB.rollback()
      state = ABORTED
    }

    override def metrics: StateStoreMetrics = {
      val rocksDBMetricsOpt = rocksDB.metricsOpt

      if (rocksDBMetricsOpt.isDefined) {
        val rocksDBMetrics = rocksDBMetricsOpt.get

        def commitLatencyMs(typ: String): Long =
          rocksDBMetrics.lastCommitLatencyMs.getOrElse(typ, 0L)

        def nativeOpsLatencyMillis(typ: String): Long = {
          rocksDBMetrics.nativeOpsMetrics.get(typ).map(_ * 1000).getOrElse(0)
        }

        def sumNativeOpsLatencyMillis(typ: String): Long = {
          rocksDBMetrics.nativeOpsHistograms.get(typ).map(_.sum / 1000).getOrElse(0)
        }

        def nativeOpsCount(typ: String): Long = {
          rocksDBMetrics.nativeOpsHistograms.get(typ).map(_.count).getOrElse(0)
        }

        def nativeOpsMetrics(typ: String): Long = {
          rocksDBMetrics.nativeOpsMetrics.getOrElse(typ, 0)
        }

        // Used for metrics reporting around internal/external column families
        def internalColFamilyCnt(): Long = {
          rocksDB.getColFamilyCount(isInternal = true)
        }

        def externalColFamilyCnt(): Long = {
          rocksDB.getColFamilyCount(isInternal = false)
        }

        val stateStoreCustomMetrics = Map[StateStoreCustomMetric, Long](
          CUSTOM_METRIC_SST_FILE_SIZE -> rocksDBMetrics.totalSSTFilesBytes,
          CUSTOM_METRIC_GET_TIME -> sumNativeOpsLatencyMillis("get"),
          CUSTOM_METRIC_PUT_TIME -> sumNativeOpsLatencyMillis("put"),
          CUSTOM_METRIC_GET_COUNT -> nativeOpsCount("get"),
          CUSTOM_METRIC_PUT_COUNT -> nativeOpsCount("put"),
          CUSTOM_METRIC_FLUSH_TIME -> commitLatencyMs("flush"),
          CUSTOM_METRIC_COMMIT_COMPACT_TIME -> commitLatencyMs("compact"),
          CUSTOM_METRIC_CHECKPOINT_TIME -> commitLatencyMs("checkpoint"),
          CUSTOM_METRIC_FILESYNC_TIME -> commitLatencyMs("fileSync"),
          CUSTOM_METRIC_BYTES_COPIED -> rocksDBMetrics.bytesCopied,
          CUSTOM_METRIC_FILES_COPIED -> rocksDBMetrics.filesCopied,
          CUSTOM_METRIC_FILES_REUSED -> rocksDBMetrics.filesReused,
          CUSTOM_METRIC_BLOCK_CACHE_MISS -> nativeOpsMetrics("readBlockCacheMissCount"),
          CUSTOM_METRIC_BLOCK_CACHE_HITS -> nativeOpsMetrics("readBlockCacheHitCount"),
          CUSTOM_METRIC_BYTES_READ -> nativeOpsMetrics("totalBytesRead"),
          CUSTOM_METRIC_BYTES_WRITTEN -> nativeOpsMetrics("totalBytesWritten"),
          CUSTOM_METRIC_ITERATOR_BYTES_READ -> nativeOpsMetrics("totalBytesReadThroughIterator"),
          CUSTOM_METRIC_STALL_TIME -> nativeOpsLatencyMillis("writerStallDuration"),
          CUSTOM_METRIC_TOTAL_COMPACT_TIME -> sumNativeOpsLatencyMillis("compaction"),
          CUSTOM_METRIC_COMPACT_READ_BYTES -> nativeOpsMetrics("totalBytesReadByCompaction"),
          CUSTOM_METRIC_COMPACT_WRITTEN_BYTES -> nativeOpsMetrics("totalBytesWrittenByCompaction"),
          CUSTOM_METRIC_FLUSH_WRITTEN_BYTES -> nativeOpsMetrics("totalBytesWrittenByFlush"),
          CUSTOM_METRIC_PINNED_BLOCKS_MEM_USAGE -> rocksDBMetrics.pinnedBlocksMemUsage,
          CUSTOM_METRIC_NUM_EXTERNAL_COL_FAMILIES -> internalColFamilyCnt(),
          CUSTOM_METRIC_NUM_INTERNAL_COL_FAMILIES -> externalColFamilyCnt()
        ) ++ rocksDBMetrics.zipFileBytesUncompressed.map(bytes =>
          Map(CUSTOM_METRIC_ZIP_FILE_BYTES_UNCOMPRESSED -> bytes)).getOrElse(Map())

        StateStoreMetrics(
          rocksDBMetrics.numUncommittedKeys,
          rocksDBMetrics.totalMemUsageBytes,
          stateStoreCustomMetrics)
      } else {
        logInfo(log"Failed to collect metrics for store_id=${MDC(STATE_STORE_ID, id)} " +
          log"and version=${MDC(VERSION_NUM, version)}")
        StateStoreMetrics(0, 0, Map.empty)
      }
    }

    override def getStateStoreCheckpointInfo(): StateStoreCheckpointInfo = {
      val checkpointInfo = rocksDB.getLatestCheckpointInfo(id.partitionId)
      checkpointInfo
    }

    override def hasCommitted: Boolean = state == COMMITTED

    override def toString: String = {
      s"RocksDBStateStore[id=(op=${id.operatorId},part=${id.partitionId})," +
        s"dir=${id.storeCheckpointLocation()}]"
    }

    /** Return the [[RocksDB]] instance in this store. This is exposed mainly for testing. */
    def dbInstance(): RocksDB = rocksDB

    /** Remove column family if exists */
    override def removeColFamilyIfExists(colFamilyName: String): Boolean = {
      verifyColFamilyCreationOrDeletion("remove_col_family", colFamilyName)
      verify(useColumnFamilies, "Column families are not supported in this store")

      val result = {
        val colFamilyId = rocksDB.removeColFamilyIfExists(colFamilyName)

        colFamilyId match {
          case Some(vcfId) =>
            val colFamilyIdBytes =
              RocksDBStateEncoder.getColumnFamilyIdBytes(vcfId)
            rocksDB.prefixScan(colFamilyIdBytes).foreach { kv =>
              rocksDB.remove(kv.key)
            }
            true
          case None => false
        }
      }
      keyValueEncoderMap.remove(colFamilyName)
      result
    }
  }

  // Test-visible method to fetch the internal RocksDBStateStore class
  private[sql] def getRocksDBStateStore(version: Long): RocksDBStateStore = {
    getStore(version).asInstanceOf[RocksDBStateStore]
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
      stateSchemaProvider: Option[StateSchemaProvider]): Unit = {
    this.stateStoreId_ = stateStoreId
    this.keySchema = keySchema
    this.valueSchema = valueSchema
    this.storeConf = storeConf
    this.hadoopConf = hadoopConf
    this.useColumnFamilies = useColumnFamilies
    this.stateStoreEncoding = storeConf.stateStoreEncodingFormat
    this.stateSchemaProvider = stateSchemaProvider

    if (useMultipleValuesPerKey) {
      require(useColumnFamilies, "Multiple values per key support requires column families to be" +
        " enabled in RocksDBStateStore.")
    }

    rocksDB // lazy initialization
    var defaultColFamilyId: Option[Short] = None

    val dataEncoderCacheKey = StateRowEncoderCacheKey(
      queryRunId = getRunId(hadoopConf),
      operatorId = stateStoreId.operatorId,
      partitionId = stateStoreId.partitionId,
      stateStoreName = stateStoreId.storeName,
      colFamilyName = StateStore.DEFAULT_COL_FAMILY_NAME)

    // For test cases only: TestStateSchemaProvider allows dynamically adding schemas
    // during test execution to verify schema evolution behavior. In production,
    // schemas are loaded from checkpoint data
    stateSchemaProvider match {
      case Some(t: TestStateSchemaProvider) =>
        t.captureSchema(StateStore.DEFAULT_COL_FAMILY_NAME, keySchema, valueSchema)
      case _ =>
    }

    defaultColFamilyId = Some(rocksDB.createColFamilyIfAbsent(StateStore.DEFAULT_COL_FAMILY_NAME))
    val columnFamilyInfo =
      Some(ColumnFamilyInfo(StateStore.DEFAULT_COL_FAMILY_NAME, defaultColFamilyId.get))

    val dataEncoder = getDataEncoder(
      stateStoreEncoding,
      dataEncoderCacheKey,
      keyStateEncoderSpec,
      valueSchema,
      stateSchemaProvider,
      columnFamilyInfo
    )

    val keyEncoder = RocksDBStateEncoder.getKeyEncoder(
      dataEncoder,
      keyStateEncoderSpec,
      useColumnFamilies,
      columnFamilyInfo
    )
    val valueEncoder = RocksDBStateEncoder.getValueEncoder(
      dataEncoder,
      valueSchema,
      useMultipleValuesPerKey
    )
    keyValueEncoderMap.putIfAbsent(StateStore.DEFAULT_COL_FAMILY_NAME, (keyEncoder, valueEncoder))
  }

  override def stateStoreId: StateStoreId = stateStoreId_

  override def getStore(version: Long, uniqueId: Option[String] = None): StateStore = {
    try {
      if (version < 0) {
        throw QueryExecutionErrors.unexpectedStateStoreVersion(version)
      }
      rocksDB.load(
        version,
        stateStoreCkptId = if (storeConf.enableStateStoreCheckpointIds) uniqueId else None)
      new RocksDBStateStore(version)
    }
    catch {
      case e: SparkException
        if Option(e.getCondition).exists(_.contains("CANNOT_LOAD_STATE_STORE")) =>
        throw e
      case e: OutOfMemoryError =>
        throw QueryExecutionErrors.notEnoughMemoryToLoadStore(
          stateStoreId.toString,
          "ROCKSDB_STORE_PROVIDER",
          e)
      case e: Throwable => throw QueryExecutionErrors.cannotLoadStore(e)
    }
  }

  override def getReadStore(version: Long, uniqueId: Option[String] = None): StateStore = {
    try {
      if (version < 0) {
        throw QueryExecutionErrors.unexpectedStateStoreVersion(version)
      }
      rocksDB.load(
        version,
        stateStoreCkptId = if (storeConf.enableStateStoreCheckpointIds) uniqueId else None,
        readOnly = true)
      new RocksDBStateStore(version)
    }
    catch {
      case e: SparkException
        if Option(e.getCondition).exists(_.contains("CANNOT_LOAD_STATE_STORE")) =>
        throw e
      case e: OutOfMemoryError =>
        throw QueryExecutionErrors.notEnoughMemoryToLoadStore(
          stateStoreId.toString,
          "ROCKSDB_STORE_PROVIDER",
          e)
      case e: Throwable => throw QueryExecutionErrors.cannotLoadStore(e)
    }
  }

  override def doMaintenance(): Unit = {
    try {
      rocksDB.doMaintenance()
    } catch {
      // SPARK-46547 - Swallow non-fatal exception in maintenance task to avoid deadlock between
      // maintenance thread and streaming aggregation operator
      case NonFatal(ex) =>
        logWarning(s"Ignoring error while performing maintenance operations with exception=",
          ex)
    }
  }

  override def close(): Unit = {
    rocksDB.close()
  }

  override def supportedCustomMetrics: Seq[StateStoreCustomMetric] = ALL_CUSTOM_METRICS

  private[state] def latestVersion: Long = rocksDB.getLatestVersion()

  /** Internal fields and methods */

  @volatile private var stateStoreId_ : StateStoreId = _
  @volatile private var keySchema: StructType = _
  @volatile private var valueSchema: StructType = _
  @volatile private var storeConf: StateStoreConf = _
  @volatile private var hadoopConf: Configuration = _
  @volatile private var useColumnFamilies: Boolean = _
  @volatile private var stateStoreEncoding: String = _
  @volatile private var stateSchemaProvider: Option[StateSchemaProvider] = _

  private[sql] lazy val rocksDB = {
    val dfsRootDir = stateStoreId.storeCheckpointLocation().toString
    val storeIdStr = s"StateStoreId(opId=${stateStoreId.operatorId}," +
      s"partId=${stateStoreId.partitionId},name=${stateStoreId.storeName})"
    val sparkConf = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf)
    val localRootDir = Utils.createTempDir(Utils.getLocalDir(sparkConf), storeIdStr)
    new RocksDB(dfsRootDir, RocksDBConf(storeConf), localRootDir, hadoopConf, storeIdStr,
      useColumnFamilies, storeConf.enableStateStoreCheckpointIds)
  }

  private val keyValueEncoderMap = new java.util.concurrent.ConcurrentHashMap[String,
    (RocksDBKeyStateEncoder, RocksDBValueStateEncoder)]

  private val multiColFamiliesDisabledStr = "multiple column families is disabled in " +
    "RocksDBStateStoreProvider"

  private def verify(condition: => Boolean, msg: String): Unit = {
    if (!condition) { throw new IllegalStateException(msg) }
  }

  /**
   * Get the state store of endVersion by applying delta files on the snapshot of snapshotVersion.
   * If snapshot for snapshotVersion does not exist, an error will be thrown.
   *
   * @param snapshotVersion checkpoint version of the snapshot to start with
   * @param endVersion   checkpoint version to end with
   * @return [[StateStore]]
   */
  override def replayStateFromSnapshot(snapshotVersion: Long, endVersion: Long): StateStore = {
    try {
      if (snapshotVersion < 1) {
        throw QueryExecutionErrors.unexpectedStateStoreVersion(snapshotVersion)
      }
      if (endVersion < snapshotVersion) {
        throw QueryExecutionErrors.unexpectedStateStoreVersion(endVersion)
      }
      rocksDB.loadFromSnapshot(snapshotVersion, endVersion)
      new RocksDBStateStore(endVersion)
    }
    catch {
      case e: OutOfMemoryError =>
        throw QueryExecutionErrors.notEnoughMemoryToLoadStore(
          stateStoreId.toString,
          "ROCKSDB_STORE_PROVIDER",
          e)
      case e: Throwable => throw QueryExecutionErrors.cannotLoadStore(e)
    }
  }

  override def getStateStoreChangeDataReader(
      startVersion: Long,
      endVersion: Long,
      colFamilyNameOpt: Option[String] = None):
    StateStoreChangeDataReader = {
    val statePath = stateStoreId.storeCheckpointLocation()
    val sparkConf = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf)
    new RocksDBStateStoreChangeDataReader(
      CheckpointFileManager.create(statePath, hadoopConf),
      statePath,
      startVersion,
      endVersion,
      CompressionCodec.createCodec(sparkConf, storeConf.compressionCodec),
      keyValueEncoderMap,
      colFamilyNameOpt)
  }

  /**
   * Function to verify invariants for column family based operations
   * such as get, put, remove etc.
   *
   * @param operationName - name of the store operation
   * @param colFamilyName - name of the column family
   */
  private def verifyColFamilyOperations(
      operationName: String,
      colFamilyName: String): Unit = {
    if (colFamilyName != StateStore.DEFAULT_COL_FAMILY_NAME) {
      // if the state store instance does not support multiple column families, throw an exception
      if (!useColumnFamilies) {
        throw StateStoreErrors.unsupportedOperationException(operationName,
          multiColFamiliesDisabledStr)
      }

      // if the column family name is empty or contains leading/trailing whitespaces, throw an
      // exception
      if (colFamilyName.isEmpty || colFamilyName.trim != colFamilyName) {
        throw StateStoreErrors.cannotUseColumnFamilyWithInvalidName(operationName, colFamilyName)
      }

      // if the column family does not exist, throw an exception
      if (!rocksDB.checkColFamilyExists(colFamilyName)) {
        throw StateStoreErrors.unsupportedOperationOnMissingColumnFamily(operationName,
          colFamilyName)
      }
    }
  }

  /**
   * Function to verify invariants for column family creation or deletion operations.
   *
   * @param operationName - name of the store operation
   * @param colFamilyName - name of the column family
   */
  private def verifyColFamilyCreationOrDeletion(
      operationName: String,
      colFamilyName: String,
      isInternal: Boolean = false): Unit = {
    // if the state store instance does not support multiple column families, throw an exception
    if (!useColumnFamilies) {
      throw StateStoreErrors.unsupportedOperationException(operationName,
        multiColFamiliesDisabledStr)
    }

    // if the column family name is empty or contains leading/trailing whitespaces
    // or using the reserved "default" column family, throw an exception
    if (colFamilyName.isEmpty
      || (colFamilyName.trim != colFamilyName)
      || (colFamilyName == StateStore.DEFAULT_COL_FAMILY_NAME && !isInternal)) {
      throw StateStoreErrors.cannotUseColumnFamilyWithInvalidName(operationName, colFamilyName)
    }

    // if the column family is not internal and uses reserved characters, throw an exception
    if (!isInternal && colFamilyName.charAt(0) == '$') {
      throw StateStoreErrors.cannotCreateColumnFamilyWithReservedChars(colFamilyName)
    }
  }
}


case class StateRowEncoderCacheKey(
    queryRunId: String,
    operatorId: Long,
    partitionId: Int,
    stateStoreName: String,
    colFamilyName: String
)

object RocksDBStateStoreProvider {
  // Version as a single byte that specifies the encoding of the row data in RocksDB
  val STATE_ENCODING_NUM_VERSION_BYTES = 1
  val STATE_ENCODING_VERSION: Byte = 0
  val VIRTUAL_COL_FAMILY_PREFIX_BYTES = 2
  val SCHEMA_ID_PREFIX_BYTES = 2

  private val MAX_AVRO_ENCODERS_IN_CACHE = 1000
  private val AVRO_ENCODER_LIFETIME_HOURS = 1L
  private val DEFAULT_SCHEMA_IDS = StateSchemaInfo(0, 0)

  // Add the cache at companion object level so it persists across provider instances
  private val dataEncoderCache: NonFateSharingCache[StateRowEncoderCacheKey, RocksDBDataEncoder] =
    NonFateSharingCache(
      maximumSize = MAX_AVRO_ENCODERS_IN_CACHE,
      expireAfterAccessTime = AVRO_ENCODER_LIFETIME_HOURS,
      expireAfterAccessTimeUnit = TimeUnit.HOURS
    )

  /**
   * Creates and returns a data encoder for the state store based on the specified encoding type.
   * This method handles caching of encoders to improve performance by reusing encoder instances
   * when possible.
   *
   * The method supports two encoding types:
   * - Avro: Uses Apache Avro for serialization with schema evolution support
   * - UnsafeRow: Uses Spark's internal row format for optimal performance
   *
   * @param stateStoreEncoding The encoding type to use ("avro" or "unsaferow")
   * @param encoderCacheKey A unique key for caching the encoder instance, typically combining
   *                       query ID, operator ID, partition ID, and column family name
   * @param keyStateEncoderSpec Specification for how to encode keys, including schema and any
   *                           prefix/range scan requirements
   * @param valueSchema The schema for the values to be encoded
   * @return A RocksDBDataEncoder instance configured for the specified encoding type
   */
  def getDataEncoder(
      stateStoreEncoding: String,
      encoderCacheKey: StateRowEncoderCacheKey,
      keyStateEncoderSpec: KeyStateEncoderSpec,
      valueSchema: StructType,
      stateSchemaProvider: Option[StateSchemaProvider],
      columnFamilyInfo: Option[ColumnFamilyInfo] = None): RocksDBDataEncoder = {
    assert(Set("avro", "unsaferow").contains(stateStoreEncoding))
    RocksDBStateStoreProvider.dataEncoderCache.get(
      encoderCacheKey,
      new java.util.concurrent.Callable[RocksDBDataEncoder] {
        override def call(): RocksDBDataEncoder = {
          if (stateStoreEncoding == Avro.toString) {
            new AvroStateEncoder(
              keyStateEncoderSpec,
              valueSchema,
              stateSchemaProvider,
              columnFamilyInfo
            )
          } else {
            new UnsafeRowDataEncoder(
              keyStateEncoderSpec,
              valueSchema
            )
          }
        }
      }
    )
  }

  private def getRunId(hadoopConf: Configuration): String = {
    val runId = hadoopConf.get(StreamExecution.RUN_ID_KEY)
    if (runId != null) {
      runId
    } else {
      assert(Utils.isTesting, "Failed to find query id/batch Id in task context")
      UUID.randomUUID().toString
    }
  }

  // Native operation latencies report as latency in microseconds
  // as SQLMetrics support millis. Convert the value to millis
  val CUSTOM_METRIC_GET_TIME = StateStoreCustomTimingMetric(
    "rocksdbGetLatency", "RocksDB: total get call latency")
  val CUSTOM_METRIC_PUT_TIME = StateStoreCustomTimingMetric(
    "rocksdbPutLatency", "RocksDB: total put call latency")

  val CUSTOM_METRIC_GET_COUNT = StateStoreCustomSumMetric(
    "rocksdbGetCount", "RocksDB: number of get calls")
  val CUSTOM_METRIC_PUT_COUNT = StateStoreCustomSumMetric(
    "rocksdbPutCount", "RocksDB: number of put calls")

  // Commit latency detailed breakdown
  val CUSTOM_METRIC_FLUSH_TIME = StateStoreCustomTimingMetric(
    "rocksdbCommitFlushLatency", "RocksDB: commit - flush time")
  val CUSTOM_METRIC_COMMIT_COMPACT_TIME = StateStoreCustomTimingMetric(
    "rocksdbCommitCompactLatency", "RocksDB: commit - compact time")
  val CUSTOM_METRIC_CHECKPOINT_TIME = StateStoreCustomTimingMetric(
    "rocksdbCommitCheckpointLatency", "RocksDB: commit - checkpoint time")
  val CUSTOM_METRIC_FILESYNC_TIME = StateStoreCustomTimingMetric(
    "rocksdbCommitFileSyncLatencyMs", "RocksDB: commit - file sync to external storage time")
  val CUSTOM_METRIC_FILES_COPIED = StateStoreCustomSumMetric(
    "rocksdbFilesCopied", "RocksDB: file manager - files copied")
  val CUSTOM_METRIC_BYTES_COPIED = StateStoreCustomSizeMetric(
    "rocksdbBytesCopied", "RocksDB: file manager - bytes copied")
  val CUSTOM_METRIC_FILES_REUSED = StateStoreCustomSumMetric(
    "rocksdbFilesReused", "RocksDB: file manager - files reused")
  val CUSTOM_METRIC_ZIP_FILE_BYTES_UNCOMPRESSED = StateStoreCustomSizeMetric(
    "rocksdbZipFileBytesUncompressed", "RocksDB: file manager - uncompressed zip file bytes")

  val CUSTOM_METRIC_BLOCK_CACHE_MISS = StateStoreCustomSumMetric(
    "rocksdbReadBlockCacheMissCount",
    "RocksDB: read - count of cache misses that required reading from local disk")
  val CUSTOM_METRIC_BLOCK_CACHE_HITS = StateStoreCustomSumMetric(
    "rocksdbReadBlockCacheHitCount",
    "RocksDB: read - count of cache hits in RocksDB block cache avoiding disk read")
  val CUSTOM_METRIC_BYTES_READ = StateStoreCustomSizeMetric(
    "rocksdbTotalBytesRead",
    "RocksDB: read - total of uncompressed bytes read (from memtables/cache/sst) from DB::Get()")
  val CUSTOM_METRIC_BYTES_WRITTEN = StateStoreCustomSizeMetric(
    "rocksdbTotalBytesWritten",
    "RocksDB: write - total of uncompressed bytes written by " +
      "DB::{Put(), Delete(), Merge(), Write()}")
  val CUSTOM_METRIC_ITERATOR_BYTES_READ = StateStoreCustomSizeMetric(
    "rocksdbTotalBytesReadThroughIterator",
    "RocksDB: read - total of uncompressed bytes read using an iterator")
  val CUSTOM_METRIC_STALL_TIME = StateStoreCustomTimingMetric(
    "rocksdbWriterStallLatencyMs",
    "RocksDB: write - writer wait time for compaction or flush to finish")
  val CUSTOM_METRIC_TOTAL_COMPACT_TIME = StateStoreCustomTimingMetric(
    "rocksdbTotalCompactionLatencyMs",
    "RocksDB: compaction - total compaction time including background")
  val CUSTOM_METRIC_COMPACT_READ_BYTES = StateStoreCustomSizeMetric(
    "rocksdbTotalBytesReadByCompaction",
    "RocksDB: compaction - total bytes read by the compaction process")
  val CUSTOM_METRIC_COMPACT_WRITTEN_BYTES = StateStoreCustomSizeMetric(
    "rocksdbTotalBytesWrittenByCompaction",
    "RocksDB: compaction - total bytes written by the compaction process")
  val CUSTOM_METRIC_FLUSH_WRITTEN_BYTES = StateStoreCustomSizeMetric(
    "rocksdbTotalBytesWrittenByFlush",
    "RocksDB: flush - total bytes written by flush")
  val CUSTOM_METRIC_PINNED_BLOCKS_MEM_USAGE = StateStoreCustomSizeMetric(
    "rocksdbPinnedBlocksMemoryUsage",
    "RocksDB: memory usage for pinned blocks")
  val CUSTOM_METRIC_NUM_EXTERNAL_COL_FAMILIES = StateStoreCustomSizeMetric(
    "rocksdbNumExternalColumnFamilies",
    "RocksDB: number of external column families")
  val CUSTOM_METRIC_NUM_INTERNAL_COL_FAMILIES = StateStoreCustomSizeMetric(
    "rocksdbNumInternalColumnFamilies",
    "RocksDB: number of internal column families")

  // Total SST file size
  val CUSTOM_METRIC_SST_FILE_SIZE = StateStoreCustomSizeMetric(
    "rocksdbSstFileSize", "RocksDB: size of all SST files")

  val ALL_CUSTOM_METRICS = Seq(
    CUSTOM_METRIC_SST_FILE_SIZE, CUSTOM_METRIC_GET_TIME, CUSTOM_METRIC_PUT_TIME,
    CUSTOM_METRIC_FLUSH_TIME, CUSTOM_METRIC_COMMIT_COMPACT_TIME,
    CUSTOM_METRIC_CHECKPOINT_TIME, CUSTOM_METRIC_FILESYNC_TIME,
    CUSTOM_METRIC_BYTES_COPIED, CUSTOM_METRIC_FILES_COPIED, CUSTOM_METRIC_FILES_REUSED,
    CUSTOM_METRIC_ZIP_FILE_BYTES_UNCOMPRESSED, CUSTOM_METRIC_GET_COUNT, CUSTOM_METRIC_PUT_COUNT,
    CUSTOM_METRIC_BLOCK_CACHE_MISS, CUSTOM_METRIC_BLOCK_CACHE_HITS, CUSTOM_METRIC_BYTES_READ,
    CUSTOM_METRIC_BYTES_WRITTEN, CUSTOM_METRIC_ITERATOR_BYTES_READ, CUSTOM_METRIC_STALL_TIME,
    CUSTOM_METRIC_TOTAL_COMPACT_TIME, CUSTOM_METRIC_COMPACT_READ_BYTES,
    CUSTOM_METRIC_COMPACT_WRITTEN_BYTES, CUSTOM_METRIC_FLUSH_WRITTEN_BYTES,
    CUSTOM_METRIC_PINNED_BLOCKS_MEM_USAGE, CUSTOM_METRIC_NUM_EXTERNAL_COL_FAMILIES,
    CUSTOM_METRIC_NUM_INTERNAL_COL_FAMILIES)
}

/** [[StateStoreChangeDataReader]] implementation for [[RocksDBStateStoreProvider]] */
class RocksDBStateStoreChangeDataReader(
    fm: CheckpointFileManager,
    stateLocation: Path,
    startVersion: Long,
    endVersion: Long,
    compressionCodec: CompressionCodec,
    keyValueEncoderMap:
      ConcurrentHashMap[String, (RocksDBKeyStateEncoder, RocksDBValueStateEncoder)],
    colFamilyNameOpt: Option[String] = None)
  extends StateStoreChangeDataReader(
    fm, stateLocation, startVersion, endVersion, compressionCodec, colFamilyNameOpt) {

  override protected var changelogSuffix: String = "changelog"

  private def getColFamilyIdBytes: Option[Array[Byte]] = {
    if (colFamilyNameOpt.isDefined) {
      val colFamilyName = colFamilyNameOpt.get
      if (!keyValueEncoderMap.containsKey(colFamilyName)) {
        throw new IllegalStateException(
          s"Column family $colFamilyName not found in the key value encoder map")
      }
      Some(keyValueEncoderMap.get(colFamilyName)._1.getColumnFamilyIdBytes())
    } else {
      None
    }
  }

  private val colFamilyIdBytesOpt: Option[Array[Byte]] = getColFamilyIdBytes

  override def getNext(): (RecordType.Value, UnsafeRow, UnsafeRow, Long) = {
    var currRecord: (RecordType.Value, Array[Byte], Array[Byte]) = null
    val currEncoder: (RocksDBKeyStateEncoder, RocksDBValueStateEncoder) =
      keyValueEncoderMap.get(colFamilyNameOpt
        .getOrElse(StateStore.DEFAULT_COL_FAMILY_NAME))

    if (colFamilyIdBytesOpt.isDefined) {
      // If we are reading records for a particular column family, the corresponding vcf id
      // will be encoded in the key byte array. We need to extract that and compare for the
      // expected column family id. If it matches, we return the record. If not, we move to
      // the next record. Note that this has be handled across multiple changelog files and we
      // rely on the currentChangelogReader to move to the next changelog file when needed.
      while (currRecord == null) {
        val reader = currentChangelogReader()
        if (reader == null) {
          return null
        }

        val nextRecord = reader.next()
        val colFamilyIdBytes: Array[Byte] = colFamilyIdBytesOpt.get
        val endIndex = colFamilyIdBytes.size
        // Function checks for byte arrays being equal
        // from index 0 to endIndex - 1 (both inclusive)
        if (java.util.Arrays.equals(nextRecord._2, 0, endIndex,
          colFamilyIdBytes, 0, endIndex)) {
          currRecord = nextRecord
        }
      }
    } else {
      val reader = currentChangelogReader()
      if (reader == null) {
        return null
      }
      currRecord = reader.next()
    }

    val keyRow = currEncoder._1.decodeKey(currRecord._2)
    if (currRecord._3 == null) {
      (currRecord._1, keyRow, null, currentChangelogVersion - 1)
    } else {
      val valueRow = currEncoder._2.decodeValue(currRecord._3)
      (currRecord._1, keyRow, valueRow, currentChangelogVersion - 1)
    }
  }
}
