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

import org.apache.spark.{SparkConf, SparkEnv, TaskContext}
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.io.CompressionCodec
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.streaming.checkpointing.CheckpointFileManager
import org.apache.spark.sql.execution.streaming.state.StateStoreEncoding.Avro
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.{NonFateSharingCache, Utils}

private[sql] class RocksDBStateStoreProvider
  extends StateStoreProvider with Logging with Closeable
  with SupportsFineGrainedReplay {
  import RocksDBStateStoreProvider._

  class RocksDBStateStore(
      lastVersion: Long,
      private[RocksDBStateStoreProvider] val stamp: Long,
      private[RocksDBStateStoreProvider] var readOnly: Boolean,
      private[RocksDBStateStoreProvider] var forceSnapshotOnCommit: Boolean) extends StateStore {

    private sealed trait OPERATION
    private case object UPDATE extends OPERATION
    private case object ABORT extends OPERATION
    private case object RELEASE extends OPERATION
    private case object COMMIT extends OPERATION
    private case object METRICS extends OPERATION

    /** Trait and classes representing the internal state of the store */
    trait STATE
    case object UPDATING extends STATE
    case object COMMITTED extends STATE
    case object ABORTED extends STATE
    case object RELEASED extends STATE

    @volatile private var state: STATE = UPDATING
    @volatile private var isValidated = false

    /**
     * Map defining all valid state transitions for the RocksDB state store.
     * Key: (currentState, operation) -> Value: nextState
     *
     * Valid transitions:
     * - (UPDATING, UPDATE) -> UPDATING: Continue updating
     * - (UPDATING, ABORT) -> ABORTED: Abort during update
     * - (UPDATING, RELEASE) -> RELEASED: Release during update
     * - (UPDATING, COMMIT) -> COMMITTED: Direct commit
     * - (COMMITTED, METRICS) -> COMMITTED: Allow metrics after commit
     * - (ABORTED, ABORT) -> ABORTED: Abort is idempotent
     * - (ABORTED, METRICS) -> ABORTED: Allow metrics after abort
     * - (RELEASED, RELEASE) -> RELEASED: Release is idempotent
     * - (RELEASED, METRICS) -> RELEASED: Allow metrics after release
     */
    private val allowedStateTransitions: Map[(STATE, OPERATION), STATE] = Map(
      // From UPDATING state
      (UPDATING, UPDATE) -> UPDATING,
      (UPDATING, ABORT) -> ABORTED,
      (UPDATING, RELEASE) -> RELEASED,
      (UPDATING, COMMIT) -> COMMITTED,
      // From COMMITTED state
      (COMMITTED, METRICS) -> COMMITTED,
      // From ABORTED state
      (ABORTED, ABORT) -> ABORTED,  // Idempotent
      (ABORTED, METRICS) -> ABORTED,
      // From RELEASED state
      (RELEASED, RELEASE) -> RELEASED,  // Idempotent
      (RELEASED, METRICS) -> RELEASED
    )

    override def id: StateStoreId = RocksDBStateStoreProvider.this.stateStoreId

    override def version: Long = lastVersion

    /**
     * Validates the expected state, throws exception if state is not as expected.
     * Returns the current state
     *
     * @param possibleStates Expected possible states
     * @return current state of StateStore
     */
    private def validateState(possibleStates: STATE*): STATE = {
      if (!possibleStates.contains(state)) {
        throw StateStoreErrors.stateStoreOperationOutOfOrder(
          s"Expected possible states ${possibleStates.mkString("(", ", ", ")")} but found $state")
      }
      state
    }

    /**
     * Throws error if transition is illegal.
     * MUST be called for every StateStore method.
     *
     * @param operation The transition type of the operation.
     */
    private def validateAndTransitionState(operation: OPERATION): Unit = {
      val oldState = state

      // Operations requiring stamp verification
      val needsStampVerification = operation match {
        case ABORT if state == ABORTED => false     // ABORT is idempotent
        case RELEASE if state == RELEASED => false  // RELEASE is idempotent
        case UPDATE | ABORT | RELEASE | COMMIT => true
        case METRICS => false
      }

      if (needsStampVerification) {
        stateMachine.verifyStamp(stamp)
      }

      val newState = allowedStateTransitions.get((oldState, operation)) match {
        case Some(nextState) => nextState
        case None =>
          val errorMsg = operation match {
            case UPDATE => s"Cannot update after ${oldState.toString}"
            case ABORT => s"Cannot abort after ${oldState.toString}"
            case RELEASE => s"Cannot release after ${oldState.toString}"
            case COMMIT => s"Cannot commit after ${oldState.toString}"
            case METRICS => s"Cannot get metrics in ${oldState} state"
          }
          throw StateStoreErrors.stateStoreOperationOutOfOrder(errorMsg)
      }

      // Special handling for COMMIT operation - release the store
      if (operation == COMMIT || operation == RELEASE) {
        stateMachine.releaseStamp(stamp)
      }

      if (operation != UPDATE) {
        logInfo(log"Transitioned state from ${MDC(LogKeys.STATE_STORE_STATE, oldState)} " +
          log"to ${MDC(LogKeys.STATE_STORE_STATE, newState)} " +
          log"for StateStoreId ${MDC(LogKeys.STATE_STORE_ID, stateStoreId)} " +
          log"with transition ${MDC(LogKeys.OPERATION, operation.toString)}")
      }
      state = newState
    }

    Option(TaskContext.get()).foreach { ctxt =>
      ctxt.addTaskCompletionListener[Unit](ctx => {
        try {
          if (state == UPDATING) {
            if (readOnly) {
              release()
            } else {
              abort() // Abort since this is an error if stateful task completes
            }
          }
        } catch {
          case NonFatal(e) =>
            logWarning("Failed to abort or release state store", e)
        } finally {
          stateMachine.releaseStamp(stamp, throwEx = false)
        }
      })
      // Abort the async commit stores only when the task has failed and store is not committed.
      ctxt.addTaskFailureListener((_, _) => {
        if (!hasCommitted) abort()
      })
      // Failure/completion listeners were invoked during adding if the task has
      // failed already. Prevent further access (resulting in invalid stamp error)
      // by throwing an error here.
      ctxt.getTaskFailure match {
        case Some(failure) => throw failure
        case None =>
      }
    }

    override def createColFamilyIfAbsent(
        colFamilyName: String,
        keySchema: StructType,
        valueSchema: StructType,
        keyStateEncoderSpec: KeyStateEncoderSpec,
        useMultipleValuesPerKey: Boolean = false,
        isInternal: Boolean = false): Unit = {
      validateAndTransitionState(UPDATE)
      verifyColFamilyCreationOrDeletion("create_col_family", colFamilyName, isInternal)
      val cfId = rocksDB.createColFamilyIfAbsent(colFamilyName, isInternal)
      val dataEncoderCacheKey = StateRowEncoderCacheKey(
        queryRunId = StateStoreProvider.getRunId(hadoopConf),
        operatorId = stateStoreId.operatorId,
        partitionId = stateStoreId.partitionId,
        stateStoreName = stateStoreId.storeName,
        colFamilyName = colFamilyName)

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
        Some(colFamilyName)
      )
      val keyEncoder = RocksDBStateEncoder.getKeyEncoder(
        dataEncoder,
        keyStateEncoderSpec,
        useColumnFamilies
      )
      val valueEncoder = RocksDBStateEncoder.getValueEncoder(
        dataEncoder,
        valueSchema,
        useMultipleValuesPerKey
      )
      keyValueEncoderMap.putIfAbsent(colFamilyName, (keyEncoder, valueEncoder, cfId))
    }

    override def get(key: UnsafeRow, colFamilyName: String): UnsafeRow = {
      validateAndTransitionState(UPDATE)
      verify(key != null, "Key cannot be null")
      verifyColFamilyOperations("get", colFamilyName)

      val kvEncoder = keyValueEncoderMap.get(colFamilyName)
      val value =
        kvEncoder._2.decodeValue(rocksDB.get(kvEncoder._1.encodeKey(key), colFamilyName))

      if (!isValidated && value != null && !useColumnFamilies) {
        StateStoreProvider.validateStateRowFormat(
          key, keySchema, value, valueSchema, stateStoreId, storeConf)
        isValidated = true
      }
      value
    }

    override def keyExists(key: UnsafeRow, colFamilyName: String): Boolean = {
      validateAndTransitionState(UPDATE)
      verify(key != null, "Key cannot be null")
      verifyColFamilyOperations("keyExists", colFamilyName)

      val kvEncoder = keyValueEncoderMap.get(colFamilyName)
      rocksDB.keyExists(kvEncoder._1.encodeKey(key), colFamilyName)
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
      validateAndTransitionState(UPDATE)
      verify(key != null, "Key cannot be null")
      verifyColFamilyOperations("valuesIterator", colFamilyName)

      val kvEncoder = keyValueEncoderMap.get(colFamilyName)
      val valueEncoder = kvEncoder._2
      val keyEncoder = kvEncoder._1

      verify(valueEncoder.supportsMultipleValuesPerKey, "valuesIterator requires a encoder " +
      "that supports multiple values for a single key.")

      if (storeConf.rowChecksumEnabled) {
        // multiGet provides better perf for row checksum, since it avoids copying values
        val encodedValuesIterator = rocksDB.multiGet(keyEncoder.encodeKey(key), colFamilyName)
        valueEncoder.decodeValues(encodedValuesIterator)
      } else {
        val encodedValues = rocksDB.get(keyEncoder.encodeKey(key), colFamilyName)
        valueEncoder.decodeValues(encodedValues)
      }
    }

    override def merge(key: UnsafeRow, value: UnsafeRow,
        colFamilyName: String = StateStore.DEFAULT_COL_FAMILY_NAME): Unit = {
      validateAndTransitionState(UPDATE)
      verify(state == UPDATING, "Cannot merge after already committed or aborted")
      verifyColFamilyOperations("merge", colFamilyName)

      val kvEncoder = keyValueEncoderMap.get(colFamilyName)
      val keyEncoder = kvEncoder._1
      val valueEncoder = kvEncoder._2
      verify(valueEncoder.supportsMultipleValuesPerKey, "Merge operation requires an encoder" +
        " which supports multiple values for a single key")
      verify(key != null, "Key cannot be null")
      require(value != null, "Cannot merge a null value")

      rocksDB.merge(keyEncoder.encodeKey(key), valueEncoder.encodeValue(value), colFamilyName)
    }

    override def mergeList(
        key: UnsafeRow,
        values: Array[UnsafeRow],
        colFamilyName: String = StateStore.DEFAULT_COL_FAMILY_NAME): Unit = {
      validateAndTransitionState(UPDATE)
      verify(state == UPDATING, "Cannot merge after already committed or aborted")
      verifyColFamilyOperations("merge", colFamilyName)

      val kvEncoder = keyValueEncoderMap.get(colFamilyName)
      val keyEncoder = kvEncoder._1
      val valueEncoder = kvEncoder._2
      verify(
        valueEncoder.supportsMultipleValuesPerKey,
        "Merge operation requires an encoder" +
          " which supports multiple values for a single key")
      verify(key != null, "Key cannot be null")
      require(values != null, "Cannot merge a null value")
      values.foreach(v => require(v != null, "Cannot merge a null value in the array"))

      rocksDB.mergeList(
        keyEncoder.encodeKey(key),
        values.map(valueEncoder.encodeValue).toList,
        colFamilyName)
    }

    override def put(key: UnsafeRow, value: UnsafeRow, colFamilyName: String): Unit = {
      validateAndTransitionState(UPDATE)
      verify(state == UPDATING, "Cannot put after already committed or aborted")
      verify(key != null, "Key cannot be null")
      require(value != null, "Cannot put a null value")
      verifyColFamilyOperations("put", colFamilyName)

      val kvEncoder = keyValueEncoderMap.get(colFamilyName)
      rocksDB.put(kvEncoder._1.encodeKey(key), kvEncoder._2.encodeValue(value), colFamilyName)
    }

    override def putList(
        key: UnsafeRow,
        values: Array[UnsafeRow],
        colFamilyName: String): Unit = {
      validateAndTransitionState(UPDATE)
      verify(state == UPDATING, "Cannot put after already committed or aborted")
      verify(key != null, "Key cannot be null")
      require(values != null, "Cannot put a null value")
      values.foreach(v => require(v != null, "Cannot put a null value in the array"))
      verifyColFamilyOperations("put", colFamilyName)

      val kvEncoder = keyValueEncoderMap.get(colFamilyName)
      verify(
        kvEncoder._2.supportsMultipleValuesPerKey,
        "Multi-value put operation requires an encoder" +
          " which supports multiple values for a single key")
      rocksDB.putList(
        kvEncoder._1.encodeKey(key),
        values.map(kvEncoder._2.encodeValue).toList,
        colFamilyName)
    }

    override def remove(key: UnsafeRow, colFamilyName: String): Unit = {
      validateAndTransitionState(UPDATE)
      verify(state == UPDATING, "Cannot remove after already committed or aborted")
      verify(key != null, "Key cannot be null")
      verifyColFamilyOperations("remove", colFamilyName)

      val kvEncoder = keyValueEncoderMap.get(colFamilyName)
      rocksDB.remove(kvEncoder._1.encodeKey(key), colFamilyName)
    }

    override def iterator(colFamilyName: String): StateStoreIterator[UnsafeRowPair] = {
      validateAndTransitionState(UPDATE)
      // Note this verify function only verify on the colFamilyName being valid,
      // we are actually doing prefix when useColumnFamilies,
      // but pass "iterator" to throw correct error message
      verifyColFamilyOperations("iterator", colFamilyName)
      val kvEncoder = keyValueEncoderMap.get(colFamilyName)
      val rowPair = new UnsafeRowPair()
      if (useColumnFamilies) {
        val rocksDbIter = rocksDB.iterator(colFamilyName)

        val iter = rocksDbIter.map { kv =>
          rowPair.withRows(kvEncoder._1.decodeKey(kv.key),
            kvEncoder._2.decodeValue(kv.value))
          if (!isValidated && rowPair.value != null && !useColumnFamilies) {
            StateStoreProvider.validateStateRowFormat(
              rowPair.key, keySchema, rowPair.value, valueSchema, stateStoreId, storeConf)
            isValidated = true
          }
          rowPair
        }

        new StateStoreIterator(iter, rocksDbIter.closeIfNeeded)
      } else {
        val rocksDbIter = rocksDB.iterator()

        val iter = rocksDbIter.map { kv =>
          rowPair.withRows(kvEncoder._1.decodeKey(kv.key),
            kvEncoder._2.decodeValue(kv.value))
          if (!isValidated && rowPair.value != null && !useColumnFamilies) {
            StateStoreProvider.validateStateRowFormat(
              rowPair.key, keySchema, rowPair.value, valueSchema, stateStoreId, storeConf)
            isValidated = true
          }
          rowPair
        }

        new StateStoreIterator(iter, rocksDbIter.closeIfNeeded)
      }
    }

    override def prefixScan(
        prefixKey: UnsafeRow,
        colFamilyName: String): StateStoreIterator[UnsafeRowPair] = {
      validateAndTransitionState(UPDATE)
      verifyColFamilyOperations("prefixScan", colFamilyName)

      val kvEncoder = keyValueEncoderMap.get(colFamilyName)
      require(kvEncoder._1.supportPrefixKeyScan,
        "Prefix scan requires setting prefix key!")

      val rowPair = new UnsafeRowPair()
      val prefix = kvEncoder._1.encodePrefixKey(prefixKey)

      val rocksDbIter = rocksDB.prefixScan(prefix, colFamilyName)
      val iter = rocksDbIter.map { kv =>
        rowPair.withRows(kvEncoder._1.decodeKey(kv.key),
          kvEncoder._2.decodeValue(kv.value))
        rowPair
      }

      new StateStoreIterator(iter, rocksDbIter.closeIfNeeded)
    }

    var checkpointInfo: Option[StateStoreCheckpointInfo] = None
    private var storedMetrics: Option[RocksDBMetrics] = None

    override def commit(): Long = synchronized {
      validateState(UPDATING)
      try {
        stateMachine.verifyStamp(stamp)
        val (newVersion, newCheckpointInfo) = rocksDB.commit(forceSnapshotOnCommit)
        checkpointInfo = Some(newCheckpointInfo)
        storedMetrics = rocksDB.metricsOpt
        validateAndTransitionState(COMMIT)
        logInfo(log"Committed ${MDC(VERSION_NUM, newVersion)} " +
          log"for ${MDC(STATE_STORE_ID, id)}")

        // Report the commit to StateStoreCoordinator for tracking
        if (storeConf.commitValidationEnabled) {
          StateStore.reportCommitToCoordinator(newVersion, stateStoreId, hadoopConf)
        }

        newVersion
      } catch {
        case e: Throwable =>
          throw QueryExecutionErrors.failedToCommitStateFileError(this.toString(), e)
      }
    }

    override def release(): Unit = {
      assert(readOnly, "Release can only be called on a read-only store")
      if (state != RELEASED) {
        logInfo(log"Releasing ${MDC(VERSION_NUM, version + 1)} " +
          log"for ${MDC(STATE_STORE_ID, id)}")
        rocksDB.release()
        validateAndTransitionState(RELEASE)
      } else {
        // Optionally log at DEBUG level that it's already released
        logDebug(log"State store already released")
      }
    }

    override def abort(): Unit = {
      if (validateState(UPDATING, ABORTED) != ABORTED) {
        try {
          validateAndTransitionState(ABORT)
          logInfo(log"Aborting ${MDC(VERSION_NUM, version + 1)} " +
            log"for ${MDC(STATE_STORE_ID, id)}")
          rocksDB.rollback()
        } finally {
          stateMachine.releaseStamp(stamp)
        }
      } else {
        logInfo(log"Skipping abort for ${MDC(VERSION_NUM, version + 1)} " +
          log"for ${MDC(STATE_STORE_ID, id)} as we already aborted")
      }
    }

    override def metrics: StateStoreMetrics = {
      validateAndTransitionState(METRICS)
      val rocksDBMetricsOpt = storedMetrics

      if (rocksDBMetricsOpt.isDefined) {
        val rocksDBMetrics = rocksDBMetricsOpt.get

        def commitLatencyMs(typ: String): Long =
          rocksDBMetrics.lastCommitLatencyMs.getOrElse(typ, 0L)

        def loadMetrics(typ: String): Long =
          rocksDBMetrics.loadMetrics.getOrElse(typ, 0L)

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
          CUSTOM_METRIC_CHANGE_LOG_WRITER_COMMIT_TIME -> commitLatencyMs("changeLogWriterCommit"),
          CUSTOM_METRIC_SAVE_ZIP_FILES_TIME -> commitLatencyMs("saveZipFiles"),

          CUSTOM_METRIC_LOAD_FROM_SNAPSHOT_TIME -> loadMetrics("loadFromSnapshot"),
          CUSTOM_METRIC_LOAD_TIME -> loadMetrics("load"),
          CUSTOM_METRIC_REPLAY_CHANGE_LOG -> loadMetrics("replayChangelog"),
          CUSTOM_METRIC_NUM_REPLAY_CHANGE_LOG_FILES -> loadMetrics("numReplayChangeLogFiles"),
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
          CUSTOM_METRIC_NUM_INTERNAL_COL_FAMILIES_KEYS -> rocksDBMetrics.numInternalKeys,
          CUSTOM_METRIC_NUM_EXTERNAL_COL_FAMILIES -> internalColFamilyCnt(),
          CUSTOM_METRIC_NUM_INTERNAL_COL_FAMILIES -> externalColFamilyCnt(),
          CUSTOM_METRIC_NUM_SNAPSHOTS_AUTO_REPAIRED -> rocksDBMetrics.numSnapshotsAutoRepaired,
          CUSTOM_METRIC_FORCE_SNAPSHOT -> (if (forceSnapshotOnCommit) 1L else 0L)
        ) ++ rocksDBMetrics.zipFileBytesUncompressed.map(bytes =>
          Map(CUSTOM_METRIC_ZIP_FILE_BYTES_UNCOMPRESSED -> bytes)).getOrElse(Map())

        val stateStoreInstanceMetrics = Map[StateStoreInstanceMetric, Long](
          CUSTOM_INSTANCE_METRIC_SNAPSHOT_LAST_UPLOADED
            .withNewId(id.partitionId, id.storeName) -> rocksDBMetrics.lastUploadedSnapshotVersion
        )

        StateStoreMetrics(
          rocksDBMetrics.numUncommittedKeys,
          rocksDBMetrics.totalMemUsageBytes,
          stateStoreCustomMetrics,
          stateStoreInstanceMetrics
        )
      } else {
        logInfo(log"Failed to collect metrics for store_id=${MDC(STATE_STORE_ID, id)} " +
          log"and version=${MDC(VERSION_NUM, version)}")
        StateStoreMetrics(0, 0, Map.empty, Map.empty)
      }
    }

    override def getStateStoreCheckpointInfo(): StateStoreCheckpointInfo = {
      validateAndTransitionState(METRICS)
      checkpointInfo match {
        case Some(info) => info
        case None => throw StateStoreErrors.stateStoreOperationOutOfOrder(
          "Cannot get checkpointInfo without committing the store")
      }
    }

    override def hasCommitted: Boolean = state == COMMITTED

    override def allColumnFamilyNames: Set[String] = {
      rocksDB.allColumnFamilyNames
    }

    override def toString: String = {
      s"RocksDBStateStore[stateStoreId=$stateStoreId_, version=$version]"
    }

    /** Return the [[RocksDB]] instance in this store. This is exposed mainly for testing. */
    def dbInstance(): RocksDB = rocksDB

    /** Remove column family if exists */
    override def removeColFamilyIfExists(colFamilyName: String): Boolean = {
      verifyColFamilyCreationOrDeletion("remove_col_family", colFamilyName)
      verify(useColumnFamilies, "Column families are not supported in this store")

      val result = rocksDB.removeColFamilyIfExists(colFamilyName)
      if (result) {
        keyValueEncoderMap.remove(colFamilyName)
      }
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
    this.rocksDBEventForwarder =
      Some(RocksDBEventForwarder(StateStoreProvider.getRunId(hadoopConf), stateStoreId))

    // Initialize StateStoreProviderId for memory tracking
    val queryRunId = UUID.fromString(StateStoreProvider.getRunId(hadoopConf))
    this.stateStoreProviderId = StateStoreProviderId(stateStoreId, queryRunId)

    if (useMultipleValuesPerKey) {
      require(useColumnFamilies, "Multiple values per key support requires column families to be" +
        " enabled in RocksDBStateStore.")
    }

    rocksDB // lazy initialization

    val dataEncoderCacheKey = StateRowEncoderCacheKey(
      queryRunId = StateStoreProvider.getRunId(hadoopConf),
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

    val dataEncoder = getDataEncoder(
      stateStoreEncoding,
      dataEncoderCacheKey,
      keyStateEncoderSpec,
      valueSchema,
      stateSchemaProvider,
      Some(StateStore.DEFAULT_COL_FAMILY_NAME))

    val keyEncoder = RocksDBStateEncoder.getKeyEncoder(
      dataEncoder,
      keyStateEncoderSpec,
      useColumnFamilies)
    val valueEncoder = RocksDBStateEncoder.getValueEncoder(
      dataEncoder,
      valueSchema,
      useMultipleValuesPerKey
    )

    var cfId: Short = 0
    if (useColumnFamilies) {
      cfId = rocksDB.createColFamilyIfAbsent(StateStore.DEFAULT_COL_FAMILY_NAME,
        isInternal = false)
    }

    keyValueEncoderMap.putIfAbsent(StateStore.DEFAULT_COL_FAMILY_NAME,
      (keyEncoder, valueEncoder, cfId))
  }

  override def stateStoreId: StateStoreId = stateStoreId_

  private lazy val stateMachine: RocksDBStateMachine =
    new RocksDBStateMachine(stateStoreId, RocksDBConf(storeConf))

  override protected def logName: String = s"${super.logName} ${stateStoreProviderId}"

  /**
   * Creates and returns a state store with the specified parameters.
   *
   * @param version The version of the state store to load
   * @param uniqueId Optional unique identifier for checkpoint
   * @param readOnly Whether to open the store in read-only mode
   * @param existingStore Optional existing store to reuse instead of creating a new one
   * @param forceSnapshotOnCommit Whether to force a snapshot upload on commit
   * @param loadEmpty If true, creates an empty store at this version without loading previous data
   * @return The loaded state store
   */
  private def loadStateStore(
      version: Long,
      uniqueId: Option[String] = None,
      readOnly: Boolean,
      existingStore: Option[RocksDBStateStore] = None,
      forceSnapshotOnCommit: Boolean = false,
      loadEmpty: Boolean = false): StateStore = {
    var acquiredStamp: Option[Long] = None
    var storeLoaded = false
    try {
      if (version < 0) {
        throw QueryExecutionErrors.unexpectedStateStoreVersion(version)
      }

      // Early validation of the existing store type before loading RocksDB
      existingStore.foreach { store =>
        if (!store.readOnly) {
          throw new IllegalArgumentException(
            s"Existing store must be readOnly, but got a read-write store")
        }
      }

      // if the existing store is None, then we need to acquire the stamp before
      // loading RocksDB
      val stamp = existingStore match {
        case None =>
          val s = stateMachine.acquireStamp()
          acquiredStamp = Some(s)
          Some(s)
        case Some(store: RocksDBStateStore) =>
          val s = store.stamp
          stateMachine.verifyStamp(s)
          Some(s)
      }

      rocksDB.load(
        version,
        stateStoreCkptId = if (storeConf.enableStateStoreCheckpointIds) uniqueId else None,
        readOnly = readOnly,
        loadEmpty = loadEmpty)

      // Create or reuse store instance
      val store = existingStore match {
        case Some(store: RocksDBStateStore) =>
          // Mark store as being used for write operations
          store.readOnly = readOnly
          store.forceSnapshotOnCommit = forceSnapshotOnCommit
          store
        case None =>
          // Create new store instance. The stamp should be defined
          // in this case
          new RocksDBStateStore(version, stamp.get, readOnly, forceSnapshotOnCommit)
      }
      storeLoaded = true
      store
    } catch {
      case e: OutOfMemoryError =>
        throw QueryExecutionErrors.notEnoughMemoryToLoadStore(
          stateStoreId.toString,
          "ROCKSDB_STORE_PROVIDER",
          e)
      case e: StateStoreInvalidStateMachineTransition =>
        throw e
      case e: Throwable => throw StateStoreErrors.cannotLoadStore(e)
    } finally {
      // If we acquired a stamp but failed to load the store, release it.
      // Note: We cannot rely on the task completion listener to clean up the stamp in this case
      // because the listener is only registered in the RocksDBStateStore constructor. If the
      // store fails to load (e.g., rocksDB.load() throws an exception), the RocksDBStateStore
      // instance is never created, so no completion listener exists to release the stamp.
      // This finally block ensures proper cleanup even when store creation fails early.
      if (!storeLoaded && acquiredStamp.isDefined) {
        acquiredStamp.foreach(stamp => stateMachine.releaseStamp(stamp, throwEx = false))
      }
    }
  }

  override def getStore(
      version: Long,
      uniqueId: Option[String] = None,
      forceSnapshotOnCommit: Boolean = false,
      loadEmpty: Boolean = false): StateStore = {
    loadStateStore(
      version,
      uniqueId,
      readOnly = false,
      forceSnapshotOnCommit = forceSnapshotOnCommit,
      loadEmpty = loadEmpty
    )
  }

  override def upgradeReadStoreToWriteStore(
      readStore: ReadStateStore,
      version: Long,
      uniqueId: Option[String] = None,
      forceSnapshotOnCommit: Boolean = false): StateStore = {
    assert(version == readStore.version,
      s"Can only upgrade readStore to writeStore with the same version," +
        s" readStoreVersion: ${readStore.version}, writeStoreVersion: ${version}")
    assert(this.stateStoreId == readStore.id, "Can only upgrade readStore to writeStore with" +
      " the same stateStoreId")
    assert(readStore.isInstanceOf[RocksDBStateStore], "Can only upgrade state store if it is a " +
      "RocksDBStateStore")
    loadStateStore(version,
      uniqueId,
      readOnly = false,
      existingStore = Some(readStore.asInstanceOf[RocksDBStateStore]),
      forceSnapshotOnCommit = forceSnapshotOnCommit)
  }

  override def getReadStore(
      version: Long, uniqueId: Option[String] = None): StateStore = {
    loadStateStore(version, uniqueId, readOnly = true)
  }

  override def doMaintenance(): Unit = {
    stateMachine.verifyForMaintenance()
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
    if (stateMachine.close()) {
      rocksDB.close()
    }
  }

  override def supportedCustomMetrics: Seq[StateStoreCustomMetric] = ALL_CUSTOM_METRICS

  override def supportedInstanceMetrics: Seq[StateStoreInstanceMetric] = ALL_INSTANCE_METRICS

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
  @volatile private var rocksDBEventForwarder: Option[RocksDBEventForwarder] = _
  @volatile private var stateStoreProviderId: StateStoreProviderId = _
  // Exposed for testing
  @volatile private[sql] var sparkConf: SparkConf = Option(SparkEnv.get).map(_.conf)
    .getOrElse(new SparkConf)

  protected def createRocksDB(
      dfsRootDir: String,
      conf: RocksDBConf,
      localRootDir: File,
      hadoopConf: Configuration,
      loggingId: String,
      useColumnFamilies: Boolean,
      enableStateStoreCheckpointIds: Boolean,
      partitionId: Int = 0,
      eventForwarder: Option[RocksDBEventForwarder] = None,
      uniqueId: Option[String] = None): RocksDB = {
    new RocksDB(
      dfsRootDir,
      conf,
      localRootDir,
      hadoopConf,
      loggingId,
      useColumnFamilies,
      enableStateStoreCheckpointIds,
      partitionId,
      eventForwarder,
      uniqueId)
  }

  private[sql] lazy val rocksDB = {
    val dfsRootDir = stateStoreId.storeCheckpointLocation().toString
    val storeIdStr = s"StateStoreId(opId=${stateStoreId.operatorId}," +
      s"partId=${stateStoreId.partitionId},name=${stateStoreId.storeName})"
    val loggingId = stateStoreProviderId.toString
    val localRootDir = Utils.createExecutorLocalTempDir(sparkConf, storeIdStr)
    createRocksDB(dfsRootDir, RocksDBConf(storeConf), localRootDir, hadoopConf, loggingId,
      useColumnFamilies, storeConf.enableStateStoreCheckpointIds, stateStoreId.partitionId,
      rocksDBEventForwarder,
      Some(s"${stateStoreProviderId.toString}_${UUID.randomUUID().toString}"))
  }

  private val keyValueEncoderMap = new java.util.concurrent.ConcurrentHashMap[String,
    (RocksDBKeyStateEncoder, RocksDBValueStateEncoder, Short)]

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
   * @param readOnly whether the state store should be read-only
   * @param snapshotVersionStateStoreCkptId state store checkpoint ID of the snapshot version
   * @param endVersionStateStoreCkptId state store checkpoint ID of the end version
   * @return [[StateStore]]
   */
  override def replayStateFromSnapshot(
      snapshotVersion: Long,
      endVersion: Long,
      readOnly: Boolean,
      snapshotVersionStateStoreCkptId: Option[String] = None,
      endVersionStateStoreCkptId: Option[String] = None): StateStore = {
    try {
      if (snapshotVersion < 1) {
        throw QueryExecutionErrors.unexpectedStateStoreVersion(snapshotVersion)
      }
      if (endVersion < snapshotVersion) {
        throw QueryExecutionErrors.unexpectedStateStoreVersion(endVersion)
      }
      val stamp = stateMachine.acquireStamp()
      try {
        rocksDB.loadFromSnapshot(
          snapshotVersion,
          endVersion,
          snapshotVersionStateStoreCkptId,
          endVersionStateStoreCkptId)
        new RocksDBStateStore(endVersion, stamp, readOnly, forceSnapshotOnCommit = false)
      } catch {
        case e: Throwable =>
          stateMachine.releaseStamp(stamp)
          throw e
      }
    }
    catch {
      case e: OutOfMemoryError =>
        throw QueryExecutionErrors.notEnoughMemoryToLoadStore(
          stateStoreId.toString,
          "ROCKSDB_STORE_PROVIDER",
          e)
      case e: Throwable => throw StateStoreErrors.cannotLoadStore(e)
    }
  }

  override def getStateStoreChangeDataReader(
      startVersion: Long,
      endVersion: Long,
      colFamilyNameOpt: Option[String] = None,
      endVersionStateStoreCkptId: Option[String] = None):
    StateStoreChangeDataReader = {
    val statePath = stateStoreId.storeCheckpointLocation()
    val sparkConf = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf)
    new RocksDBStateStoreChangeDataReader(
      stateStoreId,
      CheckpointFileManager.create(statePath, hadoopConf),
      rocksDB,
      statePath,
      startVersion,
      endVersion,
      endVersionStateStoreCkptId,
      CompressionCodec.createCodec(sparkConf, storeConf.compressionCodec),
      keyValueEncoderMap,
      storeConf,
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

  /**
   * Encodes a virtual column family ID into a byte array suitable for RocksDB.
   *
   * This method creates a fixed-size byte array prefixed with the virtual column family ID,
   * which is used to partition data within RocksDB.
   *
   * @param virtualColFamilyId The column family identifier to encode
   * @return A byte array containing the encoded column family ID
   */
  def getColumnFamilyIdAsBytes(virtualColFamilyId: Short): Array[Byte] = {
    val encodedBytes = new Array[Byte](RocksDBStateStoreProvider.VIRTUAL_COL_FAMILY_PREFIX_BYTES)
    Platform.putShort(encodedBytes, Platform.BYTE_ARRAY_OFFSET, virtualColFamilyId)
    encodedBytes
  }

  /**
   * Function to encode state row with virtual col family id prefix
   * @param data - passed byte array to be stored in state store
   * @param vcfId - virtual column family id
   * @return - encoded byte array with virtual column family id prefix
   */
  def encodeStateRowWithPrefix(
      data: Array[Byte],
      vcfId: Short): Array[Byte] = {
    // Create result array big enough for all prefixes plus data
    val result = new Array[Byte](RocksDBStateStoreProvider.VIRTUAL_COL_FAMILY_PREFIX_BYTES
      + data.length)
    val offset = Platform.BYTE_ARRAY_OFFSET +
      RocksDBStateStoreProvider.VIRTUAL_COL_FAMILY_PREFIX_BYTES

    Platform.putShort(result, Platform.BYTE_ARRAY_OFFSET, vcfId)

    // Write the actual data
    Platform.copyMemory(
      data, Platform.BYTE_ARRAY_OFFSET,
      result, offset,
      data.length
    )

    result
  }

  /**
   * Function to decode virtual column family id from byte array
   * @param data - passed byte array retrieved from state store
   * @return - virtual column family id
   */
  def getColumnFamilyBytesAsId(data: Array[Byte]): Short = {
    Platform.getShort(data, Platform.BYTE_ARRAY_OFFSET)
  }

  /**
   * Function to decode state row with virtual col family id prefix
   * @param data - passed byte array retrieved from state store
   * @return - pair of decoded byte array without virtual column family id prefix
   *           and name of column family
   */
  def decodeStateRowWithPrefix(data: Array[Byte]): Array[Byte] = {
    val offset = Platform.BYTE_ARRAY_OFFSET +
      RocksDBStateStoreProvider.VIRTUAL_COL_FAMILY_PREFIX_BYTES

    val key = new Array[Byte](data.length -
      RocksDBStateStoreProvider.VIRTUAL_COL_FAMILY_PREFIX_BYTES)
    Platform.copyMemory(
      data, offset,
      key, Platform.BYTE_ARRAY_OFFSET,
      key.length
    )

    key
  }

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
      columnFamilyName: Option[String] = None): RocksDBDataEncoder = {
    assert(Set("avro", "unsaferow").contains(stateStoreEncoding))
    RocksDBStateStoreProvider.dataEncoderCache.get(
      encoderCacheKey,
      new java.util.concurrent.Callable[RocksDBDataEncoder] {
        override def call(): RocksDBDataEncoder = {
          if (stateStoreEncoding == Avro.toString) {
            assert(columnFamilyName.isDefined,
              "Column family name must be defined for Avro encoding")
            new AvroStateEncoder(
              keyStateEncoderSpec,
              valueSchema,
              stateSchemaProvider,
              columnFamilyName.get
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
  val CUSTOM_METRIC_CHANGE_LOG_WRITER_COMMIT_TIME = StateStoreCustomTimingMetric(
    "rocksdbChangeLogWriterCommitLatencyMs",
    "RocksDB: commit - changelog commit time")
  val CUSTOM_METRIC_SAVE_ZIP_FILES_TIME = StateStoreCustomTimingMetric(
    "rocksdbSaveZipFilesLatencyMs",
    "RocksDB: commit - zip files sync to external storage time")

  val CUSTOM_METRIC_LOAD_FROM_SNAPSHOT_TIME = StateStoreCustomTimingMetric(
    "rocksdbLoadFromSnapshotLatencyMs",
    "RocksDB: load from snapshot - time taken to load the store from snapshot")
  val CUSTOM_METRIC_LOAD_TIME = StateStoreCustomTimingMetric(
    "rocksdbLoadLatencyMs",
    "RocksDB: load - time taken to load the store")
  val CUSTOM_METRIC_REPLAY_CHANGE_LOG = StateStoreCustomTimingMetric(
    "rocksdbReplayChangeLogLatencyMs",
    "RocksDB: load - time taken to replay the change log")
  val CUSTOM_METRIC_NUM_REPLAY_CHANGE_LOG_FILES = StateStoreCustomSizeMetric(
    "rocksdbNumReplayChangelogFiles",
    "RocksDB: load - number of change log files replayed")

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
  val CUSTOM_METRIC_NUM_INTERNAL_COL_FAMILIES_KEYS = StateStoreCustomSizeMetric(
    "rocksdbNumInternalColFamiliesKeys",
    "RocksDB: number of internal keys for internal column families")
  val CUSTOM_METRIC_NUM_EXTERNAL_COL_FAMILIES = StateStoreCustomSizeMetric(
    "rocksdbNumExternalColumnFamilies",
    "RocksDB: number of external column families")
  val CUSTOM_METRIC_NUM_INTERNAL_COL_FAMILIES = StateStoreCustomSizeMetric(
    "rocksdbNumInternalColumnFamilies",
    "RocksDB: number of internal column families")

  // Total SST file size
  val CUSTOM_METRIC_SST_FILE_SIZE = StateStoreCustomSizeMetric(
    "rocksdbSstFileSize", "RocksDB: size of all SST files")
  val CUSTOM_METRIC_NUM_SNAPSHOTS_AUTO_REPAIRED = StateStoreCustomSumMetric(
    "rocksdbNumSnapshotsAutoRepaired",
    "RocksDB: number of snapshots that were automatically repaired during store load")

  val CUSTOM_METRIC_FORCE_SNAPSHOT = StateStoreCustomSumMetric(
    "rocksdbForceSnapshotCount", "RocksDB: number of stores that had forced snapshot on commit")

  val ALL_CUSTOM_METRICS = Seq(
    CUSTOM_METRIC_SST_FILE_SIZE, CUSTOM_METRIC_GET_TIME, CUSTOM_METRIC_PUT_TIME,
    CUSTOM_METRIC_FLUSH_TIME, CUSTOM_METRIC_COMMIT_COMPACT_TIME,
    CUSTOM_METRIC_CHECKPOINT_TIME, CUSTOM_METRIC_FILESYNC_TIME,
    CUSTOM_METRIC_BYTES_COPIED, CUSTOM_METRIC_FILES_COPIED, CUSTOM_METRIC_FILES_REUSED,
    CUSTOM_METRIC_ZIP_FILE_BYTES_UNCOMPRESSED, CUSTOM_METRIC_CHANGE_LOG_WRITER_COMMIT_TIME,
    CUSTOM_METRIC_SAVE_ZIP_FILES_TIME, CUSTOM_METRIC_GET_COUNT, CUSTOM_METRIC_PUT_COUNT,
    CUSTOM_METRIC_BLOCK_CACHE_MISS, CUSTOM_METRIC_BLOCK_CACHE_HITS, CUSTOM_METRIC_BYTES_READ,
    CUSTOM_METRIC_BYTES_WRITTEN, CUSTOM_METRIC_ITERATOR_BYTES_READ, CUSTOM_METRIC_STALL_TIME,
    CUSTOM_METRIC_TOTAL_COMPACT_TIME, CUSTOM_METRIC_COMPACT_READ_BYTES,
    CUSTOM_METRIC_COMPACT_WRITTEN_BYTES, CUSTOM_METRIC_FLUSH_WRITTEN_BYTES,
    CUSTOM_METRIC_PINNED_BLOCKS_MEM_USAGE, CUSTOM_METRIC_NUM_INTERNAL_COL_FAMILIES_KEYS,
    CUSTOM_METRIC_NUM_EXTERNAL_COL_FAMILIES, CUSTOM_METRIC_NUM_INTERNAL_COL_FAMILIES,
    CUSTOM_METRIC_LOAD_FROM_SNAPSHOT_TIME, CUSTOM_METRIC_LOAD_TIME, CUSTOM_METRIC_REPLAY_CHANGE_LOG,
    CUSTOM_METRIC_NUM_REPLAY_CHANGE_LOG_FILES, CUSTOM_METRIC_NUM_SNAPSHOTS_AUTO_REPAIRED,
    CUSTOM_METRIC_FORCE_SNAPSHOT)

  val CUSTOM_INSTANCE_METRIC_SNAPSHOT_LAST_UPLOADED = StateStoreSnapshotLastUploadInstanceMetric()

  val ALL_INSTANCE_METRICS = Seq(CUSTOM_INSTANCE_METRIC_SNAPSHOT_LAST_UPLOADED)
}

/** [[StateStoreChangeDataReader]] implementation for [[RocksDBStateStoreProvider]] */
class RocksDBStateStoreChangeDataReader(
    storeId: StateStoreId,
    fm: CheckpointFileManager,
    rocksDB: RocksDB,
    stateLocation: Path,
    startVersion: Long,
    endVersion: Long,
    endVersionStateStoreCkptId: Option[String],
    compressionCodec: CompressionCodec,
    keyValueEncoderMap:
      ConcurrentHashMap[String, (RocksDBKeyStateEncoder, RocksDBValueStateEncoder, Short)],
    storeConf: StateStoreConf,
    colFamilyNameOpt: Option[String] = None)
  extends StateStoreChangeDataReader(
    storeId, fm, stateLocation, startVersion, endVersion, compressionCodec,
    storeConf, colFamilyNameOpt) {

  override protected val versionsAndUniqueIds: Array[(Long, Option[String])] =
    if (endVersionStateStoreCkptId.isDefined) {
      val fullVersionLineage = rocksDB.getFullLineage(
        startVersion,
        endVersion,
        endVersionStateStoreCkptId)
      fullVersionLineage
        .sortBy(_.version)
        .map(item => (item.version, Some(item.checkpointUniqueId)))
    } else {
      (startVersion to endVersion).map((_, None)).toArray
    }

  override protected val changelogSuffix: String = "changelog"

  override def getNext(): (RecordType.Value, UnsafeRow, UnsafeRow, Long) = {
    var currRecord: (RecordType.Value, Array[Byte], Array[Byte]) = null
    val currEncoder: (RocksDBKeyStateEncoder, RocksDBValueStateEncoder, Short) =
      keyValueEncoderMap.get(colFamilyNameOpt
        .getOrElse(StateStore.DEFAULT_COL_FAMILY_NAME))

    if (colFamilyNameOpt.isDefined) {
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
        val keyBytes = if (storeConf.rowChecksumEnabled
          && nextRecord._1 == RecordType.DELETE_RECORD) {
          // remove checksum and decode to the original key
          KeyValueChecksumEncoder
            .decodeAndVerifyKeyRowWithChecksum(readVerifier, nextRecord._2)
        } else {
          nextRecord._2
        }
        val colFamilyIdBytes: Array[Byte] =
          RocksDBStateStoreProvider.getColumnFamilyIdAsBytes(currEncoder._3)
        val endIndex = colFamilyIdBytes.size
        // Function checks for byte arrays being equal
        // from index 0 to endIndex - 1 (both inclusive)
        if (java.util.Arrays.equals(keyBytes, 0, endIndex,
          colFamilyIdBytes, 0, endIndex)) {
          val valueBytes = if (storeConf.rowChecksumEnabled &&
            nextRecord._1 != RecordType.DELETE_RECORD) {
            KeyValueChecksumEncoder.decodeAndVerifyValueRowWithChecksum(
              readVerifier, keyBytes, nextRecord._3)
          } else {
            nextRecord._3
          }
          val extractedKey = RocksDBStateStoreProvider.decodeStateRowWithPrefix(keyBytes)
          val result = (nextRecord._1, extractedKey, valueBytes)
          currRecord = result
        }
      }
    } else {
      val reader = currentChangelogReader()
      if (reader == null) {
        return null
      }
      val nextRecord = reader.next()
      currRecord = if (storeConf.rowChecksumEnabled) {
        nextRecord._1 match {
          case RecordType.DELETE_RECORD =>
            val key = KeyValueChecksumEncoder
              .decodeAndVerifyKeyRowWithChecksum(readVerifier, nextRecord._2)
            (nextRecord._1, key, nextRecord._3)
          case _ =>
            val value = KeyValueChecksumEncoder.decodeAndVerifyValueRowWithChecksum(
              readVerifier, nextRecord._2, nextRecord._3)
            (nextRecord._1, nextRecord._2, value)
        }
      } else {
        nextRecord
      }
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

/**
 * Class used to relay events reported from a RocksDB instance to the state store coordinator.
 *
 * We pass this into the RocksDB instance to report specific events like snapshot uploads.
 * This should only be used to report back to the coordinator for metrics and monitoring purposes.
 */
private[state] case class RocksDBEventForwarder(queryRunId: String, stateStoreId: StateStoreId) {
  // Build the state store provider ID from the query run ID and the state store ID
  private val providerId = StateStoreProviderId(stateStoreId, UUID.fromString(queryRunId))

  /**
   * Callback function from RocksDB to report events to the coordinator.
   * Information from the store provider such as the state store ID and query run ID are
   * attached here to report back to the coordinator.
   *
   * @param version The snapshot version that was just uploaded from RocksDB
   */
  def reportSnapshotUploaded(version: Long): Unit = {
    // Report the state store provider ID and the version to the coordinator
    val currentTimestamp = System.currentTimeMillis()
    StateStoreProvider.coordinatorRef.foreach(
      _.snapshotUploaded(
        providerId,
        version,
        currentTimestamp
      )
    )
  }
}
