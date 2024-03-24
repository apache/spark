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

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

private[sql] class RocksDBStateStoreProvider
  extends StateStoreProvider with Logging with Closeable {
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

    override def createColFamilyIfAbsent(
        colFamilyName: String,
        keySchema: StructType,
        numColsPrefixKey: Int,
        valueSchema: StructType,
        useMultipleValuesPerKey: Boolean = false,
        isInternal: Boolean = false): Unit = {
      verify(colFamilyName != StateStore.DEFAULT_COL_FAMILY_NAME,
        s"Failed to create column family with reserved_name=$colFamilyName")
      verify(useColumnFamilies, "Column families are not supported in this store")
      rocksDB.createColFamilyIfAbsent(colFamilyName, isInternal)
      keyValueEncoderMap.putIfAbsent(colFamilyName,
        (RocksDBStateEncoder.getKeyEncoder(keySchema, numColsPrefixKey),
         RocksDBStateEncoder.getValueEncoder(valueSchema, useMultipleValuesPerKey)))
    }

    override def get(key: UnsafeRow, colFamilyName: String): UnsafeRow = {
      verify(key != null, "Key cannot be null")
      val kvEncoder = keyValueEncoderMap.get(colFamilyName)
      val value = kvEncoder._2.decodeValue(
        rocksDB.get(kvEncoder._1.encodeKey(key), colFamilyName))
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

      val kvEncoder = keyValueEncoderMap.get(colFamilyName)
      val valueEncoder = kvEncoder._2
      val keyEncoder = kvEncoder._1

      verify(valueEncoder.supportsMultipleValuesPerKey, "valuesIterator requires a encoder " +
      "that supports multiple values for a single key.")
      val encodedKey = rocksDB.get(keyEncoder.encodeKey(key), colFamilyName)
      valueEncoder.decodeValues(encodedKey)
    }

    override def merge(key: UnsafeRow, value: UnsafeRow,
        colFamilyName: String = StateStore.DEFAULT_COL_FAMILY_NAME): Unit = {
      verify(state == UPDATING, "Cannot merge after already committed or aborted")
      val kvEncoder = keyValueEncoderMap.get(colFamilyName)
      val keyEncoder = kvEncoder._1
      val valueEncoder = kvEncoder._2
      verify(valueEncoder.supportsMultipleValuesPerKey, "Merge operation requires an encoder" +
        " which supports multiple values for a single key")
      verify(key != null, "Key cannot be null")
      require(value != null, "Cannot merge a null value")
      rocksDB.merge(keyEncoder.encodeKey(key), valueEncoder.encodeValue(value), colFamilyName)
    }

    override def put(key: UnsafeRow, value: UnsafeRow, colFamilyName: String): Unit = {
      verify(state == UPDATING, "Cannot put after already committed or aborted")
      verify(key != null, "Key cannot be null")
      require(value != null, "Cannot put a null value")
      val kvEncoder = keyValueEncoderMap.get(colFamilyName)
      rocksDB.put(kvEncoder._1.encodeKey(key),
        kvEncoder._2.encodeValue(value), colFamilyName)
    }

    override def remove(key: UnsafeRow, colFamilyName: String): Unit = {
      verify(state == UPDATING, "Cannot remove after already committed or aborted")
      verify(key != null, "Key cannot be null")
      val kvEncoder = keyValueEncoderMap.get(colFamilyName)
      rocksDB.remove(kvEncoder._1.encodeKey(key), colFamilyName)
    }

    override def iterator(colFamilyName: String): Iterator[UnsafeRowPair] = {
      val kvEncoder = keyValueEncoderMap.get(colFamilyName)
      val rowPair = new UnsafeRowPair()
      rocksDB.iterator(colFamilyName).map { kv =>
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

    override def prefixScan(prefixKey: UnsafeRow, colFamilyName: String):
      Iterator[UnsafeRowPair] = {
      val kvEncoder = keyValueEncoderMap.get(colFamilyName)
      require(kvEncoder._1.supportPrefixKeyScan,
        "Prefix scan requires setting prefix key!")

      val prefix = kvEncoder._1.encodePrefixKey(prefixKey)
      val rowPair = new UnsafeRowPair()
      rocksDB.prefixScan(prefix, colFamilyName).map { kv =>
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
        logInfo(s"Committed $newVersion for $id")
        newVersion
      } catch {
        case e: Throwable =>
          throw QueryExecutionErrors.failedToCommitStateFileError(this.toString(), e)
      }
    }

    override def abort(): Unit = {
      verify(state == UPDATING || state == ABORTED, "Cannot abort after already committed")
      logInfo(s"Aborting ${version + 1} for $id")
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
          CUSTOM_METRIC_PINNED_BLOCKS_MEM_USAGE -> rocksDBMetrics.pinnedBlocksMemUsage
        ) ++ rocksDBMetrics.zipFileBytesUncompressed.map(bytes =>
          Map(CUSTOM_METRIC_ZIP_FILE_BYTES_UNCOMPRESSED -> bytes)).getOrElse(Map())

        StateStoreMetrics(
          rocksDBMetrics.numUncommittedKeys,
          rocksDBMetrics.totalMemUsageBytes,
          stateStoreCustomMetrics)
      } else {
        logInfo(s"Failed to collect metrics for store_id=$id and version=$version")
        StateStoreMetrics(0, 0, Map.empty)
      }
    }

    override def hasCommitted: Boolean = state == COMMITTED

    override def toString: String = {
      s"RocksDBStateStore[id=(op=${id.operatorId},part=${id.partitionId})," +
        s"dir=${id.storeCheckpointLocation()}]"
    }

    /** Return the [[RocksDB]] instance in this store. This is exposed mainly for testing. */
    def dbInstance(): RocksDB = rocksDB

    /** Remove column family if exists */
    override def removeColFamilyIfExists(colFamilyName: String): Unit = {
      verify(useColumnFamilies, "Column families are not supported in this store")
      rocksDB.removeColFamilyIfExists(colFamilyName)
      keyValueEncoderMap.remove(colFamilyName)
    }
  }

  override def init(
      stateStoreId: StateStoreId,
      keySchema: StructType,
      valueSchema: StructType,
      numColsPrefixKey: Int,
      useColumnFamilies: Boolean,
      storeConf: StateStoreConf,
      hadoopConf: Configuration,
      useMultipleValuesPerKey: Boolean = false): Unit = {
    this.stateStoreId_ = stateStoreId
    this.keySchema = keySchema
    this.valueSchema = valueSchema
    this.storeConf = storeConf
    this.hadoopConf = hadoopConf
    this.useColumnFamilies = useColumnFamilies

    require((keySchema.length == 0 && numColsPrefixKey == 0) ||
      (keySchema.length > numColsPrefixKey), "The number of columns in the key must be " +
      "greater than the number of columns for prefix key!")

    if (useMultipleValuesPerKey) {
      require(numColsPrefixKey == 0, "Both multiple values per key, and prefix key are not " +
        "supported simultaneously.")
      require(useColumnFamilies, "Multiple values per key support requires column families to be" +
        " enabled in RocksDBStateStore.")
    }

    keyValueEncoderMap.putIfAbsent(StateStore.DEFAULT_COL_FAMILY_NAME,
      (RocksDBStateEncoder.getKeyEncoder(keySchema, numColsPrefixKey),
       RocksDBStateEncoder.getValueEncoder(valueSchema, useMultipleValuesPerKey)))

    rocksDB // lazy initialization
  }

  override def stateStoreId: StateStoreId = stateStoreId_

  override def getStore(version: Long): StateStore = {
    try {
      if (version < 0) {
        throw QueryExecutionErrors.unexpectedStateStoreVersion(version)
      }
      rocksDB.load(version)
      new RocksDBStateStore(version)
    }
    catch {
      case e: Throwable => throw QueryExecutionErrors.cannotLoadStore(e)
    }
  }

  override def getReadStore(version: Long): StateStore = {
    try {
      if (version < 0) {
        throw QueryExecutionErrors.unexpectedStateStoreVersion(version)
      }
      rocksDB.load(version, true)
      new RocksDBStateStore(version)
    }
    catch {
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

  private[sql] lazy val rocksDB = {
    val dfsRootDir = stateStoreId.storeCheckpointLocation().toString
    val storeIdStr = s"StateStoreId(opId=${stateStoreId.operatorId}," +
      s"partId=${stateStoreId.partitionId},name=${stateStoreId.storeName})"
    val sparkConf = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf)
    val localRootDir = Utils.createTempDir(Utils.getLocalDir(sparkConf), storeIdStr)
    new RocksDB(dfsRootDir, RocksDBConf(storeConf), localRootDir, hadoopConf, storeIdStr,
      useColumnFamilies)
  }

  private val keyValueEncoderMap = new java.util.concurrent.ConcurrentHashMap[String,
    (RocksDBKeyStateEncoder, RocksDBValueStateEncoder)]

  private def verify(condition: => Boolean, msg: String): Unit = {
    if (!condition) { throw new IllegalStateException(msg) }
  }
}

object RocksDBStateStoreProvider {
  // Version as a single byte that specifies the encoding of the row data in RocksDB
  val STATE_ENCODING_NUM_VERSION_BYTES = 1
  val STATE_ENCODING_VERSION: Byte = 0

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
    CUSTOM_METRIC_PINNED_BLOCKS_MEM_USAGE
  )
}
