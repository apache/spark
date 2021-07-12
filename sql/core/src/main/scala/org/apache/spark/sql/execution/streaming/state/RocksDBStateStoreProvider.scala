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

import org.apache.hadoop.conf.Configuration

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
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

    override def get(key: UnsafeRow): UnsafeRow = {
      verify(key != null, "Key cannot be null")
      val value = encoder.decodeValue(rocksDB.get(encoder.encodeKey(key)))
      if (!isValidated && value != null) {
        StateStoreProvider.validateStateRowFormat(
          key, keySchema, value, valueSchema, storeConf)
        isValidated = true
      }
      value
    }

    override def put(key: UnsafeRow, value: UnsafeRow): Unit = {
      verify(state == UPDATING, "Cannot put after already committed or aborted")
      verify(key != null, "Key cannot be null")
      require(value != null, "Cannot put a null value")
      rocksDB.put(encoder.encodeKey(key), encoder.encodeValue(value))
    }

    override def remove(key: UnsafeRow): Unit = {
      verify(state == UPDATING, "Cannot remove after already committed or aborted")
      verify(key != null, "Key cannot be null")
      rocksDB.remove(encoder.encodeKey(key))
    }

    override def iterator(): Iterator[UnsafeRowPair] = {
      rocksDB.iterator().map { kv =>
        val rowPair = encoder.decode(kv)
        if (!isValidated && rowPair.value != null) {
          StateStoreProvider.validateStateRowFormat(
            rowPair.key, keySchema, rowPair.value, valueSchema, storeConf)
          isValidated = true
        }
        rowPair
      }
    }

    override def prefixScan(prefixKey: UnsafeRow): Iterator[UnsafeRowPair] = {
      require(encoder.supportPrefixKeyScan, "Prefix scan requires setting prefix key!")

      val prefix = encoder.encodePrefixKey(prefixKey)
      rocksDB.prefixScan(prefix).map(kv => encoder.decode(kv))
    }

    override def commit(): Long = synchronized {
      verify(state == UPDATING, "Cannot commit after already committed or aborted")
      val newVersion = rocksDB.commit()
      state = COMMITTED
      logInfo(s"Committed $newVersion for $id")
      newVersion
    }

    override def abort(): Unit = {
      verify(state == UPDATING || state == ABORTED, "Cannot abort after already committed")
      logInfo(s"Aborting ${version + 1} for $id")
      rocksDB.rollback()
      state = ABORTED
    }

    override def metrics: StateStoreMetrics = {
      val rocksDBMetrics = rocksDB.metrics
      def commitLatencyMs(typ: String): Long = rocksDBMetrics.lastCommitLatencyMs.getOrElse(typ, 0L)
      def avgNativeOpsLatencyMs(typ: String): Long = {
        rocksDBMetrics.nativeOpsLatencyMicros.get(typ).map(_.avg).getOrElse(0.0).toLong
      }

      val stateStoreCustomMetrics = Map[StateStoreCustomMetric, Long](
        CUSTOM_METRIC_SST_FILE_SIZE -> rocksDBMetrics.totalSSTFilesBytes,
        CUSTOM_METRIC_GET_TIME -> avgNativeOpsLatencyMs("get"),
        CUSTOM_METRIC_PUT_TIME -> avgNativeOpsLatencyMs("put"),
        CUSTOM_METRIC_WRITEBATCH_TIME -> commitLatencyMs("writeBatch"),
        CUSTOM_METRIC_FLUSH_TIME -> commitLatencyMs("flush"),
        CUSTOM_METRIC_PAUSE_TIME -> commitLatencyMs("pause"),
        CUSTOM_METRIC_CHECKPOINT_TIME -> commitLatencyMs("checkpoint"),
        CUSTOM_METRIC_FILESYNC_TIME -> commitLatencyMs("fileSync"),
        CUSTOM_METRIC_BYTES_COPIED -> rocksDBMetrics.bytesCopied,
        CUSTOM_METRIC_FILES_COPIED -> rocksDBMetrics.filesCopied,
        CUSTOM_METRIC_FILES_REUSED -> rocksDBMetrics.filesReused
      ) ++ rocksDBMetrics.zipFileBytesUncompressed.map(bytes =>
        Map(CUSTOM_METRIC_ZIP_FILE_BYTES_UNCOMPRESSED -> bytes)).getOrElse(Map())

      StateStoreMetrics(
        rocksDBMetrics.numUncommittedKeys,
        rocksDBMetrics.memUsageBytes,
        stateStoreCustomMetrics)
    }

    override def hasCommitted: Boolean = state == COMMITTED

    override def toString: String = {
      s"RocksDBStateStore[id=(op=${id.operatorId},part=${id.partitionId})," +
        s"dir=${id.storeCheckpointLocation()}]"
    }

    /** Return the [[RocksDB]] instance in this store. This is exposed mainly for testing. */
    def dbInstance(): RocksDB = rocksDB
  }

  override def init(
      stateStoreId: StateStoreId,
      keySchema: StructType,
      valueSchema: StructType,
      numColsPrefixKey: Int,
      storeConf: StateStoreConf,
      hadoopConf: Configuration): Unit = {
    this.stateStoreId_ = stateStoreId
    this.keySchema = keySchema
    this.valueSchema = valueSchema
    this.storeConf = storeConf
    this.hadoopConf = hadoopConf

    require((keySchema.length == 0 && numColsPrefixKey == 0) ||
      (keySchema.length > numColsPrefixKey), "The number of columns in the key must be " +
      "greater than the number of columns for prefix key!")

    this.encoder = RocksDBStateEncoder.getEncoder(keySchema, valueSchema, numColsPrefixKey)

    rocksDB // lazy initialization
  }

  override def stateStoreId: StateStoreId = stateStoreId_

  override def getStore(version: Long): StateStore = {
    require(version >= 0, "Version cannot be less than 0")
    rocksDB.load(version)
    new RocksDBStateStore(version)
  }

  override def doMaintenance(): Unit = {
    rocksDB.cleanup()
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

  private[sql] lazy val rocksDB = {
    val dfsRootDir = stateStoreId.storeCheckpointLocation().toString
    val storeIdStr = s"StateStoreId(opId=${stateStoreId.operatorId}," +
      s"partId=${stateStoreId.partitionId},name=${stateStoreId.storeName})"
    val sparkConf = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf)
    val localRootDir = Utils.createTempDir(Utils.getLocalDir(sparkConf), storeIdStr)
    new RocksDB(dfsRootDir, RocksDBConf(storeConf), localRootDir, hadoopConf, storeIdStr)
  }

  @volatile private var encoder: RocksDBStateEncoder = _

  private def verify(condition: => Boolean, msg: String): Unit = {
    if (!condition) { throw new IllegalStateException(msg) }
  }
}

object RocksDBStateStoreProvider {
  // Version as a single byte that specifies the encoding of the row data in RocksDB
  val STATE_ENCODING_NUM_VERSION_BYTES = 1
  val STATE_ENCODING_VERSION: Byte = 0

  // Native operation latencies report as latency per 1000 calls
  // as SQLMetrics support ms latency whereas RocksDB reports it in microseconds.
  val CUSTOM_METRIC_GET_TIME = StateStoreCustomTimingMetric(
    "rocksdbGetLatency", "RocksDB: avg get latency (per 1000 calls)")
  val CUSTOM_METRIC_PUT_TIME = StateStoreCustomTimingMetric(
    "rocksdbPutLatency", "RocksDB: avg put latency (per 1000 calls)")

  // Commit latency detailed breakdown
  val CUSTOM_METRIC_WRITEBATCH_TIME = StateStoreCustomTimingMetric(
    "rocksdbCommitWriteBatchLatency", "RocksDB: commit - write batch time")
  val CUSTOM_METRIC_FLUSH_TIME = StateStoreCustomTimingMetric(
    "rocksdbCommitFlushLatency", "RocksDB: commit - flush time")
  val CUSTOM_METRIC_PAUSE_TIME = StateStoreCustomTimingMetric(
    "rocksdbCommitPauseLatency", "RocksDB: commit - pause bg time")
  val CUSTOM_METRIC_CHECKPOINT_TIME = StateStoreCustomTimingMetric(
    "rocksdbCommitCheckpointLatency", "RocksDB: commit - checkpoint time")
  val CUSTOM_METRIC_FILESYNC_TIME = StateStoreCustomTimingMetric(
    "rocksdbFileSyncTime", "RocksDB: commit - file sync time")
  val CUSTOM_METRIC_FILES_COPIED = StateStoreCustomSizeMetric(
    "rocksdbFilesCopied", "RocksDB: file manager - files copied")
  val CUSTOM_METRIC_BYTES_COPIED = StateStoreCustomSizeMetric(
    "rocksdbBytesCopied", "RocksDB: file manager - bytes copied")
  val CUSTOM_METRIC_FILES_REUSED = StateStoreCustomSizeMetric(
    "rocksdbFilesReused", "RocksDB: file manager - files reused")
  val CUSTOM_METRIC_ZIP_FILE_BYTES_UNCOMPRESSED = StateStoreCustomSizeMetric(
    "rocksdbZipFileBytesUncompressed", "RocksDB: file manager - uncompressed zip file bytes")

  // Total SST file size
  val CUSTOM_METRIC_SST_FILE_SIZE = StateStoreCustomSizeMetric(
    "rocksdbSstFileSize", "RocksDB: size of all SST files")

  val ALL_CUSTOM_METRICS = Seq(
    CUSTOM_METRIC_SST_FILE_SIZE, CUSTOM_METRIC_GET_TIME, CUSTOM_METRIC_PUT_TIME,
    CUSTOM_METRIC_WRITEBATCH_TIME, CUSTOM_METRIC_FLUSH_TIME, CUSTOM_METRIC_PAUSE_TIME,
    CUSTOM_METRIC_CHECKPOINT_TIME, CUSTOM_METRIC_FILESYNC_TIME,
    CUSTOM_METRIC_BYTES_COPIED, CUSTOM_METRIC_FILES_COPIED, CUSTOM_METRIC_FILES_REUSED,
    CUSTOM_METRIC_ZIP_FILE_BYTES_UNCOMPRESSED
  )
}
