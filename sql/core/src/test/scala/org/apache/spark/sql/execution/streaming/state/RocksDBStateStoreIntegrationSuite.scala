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

import scala.jdk.CollectionConverters.SetHasAsScala

import org.apache.hadoop.conf.Configuration
import org.scalatest.time.{Minute, Span}

import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.streaming.{MemoryStream, StreamingQueryWrapper}
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.streaming.OutputMode.Update
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

object TestStateStoreWrapper {
  // Internal list to hold checkpoint IDs (strings)
  private var checkpointInfos: List[StateStoreCheckpointInfo] = List.empty

  // Method to add a string (checkpoint ID) to the list in a synchronized way
  def addCheckpointInfo(checkpointID: StateStoreCheckpointInfo): Unit = synchronized {
    checkpointInfos = checkpointID :: checkpointInfos
  }

  // Method to read the list of checkpoint IDs in a synchronized way
  def getCheckpointInfos: List[StateStoreCheckpointInfo] = synchronized {
    checkpointInfos
  }
}

case class TestStateStoreWrapper(innerStore: StateStore) extends StateStore {

  // Implement methods from ReadStateStore (parent trait)

  override def id: StateStoreId = innerStore.id
  override def version: Long = innerStore.version

  override def get(
      key: UnsafeRow,
      colFamilyName: String = StateStore.DEFAULT_COL_FAMILY_NAME): UnsafeRow = {
    innerStore.get(key, colFamilyName)
  }

  override def valuesIterator(
      key: UnsafeRow,
      colFamilyName: String = StateStore.DEFAULT_COL_FAMILY_NAME): Iterator[UnsafeRow] = {
    innerStore.valuesIterator(key, colFamilyName)
  }

  override def prefixScan(
      prefixKey: UnsafeRow,
      colFamilyName: String = StateStore.DEFAULT_COL_FAMILY_NAME): Iterator[UnsafeRowPair] = {
    innerStore.prefixScan(prefixKey, colFamilyName)
  }

  override def iterator(
      colFamilyName: String = StateStore.DEFAULT_COL_FAMILY_NAME): Iterator[UnsafeRowPair] = {
    innerStore.iterator(colFamilyName)
  }

  override def abort(): Unit = innerStore.abort()

  // Implement methods from StateStore (current trait)

  override def removeColFamilyIfExists(colFamilyName: String): Boolean = {
    innerStore.removeColFamilyIfExists(colFamilyName)
  }

  override def createColFamilyIfAbsent(
      colFamilyName: String,
      keySchema: StructType,
      valueSchema: StructType,
      keyStateEncoderSpec: KeyStateEncoderSpec,
      useMultipleValuesPerKey: Boolean = false,
      isInternal: Boolean = false): Unit = {
    innerStore.createColFamilyIfAbsent(
      colFamilyName,
      keySchema,
      valueSchema,
      keyStateEncoderSpec,
      useMultipleValuesPerKey,
      isInternal
    )
  }

  override def put(
      key: UnsafeRow,
      value: UnsafeRow,
      colFamilyName: String = StateStore.DEFAULT_COL_FAMILY_NAME): Unit = {
    innerStore.put(key, value, colFamilyName)
  }

  override def remove(
      key: UnsafeRow,
      colFamilyName: String = StateStore.DEFAULT_COL_FAMILY_NAME): Unit = {
    innerStore.remove(key, colFamilyName)
  }

  override def merge(
      key: UnsafeRow,
      value: UnsafeRow,
      colFamilyName: String = StateStore.DEFAULT_COL_FAMILY_NAME): Unit = {
    innerStore.merge(key, value, colFamilyName)
  }

  override def commit(): Long = innerStore.commit()
  override def metrics: StateStoreMetrics = innerStore.metrics
  override def getCheckpointInfo: StateStoreCheckpointInfo = {
    val ret = innerStore.getCheckpointInfo
    TestStateStoreWrapper.addCheckpointInfo(ret)
    ret
  }
  override def hasCommitted: Boolean = innerStore.hasCommitted
}

// Wrapper class implementing StateStoreProvider
class TestStateStoreProviderWrapper extends StateStoreProvider {

  val innerProvider = new RocksDBStateStoreProvider()

  // Now, delegate all methods in the wrapper class to the inner object
  override def init(
      stateStoreId: StateStoreId,
      keySchema: StructType,
      valueSchema: StructType,
      keyStateEncoderSpec: KeyStateEncoderSpec,
      useColumnFamilies: Boolean,
      storeConfs: StateStoreConf,
      hadoopConf: Configuration,
      useMultipleValuesPerKey: Boolean = false): Unit = {
    innerProvider.init(
      stateStoreId,
      keySchema,
      valueSchema,
      keyStateEncoderSpec,
      useColumnFamilies,
      storeConfs,
      hadoopConf,
      useMultipleValuesPerKey
    )
  }

  override def stateStoreId: StateStoreId = innerProvider.stateStoreId

  override def close(): Unit = innerProvider.close()

  override def getStore(version: Long, checkpointUniqueId: Option[String] = None): StateStore = {
    val innerStateStore = innerProvider.getStore(version, checkpointUniqueId)
    TestStateStoreWrapper(innerStateStore)
  }

  override def getReadStore(version: Long, uniqueId: Option[String] = None): ReadStateStore = {
    new WrappedReadStateStore(TestStateStoreWrapper(innerProvider.getReadStore(version, uniqueId)))
  }

  override def doMaintenance(): Unit = innerProvider.doMaintenance()

  override def supportedCustomMetrics: Seq[StateStoreCustomMetric] =
    innerProvider.supportedCustomMetrics
}

class RocksDBStateStoreIntegrationSuite extends StreamTest
  with AlsoTestWithChangelogCheckpointingEnabled {
  import testImplicits._

  testWithColumnFamilies("RocksDBStateStore",
    TestWithBothChangelogCheckpointingEnabledAndDisabled) { colFamiliesEnabled =>
    withTempDir { dir =>
      val input = MemoryStream[Int]
      val conf = Map(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
        classOf[RocksDBStateStoreProvider].getName)

      testStream(input.toDF().groupBy().count(), outputMode = OutputMode.Update)(
        StartStream(checkpointLocation = dir.getAbsolutePath, additionalConfs = conf),
        AddData(input, 1, 2, 3),
        CheckAnswer(3),
        AssertOnQuery { q =>
          // Verify that RocksDBStateStore by verify the state checkpoints are [version].zip
          val storeCheckpointDir = StateStoreId(
            dir.getAbsolutePath + "/state", 0, 0).storeCheckpointLocation()
          val storeCheckpointFile = if (isChangelogCheckpointingEnabled) {
            s"$storeCheckpointDir/1.changelog"
          } else {
            s"$storeCheckpointDir/1.zip"
          }
          new File(storeCheckpointFile).exists()
        }
      )
    }
  }

  testWithColumnFamilies("SPARK-36236: query progress contains only the " +
    s"expected RocksDB store custom metrics",
    TestWithBothChangelogCheckpointingEnabledAndDisabled) { colFamiliesEnabled =>
    // fails if any new custom metrics are added to remind the author of API changes
    import testImplicits._

    withTempDir { dir =>
      withSQLConf(
        (SQLConf.STREAMING_NO_DATA_PROGRESS_EVENT_INTERVAL.key -> "10"),
        (SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName),
        (SQLConf.CHECKPOINT_LOCATION.key -> dir.getCanonicalPath),
        (SQLConf.SHUFFLE_PARTITIONS.key, "1")) {
        val inputData = MemoryStream[Int]

        val query = inputData.toDS().toDF("value")
          .select($"value")
          .groupBy($"value")
          .agg(count("*"))
          .writeStream
          .format("console")
          .outputMode("complete")
          .start()
        try {
          inputData.addData(1, 2)
          inputData.addData(2, 3)
          query.processAllAvailable()

          val progress = query.lastProgress
          assert(progress.stateOperators.length > 0)
          // Should emit new progresses every 10 ms, but we could be facing a slow Jenkins
          eventually(timeout(Span(1, Minute))) {
            val nextProgress = query.lastProgress
            assert(nextProgress != null, "progress is not yet available")
            assert(nextProgress.stateOperators.length > 0,
              "state operators are missing in metrics")
            val stateOperatorMetrics = nextProgress.stateOperators(0)
            assert(stateOperatorMetrics.customMetrics.keySet.asScala === Set(
              "rocksdbGetLatency", "rocksdbCommitCompactLatency", "rocksdbBytesCopied",
              "rocksdbPutLatency", "rocksdbFilesReused",
              "rocksdbFilesCopied", "rocksdbSstFileSize",
              "rocksdbCommitCheckpointLatency", "rocksdbZipFileBytesUncompressed",
              "rocksdbCommitFlushLatency", "rocksdbCommitFileSyncLatencyMs", "rocksdbGetCount",
              "rocksdbPutCount", "rocksdbTotalBytesRead", "rocksdbTotalBytesWritten",
              "rocksdbReadBlockCacheHitCount", "rocksdbReadBlockCacheMissCount",
              "rocksdbTotalBytesReadByCompaction", "rocksdbTotalBytesWrittenByCompaction",
              "rocksdbTotalCompactionLatencyMs", "rocksdbWriterStallLatencyMs",
              "rocksdbTotalBytesReadThroughIterator", "rocksdbTotalBytesWrittenByFlush",
              "rocksdbPinnedBlocksMemoryUsage", "rocksdbNumExternalColumnFamilies",
              "rocksdbNumInternalColumnFamilies"))
          }
        } finally {
          query.stop()
        }
      }
    }
  }

  private def getFormatVersion(query: StreamingQuery): Int = {
    query.asInstanceOf[StreamingQueryWrapper].streamingQuery.lastExecution.sparkSession
      .sessionState.conf.getConf(SQLConf.STATE_STORE_ROCKSDB_FORMAT_VERSION)
  }

  testWithColumnFamilies("SPARK-36519: store RocksDB format version in the checkpoint",
    TestWithBothChangelogCheckpointingEnabledAndDisabled) { colFamiliesEnabled =>
    withSQLConf(
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName) {
      withTempDir { dir =>
        val inputData = MemoryStream[Int]

        def startQuery(): StreamingQuery = {
          inputData.toDS().toDF("value")
            .select($"value")
            .groupBy($"value")
            .agg(count("*"))
            .writeStream
            .format("console")
            .option("checkpointLocation", dir.getCanonicalPath)
            .outputMode("complete")
            .start()
        }

        // The format version should be 5 by default
        var query = startQuery()
        inputData.addData(1, 2)
        query.processAllAvailable()
        assert(getFormatVersion(query) == 5)
        query.stop()

        // Setting the format version manually should not overwrite the value in the checkpoint
        withSQLConf(SQLConf.STATE_STORE_ROCKSDB_FORMAT_VERSION.key -> "4") {
          query = startQuery()
          inputData.addData(1, 2)
          query.processAllAvailable()
          assert(getFormatVersion(query) == 5)
          query.stop()
        }
      }
    }
  }

  testWithColumnFamilies("SPARK-36519: RocksDB format version can be set by the SQL conf",
    TestWithBothChangelogCheckpointingEnabledAndDisabled) { colFamiliesEnabled =>
    withSQLConf(
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
      // Set an unsupported RocksDB format version and the query should fail if it's passed down
      // into RocksDB
      SQLConf.STATE_STORE_ROCKSDB_FORMAT_VERSION.key -> "100") {
      val inputData = MemoryStream[Int]
      val query = inputData.toDS().toDF("value")
        .select($"value")
        .groupBy($"value")
        .agg(count("*"))
        .writeStream
        .format("console")
        .outputMode("complete")
        .start()
      inputData.addData(1, 2)
      val e = intercept[StreamingQueryException](query.processAllAvailable())
      assert(e.getCause.getCause.getMessage.contains("Unsupported BlockBasedTable format_version"))
    }
  }

  testWithColumnFamilies("SPARK-37224: numRowsTotal = 0 when " +
    s"trackTotalNumberOfRows is turned off",
    TestWithBothChangelogCheckpointingEnabledAndDisabled) { colFamiliesEnabled =>
    withTempDir { dir =>
      withSQLConf(
        (SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName),
        (SQLConf.CHECKPOINT_LOCATION.key -> dir.getCanonicalPath),
        (SQLConf.SHUFFLE_PARTITIONS.key, "1"),
        (s"${RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX}.trackTotalNumberOfRows" -> "false")) {
        val inputData = MemoryStream[Int]

        val query = inputData.toDS().toDF("value")
          .select($"value")
          .groupBy($"value")
          .agg(count("*"))
          .writeStream
          .format("console")
          .outputMode("complete")
          .start()
        try {
          inputData.addData(1, 2)
          inputData.addData(2, 3)
          query.processAllAvailable()

          val progress = query.lastProgress
          assert(progress.stateOperators.length > 0)
          eventually(timeout(Span(1, Minute))) {
            val nextProgress = query.lastProgress
            assert(nextProgress != null, "progress is not yet available")
            assert(nextProgress.stateOperators.length > 0, "state operators are missing in metrics")
            val stateOperatorMetrics = nextProgress.stateOperators(0)
            assert(stateOperatorMetrics.numRowsTotal === 0)
          }
        } finally {
          query.stop()
        }
      }
    }
  }

  testWithChangelogCheckpointingEnabled(s"checkpointFormatVersion2") {
    withSQLConf((SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key, "2")) {
      val checkpointDir = Utils.createTempDir().getCanonicalFile
      checkpointDir.delete()

      val dirForPartition0 = new File(checkpointDir.getAbsolutePath, "/state/0/0")
      val inputData = MemoryStream[Int]
      val aggregated =
        inputData
          .toDF()
          .groupBy($"value")
          .agg(count("*"))
          .as[(Int, Long)]

      testStream(aggregated, Update)(
        StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
        AddData(inputData, 3),
        CheckLastBatch((3, 1)),
        AddData(inputData, 3, 2),
        CheckLastBatch((3, 2), (2, 1)),
        StopStream
      )

      // Run the stream with changelog checkpointing enabled.
      testStream(aggregated, Update)(
        StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
        AddData(inputData, 3, 2, 1),
        CheckLastBatch((3, 3), (2, 2), (1, 1)),
        // By default we run in new tuple mode.
        AddData(inputData, 4, 4, 4, 4),
        CheckLastBatch((4, 4)),
        AddData(inputData, 5, 5),
        CheckLastBatch((5, 2))
      )

      // Run the stream with changelog checkpointing disabled.
      testStream(aggregated, Update)(
        StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
        AddData(inputData, 4),
        CheckLastBatch((4, 5))
      )
    }
  }

  testWithChangelogCheckpointingEnabled(s"checkpointFormatVersion2 validate ID") {
    val providerClassName = classOf[TestStateStoreProviderWrapper].getCanonicalName
    withSQLConf(
      (SQLConf.STATE_STORE_PROVIDER_CLASS.key -> providerClassName),
      (SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key -> "2"),
      (SQLConf.SHUFFLE_PARTITIONS.key, "2")) {
      val checkpointDir = Utils.createTempDir().getCanonicalFile
      checkpointDir.delete()

      val dirForPartition0 = new File(checkpointDir.getAbsolutePath, "/state/0/0")
      val inputData = MemoryStream[Int]
      val aggregated =
        inputData
          .toDF()
          .groupBy($"value")
          .agg(count("*"))
          .as[(Int, Long)]

      // Run the stream with changelog checkpointing disabled.
      testStream(aggregated, Update)(
        StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
        AddData(inputData, 3),
        CheckLastBatch((3, 1)),
        AddData(inputData, 3, 2),
        CheckLastBatch((3, 2), (2, 1)),
        StopStream
      )

      // Run the stream with changelog checkpointing enabled.
      testStream(aggregated, Update)(
        StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
        AddData(inputData, 3, 2, 1),
        CheckLastBatch((3, 3), (2, 2), (1, 1)),
        // By default we run in new tuple mode.
        AddData(inputData, 4, 4, 4, 4),
        CheckLastBatch((4, 4)),
        AddData(inputData, 5, 5),
        CheckLastBatch((5, 2))
      )

      // Run the stream with changelog checkpointing disabled.
      testStream(aggregated, Update)(
        StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
        AddData(inputData, 4),
        CheckLastBatch((4, 5))
      )
    }
    val checkpointInfoList = TestStateStoreWrapper.getCheckpointInfos
    assert(checkpointInfoList.size == 12)
    print(checkpointInfoList)
    checkpointInfoList.foreach { l =>
      assert(l.checkpointId.isDefined)
      if (l.batchVersion == 2 || l.batchVersion == 4 || l.batchVersion == 5) {
        assert(l.baseCheckpointId.isDefined)
      }
    }
    assert(checkpointInfoList.count(_.partitionId == 0) == 6)
    assert(checkpointInfoList.count(_.partitionId == 1) == 6)
    for (i <- 1 to 6) {
      assert(checkpointInfoList.count(_.batchVersion == i) == 2)
    }
    for {
      a <- checkpointInfoList
      b <- checkpointInfoList
      if a.partitionId == b.partitionId && a.batchVersion == b.batchVersion + 1
    } {
      // Check if checkpointId of 'a' matches baseCheckpointId of 'b'
      assert(!a.baseCheckpointId.isDefined || b.checkpointId == a.baseCheckpointId)
    }
  }

  testWithChangelogCheckpointingEnabled(
    "Streaming aggregation RocksDB State Store backward compatibility.") {
    val checkpointDir = Utils.createTempDir().getCanonicalFile
    checkpointDir.delete()

    val dirForPartition0 = new File(checkpointDir.getAbsolutePath, "/state/0/0")
    val inputData = MemoryStream[Int]
    val aggregated =
      inputData.toDF()
        .groupBy($"value")
        .agg(count("*"))
        .as[(Int, Long)]

    // Run the stream with changelog checkpointing disabled.
    testStream(aggregated, Update)(
      StartStream(checkpointLocation = checkpointDir.getAbsolutePath,
        additionalConfs = Map(rocksdbChangelogCheckpointingConfKey -> "false")),
      AddData(inputData, 3),
      CheckLastBatch((3, 1)),
      AddData(inputData, 3, 2),
      CheckLastBatch((3, 2), (2, 1)),
      StopStream
    )
    assert(changelogVersionsPresent(dirForPartition0).isEmpty)
    assert(snapshotVersionsPresent(dirForPartition0) == List(1L, 2L))

    // Run the stream with changelog checkpointing enabled.
    testStream(aggregated, Update)(
      StartStream(checkpointLocation = checkpointDir.getAbsolutePath,
        additionalConfs = Map(rocksdbChangelogCheckpointingConfKey -> "true")),
      AddData(inputData, 3, 2, 1),
      CheckLastBatch((3, 3), (2, 2), (1, 1)),
      // By default we run in new tuple mode.
      AddData(inputData, 4, 4, 4, 4),
      CheckLastBatch((4, 4))
    )
    assert(changelogVersionsPresent(dirForPartition0) == List(3L, 4L))

    // Run the stream with changelog checkpointing disabled.
    testStream(aggregated, Update)(
      StartStream(checkpointLocation = checkpointDir.getAbsolutePath,
        additionalConfs = Map(rocksdbChangelogCheckpointingConfKey -> "false")),
      AddData(inputData, 4),
      CheckLastBatch((4, 5))
    )
    assert(changelogVersionsPresent(dirForPartition0) == List(3L, 4L))
    assert(snapshotVersionsPresent(dirForPartition0).contains(5L))
  }
}
