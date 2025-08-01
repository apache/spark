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

import org.scalatest.time.{Minute, Span}

import org.apache.spark.sql.execution.streaming.{MemoryStream, StreamingQueryWrapper}
import org.apache.spark.sql.functions.{count, max}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.streaming.OutputMode.Update
import org.apache.spark.util.Utils

class RocksDBStateStoreIntegrationSuite extends StreamTest
  with AlsoTestWithRocksDBFeatures {
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
              "rocksdbPinnedBlocksMemoryUsage", "rocksdbNumInternalColFamiliesKeys",
              "rocksdbNumExternalColumnFamilies", "rocksdbNumInternalColumnFamilies",
              "SnapshotLastUploaded.partition_0_default", "rocksdbChangeLogWriterCommitLatencyMs",
              "rocksdbSaveZipFilesLatencyMs", "rocksdbLoadFromSnapshotLatencyMs",
              "rocksdbLoadLatencyMs", "rocksdbReplayChangeLogLatencyMs",
              "rocksdbNumReplayChangelogFiles"))
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

  testWithColumnFamilies("SPARK-51823: unload state stores on commit",
    TestWithBothChangelogCheckpointingEnabledAndDisabled) { colFamiliesEnabled =>
    withTempDir { dir =>
      withSQLConf(
        (SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName),
        (SQLConf.CHECKPOINT_LOCATION.key -> dir.getCanonicalPath),
        (SQLConf.SHUFFLE_PARTITIONS.key -> "1"),
        (SQLConf.STATE_STORE_UNLOAD_ON_COMMIT.key -> "true")) {
        // Make sure we start with a fresh without any stale state store entries
        Utils.clearLocalRootDirs()

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

          // StateStore should be unloaded, so its tmp dir shouldn't exist
          var tmpFiles = new File(Utils.getLocalDir(sparkConf)).listFiles()
          assert(tmpFiles.filter(_.getName().startsWith("StateStore")).isEmpty)

          inputData.addData(3, 4)
          inputData.addData(4, 5)
          query.processAllAvailable()

          tmpFiles = new File(Utils.getLocalDir(sparkConf)).listFiles()
          assert(tmpFiles.filter(_.getName().startsWith("StateStore")).isEmpty)
        } finally {
          query.stop()
        }
      }
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

  // Test with both bounded memory enabled and disabled
  Seq(true, false).foreach { boundedMemoryEnabled =>
    test(s"RocksDB memory tracking integration with UnifiedMemoryManager" +
      s" with boundedMemory=$boundedMemoryEnabled") {
      withTempDir { dir =>
        withSQLConf(
          (SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName),
          (SQLConf.CHECKPOINT_LOCATION.key -> dir.getCanonicalPath),
          (SQLConf.SHUFFLE_PARTITIONS.key -> "5"),
          (SQLConf.STREAMING_MAINTENANCE_INTERVAL.key -> (5 * 60 * 1000).toString),
          ("spark.memory.unmanagedMemoryPollingInterval" -> "100ms"),
          ("spark.sql.streaming.stateStore.rocksdb.boundedMemoryUsage" ->
            boundedMemoryEnabled.toString)) {

          import org.apache.spark.memory.UnifiedMemoryManager
          import org.apache.spark.sql.streaming.Trigger

          // Use rate stream to ensure continuous state operations that trigger memory updates
          val query = spark.readStream
            .format("rate")
            .option("rowsPerSecond", "10") // Continuous but not overwhelming
            .load()
            .selectExpr("value % 100 as key", "value")
            .groupBy("key")
            .agg(count("*").as("count"), max("value").as("max_value"))
            .writeStream
            .format("console")
            .outputMode("update")
            .trigger(Trigger.ProcessingTime(200)) // Regular triggers to ensure state operations
            .start()

          try {
            // Let the stream run to establish RocksDB instances and generate state operations
            Thread.sleep(2000) // 2 seconds should be enough for several processing cycles

            // Now check for memory tracking - the continuous stream should trigger memory updates
            var rocksDBMemory = 0L
            var attempts = 0
            val maxAttempts = 15 // 15 attempts with 1-second intervals = 15 seconds max

            while (rocksDBMemory <= 0L && attempts < maxAttempts) {
              Thread.sleep(1000) // Wait between checks to allow memory updates
              rocksDBMemory = UnifiedMemoryManager.getMemoryByComponentType("RocksDB")
              attempts += 1

              if (rocksDBMemory > 0L) {
                logInfo(s"RocksDB memory detected: $rocksDBMemory bytes " +
                  s"after $attempts attempts with boundedMemory=$boundedMemoryEnabled")
              }
            }

            // Verify memory tracking remains stable during continued operation
            Thread.sleep(2000) // Let stream continue running

            val finalMemory = UnifiedMemoryManager.getMemoryByComponentType("RocksDB")

            // Memory should still be tracked (allow for some fluctuation)
            assert(finalMemory > 0L,
              s"RocksDB memory tracking should remain active during stream processing: " +
                s"got $finalMemory bytes (initial: $rocksDBMemory) " +
                s"with boundedMemory=$boundedMemoryEnabled")

            logInfo(s"RocksDB memory tracking test completed successfully: " +
              s"initial=$rocksDBMemory bytes, final=$finalMemory bytes " +
              s"with boundedMemory=$boundedMemoryEnabled")

          } finally {
            query.stop()
            // Clean up unmanaged memory users
            UnifiedMemoryManager.clearUnmanagedMemoryUsers()
          }
        }
      }
    }
  }
}
