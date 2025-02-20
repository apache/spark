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

import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.{MapHasAsScala, SetHasAsScala}

import org.scalatest.time.{Minute, Span}

import org.apache.spark.sql.execution.streaming.{MemoryStream, StreamingQueryWrapper}
import org.apache.spark.sql.functions.{count, expr}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.streaming.OutputMode.Update
import org.apache.spark.util.Utils

// SkipMaintenanceOnCertainPartitionsProvider is a test-only provider that skips running
// maintenance for partitions 0 and 1 (these are arbitrary choices). This is used to test
// snapshot upload lag can be observed through StreamingQueryProgress metrics.
class SkipMaintenanceOnCertainPartitionsProvider extends RocksDBStateStoreProvider {
  override def doMaintenance(): Unit = {
    if (stateStoreId.partitionId == 0 || stateStoreId.partitionId == 1) {
      return
    }
    super.doMaintenance()
  }
}

class RocksDBStateStoreIntegrationSuite extends StreamTest
  with AlsoTestWithRocksDBFeatures {
  import testImplicits._

  private val SNAPSHOT_LAG_METRIC_PREFIX = "SnapshotLastUploaded.partition_"

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
        (SQLConf.STATE_STORE_INSTANCE_METRICS_REPORT_LIMIT.key -> "0"),
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
              "rocksdbNumExternalColumnFamilies", "rocksdbNumInternalColumnFamilies"))
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

  private def snapshotLagMetricName(
      partitionId: Long,
      storeName: String = StateStoreId.DEFAULT_STORE_NAME): String = {
    s"$SNAPSHOT_LAG_METRIC_PREFIX${partitionId}_$storeName"
  }

  testWithChangelogCheckpointingEnabled(
    "SPARK-51097: Verify snapshot lag metrics are updated correctly with RocksDBStateStoreProvider"
  ) {
    withSQLConf(
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
      SQLConf.STREAMING_MAINTENANCE_INTERVAL.key -> "100",
      SQLConf.STREAMING_NO_DATA_PROGRESS_EVENT_INTERVAL.key -> "10",
      SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.key -> "1",
      SQLConf.STATE_STORE_INSTANCE_METRICS_REPORT_LIMIT.key -> "3"
    ) {
      withTempDir { checkpointDir =>
        val inputData = MemoryStream[String]
        val result = inputData.toDS().dropDuplicates()

        testStream(result, outputMode = OutputMode.Update)(
          StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
          AddData(inputData, "a"),
          ProcessAllAvailable(),
          AddData(inputData, "b"),
          ProcessAllAvailable(),
          CheckNewAnswer("a", "b"),
          Execute { q =>
            // Make sure only smallest K active metrics are published
            eventually(timeout(10.seconds)) {
              val instanceMetrics = q.lastProgress
                .stateOperators(0)
                .customMetrics
                .asScala
                .view
                .filterKeys(_.startsWith(SNAPSHOT_LAG_METRIC_PREFIX))
              // Determined by STATE_STORE_INSTANCE_METRICS_REPORT_LIMIT
              assert(
                instanceMetrics.size == q.sparkSession.conf
                  .get(SQLConf.STATE_STORE_INSTANCE_METRICS_REPORT_LIMIT)
              )
              assert(instanceMetrics.forall(_._2 == 1))
            }
          },
          StopStream
        )
      }
    }
  }

  testWithChangelogCheckpointingEnabled(
    "SPARK-51097: Verify snapshot lag metrics are updated correctly with " +
    "SkipMaintenanceOnCertainPartitionsProvider"
  ) {
    withSQLConf(
      SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
        classOf[SkipMaintenanceOnCertainPartitionsProvider].getName,
      SQLConf.STREAMING_MAINTENANCE_INTERVAL.key -> "100",
      SQLConf.STREAMING_NO_DATA_PROGRESS_EVENT_INTERVAL.key -> "10",
      SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.key -> "1",
      SQLConf.STATE_STORE_INSTANCE_METRICS_REPORT_LIMIT.key -> "3"
    ) {
      withTempDir { checkpointDir =>
        val inputData = MemoryStream[String]
        val result = inputData.toDS().dropDuplicates()

        testStream(result, outputMode = OutputMode.Update)(
          StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
          AddData(inputData, "a"),
          ProcessAllAvailable(),
          AddData(inputData, "b"),
          ProcessAllAvailable(),
          CheckNewAnswer("a", "b"),
          Execute { q =>
            // Partitions getting skipped (id 0 and 1) do not have an uploaded version, leaving
            // those instance metrics as -1.
            eventually(timeout(10.seconds)) {
              assert(
                q.lastProgress
                  .stateOperators(0)
                  .customMetrics
                  .get(snapshotLagMetricName(0)) === -1
              )
              assert(
                q.lastProgress
                  .stateOperators(0)
                  .customMetrics
                  .get(snapshotLagMetricName(1)) === -1
              )
              // Make sure only smallest K active metrics are published
              val instanceMetrics = q.lastProgress
                .stateOperators(0)
                .customMetrics
                .asScala
                .view
                .filterKeys(_.startsWith(SNAPSHOT_LAG_METRIC_PREFIX))
              // Determined by STATE_STORE_INSTANCE_METRICS_REPORT_LIMIT
              assert(
                instanceMetrics.size == q.sparkSession.conf
                  .get(SQLConf.STATE_STORE_INSTANCE_METRICS_REPORT_LIMIT)
              )
              // Two metrics published are -1, the remainder should all be 1 as they
              // uploaded properly.
              assert(
                instanceMetrics.count(_._2 == 1) == q.sparkSession.conf
                  .get(SQLConf.STATE_STORE_INSTANCE_METRICS_REPORT_LIMIT) - 2
              )
            }
          },
          StopStream
        )
      }
    }
  }

  testWithChangelogCheckpointingEnabled(
    "SPARK-51097: Verify snapshot lag metrics are updated correctly for join queries with " +
    "RocksDBStateStoreProvider"
  ) {
    withSQLConf(
      SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
        classOf[RocksDBStateStoreProvider].getName,
      SQLConf.STREAMING_MAINTENANCE_INTERVAL.key -> "100",
      SQLConf.STREAMING_NO_DATA_PROGRESS_EVENT_INTERVAL.key -> "10",
      SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.key -> "1",
      SQLConf.STATE_STORE_INSTANCE_METRICS_REPORT_LIMIT.key -> "10"
    ) {
      withTempDir { checkpointDir =>
        val input1 = MemoryStream[Int]
        val input2 = MemoryStream[Int]

        val df1 = input1.toDF().select($"value" as "leftKey", ($"value" * 2) as "leftValue")
        val df2 = input2
          .toDF()
          .select($"value" as "rightKey", ($"value" * 3) as "rightValue")
        val joined = df1.join(df2, expr("leftKey = rightKey"))

        testStream(joined)(
          StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
          AddData(input1, 1, 5),
          ProcessAllAvailable(),
          AddData(input2, 1, 5, 10),
          ProcessAllAvailable(),
          CheckNewAnswer((1, 2, 1, 3), (5, 10, 5, 15)),
          Execute { q =>
            eventually(timeout(10.seconds)) {
              // Make sure only smallest K active metrics are published.
              // There are 5 * 4 = 20 metrics in total because of join, but only 10 are published.
              val instanceMetrics = q.lastProgress
                .stateOperators(0)
                .customMetrics
                .asScala
                .view
                .filterKeys(_.startsWith(SNAPSHOT_LAG_METRIC_PREFIX))
              // Determined by STATE_STORE_INSTANCE_METRICS_REPORT_LIMIT
              assert(
                instanceMetrics.size == q.sparkSession.conf
                  .get(SQLConf.STATE_STORE_INSTANCE_METRICS_REPORT_LIMIT)
              )
              // All state store instances should have uploaded a version
              assert(instanceMetrics.forall(_._2 == 1))
            }
          },
          StopStream
        )
      }
    }
  }

  testWithChangelogCheckpointingEnabled(
    "SPARK-51097: Verify snapshot lag metrics are updated correctly for join queries with " +
    "SkipMaintenanceOnCertainPartitionsProvider"
  ) {
    withSQLConf(
      SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
        classOf[SkipMaintenanceOnCertainPartitionsProvider].getName,
      SQLConf.STREAMING_MAINTENANCE_INTERVAL.key -> "100",
      SQLConf.STREAMING_NO_DATA_PROGRESS_EVENT_INTERVAL.key -> "10",
      SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.key -> "1",
      SQLConf.STATE_STORE_INSTANCE_METRICS_REPORT_LIMIT.key -> "10"
    ) {
      withTempDir { checkpointDir =>
        val input1 = MemoryStream[Int]
        val input2 = MemoryStream[Int]

        val df1 = input1.toDF().select($"value" as "leftKey", ($"value" * 2) as "leftValue")
        val df2 = input2
          .toDF()
          .select($"value" as "rightKey", ($"value" * 3) as "rightValue")
        val joined = df1.join(df2, expr("leftKey = rightKey"))

        testStream(joined)(
          StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
          AddData(input1, 1, 5),
          ProcessAllAvailable(),
          AddData(input2, 1, 5, 10),
          ProcessAllAvailable(),
          CheckNewAnswer((1, 2, 1, 3), (5, 10, 5, 15)),
          Execute { q =>
            eventually(timeout(10.seconds)) {
              // Make sure only smallest K active metrics are published.
              // There are 5 * 4 = 20 metrics in total because of join, but only 10 are published.
              val allInstanceMetrics = q.lastProgress
                .stateOperators(0)
                .customMetrics
                .asScala
                .view
                .filterKeys(_.startsWith(SNAPSHOT_LAG_METRIC_PREFIX))
              val badInstanceMetrics = allInstanceMetrics.filterKeys(
                k =>
                  k.startsWith(snapshotLagMetricName(0, "")) ||
                  k.startsWith(snapshotLagMetricName(1, ""))
              )
              // Determined by STATE_STORE_INSTANCE_METRICS_REPORT_LIMIT
              assert(
                allInstanceMetrics.size == q.sparkSession.conf
                  .get(SQLConf.STATE_STORE_INSTANCE_METRICS_REPORT_LIMIT)
              )
              // Two ids are blocked, each with four state stores
              assert(badInstanceMetrics.count(_._2 == -1) == 2 * 4)
              // The rest should have uploaded a version
              assert(
                allInstanceMetrics.count(_._2 == 1) == q.sparkSession.conf
                  .get(SQLConf.STATE_STORE_INSTANCE_METRICS_REPORT_LIMIT) - 2 * 4
              )
            }
          },
          StopStream
        )
      }
    }
  }
}
