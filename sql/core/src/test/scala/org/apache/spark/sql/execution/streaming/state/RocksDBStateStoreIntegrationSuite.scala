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

import scala.collection.JavaConverters

import org.scalatest.time.{Minute, Span}

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming._

class RocksDBStateStoreIntegrationSuite extends StreamTest {
  import testImplicits._

  test("RocksDBStateStore") {
    withTempDir { dir =>
      val input = MemoryStream[Int]
      val conf = Map(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
        classOf[RocksDBStateStoreProvider].getName)

      testStream(input.toDF.groupBy().count(), outputMode = OutputMode.Update)(
        StartStream(checkpointLocation = dir.getAbsolutePath, additionalConfs = conf),
        AddData(input, 1, 2, 3),
        CheckAnswer(3),
        AssertOnQuery { q =>
          // Verify that RocksDBStateStore by verify the state checkpoints are [version].zip
          val storeCheckpointDir = StateStoreId(
            dir.getAbsolutePath + "/state", 0, 0).storeCheckpointLocation()
          val storeCheckpointFile = storeCheckpointDir + "/1.zip"
          new File(storeCheckpointFile).exists()
        }
      )
    }
  }

  test("SPARK-36236: query progress contains only the expected RocksDB store custom metrics") {
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
          .select('value)
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
            assert(nextProgress.stateOperators.length > 0, "state operators are missing in metrics")
            val stateOperatorMetrics = nextProgress.stateOperators(0)
            assert(JavaConverters.asScalaSet(stateOperatorMetrics.customMetrics.keySet) === Set(
              "rocksdbGetLatency", "rocksdbCommitCompactLatency", "rocksdbBytesCopied",
              "rocksdbPutLatency", "rocksdbCommitPauseLatency", "rocksdbFilesReused",
              "rocksdbCommitWriteBatchLatency", "rocksdbFilesCopied", "rocksdbSstFileSize",
              "rocksdbCommitCheckpointLatency", "rocksdbZipFileBytesUncompressed",
              "rocksdbCommitFlushLatency", "rocksdbCommitFileSyncLatencyMs", "rocksdbGetCount",
              "rocksdbPutCount", "rocksdbTotalBytesRead", "rocksdbTotalBytesWritten",
              "rocksdbReadBlockCacheHitCount", "rocksdbReadBlockCacheMissCount",
              "rocksdbTotalBytesReadByCompaction", "rocksdbTotalBytesWrittenByCompaction",
              "rocksdbTotalCompactionLatencyMs", "rocksdbWriterStallLatencyMs",
              "rocksdbTotalBytesReadThroughIterator"))
          }
        } finally {
          query.stop()
        }
      }
    }
  }
}

