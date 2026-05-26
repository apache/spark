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

package org.apache.spark.sql.execution.streaming

import org.scalatest.concurrent.PatienceConfiguration.Timeout

import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.functions.{count, timestamp_seconds, window}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{OutputMode, StateOperatorProgress, StreamTest, Trigger}
import org.apache.spark.sql.streaming.util.StreamManualClock

class ProgressReporterSuite extends StreamTest {
  import testImplicits._

  test("no-data batch resets all per-batch StateOperatorProgress fields to zero" +
      " via resetExecStatsForNoExecution") {
    val clock = new StreamManualClock
    val input = MemoryStream[Int]
    val agg = input.toDF()
      .select(timestamp_seconds($"value") as "ts", $"value")
      .withWatermark("ts", "10 seconds")
      .groupBy(window($"ts", "10 seconds"))
      .agg(count("*") as "cnt")
      .select($"window".getField("start").cast("long"), $"cnt")

    // noDataProgressEventInterval=0 ensures idle-trigger progress
    // is always recorded regardless of clock gap.
    withSQLConf(
        SQLConf.STREAMING_POLLING_DELAY.key -> "0",
        SQLConf.STREAMING_NO_DATA_PROGRESS_EVENT_INTERVAL.key -> "0") {
      testStream(agg, outputMode = OutputMode.Update)(
        StartStream(
          Trigger.ProcessingTime("1 second"),
          triggerClock = clock),
        // Batch 0: [1,2,3] -> window [0s,10s) cnt=3.
        // Watermark after: max(1,2,3)-10 < 0 -> stays 0.
        AddData(input, 1, 2, 3),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer((0L, 3L)),
        // Batch 1: [21] -> window [20s,30s) cnt=1.
        // Watermark used: 0 (no eviction yet).
        // Watermark after: 21-10 = 11.
        AddData(input, 21),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer((20L, 1L)),
        // Batch 2: no-data cleanup with watermark=11.
        // Evicts window [0s,10s) (end 10 <= 11).
        AdvanceManualClock(1 * 1000),
        Execute("wait for cleanup batch") { q =>
          eventually(Timeout(streamingTimeout)) {
            assert(q.lastProgress.batchId >= 2)
          }
        },
        Execute("verify eviction") { q =>
          val removed = q.recentProgress
            .filter(_.stateOperators.nonEmpty)
            .exists(_.stateOperators.head.numRowsRemoved > 0)
          assert(removed, "Expected numRowsRemoved > 0")
        },
        // Idle trigger: finishNoExecutionTrigger calls
        // resetExecStatsForNoExecution which must zero out
        // per-batch metrics.
        AdvanceManualClock(1 * 1000),
        Execute("idle trigger must reset per-batch metrics") { q =>
          eventually(Timeout(streamingTimeout)) {
            val progress = q.recentProgress.filter(_.stateOperators.nonEmpty)
            val lastEviction = progress.lastIndexWhere { p =>
              p.durationMs.containsKey("addBatch") &&
                p.stateOperators.head.numRowsRemoved > 0
            }
            assert(lastEviction >= 0, "no eviction batch found")
            val idleIdx = progress.indexWhere(
              !_.durationMs.containsKey("addBatch"), lastEviction + 1)
            assert(idleIdx > lastEviction,
              "no idle trigger found after eviction batch")
            val so = progress(idleIdx).stateOperators.head
            assert(so.numRowsRemoved === 0, s"numRowsRemoved=${so.numRowsRemoved}")
            assert(so.numRowsUpdated === 0, s"numRowsUpdated=${so.numRowsUpdated}")
            assert(so.numRowsDroppedByWatermark === 0,
              s"numRowsDroppedByWatermark=${so.numRowsDroppedByWatermark}")
            assert(so.allUpdatesTimeMs === 0,
              s"allUpdatesTimeMs=${so.allUpdatesTimeMs}")
            assert(so.allRemovalsTimeMs === 0,
              s"allRemovalsTimeMs=${so.allRemovalsTimeMs}")
            assert(so.commitTimeMs === 0,
              s"commitTimeMs=${so.commitTimeMs}")
          }
        },
        StopStream
      )
    }
  }

  test("SPARK-56537: no-data batch resets per-batch customMetrics but" +
      " preserves snapshot customMetrics (RocksDB)") {
    val clock = new StreamManualClock
    val input = MemoryStream[Int]
    val agg = input.toDF()
      .select(timestamp_seconds($"value") as "ts", $"value")
      .withWatermark("ts", "10 seconds")
      .groupBy(window($"ts", "10 seconds"))
      .agg(count("*") as "cnt")
      .select($"window".getField("start").cast("long"), $"cnt")

    withSQLConf(
        SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
          classOf[RocksDBStateStoreProvider].getName,
        SQLConf.STREAMING_POLLING_DELAY.key -> "0",
        SQLConf.STREAMING_NO_DATA_PROGRESS_EVENT_INTERVAL.key -> "0") {
      testStream(agg, outputMode = OutputMode.Update)(
        StartStream(
          Trigger.ProcessingTime("1 second"),
          triggerClock = clock),
        // Batch 0: real data, populates customMetrics with non-zero per-batch values.
        AddData(input, 1, 2, 3),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer((0L, 3L)),
        // Idle trigger.
        AdvanceManualClock(1 * 1000),
        Execute("verify customMetrics behavior on idle trigger") { q =>
          eventually(Timeout(streamingTimeout)) {
            val progress = q.recentProgress.filter(_.stateOperators.nonEmpty)
            val lastDataIdx = progress.lastIndexWhere { p =>
              p.durationMs.containsKey("addBatch")
            }
            assert(lastDataIdx >= 0, "no data batch found")
            val idleIdx = progress.indexWhere(
              !_.durationMs.containsKey("addBatch"), lastDataIdx + 1)
            assert(idleIdx > lastDataIdx,
              "no idle trigger found after data batch")

            val dataCm = progress(lastDataIdx).stateOperators.head.customMetrics
            val idleCm = progress(idleIdx).stateOperators.head.customMetrics

            // Per-batch RocksDB metrics: zeroed on idle. The metric must be present
            // in the map (we keep keys consistent across data and idle progress).
            Seq("rocksdbCommitFlushLatency",
                "rocksdbPutCount",
                "rocksdbTotalBytesWritten").foreach { k =>
              assert(idleCm.containsKey(k), s"$k missing on idle")
              assert(idleCm.get(k) === 0L,
                s"per-batch metric $k expected 0 on idle, got ${idleCm.get(k)}")
            }

            // Snapshot RocksDB metrics: value unchanged across idle trigger.
            Seq("rocksdbPinnedBlocksMemoryUsage",
                "rocksdbNumInternalColFamiliesKeys",
                "rocksdbNumExternalColumnFamilies",
                "rocksdbNumInternalColumnFamilies",
                "rocksdbSstFileSize").foreach { k =>
              assert(idleCm.containsKey(k), s"$k missing on idle")
              assert(idleCm.get(k) === dataCm.get(k),
                s"snapshot metric $k changed across idle trigger: " +
                  s"data=${dataCm.get(k)} idle=${idleCm.get(k)}")
            }
          }
        },
        StopStream
      )
    }
  }

  test("SPARK-56537: copyForNoExecution zeroes per-batch fields and preserves snapshot fields") {
    val customMetrics = new java.util.HashMap[String, java.lang.Long]()
    customMetrics.put("perBatchTimer", 100L)
    customMetrics.put("perBatchCounter", 50L)
    customMetrics.put("snapshotSize", 999L)
    val orig = new StateOperatorProgress(
      operatorName = "op",
      numRowsTotal = 50L,
      numRowsUpdated = 10L,
      allUpdatesTimeMs = 7L,
      numRowsRemoved = 3L,
      allRemovalsTimeMs = 5L,
      commitTimeMs = 11L,
      memoryUsedBytes = 2048L,
      numRowsDroppedByWatermark = 2L,
      numShufflePartitions = 4L,
      numStateStoreInstances = 4L,
      customMetrics = customMetrics,
      snapshotCustomMetricNames = Set("snapshotSize"))

    val out = orig.copyForNoExecution()

    // Per-batch fields are zeroed.
    assert(out.numRowsUpdated === 0L)
    assert(out.allUpdatesTimeMs === 0L)
    assert(out.numRowsRemoved === 0L)
    assert(out.allRemovalsTimeMs === 0L)
    assert(out.commitTimeMs === 0L)
    assert(out.numRowsDroppedByWatermark === 0L)

    // Snapshot fields are preserved.
    assert(out.operatorName === "op")
    assert(out.numRowsTotal === 50L)
    assert(out.memoryUsedBytes === 2048L)
    assert(out.numShufflePartitions === 4L)
    assert(out.numStateStoreInstances === 4L)

    // customMetrics: per-batch zeroed, snapshot preserved.
    assert(out.customMetrics.get("perBatchTimer") === 0L)
    assert(out.customMetrics.get("perBatchCounter") === 0L)
    assert(out.customMetrics.get("snapshotSize") === 999L)

    // Original is not mutated.
    assert(orig.numRowsUpdated === 10L)
    assert(orig.customMetrics.get("perBatchTimer") === 100L)

    // snapshotCustomMetricNames is preserved so subsequent copy() round-trips work.
    assert(out.snapshotCustomMetricNames === Set("snapshotSize"))
  }
}
