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
import org.apache.spark.sql.functions.{count, timestamp_seconds, window}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{OutputMode, StreamTest, Trigger}
import org.apache.spark.sql.streaming.util.StreamManualClock

class ProgressReporterSuite extends StreamTest {
  import testImplicits._

  test("no-data batch resets numRowsRemoved to zero" +
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
        // Idle trigger — finishNoExecutionTrigger calls
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
          }
        },
        StopStream
      )
    }
  }
}
