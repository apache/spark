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

package org.apache.spark.sql.streaming

import org.apache.spark.sql.execution.streaming.StatefulStreamlineAggregateExec
import org.apache.spark.sql.execution.streaming.operators.stateful.StreamingAggregationStateManager
import org.apache.spark.sql.execution.streaming.sources.{ContinuousMemorySink, LowLatencyMemoryStream}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf

/**
 * Tests for streaming aggregation in Real-Time Mode, which is planned as
 * [[StatefulStreamlineAggregateExec]] rather than the micro-batch aggregation operators.
 */
class StreamlineStreamingAggregationRealTimeSuite extends StreamRealTimeModeSuiteBase
  with StateStoreMetricsTest {

  import testImplicits._

  def stateFormatVersions: Seq[Int] = StreamingAggregationStateManager.supportedVersions

  def executeFuncWithStateVersionSQLConf(
      stateVersion: Int,
      confPairs: Seq[(String, String)],
      func: => Any): Unit = {
    withSQLConf(confPairs ++
      Seq(SQLConf.STREAMING_AGGREGATION_STATE_FORMAT_VERSION.key -> stateVersion.toString): _*) {
      func
    }
  }

  def testWithAllStateVersions(name: String, confPairs: (String, String)*)
                              (func: => Any): Unit = {
    for (version <- stateFormatVersions) {
      test(s"$name - state format version $version") {
        executeFuncWithStateVersionSQLConf(version, confPairs, func)
      }
    }
  }

  testWithAllStateVersions("aggregation runs in Real-Time Mode") {
    val inputData = LowLatencyMemoryStream[(String, Int)](2)

    val agg = inputData.toDF().select($"_1".as("key"), $"_2".as("value"))
      .groupBy($"key")
      .agg(sum("value").as("total"))

    testStream(agg, OutputMode.Update, sink = new ContinuousMemorySink())(
      StartStream(),
      AddData(inputData, ("a", 1), ("b", 2), ("a", 3)),
      // Update mode emits an intermediate result per input row, so "a" is seen twice: once with
      // its own value and once merged with the earlier one. Micro-batch aggregation would instead
      // emit only the final value per key per batch.
      CheckAnswerWithTimeout(60000, ("a", 1L), ("b", 2L), ("a", 4L)),
      Execute { q =>
        val aggregates = q.lastExecution.executedPlan.collect {
          case a: StatefulStreamlineAggregateExec => a
        }
        assert(aggregates.size == 1,
          s"expected the streamline aggregate operator, got:\n${q.lastExecution.executedPlan}")
      },
      StopStream
    )
  }

  // The eviction counts below are per state store, so the state has to live in a single partition
  // for them to be predictable. Testing incremental cleanup without pinning the partition count
  // would need a partition-aware data generator.
  testWithAllStateVersions("update mode aggregation with incremental cleanup evicts records " +
    "not removed during incremental cleanup",
    SQLConf.STREAMING_STATE_INCREMENTAL_CLEANUP_FACTOR.key -> "2",
    SQLConf.SHUFFLE_PARTITIONS.key -> "1"
  ) {
    val inputData = LowLatencyMemoryStream[Int]
    val aggWithWatermark = inputData.toDF()
      .withColumn("eventTime", timestamp_seconds($"value"))
      .withWatermark("eventTime", "50 seconds")
      .groupBy(window($"eventTime", "10 seconds") as Symbol("window"))
      .agg(count("*") as Symbol("count"))
      .select($"window".getField("end").cast("long").as[Long], $"count".as[Long])

    // With incremental eviction, we evict incrementalCleanupFactor * numInputRows from the
    // state store.
    testStream(aggWithWatermark, OutputMode.Update, sink = new ContinuousMemorySink())(
      StartStream(),
      AddData(inputData, 9, 19, 29, 39, 49),
      WaitUntilBatchProcessed(0),
      // Batch 0: watermark is 0.
      CheckAnswerWithTimeout(60000, (10, 1), (20, 1), (30, 1), (40, 1), (50, 1)),

      // Batch 1: watermark starts at 0 and moves past the [40, 50) window end.
      AddData(inputData, 101),
      // Wait until batch 1 and the no data batch complete
      WaitUntilBatchProcessed(2),
      CheckAnswerWithTimeout(60000, (110, 1), (10, 1), (20, 1), (30, 1), (40, 1), (50, 1)),

      Execute { q =>
        val batch1Metrics = q.recentProgress.filter(_.batchId == 1).head
        assert(
          batch1Metrics.stateOperators.head.customMetrics.get("numRowsIncrementallyRemoved") == 0)
        assert(batch1Metrics.stateOperators.head.numRowsRemoved === 0)
      },

      AddData(inputData, 100),
      WaitUntilBatchProcessed(3),
      CheckAnswerWithTimeout(60000,
        (110, 1), (110, 2), (10, 1), (20, 1), (30, 1), (40, 1), (50, 1)),

      Execute { q =>
        val batch3Metrics = q.recentProgress.filter(_.batchId == 3).head
        // 1 record is in the batch, and the incremental cleanup factor is 2. Thus, 2 rows should be
        // incrementally cleaned up, but we should still remove 5 total.
        assert(
          batch3Metrics.stateOperators.head.customMetrics.get("numRowsIncrementallyRemoved") == 2)
        assert(batch3Metrics.stateOperators.head.numRowsRemoved === 5)
      }
    )
  }
}
