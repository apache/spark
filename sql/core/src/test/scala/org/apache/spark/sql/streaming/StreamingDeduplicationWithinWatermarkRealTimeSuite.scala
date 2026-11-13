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

import org.apache.spark.SparkException
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.execution.streaming.operators.stateful.StreamingDeduplicateWithinWatermarkExec
import org.apache.spark.sql.execution.streaming.sources.{ContinuousMemorySink, LowLatencyMemoryStream}
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.functions.{timestamp_seconds, udf}
import org.apache.spark.sql.internal.SQLConf

object StreamingDeduplicationWithinWatermarkRealTimeSuite {
  @volatile var failAfterCleanup = false
  @volatile var lateRowRead = false
}

class StreamingDeduplicationWithinWatermarkRealTimeSuite
  extends StreamRealTimeModeManualClockSuiteBase {

  import testImplicits._

  private val factorKey = SQLConf.STREAMING_STATE_INCREMENTAL_CLEANUP_FACTOR.key

  private def deduplicate(input: Dataset[(String, Int)]): DataFrame = {
    input.toDF()
      .withColumn("eventTime", timestamp_seconds($"_2"))
      .withWatermark("eventTime", "10 seconds")
      .dropDuplicatesWithinWatermark("_1")
      .select($"_1")
  }

  private def checkRemovalMetrics(total: Long, removed: Long, incremental: Long): AssertOnQuery = {
    Execute { q =>
      val operator = q.lastProgress.stateOperators.head
      assert(operator.numRowsTotal === total)
      assert(operator.numRowsRemoved === removed)
      assert(operator.customMetrics.get("numRowsIncrementallyRemoved").longValue() === incremental)
      assert(operator.customMetrics.get("numRowsReadDuringEviction").longValue() >= removed)
    }
  }

  private def checkExecutionMode(realTimeMode: Boolean): AssertOnQuery = {
    Execute { q =>
      val operators = q.lastExecution.executedPlan.collect {
        case d: StreamingDeduplicateWithinWatermarkExec => d
      }
      assert(operators.nonEmpty && operators.forall(_.isRealTimeMode == realTimeMode))
    }
  }

  gridTest("incremental cleanup drains remaining expired state with default and explicit factors")(
      Seq(None, Some(1L), Some(2L), Some(10L))) { factor =>
    withSQLConf((Seq(SQLConf.SHUFFLE_PARTITIONS.key -> "1") ++
        factor.map(value => factorKey -> value.toString)): _*) {
      val inputData = LowLatencyMemoryStream.singlePartition[(String, Int)]
      val result = deduplicate(inputData.toDS())

      testStream(result, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
        AddData(inputData, ("a", 1), ("b", 10), ("c", 21), ("d", 30), ("e", 42)),
        StartStream(),
        CheckAnswerWithTimeout(60000, "a", "b", "c", "d", "e"),
        advanceRealTimeClock,
        WaitUntilBatchProcessed(0),

        AddData(inputData, ("g", 51)),
        CheckAnswerWithTimeout(60000, "a", "b", "c", "d", "e", "g"),
        advanceRealTimeClock,
        WaitUntilBatchProcessed(1),
        checkRemovalMetrics(total = 6, removed = 0, incremental = 0),

        // Three entries expire at the late-events watermark of 32. A single input removes up
        // to the factor incrementally; completion drains the rest of the same iterator.
        AddData(inputData, ("h", 51)),
        CheckAnswerWithTimeout(60000, "a", "b", "c", "d", "e", "g", "h"),
        advanceRealTimeClock,
        WaitUntilBatchProcessed(2),
        checkRemovalMetrics(total = 4, removed = 3, incremental = factor.getOrElse(2L).min(3L)),
        checkExecutionMode(realTimeMode = true),
        Execute { q =>
          assert(q.sparkSessionForStream.conf.get(factorKey) === factor.getOrElse(2L).toString)
        },
        StopStream
      )
    }
  }

  gridTest("completion drains at the late-events watermark without surviving input")(
      Seq(false, true)) { onlyLateInput =>
    withSQLConf(factorKey -> "1", SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      val inputData = LowLatencyMemoryStream.singlePartition[(String, Int)]
      StreamingDeduplicationWithinWatermarkRealTimeSuite.lateRowRead = false
      val result = deduplicate(inputData.toDS().map { row =>
        if (row._1 == "late") {
          StreamingDeduplicationWithinWatermarkRealTimeSuite.lateRowRead = true
        }
        row
      })
      val lastInput = if (onlyLateInput) Seq("late" -> 20) else Seq.empty[(String, Int)]

      try {
        testStream(result, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
          AddData(inputData, ("a", 10), ("b", 15), ("clock", 30)),
          StartStream(),
          CheckAnswerWithTimeout(60000, "a", "b", "clock"),
          advanceRealTimeClock,
          WaitUntilBatchProcessed(0),
          AddData(inputData, ("advance", 40)),
          CheckAnswerWithTimeout(60000, "a", "b", "clock", "advance"),
          advanceRealTimeClock,
          WaitUntilBatchProcessed(1),
          checkRemovalMetrics(total = 4, removed = 0, incremental = 0),

          AddData(inputData, lastInput: _*),
          Execute { q =>
            eventually {
              assert(q.lastExecution.currentBatchId === 2)
            }
            waitForTasksToStart(2)
            // A task-start event does not prove the reader consumed the late row before its
            // deadline. Acknowledge that read without adding a surviving sentinel record.
            if (onlyLateInput) {
              eventually {
                assert(StreamingDeduplicationWithinWatermarkRealTimeSuite.lateRowRead)
              }
            }
          },
          advanceRealTimeClock,
          WaitUntilBatchProcessed(2),
          // At late watermark 20, only "a" (expiry 20) is eligible. "b" (expiry 25) survives,
          // even though the newer batch-end eviction watermark is 30.
          checkRemovalMetrics(total = 3, removed = 1, incremental = 0),
          Execute { q =>
            assert(q.lastProgress.numInputRows === (if (onlyLateInput) 1L else 0L))
            assert(q.lastProgress.stateOperators.head.numRowsDroppedByWatermark ===
              (if (onlyLateInput) 1L else 0L))
          },
          CheckAnswerWithTimeout(60000, "a", "b", "clock", "advance"),
          StopStream
        )
      } finally {
        StreamingDeduplicationWithinWatermarkRealTimeSuite.lateRowRead = false
      }
    }
  }

  gridTest("incremental cleanup can readmit a key outside the watermark delay")(
      Seq(false, true)) { duplicateFirst =>
    withSQLConf(factorKey -> "1", SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      val inputData = LowLatencyMemoryStream.singlePartition[(String, Int)]
      val result = deduplicate(inputData.toDS())
      val rows = if (duplicateFirst) Seq("a" -> 28, "probe" -> 40)
        else Seq("probe" -> 40, "a" -> 28)
      val expected = Seq("a", "clock", "advance", "probe") ++
        (if (duplicateFirst) Seq.empty else Seq("a"))

      testStream(result, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
        AddData(inputData, ("a", 10), ("clock", 35)),
        StartStream(),
        CheckAnswerWithTimeout(60000, "a", "clock"),
        advanceRealTimeClock,
        WaitUntilBatchProcessed(0),
        AddData(inputData, ("advance", 40)),
        CheckAnswerWithTimeout(60000, "a", "clock", "advance"),
        advanceRealTimeClock,
        WaitUntilBatchProcessed(1),

        // The lookup precedes cleanup. If "a" arrives first, it is suppressed and its old
        // entry removed; otherwise cleanup on "probe" removes it before the next lookup.
        // The 18-second timestamp spread is outside the guaranteed 10-second interval.
        AddData(inputData, rows: _*),
        CheckAnswerWithTimeout(60000, expected: _*),
        advanceRealTimeClock,
        WaitUntilBatchProcessed(2),
        checkRemovalMetrics(total = if (duplicateFirst) 3 else 4, removed = 1, incremental = 1),
        StopStream
      )
    }
  }

  test("cleanup can be enabled and disabled across checkpoint restarts") {
    withSQLConf(factorKey -> "0", SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      withTempDir { checkpoint =>
        val inputData = LowLatencyMemoryStream.singlePartition[(String, Int)]
        val result = deduplicate(inputData.toDS())

        testStream(result, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
          AddData(inputData, ("a", 15), ("clock", 30)),
          StartStream(checkpointLocation = checkpoint.getAbsolutePath),
          CheckAnswerWithTimeout(60000, "a", "clock"),
          advanceRealTimeClock,
          WaitUntilBatchProcessed(0),
          AddData(inputData, ("advance", 40)),
          CheckAnswerWithTimeout(60000, "a", "clock", "advance"),
          advanceRealTimeClock,
          WaitUntilBatchProcessed(1),
          StopStream
        )

        testStream(result, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
          StartStream(checkpointLocation = checkpoint.getAbsolutePath,
            additionalConfs = Map(factorKey -> "1")),
          AddData(inputData, ("probe", 40)),
          CheckAnswerWithTimeout(60000, "probe"),
          advanceRealTimeClock,
          WaitUntilBatchProcessed(2),
          checkRemovalMetrics(total = 4, removed = 0, incremental = 0),
          StopStream
        )

        testStream(result, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
          StartStream(checkpointLocation = checkpoint.getAbsolutePath),
          AddData(inputData, ("tick", 50)),
          CheckAnswerWithTimeout(60000, "tick"),
          advanceRealTimeClock,
          WaitUntilBatchProcessed(3),
          checkRemovalMetrics(total = 4, removed = 1, incremental = 0),
          StopStream
        )
      }
    }
  }

  test("cleanup follows the trigger when restarting the same checkpoint in MBM and RTM") {
    withSQLConf(
        factorKey -> "1",
        SQLConf.SHUFFLE_PARTITIONS.key -> "1",
        SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false",
        SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key -> "2",
        SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName) {
      val inputData = LowLatencyMemoryStream.singlePartition[(String, Int)]
      val result = deduplicate(inputData.toDS())

      testStream(result, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
        AddData(inputData, ("a", 1), ("b", 15), ("clock", 30)),
        StartStream(Trigger.ProcessingTime(0)),
        CheckAnswer("a", "b", "clock"),
        checkExecutionMode(realTimeMode = false),
        checkRemovalMetrics(total = 3, removed = 0, incremental = 0),
        StopStream,

        AddData(inputData, ("advance", 40)),
        StartStream(defaultTrigger),
        CheckAnswerWithTimeout(60000, "a", "b", "clock", "advance"),
        advanceRealTimeClock,
        WaitUntilBatchProcessed(1),
        AddData(inputData, ("g", 40)),
        CheckAnswerWithTimeout(60000, "a", "b", "clock", "advance", "g"),
        advanceRealTimeClock,
        WaitUntilBatchProcessed(2),
        checkExecutionMode(realTimeMode = true),
        // The late-events watermark is 20, so "a" (expiry 11) is removed but "b" survives.
        checkRemovalMetrics(total = 4, removed = 1, incremental = 1),
        StopStream,

        AddData(inputData, ("tick", 50)),
        StartStream(Trigger.ProcessingTime(0)),
        CheckAnswer("a", "b", "clock", "advance", "g", "tick"),
        checkExecutionMode(realTimeMode = false),
        // MBM removes "b" (expiry 25) at its eviction watermark of 30 after consuming input.
        checkRemovalMetrics(total = 4, removed = 1, incremental = 0),
        StopStream
      )
    }
  }

  test("an uncommitted batch rolls back incremental cleanup before checkpoint recovery") {
    withSQLConf(factorKey -> "1", SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      withTempDir { checkpoint =>
        val inputData = LowLatencyMemoryStream.singlePartition[(String, Int)]
        val failUDF = udf { key: String =>
          if (key == "probe" &&
              StreamingDeduplicationWithinWatermarkRealTimeSuite.failAfterCleanup) {
            throw new IllegalStateException("injected failure after incremental cleanup")
          }
          key
        }
        val result = deduplicate(inputData.toDS()).select(failUDF($"_1").as("key"))

        try {
          testStream(result, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
            AddData(inputData, ("expired", 1), ("a", 15), ("clock", 30)),
            StartStream(checkpointLocation = checkpoint.getAbsolutePath),
            CheckAnswerWithTimeout(60000, "expired", "a", "clock"),
            advanceRealTimeClock,
            WaitUntilBatchProcessed(0),
            AddData(inputData, ("advance", 40)),
            CheckAnswerWithTimeout(60000, "expired", "a", "clock", "advance"),
            advanceRealTimeClock,
            WaitUntilBatchProcessed(1),
            Execute { _ =>
              StreamingDeduplicationWithinWatermarkRealTimeSuite.failAfterCleanup = true
            },
            AddData(inputData, ("probe", 40), ("a", 24)),
            ExpectFailure[SparkException] { error =>
              assert(error.getMessage.contains("injected failure after incremental cleanup"))
            }
          )

          StreamingDeduplicationWithinWatermarkRealTimeSuite.failAfterCleanup = false
          testStream(result, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
            StartStream(checkpointLocation = checkpoint.getAbsolutePath),
            AddData(inputData, ("recovery", 50)),
            CheckAnswerWithTimeout(60000, "probe", "recovery"),
            advanceRealTimeClock,
            WaitUntilBatchProcessed(2),
            checkRemovalMetrics(total = 5, removed = 1, incremental = 1),
            StopStream
          )
        } finally {
          StreamingDeduplicationWithinWatermarkRealTimeSuite.failAfterCleanup = false
        }
      }
    }
  }
}
