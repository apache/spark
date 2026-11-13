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
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, SaveMode}
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes.Append
import org.apache.spark.sql.execution.streaming.operators.stateful.{StatefulOperatorsUtils, StreamingDeduplicateWithinWatermarkExec}
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.functions.timestamp_seconds
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.util.StreamManualClock
import org.apache.spark.sql.types.{LongType, StringType, StructType}
import org.apache.spark.tags.SlowSQLTest

object StreamingDeduplicationWithinWatermarkSuite {
  @volatile var failBeforeCommit = false
}

@SlowSQLTest
class StreamingDeduplicationWithinWatermarkSuite extends StateStoreMetricsTest
  with StreamingDeduplicationSuiteBase {

  import testImplicits._

  test("deduplicate in batch DataFrame") {
    def testAndVerify(df: Dataset[_]): Unit = {
      val exc = intercept[AnalysisException] {
        df.write.format("noop").mode(SaveMode.Append).save()
      }

      assert(exc.getMessage.contains("dropDuplicatesWithinWatermark is not supported"))
      assert(exc.getMessage.contains("batch DataFrames/DataSets"))
    }

    val result = spark.range(10).dropDuplicatesWithinWatermark()
    testAndVerify(result)

    val result2 = spark.range(10).withColumn("newcol", $"id")
      .dropDuplicatesWithinWatermark("newcol")
    testAndVerify(result2)
  }

  test("deduplicate without event time column should result in error") {
    def testAndVerify(df: Dataset[_]): Unit = {
      val exc = intercept[AnalysisException] {
        df.writeStream.format("noop").start()
      }

      assert(exc.getMessage.contains("dropDuplicatesWithinWatermark is not supported"))
      assert(exc.getMessage.contains("streaming DataFrames/DataSets without watermark"))
    }

    val inputData = MemoryStream[String]
    val result = inputData.toDS().dropDuplicatesWithinWatermark()
    testAndVerify(result)

    val result2 = inputData.toDS().withColumn("newcol", $"value")
      .dropDuplicatesWithinWatermark("newcol")
    testAndVerify(result2)

    val inputData2 = MemoryStream[(String, Int)]
    val otherSideForJoin = inputData2.toDF()
      .select($"_1" as "key", timestamp_seconds($"_2") as "time")
      .withWatermark("Time", "10 seconds")

    val result3 = inputData.toDS()
      .select($"value".as("key"))
      // there are two streams which one stream only defines the watermark. the stream which
      // contains dropDuplicatesWithinWatermark does not define the watermark, which is not
      // supported.
      .dropDuplicatesWithinWatermark()
      .join(otherSideForJoin, "key")
    testAndVerify(result3)
  }

  test("deduplicate with all columns with event time column in DataFrame") {
    val inputData = MemoryStream[Int]
    val result = inputData.toDS()
      .withColumn("eventTime", timestamp_seconds($"value"))
      .withWatermark("eventTime", "10 seconds")
      .dropDuplicatesWithinWatermark()
      .select($"eventTime".cast("long").as[Long])

    testStream(result, Append)(
      // Advance watermark to 5 secs, no-data-batch does not drop state rows
      AddData(inputData, (1 to 5).flatMap(_ => (10 to 15)): _*),
      CheckAnswer(10 to 15: _*),
      assertNumStateRows(total = 6, updated = 6),

      // Advance watermark to 7 secs, no-data-batch does not drop state rows
      AddData(inputData, (13 to 17): _*),
      // 13 to 15 are duplicated
      CheckNewAnswer(16, 17),
      assertNumStateRows(total = 8, updated = 2),

      AddData(inputData, 5), // Should not emit anything as data less than watermark
      CheckNewAnswer(),
      assertNumStateRows(total = 8, updated = 0, droppedByWatermark = 1),

      // Advance watermark to 25 secs, no-data-batch drops state rows having expired time <= 25
      AddData(inputData, 35),
      CheckNewAnswer(35),
      assertNumStateRows(total = 3, updated = 1),

      // Advance watermark to 45 seconds, no-data-batch drops state rows having expired time <= 45
      AddData(inputData, 55),
      CheckNewAnswer(55),
      assertNumStateRows(total = 1, updated = 1)
    )
  }

  test("deduplicate with subset of columns which event time column is not in subset") {
    val inputData = MemoryStream[(String, Int)]
    val result = inputData.toDS()
      .withColumn("eventTime", timestamp_seconds($"_2"))
      .withWatermark("eventTime", "2 seconds")
      .dropDuplicatesWithinWatermark("_1")
      .select($"_1", $"eventTime".cast("long").as[Long])

    testStream(result, Append)(
      // Advances watermark to 15
      AddData(inputData, "a" -> 17),
      CheckNewAnswer("a" -> 17),
      // expired time is set to 19
      assertNumStateRows(total = 1, updated = 1),

      // Watermark does not advance
      AddData(inputData, "a" -> 16),
      CheckNewAnswer(),
      assertNumStateRows(total = 1, updated = 0),

      // Watermark does not advance
      // Should not emit anything as data less than watermark
      AddData(inputData, "a" -> 13),
      CheckNewAnswer(),
      assertNumStateRows(total = 1, updated = 0, droppedByWatermark = 1),

      // Advances watermark to 20. no-data batch drops state row ("a" -> 19)
      AddData(inputData, "b" -> 22, "c" -> 21),
      CheckNewAnswer("b" -> 22, "c" -> 21),
      // expired time is set to 24 and 23
      assertNumStateRows(total = 2, updated = 2),

      // Watermark does not advance
      AddData(inputData, "a" -> 21),
      // "a" is identified as new event since previous batch dropped state row ("a" -> 19)
      CheckNewAnswer("a" -> 21),
      // expired time is set to 23
      assertNumStateRows(total = 3, updated = 1),

      // Advances watermark to 23. no-data batch drops state row ("a" -> 23), ("c" -> 23)
      AddData(inputData, "d" -> 25),
      CheckNewAnswer("d" -> 25),
      assertNumStateRows(total = 2, updated = 1)
    )
  }

  private def deduplicateWithinWatermark(input: MemoryStream[(String, Int)]): DataFrame = {
    input.toDS()
      .withColumn("eventTime", timestamp_seconds($"_2"))
      .withWatermark("eventTime", "10 seconds")
      .dropDuplicatesWithinWatermark("_1")
      .select($"_1", $"eventTime".cast("long").as[Long])
  }

  private def assertCleanupMetrics(
      removed: Long,
      incremental: Long,
      scanned: Option[Long] = None): AssertOnQuery = {
    AssertOnQuery(s"Check cleanup metrics: removed=$removed, incremental=$incremental") { q =>
      val operator = q.lastProgress.stateOperators.head
      assert(operator.numRowsRemoved === removed)
      assert(operator.customMetrics.get("numRowsIncrementallyRemoved").longValue() === incremental)
      val numScanned = operator.customMetrics.get("numRowsReadDuringEviction").longValue()
      assert(numScanned >= removed)
      scanned.foreach(expected => assert(numScanned === expected))
      true
    }
  }

  gridTest("micro-batch cleanup ignores the shared factor and protects within-window duplicates")(
      Seq(0L, 1L, 2L, 10L)) { factor =>
    withSQLConf(
      SQLConf.STREAMING_STATE_INCREMENTAL_CLEANUP_FACTOR.key -> factor.toString,
      SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false",
      SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      val inputData = MemoryStream[(String, Int)](1)
      val result = deduplicateWithinWatermark(inputData)

      testStream(result, Append)(
        AddData(inputData, "a" -> 15, "clock" -> 30),
        CheckNewAnswer("a" -> 15, "clock" -> 30),
        assertNumStateRows(total = 2, updated = 2),

        AddData(inputData, "advance" -> 40),
        CheckNewAnswer("advance" -> 40),
        assertNumStateRows(total = 3, updated = 1),

        // Late input is filtered at 20, but batch-end eviction can use 30. "a" expires at 25;
        // evicting it incrementally at 30 would let the non-late duplicate at 24 through.
        AddData(inputData, "probe" -> 40, "a" -> 24, "a" -> 20),
        CheckNewAnswer("probe" -> 40),
        assertNumStateRows(total = 3, updated = 1, droppedByWatermark = 1),
        assertCleanupMetrics(removed = 1, incremental = 0),
        Execute { q =>
          val operators = q.lastExecution.executedPlan.collect {
            case d: StreamingDeduplicateWithinWatermarkExec => d
          }
          assert(operators.nonEmpty && operators.forall(!_.isRealTimeMode))
          assert(q.sparkSessionForStream.conf.get(
            SQLConf.STREAMING_STATE_INCREMENTAL_CLEANUP_FACTOR.key) === factor.toString)
        }
      )
    }
  }

  gridTest("micro-batch cleanup retains batch-end eviction after checkpoint restart")(
      Seq(1L, 2L, 10L)) { factor =>
    withSQLConf(
      SQLConf.STREAMING_STATE_INCREMENTAL_CLEANUP_FACTOR.key -> factor.toString,
      SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false",
      SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      val inputData = MemoryStream[(String, Int)](1)
      val result = deduplicateWithinWatermark(inputData)

      testStream(result, Append)(
        AddData(inputData, "a" -> 1, "b" -> 10, "c" -> 21, "d" -> 30, "e" -> 42),
        CheckNewAnswer("a" -> 1, "b" -> 10, "c" -> 21, "d" -> 30, "e" -> 42),
        assertNumStateRows(total = 5, updated = 5),

        AddData(inputData, "g" -> 51),
        CheckNewAnswer("g" -> 51),
        assertNumStateRows(total = 3, updated = 1),
        assertCleanupMetrics(removed = 3, incremental = 0),
        StopStream,
        StartStream(),

        // The restored eviction watermark is 41. "d" (expiry 40) is removed at batch end,
        // regardless of the positive shared cleanup factor.
        AddData(inputData, "h" -> 51),
        CheckNewAnswer("h" -> 51),
        assertNumStateRows(total = 3, updated = 1),
        assertCleanupMetrics(removed = 1, incremental = 0),
        StopStream,
        StartStream(),

        // A removed key can be admitted again, but its next within-window duplicate is dropped.
        AddData(inputData, "a" -> 52, "a" -> 53),
        CheckNewAnswer("a" -> 52),
        assertNumStateRows(total = 4, updated = 1),
        assertCleanupMetrics(removed = 0, incremental = 0)
      )
    }
  }

  test("micro-batch cleanup uses the eviction watermark in a no-data batch") {
    withSQLConf(
      SQLConf.STREAMING_STATE_INCREMENTAL_CLEANUP_FACTOR.key -> "1",
      SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "true",
      SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      val inputData = MemoryStream[(String, Int)](1)
      val result = deduplicateWithinWatermark(inputData)
      val clock = new StreamManualClock

      testStream(result, Append)(
        AddData(inputData, "a" -> 10, "b" -> 15, "clock" -> 30),
        StartStream(Trigger.ProcessingTime("1 second"), clock),
        CheckNewAnswer("a" -> 10, "b" -> 15, "clock" -> 30),
        WaitUntilBatchProcessed(0),
        assertNumStateRows(total = 3, updated = 3),

        AddData(inputData, "advance" -> 40),
        AdvanceManualClock(1000L),
        WaitUntilBatchProcessed(1),
        CheckNewAnswer("advance" -> 40),
        assertNumStateRows(total = 3, updated = 1),
        assertCleanupMetrics(removed = 1, incremental = 0),

        // The empty batch has late watermark 20 and eviction watermark 30. "a" was already
        // removed at 20; "b" (expiry 25) is now removed using the newer eviction watermark.
        AdvanceManualClock(1000L),
        WaitUntilBatchProcessed(2),
        CheckNewAnswer(),
        Execute { q =>
          assert(q.lastProgress.batchId === 2)
          assert(q.lastProgress.numInputRows === 0)
        },
        assertNumStateRows(total = 2, updated = 0),
        assertCleanupMetrics(removed = 1, incremental = 0, scanned = Some(3L))
      )
    }
  }

  test("micro-batch cleanup ignores a positive factor with the legacy watermark propagator") {
    withSQLConf(
      SQLConf.STREAMING_STATE_INCREMENTAL_CLEANUP_FACTOR.key -> "1",
      SQLConf.STATEFUL_OPERATOR_ALLOW_MULTIPLE.key -> "false",
      SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false",
      SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      val inputData = MemoryStream[(String, Int)](1)
      val result = deduplicateWithinWatermark(inputData)

      testStream(result, Append)(
        AddData(inputData, "a" -> 10, "clock" -> 30),
        CheckNewAnswer("a" -> 10, "clock" -> 30),
        assertNumStateRows(total = 2, updated = 2),

        // Both watermark thresholds are 20 with the legacy propagator, so "a" can be evicted.
        AddData(inputData, "advance" -> 40),
        CheckNewAnswer("advance" -> 40),
        assertNumStateRows(total = 2, updated = 1),
        assertCleanupMetrics(removed = 1, incremental = 0),

        AddData(inputData, "a" -> 31, "a" -> 32),
        CheckNewAnswer("a" -> 31),
        assertNumStateRows(total = 3, updated = 1),
        assertCleanupMetrics(removed = 0, incremental = 0)
      )
    }
  }

  test("micro-batch cleanup with a positive shared factor rolls back an uncommitted batch") {
    withSQLConf(
      SQLConf.STREAMING_STATE_INCREMENTAL_CLEANUP_FACTOR.key -> "1",
      SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false",
      SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      val inputData = MemoryStream[(String, Int)](1)
      val result = deduplicateWithinWatermark(inputData).as[(String, Long)].map { row =>
        // Fail before batch-end eviction and commit to test replay with a positive shared factor.
        if (row._1 == "probe" && StreamingDeduplicationWithinWatermarkSuite.failBeforeCommit) {
          sys.error("injected failure before commit")
        }
        row
      }

      try {
        testStream(result, Append)(
          AddData(inputData, "expired" -> 1, "a" -> 15, "clock" -> 30),
          CheckNewAnswer("expired" -> 1, "a" -> 15, "clock" -> 30),
          assertNumStateRows(total = 3, updated = 3),
          AddData(inputData, "advance" -> 40),
          CheckNewAnswer("advance" -> 40),
          assertNumStateRows(total = 3, updated = 1),

          // "expired" was removed at batch end. "a" must survive until every non-late duplicate
          // in the replayed batch is processed, then be removed at the eviction watermark of 30.
          Execute { _ => StreamingDeduplicationWithinWatermarkSuite.failBeforeCommit = true },
          AddData(inputData, "probe" -> 40, "a" -> 24),
          ExpectFailure[SparkException] { error =>
            assert(error.getMessage.contains("injected failure before commit"))
          },
          Execute { _ => StreamingDeduplicationWithinWatermarkSuite.failBeforeCommit = false },
          StartStream(),
          CheckNewAnswer("probe" -> 40),
          assertNumStateRows(total = 3, updated = 1),
          assertCleanupMetrics(removed = 1, incremental = 0)
        )
      } finally {
        StreamingDeduplicationWithinWatermarkSuite.failBeforeCommit = false
      }
    }
  }

  gridTest("micro-batch cleanup expires entries at the eviction watermark boundary")(
      Seq(false, true)) { onlyLateInput =>
    withSQLConf(
      SQLConf.STREAMING_STATE_INCREMENTAL_CLEANUP_FACTOR.key -> "1",
      SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false",
      SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      val inputData = MemoryStream[(String, Int)](1)
      val result = deduplicateWithinWatermark(inputData)
      val lastInput = if (onlyLateInput) "a" -> 20 else "probe" -> 40
      val lastAnswer = if (onlyLateInput) CheckNewAnswer() else CheckNewAnswer("probe" -> 40)

      testStream(result, Append)(
        AddData(inputData, "a" -> 10, "clock" -> 30),
        CheckNewAnswer("a" -> 10, "clock" -> 30),
        assertNumStateRows(total = 2, updated = 2),
        AddData(inputData, "advance" -> 40),
        CheckNewAnswer("advance" -> 40),
        assertNumStateRows(total = 2, updated = 1),
        assertCleanupMetrics(removed = 1, incremental = 0),

        // "a" was removed at the eviction watermark of 20. The next batch must not count it
        // again, whether its input is all late or contains a new key.
        AddData(inputData, lastInput),
        lastAnswer,
        assertNumStateRows(
          total = if (onlyLateInput) 2 else 3,
          updated = if (onlyLateInput) 0 else 1,
          droppedByWatermark = if (onlyLateInput) 1 else 0),
        assertCleanupMetrics(
          removed = 0,
          incremental = 0,
          scanned = if (onlyLateInput) Some(2L) else None)
      )
    }
  }

  test("micro-batch cleanup keeps its policy when the shared factor changes across restarts") {
    val factorKey = SQLConf.STREAMING_STATE_INCREMENTAL_CLEANUP_FACTOR.key
    withSQLConf(
      factorKey -> "0",
      SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false",
      SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      val inputData = MemoryStream[(String, Int)](1)
      val result = deduplicateWithinWatermark(inputData)

      testStream(result, Append)(
        AddData(inputData, "a" -> 15, "clock" -> 30),
        CheckNewAnswer("a" -> 15, "clock" -> 30),
        assertNumStateRows(total = 2, updated = 2),
        AddData(inputData, "advance" -> 40),
        CheckNewAnswer("advance" -> 40),
        assertNumStateRows(total = 3, updated = 1),
        StopStream,

        // Setting a positive factor must not change this operator's micro-batch policy.
        // "a" (expiry 25) is removed at the eviction watermark of 30, at batch end.
        StartStream(additionalConfs = Map(factorKey -> "1")),
        AddData(inputData, "probe" -> 40),
        CheckNewAnswer("probe" -> 40),
        assertNumStateRows(total = 3, updated = 1),
        assertCleanupMetrics(removed = 1, incremental = 0),
        StopStream,

        // Returning the factor to 0 reads the same checkpoint without a state migration.
        StartStream(additionalConfs = Map(factorKey -> "0")),
        AddData(inputData, "tick" -> 50),
        CheckNewAnswer("tick" -> 50),
        assertNumStateRows(total = 4, updated = 1),
        assertCleanupMetrics(removed = 0, incremental = 0)
      )
    }
  }

  gridTest("micro-batch output outside the watermark delay does not depend on cleanup order")(
      Seq(false, true)) { duplicateFirst =>
    withSQLConf(
      SQLConf.STREAMING_STATE_INCREMENTAL_CLEANUP_FACTOR.key -> "1",
      SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false",
      SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      val inputData = MemoryStream[(String, Int)](1)
      val result = deduplicateWithinWatermark(inputData)
      val rows = if (duplicateFirst) {
        Seq("a" -> 28, "probe" -> 40)
      } else {
        Seq("probe" -> 40, "a" -> 28)
      }
      val expected = rows

      testStream(result, Append)(
        AddData(inputData, "a" -> 10, "clock" -> 35),
        CheckNewAnswer("a" -> 10, "clock" -> 35),
        assertNumStateRows(total = 2, updated = 2),
        AddData(inputData, "advance" -> 40),
        CheckNewAnswer("advance" -> 40),
        assertNumStateRows(total = 2, updated = 1),
        assertCleanupMetrics(removed = 1, incremental = 0),

        // The previous batch removed "a" at its eviction watermark of 25. Either input order
        // admits the new "a", even though the shared cleanup factor is positive.
        AddData(inputData, rows: _*),
        CheckNewAnswer(expected.head, expected.tail: _*),
        assertNumStateRows(total = 4, updated = 2),
        assertCleanupMetrics(removed = 0, incremental = 0),
        StopStream,
        StartStream(),

        // Reopening preserves the replacement entry in either input order.
        AddData(inputData, "a" -> 31, "a" -> 32),
        CheckNewAnswer(),
        assertNumStateRows(total = 4, updated = 0),
        assertCleanupMetrics(removed = 0, incremental = 0)
      )
    }
  }

  test("SPARK-39650: duplicate with specific keys should allow input to change schema") {
    withTempDir { checkpoint =>
      val dedupeInputData = MemoryStream[(String, Int)]
      val dedupe = dedupeInputData.toDS()
        .withColumn("eventTime", timestamp_seconds($"_2"))
        .withWatermark("eventTime", "10 second")
        .dropDuplicatesWithinWatermark("_1")
        .select($"_1", $"eventTime".cast("long").as[Long])

      testStream(dedupe, Append)(
        StartStream(checkpointLocation = checkpoint.getCanonicalPath),

        AddData(dedupeInputData, "a" -> 1),
        CheckNewAnswer("a" -> 1),

        AddData(dedupeInputData, "a" -> 2, "b" -> 3),
        CheckNewAnswer("b" -> 3)
      )

      val dedupeInputData2 = MemoryStream[(String, Int, String)]
      val dedupe2 = dedupeInputData2.toDS()
        .withColumn("eventTime", timestamp_seconds($"_2"))
        .withWatermark("eventTime", "10 second")
        .dropDuplicatesWithinWatermark(Seq("_1"))
        .select($"_1", $"eventTime".cast("long").as[Long], $"_3")

      // initialize new memory stream with previously executed batches
      dedupeInputData2.addData(("a", 1, "dummy"))
      dedupeInputData2.addData(Seq(("a", 2, "dummy"), ("b", 3, "dummy")))

      testStream(dedupe2, Append)(
        StartStream(checkpointLocation = checkpoint.getCanonicalPath),

        AddData(dedupeInputData2, ("a", 5, "a"), ("b", 2, "b"), ("c", 9, "c")),
        CheckNewAnswer(("c", 9, "c"))
      )
    }
  }

  gridTest("SPARK-46676: canonicalization of StreamingDeduplicateWithinWatermarkExec should work")(
      Seq(0L, 1L)) { factor =>
    withSQLConf(SQLConf.STREAMING_STATE_INCREMENTAL_CLEANUP_FACTOR.key -> factor.toString) {
      withTempDir { checkpoint =>
        val dedupeInputData = MemoryStream[(String, Int)]
        val dedupe = dedupeInputData.toDS()
          .withColumn("eventTime", timestamp_seconds($"_2"))
          .withWatermark("eventTime", "10 second")
          .dropDuplicatesWithinWatermark("_1")
          .select($"_1", $"eventTime".cast("long").as[Long])

        testStream(dedupe, Append)(
          StartStream(checkpointLocation = checkpoint.getCanonicalPath),
          AddData(dedupeInputData, "a" -> 1),
          CheckNewAnswer("a" -> 1),
          Execute { q =>
            // This threw out error before SPARK-46676.
            q.lastExecution.executedPlan.canonicalized
          }
        )
      }
    }
  }

  test("SPARK-50492: drop event time column after dropDuplicatesWithinWatermark") {
    val inputData = MemoryStream[(Int, Int)]
    val result = inputData.toDS()
      .withColumn("first", timestamp_seconds($"_1"))
      .withWatermark("first", "10 seconds")
      .dropDuplicatesWithinWatermark("_2")
      .select("_2")

    testStream(result, Append)(
      AddData(inputData, (1, 2)),
      CheckAnswer(2)
    )
  }

  test("Partition key extraction - DedupeWithinWatermark") {
    val df = (input: Dataset[(String, Int)]) => {
      input.withColumn("eventTime", timestamp_seconds($"_2"))
        .withWatermark("eventTime", "10 seconds")
        .dropDuplicatesWithinWatermark("_1")
        .select($"_1", $"eventTime".cast("long").as[Long])
    }

    testPartitionKeyExtraction(
      StatefulOperatorsUtils.DEDUPLICATE_WITHIN_WATERMARK_EXEC_OP_NAME,
      inputData = Seq(("a", 17), ("b", 22), ("c", 21)),
      dedupeDF = df,
      // The key schema for dedup within watermark is just the _1 column (String)
      keySchema = new StructType().add("_1", StringType),
      // Value schema includes the expiration time
      valueSchema = new StructType().add("expiresAtMicros", LongType),
      sqlConf = spark.sessionState.conf
    )
  }
}

@SlowSQLTest
class RocksDBStateStoreStreamingDeduplicationWithinWatermarkSuite
  extends StreamingDeduplicationWithinWatermarkSuite with RocksDBStateStoreTest
