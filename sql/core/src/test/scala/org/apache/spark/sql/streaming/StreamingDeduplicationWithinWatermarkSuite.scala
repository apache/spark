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

import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, SaveMode}
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes.Append
import org.apache.spark.sql.execution.streaming.operators.stateful.StatefulOperatorsUtils
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.functions.timestamp_seconds
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{LongType, StringType, StructType}
import org.apache.spark.tags.SlowSQLTest

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

  private def assertCleanupMetrics(removed: Long, incremental: Long): AssertOnQuery = {
    AssertOnQuery(s"Check cleanup metrics: removed=$removed, incremental=$incremental") { q =>
      val operator = q.lastProgress.stateOperators.head
      assert(operator.numRowsRemoved === removed)
      assert(operator.customMetrics.get("numRowsIncrementallyRemoved").longValue() === incremental)
      assert(operator.customMetrics.get("numRowsReadDuringEviction").longValue() >= removed)
      true
    }
  }

  gridTest("incremental cleanup protects duplicates within the watermark delay")(
      Seq(0L, 1L)) { factor =>
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
        assertNumStateRows(
          total = if (factor == 0) 3 else 4, updated = 1, droppedByWatermark = 1),
        assertCleanupMetrics(removed = if (factor == 0) 1 else 0, incremental = 0)
      )
    }
  }

  gridTest("incremental cleanup drains remaining expired state after checkpoint restart")(
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
        assertNumStateRows(total = 6, updated = 1),
        assertCleanupMetrics(removed = 0, incremental = 0),
        StopStream,
        StartStream(),

        // The restored late-events watermark is 32. Three old entries have expired; one input
        // removes at most factor entries incrementally, and completion must remove the rest.
        AddData(inputData, "h" -> 51),
        CheckNewAnswer("h" -> 51),
        assertNumStateRows(total = 4, updated = 1),
        assertCleanupMetrics(removed = 3, incremental = math.min(factor, 3L)),

        // A removed key can be admitted again, but its next within-window duplicate is dropped.
        AddData(inputData, "a" -> 52, "a" -> 53),
        CheckNewAnswer("a" -> 52),
        assertNumStateRows(total = 4, updated = 1),
        assertCleanupMetrics(removed = 1, incremental = 1)
      )
    }
  }

  gridTest("incremental cleanup expires entries at the watermark boundary")(
      Seq(false, true)) { onlyLateInput =>
    withSQLConf(
      SQLConf.STREAMING_STATE_INCREMENTAL_CLEANUP_FACTOR.key -> "1",
      SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false",
      SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      val inputData = MemoryStream[(String, Int)](1)
      val result = deduplicateWithinWatermark(inputData)
      val lastInput = if (onlyLateInput) "a" -> 20 else "probe" -> 40
      val lastAnswer = if (onlyLateInput) Seq.empty else Seq("probe" -> 40)

      testStream(result, Append)(
        AddData(inputData, "a" -> 10, "clock" -> 30),
        CheckNewAnswer("a" -> 10, "clock" -> 30),
        assertNumStateRows(total = 2, updated = 2),
        AddData(inputData, "advance" -> 40),
        CheckNewAnswer("advance" -> 40),
        assertNumStateRows(total = 3, updated = 1),

        // The late-events watermark equals "a"'s expiration, 20. Even when all input is late,
        // the completion-time drain must evict the entry without counting it as incremental.
        AddData(inputData, lastInput),
        CheckNewAnswer(lastAnswer: _*),
        assertNumStateRows(
          total = if (onlyLateInput) 2 else 3,
          updated = if (onlyLateInput) 0 else 1,
          droppedByWatermark = if (onlyLateInput) 1 else 0),
        assertCleanupMetrics(removed = 1, incremental = if (onlyLateInput) 0 else 1)
      )
    }
  }

  test("incremental cleanup can be enabled and disabled across checkpoint restarts") {
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

        // Read the existing batch-end-only state with incremental cleanup enabled. At the
        // late-events watermark of 20, "a" (expiry 25) is not yet eligible for removal.
        StartStream(additionalConfs = Map(factorKey -> "1")),
        AddData(inputData, "probe" -> 40),
        CheckNewAnswer("probe" -> 40),
        assertNumStateRows(total = 4, updated = 1),
        assertCleanupMetrics(removed = 0, incremental = 0),
        StopStream,

        // The same checkpoint can return to batch-end cleanup without a state migration.
        StartStream(additionalConfs = Map(factorKey -> "0")),
        AddData(inputData, "tick" -> 50),
        CheckNewAnswer("tick" -> 50),
        assertNumStateRows(total = 4, updated = 1),
        assertCleanupMetrics(removed = 1, incremental = 0)
      )
    }
  }

  gridTest("incremental cleanup can readmit a key outside the watermark delay")(
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
      val expected = if (duplicateFirst) Seq("probe" -> 40) else rows

      testStream(result, Append)(
        AddData(inputData, "a" -> 10, "clock" -> 35),
        CheckNewAnswer("a" -> 10, "clock" -> 35),
        assertNumStateRows(total = 2, updated = 2),
        AddData(inputData, "advance" -> 40),
        CheckNewAnswer("advance" -> 40),
        assertNumStateRows(total = 3, updated = 1),

        // Only "a" has expired at the late-events watermark of 25. The lookup precedes cleanup,
        // so a first-row duplicate is suppressed, but cleanup on "probe" can admit a later one.
        // The 18-second timestamp spread is outside the guaranteed 10-second interval.
        AddData(inputData, rows: _*),
        CheckNewAnswer(expected: _*),
        assertNumStateRows(
          total = if (duplicateFirst) 3 else 4, updated = if (duplicateFirst) 1 else 2),
        assertCleanupMetrics(removed = 1, incremental = 1)
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

  test("SPARK-46676: canonicalization of StreamingDeduplicateWithinWatermarkExec should work") {
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
