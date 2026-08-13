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

import org.apache.spark.sql.{AnalysisException, Dataset, SaveMode}
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes.Append
import org.apache.spark.sql.execution.streaming.operators.stateful.StatefulOperatorsUtils
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.functions.{timestamp_nanos, timestamp_seconds}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{LongType, StringType, StructType, TimestampNTZNanosType}
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

  test("SPARK-57843: dropDuplicatesWithinWatermark with nanosecond-precision event-time") {
    // Verify that nanosecond-precision event-time columns work correctly with
    // dropDuplicatesWithinWatermark. Event-time values are converted to micros internally
    // for the expiration state, ensuring correct dedup and eviction behavior.
    val inputData = MemoryStream[(String, Long)]
    val result = inputData.toDS()
      .withColumn("eventTime", timestamp_nanos($"_2"))
      .withWatermark("eventTime", "10 seconds")
      .dropDuplicatesWithinWatermark("_1")
      .select($"_1")

    testStream(result, Append)(
      // "a" at 17s, "b" at 22s, "c" at 21s (nanos). Watermark -> max(17,22,21) - 10 = 12s.
      AddData(inputData,
        ("a", 17L * 1000000000L),
        ("b", 22L * 1000000000L),
        ("c", 21L * 1000000000L)),
      CheckNewAnswer("a", "b", "c"),
      assertNumStateRows(total = 3, updated = 3),

      // Duplicate "a" at 16s. Should not emit (within watermark, key exists).
      AddData(inputData, ("a", 16L * 1000000000L)),
      CheckNewAnswer(),
      assertNumStateRows(total = 3, updated = 0),

      // "a" at 11s: older than watermark (12s), should be dropped as late.
      AddData(inputData, ("a", 11L * 1000000000L)),
      CheckNewAnswer(),
      assertNumStateRows(total = 3, updated = 0, droppedByWatermark = 1),

      // "d" at 35s. Watermark advances to 25s.
      // "a" expiresAt = 17+10=27s > 25 (kept), "b" expiresAt = 22+10=32s > 25 (kept),
      // "c" expiresAt = 21+10=31s > 25 (kept). All kept.
      AddData(inputData, ("d", 35L * 1000000000L)),
      CheckNewAnswer("d"),
      assertNumStateRows(total = 4, updated = 1),

      // "e" at 55s. Watermark advances to 45s.
      // "a" expiresAt=27 <= 45 (evicted), "b" expiresAt=32 <= 45 (evicted),
      // "c" expiresAt=31 <= 45 (evicted), "d" expiresAt=45 <= 45 (evicted).
      AddData(inputData, ("e", 55L * 1000000000L)),
      CheckNewAnswer("e"),
      assertNumStateRows(total = 1, updated = 1)
    )
  }

  test("SPARK-57843: sub-microsecond dedup semantics - epochMicros truncation dedup key") {
    // Two rows with the SAME epochMicros but DIFFERENT nanosWithinMicro values.
    // dropDuplicatesWithinWatermark uses the full (key-columns) for dedup grouping,
    // so both rows should be emitted because their _1 (key) differs, even though
    // their timestamps truncate to the same microsecond. This documents the intent
    // that the dedup key is NOT the truncated timestamp itself.
    val inputData = MemoryStream[(String, Long)]
    val result = inputData.toDS()
      .withColumn("eventTime", timestamp_nanos($"_2"))
      .withWatermark("eventTime", "10 seconds")
      .dropDuplicatesWithinWatermark("_1")
      .select($"_1")

    // 20_000_000_001 ns = 20.000000001s; 20_000_000_999 ns = 20.000000999s
    // Both truncate to the same epochMicros (20_000_000) but are distinct nanos.
    // With different keys ("x" vs "y"), both should emit.
    testStream(result, Append)(
      AddData(inputData,
        ("x", 20000000001L),
        ("y", 20000000999L)),
      CheckNewAnswer("x", "y"),
      assertNumStateRows(total = 2, updated = 2),

      // Same key "x" at a slightly different nano offset (same micro) -> duplicate, not emitted
      AddData(inputData, ("x", 20000000500L)),
      CheckNewAnswer(),
      assertNumStateRows(total = 2, updated = 0)
    )
  }

  test("SPARK-57843: dropDuplicatesWithinWatermark with NTZ nanosecond-precision event-time") {
    // Same as the LTZ-nanos test above but using TimestampNTZNanosType for the event-time
    // column. Verifies that the NTZ nanos path through watermark extraction and dedup
    // expiration state works correctly.
    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      val inputData = MemoryStream[(String, Long)]
      val result = inputData.toDS()
        .withColumn("eventTime", timestamp_nanos($"_2").cast(TimestampNTZNanosType(9)))
        .withWatermark("eventTime", "10 seconds")
        .dropDuplicatesWithinWatermark("_1")
        .select($"_1")

      testStream(result, Append)(
        // "a" at 17s, "b" at 22s, "c" at 21s (nanos). Watermark -> max(17,22,21) - 10 = 12s.
        AddData(inputData,
          ("a", 17L * 1000000000L),
          ("b", 22L * 1000000000L),
          ("c", 21L * 1000000000L)),
        CheckNewAnswer("a", "b", "c"),
        assertNumStateRows(total = 3, updated = 3),

        // Duplicate "a" at 16s. Should not emit (within watermark, key exists).
        AddData(inputData, ("a", 16L * 1000000000L)),
        CheckNewAnswer(),
        assertNumStateRows(total = 3, updated = 0),

        // "a" at 11s: older than watermark (12s), should be dropped as late.
        AddData(inputData, ("a", 11L * 1000000000L)),
        CheckNewAnswer(),
        assertNumStateRows(total = 3, updated = 0, droppedByWatermark = 1),

        // "d" at 35s. Watermark advances to 25s.
        // "a" expiresAt = 17+10=27s > 25 (kept), "b" expiresAt = 22+10=32s > 25 (kept),
        // "c" expiresAt = 21+10=31s > 25 (kept). All kept.
        AddData(inputData, ("d", 35L * 1000000000L)),
        CheckNewAnswer("d"),
        assertNumStateRows(total = 4, updated = 1),

        // "e" at 55s. Watermark advances to 45s.
        // "a" expiresAt=27 <= 45 (evicted), "b" expiresAt=32 <= 45 (evicted),
        // "c" expiresAt=31 <= 45 (evicted), "d" expiresAt=45 <= 45 (evicted).
        AddData(inputData, ("e", 55L * 1000000000L)),
        CheckNewAnswer("e"),
        assertNumStateRows(total = 1, updated = 1)
      )
    }
  }
}
