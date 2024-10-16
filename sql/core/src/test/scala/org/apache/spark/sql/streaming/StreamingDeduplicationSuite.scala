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

import java.io.File

import org.apache.commons.io.FileUtils

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes._
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StringType
import org.apache.spark.tags.SlowSQLTest
import org.apache.spark.util.Utils

@SlowSQLTest
class StreamingDeduplicationSuite extends StateStoreMetricsTest {

  import testImplicits._

  test("deduplicate with all columns") {
    val inputData = MemoryStream[String]
    val result = inputData.toDS().dropDuplicates()

    testStream(result, Append)(
      AddData(inputData, "a"),
      CheckLastBatch("a"),
      assertNumStateRows(total = 1, updated = 1),
      AddData(inputData, "a"),
      CheckLastBatch(),
      assertNumStateRows(total = 1, updated = 0),
      AddData(inputData, "b"),
      CheckLastBatch("b"),
      assertNumStateRows(total = 2, updated = 1)
    )
  }

  test("deduplicate with some columns") {
    val inputData = MemoryStream[(String, Int)]
    val result = inputData.toDS().dropDuplicates("_1")

    testStream(result, Append)(
      AddData(inputData, "a" -> 1),
      CheckLastBatch("a" -> 1),
      assertNumStateRows(total = 1, updated = 1, droppedByWatermark = 0),
      AddData(inputData, "a" -> 2), // Dropped
      CheckLastBatch(),
      assertNumStateRows(total = 1, updated = 0, droppedByWatermark = 0),
      AddData(inputData, "b" -> 1),
      CheckLastBatch("b" -> 1),
      assertNumStateRows(total = 2, updated = 1, droppedByWatermark = 0)
    )
  }

  test("multiple deduplicates") {
    val inputData = MemoryStream[(String, Int)]
    val result = inputData.toDS().dropDuplicates().dropDuplicates("_1")

    testStream(result, Append)(
      AddData(inputData, "a" -> 1),
      CheckLastBatch("a" -> 1),
      assertNumStateRows(total = Seq(1L, 1L), updated = Seq(1L, 1L)),

      AddData(inputData, "a" -> 2), // Dropped from the second `dropDuplicates`
      CheckLastBatch(),
      assertNumStateRows(total = Seq(1L, 2L), updated = Seq(0L, 1L)),

      AddData(inputData, "b" -> 1),
      CheckLastBatch("b" -> 1),
      assertNumStateRows(total = Seq(2L, 3L), updated = Seq(1L, 1L))
    )
  }

  test("SPARK-35896: metrics in StateOperatorProgress are output correctly") {
    val inputData = MemoryStream[Int]
    val result = inputData.toDS()
      .withColumn("eventTime", timestamp_seconds($"value"))
      .withWatermark("eventTime", "10 seconds")
      .dropDuplicates()
      .select($"eventTime".cast("long").as[Long])

    testStream(result, Append)(
      StartStream(additionalConfs = Map(SQLConf.SHUFFLE_PARTITIONS.key -> "2")),
      AddData(inputData, (1 to 5).flatMap(_ => (10 to 15)): _*),
      CheckAnswer(10 to 15: _*),
      assertNumStateRows(
        total = Seq(6), updated = Seq(6), droppedByWatermark = Seq(0), removed = Some(Seq(0))),

      AddData(inputData, 25), // Advance watermark to 15 secs, no-data-batch drops rows <= 15
      CheckNewAnswer(25),
      assertNumStateRows(
        total = Seq(1), updated = Seq(1), droppedByWatermark = Seq(0), removed = Some(Seq(6))),

      AddData(inputData, 10), // Should not emit anything as data less than watermark
      CheckNewAnswer(),
      assertNumStateRows(
        total = Seq(1), updated = Seq(0), droppedByWatermark = Seq(1), removed = Some(Seq(0))),

      AddData(inputData, 10),
      CheckNewAnswer(),
      assertStateOperatorProgressMetric(
        operatorName = "dedupe", numShufflePartitions = 2, numStateStoreInstances = 2)
    )
  }

  test("deduplicate with watermark") {
    val inputData = MemoryStream[Int]
    val result = inputData.toDS()
      .withColumn("eventTime", timestamp_seconds($"value"))
      .withWatermark("eventTime", "10 seconds")
      .dropDuplicates()
      .select($"eventTime".cast("long").as[Long])

    testStream(result, Append)(
      AddData(inputData, (1 to 5).flatMap(_ => (10 to 15)): _*),
      CheckAnswer(10 to 15: _*),
      assertNumStateRows(total = 6, updated = 6),

      AddData(inputData, 25), // Advance watermark to 15 secs, no-data-batch drops rows <= 15
      CheckNewAnswer(25),
      assertNumStateRows(total = 1, updated = 1),

      AddData(inputData, 10), // Should not emit anything as data less than watermark
      CheckNewAnswer(),
      assertNumStateRows(total = 1, updated = 0, droppedByWatermark = 1),

      AddData(inputData, 45), // Advance watermark to 35 seconds, no-data-batch drops row 25
      CheckNewAnswer(45),
      assertNumStateRows(total = 1, updated = 1)
    )
  }

  test("deduplicate with aggregate - append mode") {
    val inputData = MemoryStream[Int]
    val windowedaggregate = inputData.toDS()
      .withColumn("eventTime", timestamp_seconds($"value"))
      .withWatermark("eventTime", "10 seconds")
      .dropDuplicates()
      .groupBy(window($"eventTime", "5 seconds") as Symbol("window"))
      .agg(count("*") as Symbol("count"))
      .select($"window".getField("start").cast("long").as[Long], $"count".as[Long])

    testStream(windowedaggregate)(
      AddData(inputData, (1 to 5).flatMap(_ => (10 to 15)): _*),
      CheckLastBatch(),
      // states in aggregate in [10, 14), [15, 20) (2 windows)
      // states in deduplicate is 10 to 15
      assertNumStateRows(total = Seq(2L, 6L), updated = Seq(2L, 6L)),

      AddData(inputData, 25), // Advance watermark to 15 seconds
      CheckLastBatch((10 -> 5)), // 5 items (10 to 14) after deduplicate, emitted with no-data-batch
      // states in aggregate in [15, 20) and [25, 30); no-data-batch removed [10, 14)
      // states in deduplicate is 25, no-data-batch removed 10 to 14
      assertNumStateRows(total = Seq(2L, 1L), updated = Seq(1L, 1L)),

      AddData(inputData, 10), // Should not emit anything as data less than watermark
      CheckLastBatch(),
      assertNumStateRows(total = Seq(2L, 1L), updated = Seq(0L, 0L),
        droppedByWatermark = Seq(0L, 1L), None),

      AddData(inputData, 40), // Advance watermark to 30 seconds
      CheckLastBatch((15 -> 1), (25 -> 1)),
      // states in aggregate is [40, 45); no-data-batch removed [15, 20) and [25, 30)
      // states in deduplicate is 40; no-data-batch removed 25
      assertNumStateRows(total = Seq(1L, 1L), updated = Seq(1L, 1L))
    )
  }

  test("deduplicate with aggregate - update mode") {
    val inputData = MemoryStream[(String, Int)]
    val result = inputData.toDS()
      .select($"_1" as "str", $"_2" as "num")
      .dropDuplicates()
      .groupBy("str")
      .agg(sum("num"))
      .as[(String, Long)]

    testStream(result, Update)(
      AddData(inputData, "a" -> 1),
      CheckLastBatch("a" -> 1L),
      assertNumStateRows(total = Seq(1L, 1L), updated = Seq(1L, 1L)),
      AddData(inputData, "a" -> 1), // Dropped
      CheckLastBatch(),
      assertNumStateRows(total = Seq(1L, 1L), updated = Seq(0L, 0L)),
      AddData(inputData, "a" -> 2),
      CheckLastBatch("a" -> 3L),
      assertNumStateRows(total = Seq(1L, 2L), updated = Seq(1L, 1L)),
      AddData(inputData, "b" -> 1),
      CheckLastBatch("b" -> 1L),
      assertNumStateRows(total = Seq(2L, 3L), updated = Seq(1L, 1L))
    )
  }

  test("deduplicate with aggregate - complete mode") {
    val inputData = MemoryStream[(String, Int)]
    val result = inputData.toDS()
      .select($"_1" as "str", $"_2" as "num")
      .dropDuplicates()
      .groupBy("str")
      .agg(sum("num"))
      .as[(String, Long)]

    testStream(result, Complete)(
      AddData(inputData, "a" -> 1),
      CheckLastBatch("a" -> 1L),
      assertNumStateRows(total = Seq(1L, 1L), updated = Seq(1L, 1L)),
      AddData(inputData, "a" -> 1), // Dropped
      CheckLastBatch("a" -> 1L),
      assertNumStateRows(total = Seq(1L, 1L), updated = Seq(0L, 0L)),
      AddData(inputData, "a" -> 2),
      CheckLastBatch("a" -> 3L),
      assertNumStateRows(total = Seq(1L, 2L), updated = Seq(1L, 1L)),
      AddData(inputData, "b" -> 1),
      CheckLastBatch("a" -> 3L, "b" -> 1L),
      assertNumStateRows(total = Seq(2L, 3L), updated = Seq(1L, 1L))
    )
  }

  test("deduplicate with file sink") {
    withTempDir { output =>
      withTempDir { checkpointDir =>
        val outputPath = output.getAbsolutePath
        val inputData = MemoryStream[String]
        val result = inputData.toDS().dropDuplicates()
        val q = result.writeStream
          .format("parquet")
          .outputMode(Append)
          .option("checkpointLocation", checkpointDir.getPath)
          .start(outputPath)
        try {
          inputData.addData("a")
          q.processAllAvailable()
          checkDataset(spark.read.parquet(outputPath).as[String], "a")

          inputData.addData("a") // Dropped
          q.processAllAvailable()
          checkDataset(spark.read.parquet(outputPath).as[String], "a")

          inputData.addData("b")
          q.processAllAvailable()
          checkDataset(spark.read.parquet(outputPath).as[String], "a", "b")
        } finally {
          q.stop()
        }
      }
    }
  }

  test("SPARK-19841: watermarkPredicate should filter based on keys") {
    val input = MemoryStream[(Int, Int)]
    val df = input.toDS().toDF("time", "id")
      .withColumn("time", timestamp_seconds($"time"))
      .withWatermark("time", "1 second")
      .dropDuplicates("id", "time") // Change the column positions
      .select($"id")
    testStream(df)(
      AddData(input, 1 -> 1, 1 -> 1, 1 -> 2),
      CheckAnswer(1, 2),
      AddData(input, 1 -> 1, 2 -> 3, 2 -> 4),
      CheckNewAnswer(3, 4),
      AddData(input, 1 -> 0, 1 -> 1, 3 -> 5, 3 -> 6), // Drop (1 -> 0, 1 -> 1) due to watermark
      CheckNewAnswer(5, 6),
      AddData(input, 1 -> 0, 4 -> 7), // Drop (1 -> 0) due to watermark
      CheckNewAnswer(7)
    )
  }

  test("SPARK-21546: dropDuplicates should ignore watermark when it's not a key") {
    val input = MemoryStream[(Int, Int)]
    val df = input.toDS().toDF("id", "time")
      .withColumn("time", timestamp_seconds($"time"))
      .withWatermark("time", "1 second")
      .dropDuplicates("id")
      .select($"id", $"time".cast("long"))
    testStream(df)(
      AddData(input, 1 -> 1, 1 -> 2, 2 -> 2),
      CheckAnswer(1 -> 1, 2 -> 2)
    )
  }

  test("test no-data flag") {
    val flagKey = SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key

    def testWithFlag(flag: Boolean): Unit = withClue(s"with $flagKey = $flag") {
      val inputData = MemoryStream[Int]
      val result = inputData.toDS()
        .withColumn("eventTime", timestamp_seconds($"value"))
        .withWatermark("eventTime", "10 seconds")
        .dropDuplicates()
        .select($"eventTime".cast("long").as[Long])

      testStream(result, Append)(
        StartStream(additionalConfs = Map(flagKey -> flag.toString)),
        AddData(inputData, 10, 11, 12, 13, 14, 15),
        CheckAnswer(10, 11, 12, 13, 14, 15),
        assertNumStateRows(total = 6, updated = 6),

        AddData(inputData, 25), // Advance watermark to 15 seconds
        CheckNewAnswer(25),
        { // State should have been cleaned if flag is set, otherwise should not have been cleaned
          if (flag) assertNumStateRows(total = 1, updated = 1)
          else assertNumStateRows(total = 7, updated = 1)
        },
        AssertOnQuery { q =>
          eventually(timeout(streamingTimeout)) {
            q.lastProgress.sink.numOutputRows == 0L
            true
          }
        }
      )
    }

    testWithFlag(true)
    testWithFlag(false)
  }

  test("SPARK-29438: ensure UNION doesn't lead streaming deduplication to use" +
    " shifted partition IDs") {
    def constructUnionDf(desiredPartitionsForInput1: Int)
      : (MemoryStream[Int], MemoryStream[Int], DataFrame) = {
      val input1 = MemoryStream[Int](desiredPartitionsForInput1)
      val input2 = MemoryStream[Int]
      val df1 = input1.toDF().select($"value")
      val df2 = input2.toDF().dropDuplicates("value")

      // Unioned DF would have columns as (Int)
      (input1, input2, df1.union(df2))
    }

    withTempDir { checkpointDir =>
      val (input1, input2, unionDf) = constructUnionDf(2)
      testStream(unionDf, Append)(
        StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
        MultiAddData(input1, 11, 12)(input2, 21, 22),
        CheckNewAnswer(11, 12, 21, 22),
        StopStream
      )

      // We're restoring the query with different number of partitions in left side of UNION,
      // which may lead right side of union to have mismatched partition IDs (e.g. if it relies on
      // TaskContext.partitionId()). This test will verify streaming deduplication doesn't have
      // such issue.

      val (newInput1, newInput2, newUnionDf) = constructUnionDf(3)

      newInput1.addData(11, 12)
      newInput2.addData(21, 22)

      testStream(newUnionDf, Append)(
        StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
        MultiAddData(newInput1, 13, 14)(newInput2, 22, 23),
        CheckNewAnswer(13, 14, 23)
      )
    }
  }

  test("SPARK-35880: custom metric numDroppedDuplicateRows in state operator progress") {
    val dedupeInputData = MemoryStream[(String, Int)]
    val dedupe = dedupeInputData.toDS().dropDuplicates("_1")

    testStream(dedupe, Append)(
      AddData(dedupeInputData, "a" -> 1),
      CheckLastBatch("a" -> 1),
      assertStateOperatorCustomMetric("numDroppedDuplicateRows", expected = 0),

      AddData(dedupeInputData, "a" -> 2, "b" -> 3),
      CheckLastBatch("b" -> 3),
      assertStateOperatorCustomMetric("numDroppedDuplicateRows", expected = 1),

      AddData(dedupeInputData, "a" -> 5, "b" -> 2, "c" -> 9),
      CheckLastBatch("c" -> 9),
      assertStateOperatorCustomMetric("numDroppedDuplicateRows", expected = 2)
    )

    // with watermark
    val dedupeWithWMInputData = MemoryStream[Int]
    val dedupeWithWatermark = dedupeWithWMInputData.toDS()
      .withColumn("eventTime", timestamp_seconds($"value"))
      .withWatermark("eventTime", "10 seconds")
      .dropDuplicates()
      .select($"eventTime".cast("long").as[Long])

    testStream(dedupeWithWatermark, Append)(
      AddData(dedupeWithWMInputData, (1 to 5).flatMap(_ => (10 to 15)): _*),
      CheckAnswer(10 to 15: _*),
      assertStateOperatorCustomMetric("numDroppedDuplicateRows", expected = 24),

      AddData(dedupeWithWMInputData, 14),
      CheckNewAnswer(),
      assertStateOperatorCustomMetric("numDroppedDuplicateRows", expected = 1),

      // Advance watermark to 15 secs, no-data-batch drops rows <= 15
      AddData(dedupeWithWMInputData, 25),
      CheckNewAnswer(25),
      assertStateOperatorCustomMetric("numDroppedDuplicateRows", expected = 0),

      AddData(dedupeWithWMInputData, 10), // Should not emit anything as data less than watermark
      CheckNewAnswer(),
      assertStateOperatorCustomMetric("numDroppedDuplicateRows", expected = 0),

      AddData(dedupeWithWMInputData, 26, 26),
      CheckNewAnswer(26),
      assertStateOperatorCustomMetric("numDroppedDuplicateRows", expected = 1)
    )
  }

  test("SPARK-39650: duplicate with specific keys should allow input to change schema") {
    withTempDir { checkpoint =>
      val dedupeInputData = MemoryStream[(String, Int)]
      val dedupe = dedupeInputData.toDS().dropDuplicates("_1")

      testStream(dedupe, Append)(
        StartStream(checkpointLocation = checkpoint.getCanonicalPath),

        AddData(dedupeInputData, "a" -> 1),
        CheckLastBatch("a" -> 1),

        AddData(dedupeInputData, "a" -> 2, "b" -> 3),
        CheckLastBatch("b" -> 3)
      )

      val dedupeInputData2 = MemoryStream[(String, Int, String)]
      val dedupe2 = dedupeInputData2.toDS().dropDuplicates("_1")

      // initialize new memory stream with previously executed batches
      dedupeInputData2.addData(("a", 1, "dummy"))
      dedupeInputData2.addData(Seq(("a", 2, "dummy"), ("b", 3, "dummy")))

      testStream(dedupe2, Append)(
        StartStream(checkpointLocation = checkpoint.getCanonicalPath),

        AddData(dedupeInputData2, ("a", 5, "a"), ("b", 2, "b"), ("c", 9, "c")),
        CheckLastBatch(("c", 9, "c"))
      )
    }
  }

  Seq("3.3.0", "3.5.1").foreach { sparkVersion =>
    test("SPARK-39650: recovery from checkpoint having all columns as value schema " +
      s"with sparkVersion=$sparkVersion") {
      // NOTE: We are also changing the schema of input compared to the checkpoint.
      // In the checkpoint we define the input schema as (String, Int).
      val inputData = MemoryStream[(String, Int, String)]
      val dedupe = inputData.toDS().dropDuplicates("_1")

      val resourcePath = "/structured-streaming/checkpoint-version-" + sparkVersion +
        "-streaming-deduplication/"
      val resourceUri = this.getClass.getResource(resourcePath).toURI

      val checkpointDir = Utils.createTempDir().getCanonicalFile
      // Copy the checkpoint to a temp dir to prevent changes to the original.
      // Not doing this will lead to the test passing on the first run, but fail subsequent runs.
      FileUtils.copyDirectory(new File(resourceUri), checkpointDir)

      inputData.addData(("a", 1, "dummy"))
      inputData.addData(("a", 2, "dummy"), ("b", 3, "dummy"))

      testStream(dedupe, Append)(
        StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
        /*
        Note: The checkpoint was generated using the following input in Spark version 3.3.0
        AddData(inputData, ("a", 1)),
        CheckLastBatch(("a", 1)),
        AddData(inputData, ("a", 2), ("b", 3)),
        CheckLastBatch(("b", 3))
       */

        AddData(inputData, ("a", 5, "a"), ("b", 2, "b"), ("c", 9, "c")),
        CheckLastBatch(("c", 9, "c"))
      )
    }
  }

  Seq("3.3.0", "3.5.1").foreach { sparkVersion =>
    test("SPARK-39650: recovery from checkpoint with changes on key schema " +
      s"are not allowed with sparkVersion=$sparkVersion") {
      // NOTE: We are also changing the schema of input compared to the checkpoint.
      // In the checkpoint we define the input schema as (String, Int).
      val inputData = MemoryStream[(String, Int, String)]
      val dedupe = inputData.toDS().dropDuplicates("_1", "_2")

      val resourcePath = "/structured-streaming/checkpoint-version-" + sparkVersion +
        "-streaming-deduplication/"
      val resourceUri = this.getClass.getResource(resourcePath).toURI

      val checkpointDir = Utils.createTempDir().getCanonicalFile
      // Copy the checkpoint to a temp dir to prevent changes to the original.
      // Not doing this will lead to the test passing on the first run, but fail subsequent runs.
      FileUtils.copyDirectory(new File(resourceUri), checkpointDir)

      inputData.addData(("a", 1, "dummy"))
      inputData.addData(("a", 2, "dummy"), ("b", 3, "dummy"))

      // trying to evolve the key schema is not allowed and should throw an exception
      val ex = intercept[StreamingQueryException] {
        testStream(dedupe, Append)(
          StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
          AddData(inputData, ("a", 5, "a"), ("b", 2, "b"), ("c", 9, "c")),
          CheckLastBatch(("a", 5, "a"), ("b", 2, "b"), ("c", 9, "c"))
        )
      }

      // verify that the key schema not compatible error is thrown
      checkError(
        ex.getCause.asInstanceOf[SparkUnsupportedOperationException],
        condition = "STATE_STORE_KEY_SCHEMA_NOT_COMPATIBLE",
        parameters = Map("storedKeySchema" -> ".*",
          "newKeySchema" -> ".*"),
        matchPVals = true
      )
    }
  }

  test("collation aware deduplication") {
    val inputData = MemoryStream[(String, Int)]
    val result = inputData.toDF()
      .select(col("_1")
        .try_cast(StringType("UTF8_BINARY")).as("str"),
        col("_2").as("int"))
      .dropDuplicates("str")

    testStream(result, Append)(
      AddData(inputData, "a" -> 1),
      CheckLastBatch("a" -> 1),
      assertNumStateRows(total = 1, updated = 1, droppedByWatermark = 0),
      AddData(inputData, "a" -> 2), // Dropped
      CheckLastBatch(),
      assertNumStateRows(total = 1, updated = 0, droppedByWatermark = 0),
      // scalastyle:off
      AddData(inputData, "ä" -> 1),
      CheckLastBatch("ä" -> 1),
      // scalastyle:on
      assertNumStateRows(total = 2, updated = 1, droppedByWatermark = 0)
    )
  }

  test("non-binary collation aware deduplication not supported") {
    val inputData = MemoryStream[(String)]
    val result = inputData.toDF()
      .select(col("value")
        .try_cast(StringType("UTF8_LCASE")).as("str"))
      .dropDuplicates("str")

    val ex = intercept[StreamingQueryException] {
      testStream(result, Append)(
        AddData(inputData, "a"),
        CheckLastBatch("a"))
    }

    checkError(
      ex.getCause.asInstanceOf[SparkUnsupportedOperationException],
      condition = "STATE_STORE_UNSUPPORTED_OPERATION_BINARY_INEQUALITY",
      parameters = Map(
        "schema" -> ".+\"str\":\"spark.UTF8_LCASE\".+"
      ),
      matchPVals = true
    )
  }
}

@SlowSQLTest
class RocksDBStateStoreStreamingDeduplicationSuite
  extends StreamingDeduplicationSuite with RocksDBStateStoreTest
