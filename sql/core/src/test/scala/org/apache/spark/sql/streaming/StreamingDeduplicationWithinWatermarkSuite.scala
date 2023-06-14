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
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.timestamp_seconds
import org.apache.spark.tags.SlowSQLTest

@SlowSQLTest
class StreamingDeduplicationWithinWatermarkSuite extends StateStoreMetricsTest {

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
}
