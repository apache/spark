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

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.catalyst.streaming.InternalOutputModes._
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.state.StateStore
import org.apache.spark.sql.functions._

class DeduplicationSuite extends StreamTest with BeforeAndAfterAll {

  import testImplicits._

  override def afterAll(): Unit = {
    super.afterAll()
    StateStore.stop()
  }

  test("deduplication - complete") {
    val inputData = MemoryStream[String]
    val result = inputData.toDS().dropDuplicates()

    testStream(result, Complete)(
      AddData(inputData, "a"),
      CheckLastBatch("a"),
      assertNumStateRows(total = 1, updated = 1),
      AddData(inputData, "a"),
      CheckLastBatch("a"),
      assertNumStateRows(total = 1, updated = 0),
      AddData(inputData, "b"),
      CheckLastBatch("a", "b"),
      assertNumStateRows(total = 2, updated = 1)
    )
  }

  test("deduplication - append/update") {
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

  test("deduplication with columns - complete") {
    val inputData = MemoryStream[(String, Int)]
    val result = inputData.toDS().dropDuplicates("_1")

    testStream(result, Complete)(
      AddData(inputData, "a" -> 1),
      CheckLastBatch("a" -> 1),
      assertNumStateRows(total = 1, updated = 1),
      AddData(inputData, "a" -> 2), // Dropped
      CheckLastBatch("a" -> 1),
      assertNumStateRows(total = 1, updated = 0),
      AddData(inputData, "b" -> 1),
      CheckLastBatch("a" -> 1, "b" -> 1),
      assertNumStateRows(total = 2, updated = 1)
    )
  }

  test("deduplication with columns - append/update") {
    val inputData = MemoryStream[(String, Int)]
    val result = inputData.toDS().dropDuplicates("_1")

    testStream(result, Append)(
      AddData(inputData, "a" -> 1),
      CheckLastBatch("a" -> 1),
      assertNumStateRows(total = 1, updated = 1),
      AddData(inputData, "a" -> 2), // Dropped
      CheckLastBatch(),
      assertNumStateRows(total = 1, updated = 0),
      AddData(inputData, "b" -> 1),
      CheckLastBatch("b" -> 1),
      assertNumStateRows(total = 2, updated = 1)
    )
  }

  test("deduplication with watermark - append/update") {
    val inputData = MemoryStream[Int]
    val result = inputData.toDS()
      .withColumn("eventTime", $"value".cast("timestamp"))
      .withWatermark("eventTime", "10 seconds")
      .dropDuplicates()
      .select($"eventTime".cast("long").as[Long])

    testStream(result, Append)(
      AddData(inputData, (1 to 5).flatMap(_ => (10 to 15)): _*),
      CheckLastBatch(10 to 15: _*),
      assertNumStateRows(total = 6, updated = 6),

      AddData(inputData, 25), // Advance watermark to 15 seconds
      CheckLastBatch(25),
      assertNumStateRows(total = 7, updated = 1),

      AddData(inputData, 25), // Drop states less than watermark
      CheckLastBatch(),
      assertNumStateRows(total = 1, updated = 0),

      AddData(inputData, 10), // Should not emit anything as data less than watermark
      CheckLastBatch(),
      assertNumStateRows(total = 1, updated = 0),

      AddData(inputData, 45), // Advance watermark to 35 seconds
      CheckLastBatch(45),
      assertNumStateRows(total = 2, updated = 1),

      AddData(inputData, 45), // Drop states less than watermark
      CheckLastBatch(),
      assertNumStateRows(total = 1, updated = 0)
    )
  }

  test("deduplication with aggregation - append") {
    val inputData = MemoryStream[Int]
    val windowedAggregation = inputData.toDS()
      .withColumn("eventTime", $"value".cast("timestamp"))
      .withWatermark("eventTime", "10 seconds")
      .dropDuplicates()
      .withWatermark("eventTime", "10 seconds")
      .groupBy(window($"eventTime", "5 seconds") as 'window)
      .agg(count("*") as 'count)
      .select($"window".getField("start").cast("long").as[Long], $"count".as[Long])

    testStream(windowedAggregation)(
      AddData(inputData, (1 to 5).flatMap(_ => (10 to 15)): _*),
      CheckLastBatch(),
      // states in aggregation in [10, 14), [15, 20) (2 windows)
      // states in deduplication is 10 to 15
      assertNumStateRows(total = Seq(2L, 6L), updated = Seq(2L, 6L)),

      AddData(inputData, 25), // Advance watermark to 15 seconds
      CheckLastBatch(),
      // states in aggregation in [10, 14), [15, 20) and [25, 30) (3 windows)
      // states in deduplication is 10 to 15 and 25
      assertNumStateRows(total = Seq(3L, 7L), updated = Seq(1L, 1L)),

      AddData(inputData, 25), // Emit items less than watermark and drop their state
      CheckLastBatch((10 -> 5)), // 5 items (10 to 14) after deduplication
      // states in aggregation in [15, 20) and [25, 30) (2 windows, note aggregation uses the end of
      // window to evict items, so [15, 20) is still in the state store)
      // states in deduplication is 25
      assertNumStateRows(total = Seq(2L, 1L), updated = Seq(0L, 0L)),

      AddData(inputData, 10), // Should not emit anything as data less than watermark
      CheckLastBatch(),
      assertNumStateRows(total = Seq(2L, 1L), updated = Seq(0L, 0L)),

      AddData(inputData, 40), // Advance watermark to 30 seconds
      CheckLastBatch(),
      // states in aggregation in [15, 20), [25, 30) and [40, 45)
      // states in deduplication is 25 and 40,
      assertNumStateRows(total = Seq(3L, 2L), updated = Seq(1L, 1L)),

      AddData(inputData, 40), // Emit items less than watermark and drop their state
      CheckLastBatch((15 -> 1), (25 -> 1)),
        // states in aggregation in [40, 45)
      // states in deduplication is 40,
      assertNumStateRows(total = Seq(1L, 1L), updated = Seq(0L, 0L))
    )
  }

  test("deduplication with aggregation - update") {
    val inputData = MemoryStream[(String, Int)]
    val result = inputData.toDS()
      .dropDuplicates()
      .groupBy($"_1")
      .agg(sum($"_2"))
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

  private def assertNumStateRows(total: Seq[Long], updated: Seq[Long]): AssertOnQuery =
    AssertOnQuery { q =>
      val progressWithData = q.recentProgress.filter(_.numInputRows > 0).lastOption.get
      assert(
        progressWithData.stateOperators.map(_.numRowsTotal) === total,
        "incorrect total rows")
      assert(
        progressWithData.stateOperators.map(_.numRowsUpdated) === updated,
        "incorrect updates rows")
      true
    }

  private def assertNumStateRows(total: Long, updated: Long): AssertOnQuery =
    assertNumStateRows(Seq(total), Seq(updated))
}
