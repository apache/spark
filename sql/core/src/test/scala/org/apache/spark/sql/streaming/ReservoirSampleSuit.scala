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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes._
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.state.StateStore

class ReservoirSampleSuit extends StateStoreMetricsTest with BeforeAndAfterAll {

  import testImplicits._

  override def afterAll(): Unit = {
    super.afterAll()
    StateStore.stop()
  }

  test("streaming reservoir sample: reservoir size is larger than stream data size - update mode") {
    val inputData = MemoryStream[String]
    val result = inputData.toDS().reservoir(4)

    testStream(result, Update)(
      AddData(inputData, "a", "b"),
      CheckAnswer(Row("a"), Row("b")),
      AddData(inputData, "a"),
      CheckAnswer(Row("a"), Row("b"), Row("a"))
    )
  }

  test("streaming reservoir sample: reservoir size is less than stream data size - update mode") {
    val inputData = MemoryStream[String]
    val result = inputData.toDS().reservoir(1)

    testStream(result, Update)(
      AddData(inputData, "a", "a"),
      CheckLastBatch(Row("a")),
      AddData(inputData, "b", "b", "b", "b", "b", "b", "b", "b"),
      CheckLastBatch(Row("b"))
    )
  }

  test("streaming reservoir sample with aggregation - update mode") {
    val inputData = MemoryStream[String]
    val result = inputData.toDS().reservoir(3).groupBy("value").count()

    testStream(result, Update)(
      AddData(inputData, "a"),
      CheckAnswer(Row("a", 1)),
      AddData(inputData, "b"),
      CheckAnswer(Row("a", 1), Row("b", 1))
    )
  }

  test("streaming reservoir sample with watermark") {
    val inputData = MemoryStream[Int]
    val result = inputData.toDS()
      .withColumn("eventTime", $"value".cast("timestamp"))
      .withWatermark("eventTime", "10 seconds")
      .reservoir(10)
      .select($"eventTime".cast("long").as[Long])

    testStream(result, Update)(
      AddData(inputData, (1 to 1).flatMap(_ => (11 to 15)): _*),
      CheckLastBatch(11 to 15: _*),
      AddData(inputData, 25), // Advance watermark to 15 seconds
      CheckLastBatch(25),
      AddData(inputData, 25), // Drop states less than watermark
      CheckLastBatch(25),
      AddData(inputData, 10), // Should not emit anything as data less than watermark
      CheckLastBatch(),
      AddData(inputData, 45), // Advance watermark to 35 seconds
      CheckLastBatch(45),
      AddData(inputData, 25), // Should not emit anything as data less than watermark
      CheckLastBatch()
    )
  }

  test("streaming reservoir sample with aggregation - complete mode") {
    val inputData = MemoryStream[(String, Int)]
    val result = inputData.toDS().select($"_1" as "key", $"_2" as "value")
      .reservoir(3).groupBy("key").max("value")

    testStream(result, Complete)(
      AddData(inputData, ("a", 1)),
      CheckAnswer(Row("a", 1)),
      AddData(inputData, ("b", 2)),
      CheckAnswer(Row("a", 1), Row("b", 2)),
      StopStream,
      StartStream(),
      AddData(inputData, ("a", 10)),
      CheckAnswer(Row("a", 10), Row("b", 2)),
      AddData(inputData, (1 to 10).map(e => ("c", 100)): _*),
      CheckAnswer(Row("a", 10), Row("b", 2), Row("c", 100))
    )
  }

  test("batch reservoir sample") {
    val df = spark.createDataset(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 0))
    assert(df.reservoir(3).count() == 3, "")
  }

  test("batch reservoir sample after aggregation") {
    val df = spark.createDataset((1 to 10).map(e => (e, s"val_$e")))
        .select($"_1" as "key", $"_2" as "value")
        .groupBy("value").count()
    assert(df.reservoir(3).count() == 3, "")
  }

  test("batch reservoir sample before aggregation") {
    val df = spark.createDataset((1 to 10).map(e => (e, s"val_$e")))
      .select($"_1" as "key", $"_2" as "value")
      .reservoir(3)
      .groupBy("value").count()
    assert(df.count() == 3, "")
  }
}
