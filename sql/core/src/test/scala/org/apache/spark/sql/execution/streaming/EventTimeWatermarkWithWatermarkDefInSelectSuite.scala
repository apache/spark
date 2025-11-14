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

import java.{util => ju}
import java.text.SimpleDateFormat

import org.scalatest.BeforeAndAfter

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.UTC
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.functions.{col, timestamp_seconds}
import org.apache.spark.sql.streaming.StateStoreMetricsTest

class EventTimeWatermarkWithWatermarkDefInSelectSuite
  extends StateStoreMetricsTest
  with BeforeAndAfter
  with Logging {

  import testImplicits._

  after {
    sqlContext.streams.active.foreach(_.stop())
  }

  test("event time and watermark metrics with watermark in select statement - case 1") {
    // All event time metrics where watermarking is set
    val inputData = MemoryStream[Int]
    val df = inputData.toDF()
      .withColumn("eventTime", timestamp_seconds(col("value")))
    df.createOrReplaceTempView("stream_src")
    val aggWithWatermark = spark.sql(
      """
        |SELECT
        |    CAST(window.start AS LONG), CAST(count(*) AS LONG) AS count
        |FROM
        |    stream_src WATERMARK eventTime DELAY OF INTERVAL 10 seconds
        |GROUP BY window(eventTime, '5 seconds')
        |""".stripMargin)

    testWindowedAggregation(inputData, aggWithWatermark)
  }

  test("event time and watermark metrics with watermark in select statement - case 2") {
    // All event time metrics where watermarking is set
    val inputData = MemoryStream[Int]
    val df = inputData.toDF()
    df.createOrReplaceTempView("stream_src")
    val aggWithWatermark = spark.sql(
      """
        |SELECT
        |    CAST(window.start AS LONG), CAST(count(*) AS LONG) AS count
        |FROM
        |    stream_src
        |    WATERMARK timestamp_seconds(value) AS eventTime DELAY OF INTERVAL 10 seconds
        |GROUP BY window(eventTime, '5 seconds')
        |""".stripMargin)

    testWindowedAggregation(inputData, aggWithWatermark)
  }

  private def testWindowedAggregation(
      inputData: MemoryStream[Int],
      dataFrame: DataFrame): Unit = {
    testStream(dataFrame)(
      AddData(inputData, 15),
      CheckAnswer(),
      assertEventStats(min = 15, max = 15, avg = 15, wtrmark = 0),
      AddData(inputData, 10, 12, 14),
      CheckAnswer(),
      assertEventStats(min = 10, max = 14, avg = 12, wtrmark = 5),
      AddData(inputData, 25),
      CheckAnswer((10, 3)),
      assertEventStats(min = 25, max = 25, avg = 25, wtrmark = 5)
    )
  }

  test("stream-stream join with watermark in select statement - case 1") {
    val leftInput = MemoryStream[(Int, Int)]
    val rightInput = MemoryStream[(Int, Int)]

    val df1 = leftInput.toDF().toDF("leftKey", "time")
      .select($"leftKey", timestamp_seconds($"time") as "leftTime",
        ($"leftKey" * 2) as "leftValue")
    val df2 = rightInput.toDF().toDF("rightKey", "time")
      .select($"rightKey", timestamp_seconds($"time") as "rightTime",
        ($"rightKey" * 3) as "rightValue")

    df1.createOrReplaceTempView("stream_left")
    df2.createOrReplaceTempView("stream_right")

    val joined = spark.sql(
      """
        |SELECT
        |    leftKey, rightKey, CAST(leftTime AS INTEGER), CAST(rightTime AS INTEGER)
        |FROM
        |    stream_left WATERMARK leftTime DELAY OF INTERVAL 0 second
        |FULL OUTER JOIN
        |    stream_right WATERMARK rightTime DELAY OF INTERVAL 0 second
        |ON
        |    leftKey = rightKey AND leftTime BETWEEN rightTime - INTERVAL 5 SECONDS
        |    AND rightTime + INTERVAL 5 SECONDS
        |""".stripMargin)

    testStreamStreamTimeIntervalJoin(leftInput, rightInput, joined)
  }

  test("stream-stream join with watermark in select statement - case 2") {
    val leftInput = MemoryStream[(Int, Int)]
    val rightInput = MemoryStream[(Int, Int)]

    val df1 = leftInput.toDF().toDF("leftKey", "time")
    val df2 = rightInput.toDF().toDF("rightKey", "time")

    df1.createOrReplaceTempView("stream_left")
    df2.createOrReplaceTempView("stream_right")

    val joined = spark.sql(
      """
        |SELECT
        |    leftKey, rightKey, CAST(leftTime AS INTEGER), CAST(rightTime AS INTEGER)
        |FROM
        |(
        |    SELECT
        |        leftKey, leftTime, leftKey * 2 AS leftValue
        |    FROM
        |        stream_left
        |        WATERMARK timestamp_seconds(time) AS leftTime DELAY OF INTERVAL 0 second
        |)
        |FULL OUTER JOIN
        |(
        |    SELECT
        |        rightKey, rightTime, rightKey * 3 AS rightValue
        |    FROM
        |        stream_right
        |        WATERMARK timestamp_seconds(time) AS rightTime DELAY OF INTERVAL 0 second
        |)
        |ON
        |    leftKey = rightKey AND leftTime BETWEEN rightTime - INTERVAL 5 SECONDS
        |    AND rightTime + INTERVAL 5 SECONDS
        |""".stripMargin)

    testStreamStreamTimeIntervalJoin(leftInput, rightInput, joined)
  }

  private def testStreamStreamTimeIntervalJoin(
      leftInput: MemoryStream[(Int, Int)],
      rightInput: MemoryStream[(Int, Int)],
      dataFrame: DataFrame): Unit = {
    testStream(dataFrame)(
      AddData(leftInput, (1, 5), (3, 5)),
      CheckNewAnswer(),
      // states
      // left: (1, 5), (3, 5)
      // right: nothing
      assertNumStateRows(total = 2, updated = 2),
      AddData(rightInput, (1, 10), (2, 5)),
      // Match left row in the state.
      CheckNewAnswer(Row(1, 1, 5, 10)),
      // states
      // left: (1, 5), (3, 5)
      // right: (1, 10), (2, 5)
      assertNumStateRows(total = 4, updated = 2),
      AddData(rightInput, (1, 9)),
      // Match left row in the state.
      CheckNewAnswer(Row(1, 1, 5, 9)),
      // states
      // left: (1, 5), (3, 5)
      // right: (1, 10), (2, 5), (1, 9)
      assertNumStateRows(total = 5, updated = 1),
      // Increase event time watermark to 20s by adding data with time = 30s on both inputs.
      AddData(leftInput, (1, 7), (1, 30)),
      CheckNewAnswer(Row(1, 1, 7, 9), Row(1, 1, 7, 10)),
      // states
      // left: (1, 5), (3, 5), (1, 7), (1, 30)
      // right: (1, 10), (2, 5), (1, 9)
      assertNumStateRows(total = 7, updated = 2),
      // Watermark = 30 - 10 = 20, no matched row.
      // Generate outer join result for all non-matched rows when the watermark advances.
      AddData(rightInput, (0, 30)),
      CheckNewAnswer(Row(3, null, 5, null), Row(null, 2, null, 5)),
      // states
      // left: (1, 30)
      // right: (0, 30)
      //
      // states evicted
      // left: (1, 5), (3, 5), (1, 5) (below watermark = 20)
      // right: (1, 10), (2, 5), (1, 9) (below watermark = 20)
      assertNumStateRows(total = 2, updated = 1)
    )
  }

  test("stream-batch join followed by time window aggregation") {
    val inputData = MemoryStream[Int]
    val df = inputData.toDF()
      .withColumn("eventTime", timestamp_seconds(col("value")))
    df.createOrReplaceTempView("stream_src")

    val batchDf = spark.range(0, 50).map { i =>
      if (i % 2 == 0) (i, "even") else (i, "odd")
    }.toDF("value", "batch_value")
    batchDf.createOrReplaceTempView("batch_src")

    val agg = spark.sql(
      """
        |SELECT
        |    CAST(window.start AS LONG), batch_value, CAST(count(*) AS LONG) AS count
        |FROM
        |    stream_src WATERMARK eventTime DELAY OF INTERVAL 10 seconds
        |JOIN
        |    batch_src
        |ON
        |    stream_src.value = batch_src.value
        |GROUP BY batch_src.batch_value, window(eventTime, '5 seconds')
        |""".stripMargin)

    testStream(agg)(
      AddData(inputData, 15),
      CheckAnswer(),
      AddData(inputData, 10, 11, 14),
      CheckAnswer(),
      AddData(inputData, 25),
      CheckAnswer((10, "even", 2), (10, "odd", 1))
    )
  }

  /** Assert event stats generated on that last batch with data in it */
  private def assertEventStats(body: ju.Map[String, String] => Unit): AssertOnQuery = {
    Execute("AssertEventStats") { q =>
      body(q.recentProgress.filter(_.numInputRows > 0).lastOption.get.eventTime)
    }
  }

  /** Assert event stats generated on that last batch with data in it */
  private def assertEventStats(min: Long, max: Long, avg: Double, wtrmark: Long): AssertOnQuery = {
    assertEventStats { e =>
      assert(e.get("min") === formatTimestamp(min), s"min value mismatch")
      assert(e.get("max") === formatTimestamp(max), s"max value mismatch")
      assert(e.get("avg") === formatTimestamp(avg.toLong), s"avg value mismatch")
      assert(e.get("watermark") === formatTimestamp(wtrmark), s"watermark value mismatch")
    }
  }

  private val timestampFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") // ISO8601
  timestampFormat.setTimeZone(ju.TimeZone.getTimeZone(UTC))

  private def formatTimestamp(sec: Long): String = {
    timestampFormat.format(new ju.Date(sec * 1000))
  }
}
