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

import java.sql.Timestamp

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.streaming.{MemoryStream, StateStoreSaveExec, StreamingSymmetricHashJoinExec}
import org.apache.spark.sql.execution.streaming.state.StateStore
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.tags.SlowSQLTest

// Tests for the multiple stateful operators support.
@SlowSQLTest
class MultiStatefulOperatorsSuite
  extends StreamTest with StateStoreMetricsTest with BeforeAndAfter {

  import testImplicits._

  before {
    SparkSession.setActiveSession(spark) // set this before force initializing 'joinExec'
    spark.streams.stateStoreCoordinator // initialize the lazy coordinator
  }

  after {
    StateStore.stop()
  }

  test("window agg -> window agg, append mode") {
    val inputData = MemoryStream[Int]

    val stream = inputData.toDF()
      .withColumn("eventTime", timestamp_seconds($"value"))
      .withWatermark("eventTime", "0 seconds")
      .groupBy(window($"eventTime", "5 seconds").as("window"))
      .agg(count("*").as("count"))
      .groupBy(window($"window", "10 seconds"))
      .agg(count("*").as("count"), sum("count").as("sum"))
      .select($"window".getField("start").cast("long").as[Long],
        $"count".as[Long], $"sum".as[Long])

    testStream(stream)(
      AddData(inputData, 10 to 21: _*),
      // op1 W (0, 0)
      // agg: [10, 15) 5, [15, 20) 5, [20, 25) 2
      // output: None
      // state: [10, 15) 5, [15, 20) 5, [20, 25) 2
      // op2 W (0, 0)
      // agg: None
      // output: None
      // state: None

      // no-data batch triggered

      // op1 W (0, 21)
      // agg: None
      // output: [10, 15) 5, [15, 20) 5
      // state: [20, 25) 2
      // op2 W (0, 21)
      // agg: [10, 20) (2, 10)
      // output: [10, 20) (2, 10)
      // state: None
      CheckNewAnswer((10, 2, 10)),
      assertNumStateRows(Seq(0, 1)),
      assertNumRowsDroppedByWatermark(Seq(0, 0)),

      AddData(inputData, 10 to 29: _*),
      // op1 W (21, 21)
      // agg: [10, 15) 5 - late, [15, 20) 5 - late, [20, 25) 5, [25, 30) 5
      // output: None
      // state: [20, 25) 7, [25, 30) 5
      // op2 W (21, 21)
      // agg: None
      // output: None
      // state: None

      // no-data batch triggered

      // op1 W (21, 29)
      // agg: None
      // output: [20, 25) 7
      // state: [25, 30) 5
      // op2 W (21, 29)
      // agg: [20, 30) (1, 7)
      // output: None
      // state: [20, 30) (1, 7)
      CheckNewAnswer(),
      assertNumStateRows(Seq(1, 1)),
      assertNumRowsDroppedByWatermark(Seq(0, 2)),

      // Move the watermark.
      AddData(inputData, 30, 31),
      // op1 W (29, 29)
      // agg: [30, 35) 2
      // output: None
      // state: [25, 30) 5 [30, 35) 2
      // op2 W (29, 29)
      // agg: None
      // output: None
      // state: [20, 30) (1, 7)

      // no-data batch triggered

      // op1 W (29, 31)
      // agg: None
      // output: [25, 30) 5
      // state: [30, 35) 2
      // op2 W (29, 31)
      // agg: [20, 30) (2, 12)
      // output: [20, 30) (2, 12)
      // state: None
      CheckNewAnswer((20, 2, 12)),
      assertNumStateRows(Seq(0, 1)),
      assertNumRowsDroppedByWatermark(Seq(0, 0))
    )
  }

  test("agg -> agg -> agg, append mode") {
    val inputData = MemoryStream[Int]

    val stream = inputData.toDF()
      .withColumn("eventTime", timestamp_seconds($"value"))
      .withWatermark("eventTime", "0 seconds")
      .groupBy(window($"eventTime", "5 seconds").as("window"))
      .agg(count("*").as("count"))
      .groupBy(window(window_time($"window"), "10 seconds"))
      .agg(count("*").as("count"), sum("count").as("sum"))
      .groupBy(window(window_time($"window"), "20 seconds"))
      .agg(count("*").as("count"), sum("sum").as("sum"))
      .select(
        $"window".getField("start").cast("long").as[Long],
        $"window".getField("end").cast("long").as[Long],
        $"count".as[Long], $"sum".as[Long])

    testStream(stream)(
      AddData(inputData, 0 to 37: _*),
      // op1 W (0, 0)
      // agg: [0, 5) 5, [5, 10) 5, [10, 15) 5, [15, 20) 5, [20, 25) 5, [25, 30) 5, [30, 35) 5,
      //   [35, 40) 3
      // output: None
      // state: [0, 5) 5, [5, 10) 5, [10, 15) 5, [15, 20) 5, [20, 25) 5, [25, 30) 5, [30, 35) 5,
      //   [35, 40) 3
      // op2 W (0, 0)
      // agg: None
      // output: None
      // state: None
      // op3 W (0, 0)
      // agg: None
      // output: None
      // state: None

      // no-data batch triggered

      // op1 W (0, 37)
      // agg: None
      // output: [0, 5) 5, [5, 10) 5, [10, 15) 5, [15, 20) 5, [20, 25) 5, [25, 30) 5, [30, 35) 5
      // state: [35, 40) 3
      // op2 W (0, 37)
      // agg: [0, 10) (2, 10), [10, 20) (2, 10), [20, 30) (2, 10), [30, 40) (1, 5)
      // output: [0, 10) (2, 10), [10, 20) (2, 10), [20, 30) (2, 10)
      // state: [30, 40) (1, 5)
      // op3 W (0, 37)
      // agg: [0, 20) (2, 20), [20, 40) (1, 10)
      // output: [0, 20) (2, 20)
      // state: [20, 40) (1, 10)
      CheckNewAnswer((0, 20, 2, 20)),
      assertNumStateRows(Seq(1, 1, 1)),
      assertNumRowsDroppedByWatermark(Seq(0, 0, 0)),

      AddData(inputData, 30 to 60: _*),
      // op1 W (37, 37)
      // dropped rows: [30, 35), 1 row <= note that 35, 36, 37 are still in effect
      // agg: [35, 40) 8, [40, 45) 5, [45, 50) 5, [50, 55) 5, [55, 60) 5, [60, 65) 1
      // output: None
      // state: [35, 40) 8, [40, 45) 5, [45, 50) 5, [50, 55) 5, [55, 60) 5, [60, 65) 1
      // op2 W (37, 37)
      // output: None
      // state: [30, 40) (1, 5)
      // op3 W (37, 37)
      // output: None
      // state: [20, 40) (1, 10)

      // no-data batch
      // op1 W (37, 60)
      // output: [35, 40) 8, [40, 45) 5, [45, 50) 5, [50, 55) 5, [55, 60) 5
      // state: [60, 65) 1
      // op2 W (37, 60)
      // agg: [30, 40) (2, 13), [40, 50) (2, 10), [50, 60), (2, 10)
      // output: [30, 40) (2, 13), [40, 50) (2, 10), [50, 60), (2, 10)
      // state: None
      // op3 W (37, 60)
      // agg: [20, 40) (2, 23), [40, 60) (2, 20)
      // output: [20, 40) (2, 23), [40, 60) (2, 20)
      // state: None

      CheckNewAnswer((20, 40, 2, 23), (40, 60, 2, 20)),
      assertNumStateRows(Seq(0, 0, 1)),
      assertNumRowsDroppedByWatermark(Seq(0, 0, 1))
    )
  }

  test("stream deduplication -> aggregation, append mode") {
    val inputData = MemoryStream[Int]

    val deduplication = inputData.toDF()
      .withColumn("eventTime", timestamp_seconds($"value"))
      .withWatermark("eventTime", "10 seconds")
      .dropDuplicates("value", "eventTime")

    val windowedAggregation = deduplication
      .groupBy(window($"eventTime", "5 seconds").as("window"))
      .agg(count("*").as("count"), sum("value").as("sum"))
      .select($"window".getField("start").cast("long").as[Long],
        $"count".as[Long])

    testStream(windowedAggregation)(
      AddData(inputData, 1 to 15: _*),
      // op1 W (0, 0)
      // input: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
      // deduplicated: None
      // output: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
      // state: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
      // op2 W (0, 0)
      // agg: [0, 5) 4, [5, 10) 5 [10, 15) 5, [15, 20) 1
      // output: None
      // state: [0, 5) 4, [5, 10) 5 [10, 15) 5, [15, 20) 1

      // no-data batch triggered

      // op1 W (0, 5)
      // agg: None
      // output: None
      // state: 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
      // op2 W (0, 5)
      // agg: None
      // output: [0, 5) 4
      // state: [5, 10) 5 [10, 15) 5, [15, 20) 1
      CheckNewAnswer((0, 4)),
      assertNumStateRows(Seq(3, 10)),
      assertNumRowsDroppedByWatermark(Seq(0, 0))
    )
  }

  test("join -> window agg, append mode") {
    val input1 = MemoryStream[Int]
    val inputDF1 = input1.toDF()
      .withColumnRenamed("value", "value1")
      .withColumn("eventTime1", timestamp_seconds($"value1"))
      .withWatermark("eventTime1", "0 seconds")

    val input2 = MemoryStream[Int]
    val inputDF2 = input2.toDF()
      .withColumnRenamed("value", "value2")
      .withColumn("eventTime2", timestamp_seconds($"value2"))
      .withWatermark("eventTime2", "0 seconds")

    val stream = inputDF1.join(inputDF2, expr("eventTime1 = eventTime2"), "inner")
      .groupBy(window($"eventTime1", "5 seconds").as("window"))
      .agg(count("*").as("count"))
      .select($"window".getField("start").cast("long").as[Long], $"count".as[Long])

    testStream(stream)(
      MultiAddData(input1, 1 to 4: _*)(input2, 1 to 4: _*),

      // op1 W (0, 0)
      // join output: (1, 1), (2, 2), (3, 3), (4, 4)
      // state: (1, 1), (2, 2), (3, 3), (4, 4)
      // op2 W (0, 0)
      // agg: [0, 5) 4
      // output: None
      // state: [0, 5) 4

      // no-data batch triggered

      // op1 W (0, 4)
      // join output: None
      // state: None
      // op2 W (0, 4)
      // agg: None
      // output: None
      // state: [0, 5) 4
      CheckNewAnswer(),
      assertNumStateRows(Seq(1, 0)),
      assertNumRowsDroppedByWatermark(Seq(0, 0)),

      // Move the watermark
      MultiAddData(input1, 5)(input2, 5),

      // op1 W (4, 4)
      // join output: (5, 5)
      // state: (5, 5)
      // op2 W (4, 4)
      // agg: [5, 10) 1
      // output: None
      // state: [0, 5) 4, [5, 10) 1

      // no-data batch triggered

      // op1 W (4, 5)
      // join output: None
      // state: None
      // op2 W (4, 5)
      // agg: None
      // output: [0, 5) 4
      // state: [5, 10) 1
      CheckNewAnswer((0, 4)),
      assertNumStateRows(Seq(1, 0)),
      assertNumRowsDroppedByWatermark(Seq(0, 0))
    )
  }

  test("aggregation -> stream deduplication, append mode") {
    val inputData = MemoryStream[Int]

    val aggStream = inputData.toDF()
      .withColumn("eventTime", timestamp_seconds($"value"))
      .withWatermark("eventTime", "0 seconds")
      .groupBy(window($"eventTime", "5 seconds").as("window"))
      .agg(count("*").as("count"))
      .withColumn("windowEnd", expr("window.end"))

    // dropDuplicates from aggStream without event time column for dropDuplicates - the
    // state does not get trimmed due to watermark advancement.
    val dedupNoEventTime = aggStream
      .dropDuplicates("count", "windowEnd")
      .select(
        $"windowEnd".cast("long").as[Long],
        $"count".as[Long])

    testStream(dedupNoEventTime)(
      AddData(inputData, 1, 5, 10, 15),

      // op1 W (0, 0)
      // agg: [0, 5) 1, [5, 10) 1, [10, 15) 1, [15, 20) 1
      // output: None
      // state: [0, 5) 1, [5, 10) 1, [10, 15) 1, [15, 20) 1
      // op2 W (0, 0)
      // output: None
      // state: None

      // no-data batch triggered

      // op1 W (0, 15)
      // agg: None
      // output: [0, 5) 1, [5, 10) 1, [10, 15) 1
      // state: [15, 20) 1
      // op2 W (0, 15)
      // output: (5, 1), (10, 1), (15, 1)
      // state: (5, 1), (10, 1), (15, 1)

      CheckNewAnswer((5, 1), (10, 1), (15, 1)),
      assertNumStateRows(Seq(3, 1)),
      assertNumRowsDroppedByWatermark(Seq(0, 0))
    )

    // Similar to the above but add event time. The dedup state will get trimmed.
    val dedupWithEventTime = aggStream
      .withColumn("windowTime", expr("window_time(window)"))
      .withColumn("windowTimeMicros", expr("unix_micros(windowTime)"))
      .dropDuplicates("count", "windowEnd", "windowTime")
      .select(
        $"windowEnd".cast("long").as[Long],
        $"windowTimeMicros".cast("long").as[Long],
        $"count".as[Long])

    testStream(dedupWithEventTime)(
      AddData(inputData, 1, 5, 10, 15),

      // op1 W (0, 0)
      // agg: [0, 5) 1, [5, 10) 1, [10, 15) 1, [15, 20) 1
      // output: None
      // state: [0, 5) 1, [5, 10) 1, [10, 15) 1, [15, 20) 1
      // op2 W (0, 0)
      // output: None
      // state: None

      // no-data batch triggered

      // op1 W (0, 15)
      // agg: None
      // output: [0, 5) 1, [5, 10) 1, [10, 15) 1
      // state: [15, 20) 1
      // op2 W (0, 15)
      // output: (5, 4999999, 1), (10, 9999999, 1), (15, 14999999, 1)
      // state: None - trimmed by watermark

      CheckNewAnswer((5, 4999999, 1), (10, 9999999, 1), (15, 14999999, 1)),
      assertNumStateRows(Seq(0, 1)),
      assertNumRowsDroppedByWatermark(Seq(0, 0))
    )
  }

  test("join with range join on non-time intervals -> window agg, append mode, shouldn't fail") {
    val input1 = MemoryStream[Int]
    val inputDF1 = input1.toDF()
      .withColumnRenamed("value", "value1")
      .withColumn("eventTime1", timestamp_seconds($"value1"))
      .withColumn("v1", timestamp_seconds($"value1"))
      .withWatermark("eventTime1", "0 seconds")

    val input2 = MemoryStream[(Int, Int)]
    val inputDF2 = input2.toDS().toDF("start", "end")
      .withColumn("eventTime2Start", timestamp_seconds($"start"))
      .withColumn("start2", timestamp_seconds($"start"))
      .withColumn("end2", timestamp_seconds($"end"))
      .withWatermark("eventTime2Start", "0 seconds")

    val stream = inputDF1.join(inputDF2,
      expr("v1 >= start2 AND v1 < end2 " +
        "AND eventTime1 = start2"), "inner")
      .groupBy(window($"eventTime1", "5 seconds") as Symbol("window"))
      .agg(count("*") as Symbol("count"))
      .select($"window".getField("start").cast("long").as[Long], $"count".as[Long])

    testStream(stream)(
      AddData(input1, 1, 2, 3, 4),
      AddData(input2, (1, 2), (2, 3), (3, 4), (4, 5)),
      CheckNewAnswer(),
      assertNumStateRows(Seq(1, 0)),
      assertNumRowsDroppedByWatermark(Seq(0, 0))
    )
  }

  test("stream-stream time interval left outer join -> aggregation, append mode") {
    // This test performs stream-stream time interval left outer join against two streams, and
    // applies tumble time window aggregation based on the event time column from the output of
    // stream-stream join.
    val input1 = MemoryStream[(String, Timestamp)]
    val input2 = MemoryStream[(String, Timestamp)]

    val s1 = input1.toDF()
      .toDF("id1", "timestamp1")
      .withWatermark("timestamp1", "0 seconds")
      .as("s1")

    val s2 = input2.toDF()
      .toDF("id2", "timestamp2")
      .withWatermark("timestamp2", "0 seconds")
      .as("s2")

    val s3 = s1.join(s2, expr("s1.id1 = s2.id2 AND (s1.timestamp1 BETWEEN " +
      "s2.timestamp2 - INTERVAL 1 hour AND s2.timestamp2 + INTERVAL 1 hour)"), "leftOuter")

    val agg = s3.groupBy(window($"timestamp1", "10 minutes"))
      .agg(count("*").as("cnt"))
      .selectExpr("CAST(window.start AS STRING) AS window_start",
        "CAST(window.end AS STRING) AS window_end", "cnt")

    // for ease of verification, we change the session timezone to UTC
    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      testStream(agg)(
        MultiAddData(
          (input1, Seq(
            ("1", Timestamp.valueOf("2023-01-01 01:00:10")),
            ("2", Timestamp.valueOf("2023-01-01 01:00:30")))
          ),
          (input2, Seq(
            ("1", Timestamp.valueOf("2023-01-01 01:00:20"))))
        ),

        // < data batch >
        // global watermark (0, 0)
        // op1 (join)
        // -- IW (0, 0)
        // -- OW 0
        // -- left state
        // ("1", "2023-01-01 01:00:10", matched=true)
        // ("1", "2023-01-01 01:00:30", matched=false)
        // -- right state
        // ("1", "2023-01-01 01:00:20")
        // -- result
        // ("1", "2023-01-01 01:00:10", "1", "2023-01-01 01:00:20")
        // op2 (aggregation)
        // -- IW (0, 0)
        // -- OW 0
        // -- state row
        // ("2023-01-01 01:00:00", "2023-01-01 01:10:00", 1)
        // -- result
        // None

        // -- watermark calculation
        // watermark in left input: 2023-01-01 01:00:30
        // watermark in right input: 2023-01-01 01:00:20
        // origin watermark: 2023-01-01 01:00:20

        // < no-data batch >
        // global watermark (0, 2023-01-01 01:00:20)
        // op1 (join)
        // -- IW (0, 2023-01-01 01:00:20)
        // -- OW 2023-01-01 00:00:19.999999
        // -- left state
        // ("1", "2023-01-01 01:00:10", matched=true)
        // ("1", "2023-01-01 01:00:30", matched=false)
        // -- right state
        // ("1", "2023-01-01 01:00:20")
        // -- result
        // None
        // op2 (aggregation)
        // -- IW (0, 2023-01-01 00:00:19.999999)
        // -- OW 2023-01-01 00:00:19.999999
        // -- state row
        // ("2023-01-01 01:00:00", "2023-01-01 01:10:00", 1)
        // -- result
        // None
        CheckAnswer(),

        Execute { query =>
          val lastExecution = query.lastExecution
          val joinOperator = lastExecution.executedPlan.collect {
            case j: StreamingSymmetricHashJoinExec => j
          }.head
          val aggSaveOperator = lastExecution.executedPlan.collect {
            case j: StateStoreSaveExec => j
          }.head

          assert(joinOperator.eventTimeWatermarkForLateEvents === Some(0))
          assert(joinOperator.eventTimeWatermarkForEviction ===
            Some(Timestamp.valueOf("2023-01-01 01:00:20").getTime))

          assert(aggSaveOperator.eventTimeWatermarkForLateEvents === Some(0))
          assert(aggSaveOperator.eventTimeWatermarkForEviction ===
            Some(Timestamp.valueOf("2023-01-01 00:00:20").getTime - 1))
        },

        MultiAddData(
          (input1, Seq(("5", Timestamp.valueOf("2023-01-01 01:15:00")))),
          (input2, Seq(("6", Timestamp.valueOf("2023-01-01 01:15:00"))))
        ),

        // < data batch >
        // global watermark (2023-01-01 01:00:20, 2023-01-01 01:00:20)
        // op1 (join)
        // -- IW (2023-01-01 01:00:20, 2023-01-01 01:00:20)
        // -- OW 2023-01-01 00:00:19.999999
        // -- left state
        // ("1", "2023-01-01 01:00:10", matched=true)
        // ("1", "2023-01-01 01:00:30", matched=false)
        // ("5", "2023-01-01 01:15:00", matched=false)
        // -- right state
        // ("1", "2023-01-01 01:00:20")
        // ("6", "2023-01-01 01:15:00")
        // -- result
        // None
        // op2 (aggregation)
        // -- IW (2023-01-01 00:00:19.999999, 2023-01-01 00:00:19.999999)
        // -- OW 2023-01-01 00:00:19.999999
        // -- state row
        // ("2023-01-01 01:00:00", "2023-01-01 01:10:00", 1)
        // -- result
        // None

        // -- watermark calculation
        // watermark in left input: 2023-01-01 01:15:00
        // watermark in right input: 2023-01-01 01:15:00
        // origin watermark: 2023-01-01 01:15:00

        // < no-data batch >
        // global watermark (2023-01-01 01:00:20, 2023-01-01 01:15:00)
        // op1 (join)
        // -- IW (2023-01-01 01:00:20, 2023-01-01 01:15:00)
        // -- OW 2023-01-01 00:14:59.999999
        // -- left state
        // ("1", "2023-01-01 01:00:10", matched=true)
        // ("1", "2023-01-01 01:00:30", matched=false)
        // ("5", "2023-01-01 01:15:00", matched=false)
        // -- right state
        // ("1", "2023-01-01 01:00:20")
        // ("6", "2023-01-01 01:15:00")
        // -- result
        // None
        // op2 (aggregation)
        // -- IW (2023-01-01 00:00:19.999999, 2023-01-01 00:14:59.999999)
        // -- OW 2023-01-01 00:14:59.999999
        // -- state row
        // ("2023-01-01 01:00:00", "2023-01-01 01:10:00", 1)
        // -- result
        // None
        CheckAnswer(),

        Execute { query =>
          val lastExecution = query.lastExecution
          val joinOperator = lastExecution.executedPlan.collect {
            case j: StreamingSymmetricHashJoinExec => j
          }.head
          val aggSaveOperator = lastExecution.executedPlan.collect {
            case j: StateStoreSaveExec => j
          }.head

          assert(joinOperator.eventTimeWatermarkForLateEvents ===
            Some(Timestamp.valueOf("2023-01-01 01:00:20").getTime))
          assert(joinOperator.eventTimeWatermarkForEviction ===
            Some(Timestamp.valueOf("2023-01-01 01:15:00").getTime))

          assert(aggSaveOperator.eventTimeWatermarkForLateEvents ===
            Some(Timestamp.valueOf("2023-01-01 00:00:20").getTime - 1))
          assert(aggSaveOperator.eventTimeWatermarkForEviction ===
            Some(Timestamp.valueOf("2023-01-01 00:15:00").getTime - 1))
        },

        MultiAddData(
          (input1, Seq(
            ("5", Timestamp.valueOf("2023-01-01 02:16:00")))),
          (input2, Seq(
            ("6", Timestamp.valueOf("2023-01-01 02:16:00"))))
        ),

        // < data batch >
        // global watermark (2023-01-01 01:15:00, 2023-01-01 01:15:00)
        // op1 (join)
        // -- IW (2023-01-01 01:15:00, 2023-01-01 01:15:00)
        // -- OW 2023-01-01 00:14:59.999999
        // -- left state
        // ("1", "2023-01-01 01:00:10", matched=true)
        // ("1", "2023-01-01 01:00:30", matched=false)
        // ("5", "2023-01-01 01:15:00", matched=false)
        // ("5", "2023-01-01 02:16:00", matched=false)
        // -- right state
        // ("1", "2023-01-01 01:00:20")
        // ("6", "2023-01-01 01:15:00")
        // ("6", "2023-01-01 02:16:00")
        // -- result
        // None
        // op2 (aggregation)
        // -- IW (2023-01-01 00:14:59.999999, 2023-01-01 00:14:59.999999)
        // -- OW 2023-01-01 00:14:59.999999
        // -- state row
        // ("2023-01-01 01:00:00", "2023-01-01 01:10:00", 1)
        // -- result
        // None

        // -- watermark calculation
        // watermark in left input: 2023-01-01 02:16:00
        // watermark in right input: 2023-01-01 02:16:00
        // origin watermark: 2023-01-01 02:16:00

        // < no-data batch >
        // global watermark (2023-01-01 01:15:00, 2023-01-01 02:16:00)
        // op1 (join)
        // -- IW (2023-01-01 01:15:00, 2023-01-01 02:16:00)
        // -- OW 2023-01-01 01:15:59.999999
        // -- left state
        // ("5", "2023-01-01 02:16:00", matched=false)
        // -- right state
        // ("6", "2023-01-01 02:16:00")
        // -- result
        // ("1", "2023-01-01 01:00:30", null, null)
        // ("5", "2023-01-01 01:15:00", null, null)
        // op2 (aggregation)
        // -- IW (2023-01-01 00:14:59.999999, 2023-01-01 01:15:59.999999)
        // -- OW 2023-01-01 01:15:59.999999
        // -- state row
        // ("2023-01-01 01:10:00", "2023-01-01 01:20:00", 1)
        // -- result
        // ("2023-01-01 01:00:00", "2023-01-01 01:10:00", 2)
        CheckAnswer(
          ("2023-01-01 01:00:00", "2023-01-01 01:10:00", 2)
        ),

        Execute { query =>
          val lastExecution = query.lastExecution
          val joinOperator = lastExecution.executedPlan.collect {
            case j: StreamingSymmetricHashJoinExec => j
          }.head
          val aggSaveOperator = lastExecution.executedPlan.collect {
            case j: StateStoreSaveExec => j
          }.head

          assert(joinOperator.eventTimeWatermarkForLateEvents ===
            Some(Timestamp.valueOf("2023-01-01 01:15:00").getTime))
          assert(joinOperator.eventTimeWatermarkForEviction ===
            Some(Timestamp.valueOf("2023-01-01 02:16:00").getTime))

          assert(aggSaveOperator.eventTimeWatermarkForLateEvents ===
            Some(Timestamp.valueOf("2023-01-01 00:15:00").getTime - 1))
          assert(aggSaveOperator.eventTimeWatermarkForEviction ===
            Some(Timestamp.valueOf("2023-01-01 01:16:00").getTime - 1))
        }
      )
    }
  }

  // This test case simply swaps the left and right from the test case "stream-stream time interval
  // left outer join -> aggregation, append mode". This test case intends to verify the behavior
  // that both event time columns from both inputs are available to use after stream-stream join.
  // For explanation of the behavior, please refer to the test case "stream-stream time interval
  // left outer join -> aggregation, append mode".
  test("stream-stream time interval right outer join -> aggregation, append mode") {
    val input1 = MemoryStream[(String, Timestamp)]
    val input2 = MemoryStream[(String, Timestamp)]

    val s1 = input1.toDF()
      .toDF("id1", "timestamp1")
      .withWatermark("timestamp1", "0 seconds")
      .as("s1")

    val s2 = input2.toDF()
      .toDF("id2", "timestamp2")
      .withWatermark("timestamp2", "0 seconds")
      .as("s2")

    val s3 = s1.join(s2, expr("s1.id1 = s2.id2 AND (s1.timestamp1 BETWEEN " +
      "s2.timestamp2 - INTERVAL 1 hour AND s2.timestamp2 + INTERVAL 1 hour)"), "rightOuter")

    val agg = s3.groupBy(window($"timestamp2", "10 minutes"))
      .agg(count("*").as("cnt"))
      .selectExpr("CAST(window.start AS STRING) AS window_start",
        "CAST(window.end AS STRING) AS window_end", "cnt")

    // for ease of verification, we change the session timezone to UTC
    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      testStream(agg)(
        MultiAddData(
          (input2, Seq(
            ("1", Timestamp.valueOf("2023-01-01 01:00:10")),
            ("2", Timestamp.valueOf("2023-01-01 01:00:30")))
          ),
          (input1, Seq(
            ("1", Timestamp.valueOf("2023-01-01 01:00:20"))))
        ),
        CheckAnswer(),

        Execute { query =>
          val lastExecution = query.lastExecution
          val joinOperator = lastExecution.executedPlan.collect {
            case j: StreamingSymmetricHashJoinExec => j
          }.head
          val aggSaveOperator = lastExecution.executedPlan.collect {
            case j: StateStoreSaveExec => j
          }.head

          assert(joinOperator.eventTimeWatermarkForLateEvents === Some(0))
          assert(joinOperator.eventTimeWatermarkForEviction ===
            Some(Timestamp.valueOf("2023-01-01 01:00:20").getTime))

          assert(aggSaveOperator.eventTimeWatermarkForLateEvents === Some(0))
          assert(aggSaveOperator.eventTimeWatermarkForEviction ===
            Some(Timestamp.valueOf("2023-01-01 00:00:20").getTime - 1))
        },

        MultiAddData(
          (input2, Seq(("5", Timestamp.valueOf("2023-01-01 01:15:00")))),
          (input1, Seq(("6", Timestamp.valueOf("2023-01-01 01:15:00"))))
        ),
        CheckAnswer(),

        Execute { query =>
          val lastExecution = query.lastExecution
          val joinOperator = lastExecution.executedPlan.collect {
            case j: StreamingSymmetricHashJoinExec => j
          }.head
          val aggSaveOperator = lastExecution.executedPlan.collect {
            case j: StateStoreSaveExec => j
          }.head

          assert(joinOperator.eventTimeWatermarkForLateEvents ===
            Some(Timestamp.valueOf("2023-01-01 01:00:20").getTime))
          assert(joinOperator.eventTimeWatermarkForEviction ===
            Some(Timestamp.valueOf("2023-01-01 01:15:00").getTime))

          assert(aggSaveOperator.eventTimeWatermarkForLateEvents ===
            Some(Timestamp.valueOf("2023-01-01 00:00:20").getTime - 1))
          assert(aggSaveOperator.eventTimeWatermarkForEviction ===
            Some(Timestamp.valueOf("2023-01-01 00:15:00").getTime - 1))
        },

        MultiAddData(
          (input2, Seq(
            ("5", Timestamp.valueOf("2023-01-01 02:16:00")))),
          (input1, Seq(
            ("6", Timestamp.valueOf("2023-01-01 02:16:00"))))
        ),
        CheckAnswer(
          ("2023-01-01 01:00:00", "2023-01-01 01:10:00", 2)
        ),

        Execute { query =>
          val lastExecution = query.lastExecution
          val joinOperator = lastExecution.executedPlan.collect {
            case j: StreamingSymmetricHashJoinExec => j
          }.head
          val aggSaveOperator = lastExecution.executedPlan.collect {
            case j: StateStoreSaveExec => j
          }.head

          assert(joinOperator.eventTimeWatermarkForLateEvents ===
            Some(Timestamp.valueOf("2023-01-01 01:15:00").getTime))
          assert(joinOperator.eventTimeWatermarkForEviction ===
            Some(Timestamp.valueOf("2023-01-01 02:16:00").getTime))

          assert(aggSaveOperator.eventTimeWatermarkForLateEvents ===
            Some(Timestamp.valueOf("2023-01-01 00:15:00").getTime - 1))
          assert(aggSaveOperator.eventTimeWatermarkForEviction ===
            Some(Timestamp.valueOf("2023-01-01 01:16:00").getTime - 1))
        }
      )
    }
  }

  test("stream-stream time interval join - output watermark for various intervals") {
    def testOutputWatermarkInJoin(
        df: DataFrame,
        input: MemoryStream[(String, Timestamp)],
        expectedOutputWatermark: Long): Unit = {
      testStream(df)(
        // dummy row to trigger execution
        AddData(input, ("1", Timestamp.valueOf("2023-01-01 01:00:10"))),
        CheckAnswer(),
        Execute { query =>
          val lastExecution = query.lastExecution
          val joinOperator = lastExecution.executedPlan.collect {
            case j: StreamingSymmetricHashJoinExec => j
          }.head

          val outputWatermark = joinOperator.produceOutputWatermark(0)
          assert(outputWatermark.get === expectedOutputWatermark)
        }
      )
    }

    val input1 = MemoryStream[(String, Timestamp)]
    val df1 = input1.toDF()
      .selectExpr("_1 as leftId", "_2 as leftEventTime")
      .withWatermark("leftEventTime", "5 minutes")

    val input2 = MemoryStream[(String, Timestamp)]
    val df2 = input2.toDF()
      .selectExpr("_1 as rightId", "_2 as rightEventTime")
      .withWatermark("rightEventTime", "10 minutes")

    val join1 = df1.join(df2,
      expr(
        """
          |leftId = rightId AND leftEventTime BETWEEN
          |  rightEventTime AND rightEventTime + INTERVAL 40 seconds
          |""".stripMargin))

    // right row should wait for additional 40 seconds (+ 1 ms) to be matched with left rows
    testOutputWatermarkInJoin(join1, input1, -40L * 1000 - 1)

    val join2 = df1.join(df2,
      expr(
        """
          |leftId = rightId AND leftEventTime BETWEEN
          |  rightEventTime - INTERVAL 30 seconds AND rightEventTime
          |""".stripMargin))

    // left row should wait for additional 30 seconds (+ 1 ms) to be matched with left rows
    testOutputWatermarkInJoin(join2, input1, -30L * 1000 - 1)

    val join3 = df1.join(df2,
      expr(
        """
          |leftId = rightId AND leftEventTime BETWEEN
          |  rightEventTime - INTERVAL 30 seconds AND rightEventTime + INTERVAL 40 seconds
          |""".stripMargin))

    // left row should wait for additional 30 seconds (+ 1 ms) to be matched with left rows
    // right row should wait for additional 40 seconds (+ 1 ms) to be matched with right rows
    // taking minimum of both criteria - 40 seconds (+ 1 ms)
    testOutputWatermarkInJoin(join3, input1, -40L * 1000 - 1)
  }

  // NOTE: This is the revise of the reproducer in SPARK-45637. CREDIT goes to @andrezjzera.
  test("SPARK-49829 time window agg per each source followed by stream-stream join") {
    val inputStream1 = MemoryStream[Long]
    val inputStream2 = MemoryStream[Long]

    val df1 = inputStream1.toDF()
      .selectExpr("value", "timestamp_seconds(value) AS ts")
      .withWatermark("ts", "5 seconds")

    val df2 = inputStream2.toDF()
      .selectExpr("value", "timestamp_seconds(value) AS ts")
      .withWatermark("ts", "5 seconds")

    val df1Window = df1.groupBy(
      window($"ts", "10 seconds")
    ).agg(sum("value").as("sum_df1"))

    val df2Window = df2.groupBy(
      window($"ts", "10 seconds")
    ).agg(sum("value").as("sum_df2"))

    val joined = df1Window.join(df2Window, "window", "inner")
      .selectExpr("CAST(window.end AS long) AS window_end", "sum_df1", "sum_df2")

    // The test verifies the case where both sides produce input as time window (append mode)
    // for stream-stream join having join condition for equality of time window.
    // Inputs are produced into stream-stream join when the time windows are completed, meaning
    // they will be evicted in this batch for stream-stream join as well. (NOTE: join condition
    // does not delay the state watermark in stream-stream join).
    // Before SPARK-49829, left side does not add the input to state store if it's going to evict
    // in this batch, which breaks the match between input from left side and input from right
    // side "for this batch".
    testStream(joined)(
      MultiAddData(
        (inputStream1, Seq(1L, 2L, 3L, 4L, 5L)),
        (inputStream2, Seq(5L, 6L, 7L, 8L, 9L))
      ),
      // watermark: 5 - 5 = 0
      CheckNewAnswer(),
      MultiAddData(
        (inputStream1, Seq(11L, 12L, 13L, 14L, 15L)),
        (inputStream2, Seq(15L, 16L, 17L, 18L, 19L))
      ),
      // watermark: 15 - 5 = 10 (windows for [0, 10) are completed)
      CheckNewAnswer((10L, 15L, 35L)),
      MultiAddData(
        (inputStream1, Seq(100L)),
        (inputStream2, Seq(101L))
      ),
      // watermark: 100 - 5 = 95 (windows for [0, 20) are completed)
      CheckNewAnswer((20L, 65L, 85L))
    )
  }

  private def assertNumStateRows(numTotalRows: Seq[Long]): AssertOnQuery = AssertOnQuery { q =>
    q.processAllAvailable()
    val progressWithData = q.recentProgress.lastOption.get
    val stateOperators = progressWithData.stateOperators
    assert(stateOperators.length === numTotalRows.size)
    assert(stateOperators.map(_.numRowsTotal).toSeq === numTotalRows)
    true
  }

  private def assertNumRowsDroppedByWatermark(
      numRowsDroppedByWatermark: Seq[Long]): AssertOnQuery = AssertOnQuery { q =>
    q.processAllAvailable()
    val progressWithData = q.recentProgress.filterNot { p =>
      // filter out batches which are falling into one of types:
      // 1) doesn't execute the batch run
      // 2) empty input batch
      p.numInputRows == 0
    }.lastOption.get
    val stateOperators = progressWithData.stateOperators
    assert(stateOperators.length === numRowsDroppedByWatermark.size)
    assert(stateOperators.map(_.numRowsDroppedByWatermark).toSeq === numRowsDroppedByWatermark)
    true
  }
}
