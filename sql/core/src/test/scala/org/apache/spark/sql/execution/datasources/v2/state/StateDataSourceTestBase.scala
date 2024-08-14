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
package org.apache.spark.sql.execution.datasources.v2.state

import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.state.StateStore
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.streaming.util.StreamManualClock

trait StateDataSourceTestBase extends StreamTest with StateStoreMetricsTest {
  import testImplicits._

  override def beforeEach(): Unit = {
    super.beforeEach()
    spark.streams.stateStoreCoordinator // initialize the lazy coordinator
  }

  override def afterEach(): Unit = {
    // Stop maintenance tasks because they may access already deleted checkpoint.
    StateStore.stop()
    super.afterEach()
  }

  protected def runCompositeKeyStreamingAggregationQuery(checkpointRoot: String): Unit = {
    val inputData = MemoryStream[Int]
    val aggregated = getCompositeKeyStreamingAggregationQuery(inputData)

    testStream(aggregated, OutputMode.Update)(
      StartStream(checkpointLocation = checkpointRoot),
      // batch 0
      AddData(inputData, 0 to 5: _*),
      CheckLastBatch(
        (0, "Apple", 1, 0, 0, 0),
        (1, "Banana", 1, 1, 1, 1),
        (0, "Strawberry", 1, 2, 2, 2),
        (1, "Apple", 1, 3, 3, 3),
        (0, "Banana", 1, 4, 4, 4),
        (1, "Strawberry", 1, 5, 5, 5)
      ),
      // batch 1
      AddData(inputData, 6 to 10: _*),
      // state also contains (1, "Strawberry", 1, 5, 5, 5) but not updated here
      CheckLastBatch(
        (0, "Apple", 2, 6, 6, 0), // 0, 6
        (1, "Banana", 2, 8, 7, 1), // 1, 7
        (0, "Strawberry", 2, 10, 8, 2), // 2, 8
        (1, "Apple", 2, 12, 9, 3), // 3, 9
        (0, "Banana", 2, 14, 10, 4) // 4, 10
      ),
      StopStream,
      StartStream(checkpointLocation = checkpointRoot),
      // batch 2
      AddData(inputData, 3, 2, 1),
      CheckLastBatch(
        (1, "Banana", 3, 9, 7, 1), // 1, 7, 1
        (0, "Strawberry", 3, 12, 8, 2), // 2, 8, 2
        (1, "Apple", 3, 15, 9, 3) // 3, 9, 3
      )
    )
  }

  private def getCompositeKeyStreamingAggregationQuery(
      inputData: MemoryStream[Int]): Dataset[(Int, String, Long, Long, Int, Int)] = {
    inputData.toDF()
      .selectExpr("value", "value % 2 AS groupKey",
        "(CASE value % 3 WHEN 0 THEN 'Apple' WHEN 1 THEN 'Banana' ELSE 'Strawberry' END) AS fruit")
      .groupBy($"groupKey", $"fruit")
      .agg(
        count("*").as("cnt"),
        sum("value").as("sum"),
        max("value").as("max"),
        min("value").as("min")
      )
      .as[(Int, String, Long, Long, Int, Int)]
  }

  protected def runLargeDataStreamingAggregationQuery(checkpointRoot: String): Unit = {
    val inputData = MemoryStream[Int]
    val aggregated = getLargeDataStreamingAggregationQuery(inputData)

    // check with more data - leverage full partitions
    testStream(aggregated, OutputMode.Update)(
      StartStream(checkpointLocation = checkpointRoot),
      // batch 0
      AddData(inputData, 0 until 20: _*),
      CheckLastBatch(
        (0, 2, 10, 10, 0), // 0, 10
        (1, 2, 12, 11, 1), // 1, 11
        (2, 2, 14, 12, 2), // 2, 12
        (3, 2, 16, 13, 3), // 3, 13
        (4, 2, 18, 14, 4), // 4, 14
        (5, 2, 20, 15, 5), // 5, 15
        (6, 2, 22, 16, 6), // 6, 16
        (7, 2, 24, 17, 7), // 7, 17
        (8, 2, 26, 18, 8), // 8, 18
        (9, 2, 28, 19, 9) // 9, 19
      ),
      // batch 1
      AddData(inputData, 20 until 40: _*),
      CheckLastBatch(
        (0, 4, 60, 30, 0), // 0, 10, 20, 30
        (1, 4, 64, 31, 1), // 1, 11, 21, 31
        (2, 4, 68, 32, 2), // 2, 12, 22, 32
        (3, 4, 72, 33, 3), // 3, 13, 23, 33
        (4, 4, 76, 34, 4), // 4, 14, 24, 34
        (5, 4, 80, 35, 5), // 5, 15, 25, 35
        (6, 4, 84, 36, 6), // 6, 16, 26, 36
        (7, 4, 88, 37, 7), // 7, 17, 27, 37
        (8, 4, 92, 38, 8), // 8, 18, 28, 38
        (9, 4, 96, 39, 9) // 9, 19, 29, 39
      ),
      StopStream,
      StartStream(checkpointLocation = checkpointRoot),
      // batch 2
      AddData(inputData, 0, 1, 2),
      CheckLastBatch(
        (0, 5, 60, 30, 0), // 0, 10, 20, 30, 0
        (1, 5, 65, 31, 1), // 1, 11, 21, 31, 1
        (2, 5, 70, 32, 2) // 2, 12, 22, 32, 2
      )
    )
  }

  private def getLargeDataStreamingAggregationQuery(
      inputData: MemoryStream[Int]): Dataset[(Int, Long, Long, Int, Int)] = {
    inputData.toDF()
      .selectExpr("value", "value % 10 AS groupKey")
      .groupBy($"groupKey")
      .agg(
        count("*").as("cnt"),
        sum("value").as("sum"),
        max("value").as("max"),
        min("value").as("min")
      )
      .as[(Int, Long, Long, Int, Int)]
  }

  protected def runDropDuplicatesQuery(checkpointRoot: String): Unit = {
    val inputData = MemoryStream[Int]
    val deduplicated = getDropDuplicatesQuery(inputData)

    testStream(deduplicated, OutputMode.Append())(
      StartStream(checkpointLocation = checkpointRoot),

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

  private def getDropDuplicatesQuery(inputData: MemoryStream[Int]): Dataset[Long] = {
    inputData.toDS()
      .withColumn("eventTime", timestamp_seconds($"value"))
      .withWatermark("eventTime", "10 seconds")
      .dropDuplicates()
      .select($"eventTime".cast("long").as[Long])
  }

  protected def runDropDuplicatesQueryWithColumnSpecified(checkpointRoot: String): Unit = {
    val inputData = MemoryStream[(String, Int)]
    val deduplicated = getDropDuplicatesQueryWithColumnSpecified(inputData)

    testStream(deduplicated, OutputMode.Append())(
      StartStream(checkpointLocation = checkpointRoot),

      AddData(inputData, ("A", 1), ("B", 2), ("C", 3)),
      CheckAnswer(("A", 1), ("B", 2), ("C", 3)),
      assertNumStateRows(total = 3, updated = 3),

      AddData(inputData, ("B", 4), ("D", 5)),
      CheckNewAnswer(("D", 5)),
      assertNumStateRows(total = 4, updated = 1)
    )
  }

  private def getDropDuplicatesQueryWithColumnSpecified(
      inputData: MemoryStream[(String, Int)]): Dataset[(String, Int)] = {
    inputData.toDS()
      .selectExpr("_1 AS col1", "_2 AS col2")
      .dropDuplicates("col1")
      .as[(String, Int)]
  }

  protected def runDropDuplicatesWithinWatermarkQuery(checkpointRoot: String): Unit = {
    val inputData = MemoryStream[(String, Int)]
    val deduplicated = getDropDuplicatesWithinWatermarkQuery(inputData)

    testStream(deduplicated, OutputMode.Append())(
      StartStream(checkpointLocation = checkpointRoot),

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

  private def getDropDuplicatesWithinWatermarkQuery(
      inputData: MemoryStream[(String, Int)]): DataFrame = {
    inputData.toDS()
      .withColumn("eventTime", timestamp_seconds($"_2"))
      .withWatermark("eventTime", "2 seconds")
      .dropDuplicatesWithinWatermark("_1")
      .select($"_1", $"eventTime".cast("long").as[Long])
  }

  protected def runFlatMapGroupsWithStateQuery(checkpointRoot: String): Unit = {
    val clock = new StreamManualClock

    val inputData = MemoryStream[(String, Long)]
    val remapped = getFlatMapGroupsWithStateQuery(inputData)

    testStream(remapped, OutputMode.Update)(
      // batch 0
      StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock,
        checkpointLocation = checkpointRoot),
      AddData(inputData, ("hello world", 1L), ("hello scala", 2L)),
      AdvanceManualClock(1 * 1000),
      CheckNewAnswer(
        ("hello", 2, 1000, false),
        ("world", 1, 0, false),
        ("scala", 1, 0, false)
      ),
      // batch 1
      AddData(inputData, ("hello world", 3L), ("hello scala", 4L)),
      AdvanceManualClock(1 * 1000),
      CheckNewAnswer(
        ("hello", 4, 3000, false),
        ("world", 2, 2000, false),
        ("scala", 2, 2000, false)
      )
    )
  }

  private def getFlatMapGroupsWithStateQuery(
      inputData: MemoryStream[(String, Long)]): Dataset[(String, Int, Long, Boolean)] = {
    // scalastyle:off line.size.limit
    // This test code is borrowed from Sessionization example, with modification a bit to run with testStream
    // https://github.com/apache/spark/blob/v2.4.1/examples/src/main/scala/org/apache/spark/examples/sql/streaming/StructuredSessionization.scala
    // scalastyle:on

    val events = inputData.toDF()
      .as[(String, Timestamp)]
      .flatMap { case (line, timestamp) =>
        line.split(" ").map(word => Event(sessionId = word, timestamp))
      }

    val sessionUpdates = events
      .groupByKey(event => event.sessionId)
      .mapGroupsWithState[SessionInfo, SessionUpdate](GroupStateTimeout.ProcessingTimeTimeout) {

        case (sessionId: String, events: Iterator[Event], state: GroupState[SessionInfo]) =>
          if (state.hasTimedOut) {
            val finalUpdate =
              SessionUpdate(sessionId, state.get.durationMs, state.get.numEvents, expired = true)
            state.remove()
            finalUpdate
          } else {
            val timestamps = events.map(_.timestamp.getTime).toSeq
            val updatedSession = if (state.exists) {
              val oldSession = state.get
              SessionInfo(
                oldSession.numEvents + timestamps.size,
                oldSession.startTimestampMs,
                math.max(oldSession.endTimestampMs, timestamps.max))
            } else {
              SessionInfo(timestamps.size, timestamps.min, timestamps.max)
            }
            state.update(updatedSession)

            state.setTimeoutDuration("10 seconds")
            SessionUpdate(sessionId, state.get.durationMs, state.get.numEvents, expired = false)
          }
      }

    sessionUpdates.map(si => (si.id, si.numEvents, si.durationMs, si.expired))
  }

  protected def runStreamStreamJoinQuery(checkpointRoot: String): Unit = {
    val inputData = MemoryStream[(Int, Long)]
    val query = getStreamStreamJoinQuery(inputData)

    testStream(query)(
      StartStream(checkpointLocation = checkpointRoot),
      AddData(inputData, (1, 1L), (2, 2L), (3, 3L), (4, 4L), (5, 5L)),
      // batch 1 - global watermark = 0
      // states
      // left: (2, 2L), (4, 4L)
      // right: (2, 2L), (4, 4L)
      CheckNewAnswer((2, 2L, 2, 2L), (4, 4L, 4, 4L)),
      // filter is applied to both sides as an optimization
      assertNumStateRows(4, 4),
      AddData(inputData, (6, 6L), (7, 7L), (8, 8L), (9, 9L), (10, 10L)),
      // batch 2 - global watermark = 5
      // states
      // left: (2, 2L), (4, 4L), (6, 6L), (8, 8L), (10, 10L)
      // right: (6, 6L), (8, 8L), (10, 10L)
      // states evicted
      // left: nothing (it waits for 5 seconds more than watermark due to join condition)
      // right: (2, 2L), (4, 4L)
      // NOTE: look for evicted rows in right which are not evicted from left - they were
      // properly joined in batch 1
      CheckNewAnswer((6, 6L, 6, 6L), (8, 8L, 8, 8L), (10, 10L, 10, 10L)),
      assertNumStateRows(8, 6)
    )
  }

  protected def runStreamStreamJoinQueryWithOneThousandInputs(checkpointRoot: String): Unit = {
    val inputData = MemoryStream[(Int, Long)]
    val query = getStreamStreamJoinQuery(inputData)

    // To ease of tests, we do not run a no-data microbatch to not advance watermark.
    withSQLConf(SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false") {
      testStream(query)(
        StartStream(checkpointLocation = checkpointRoot),
        AddData(inputData,
          (1 to 1000).map { i => (i, i.toLong) }: _*),
        ProcessAllAvailable(),
        // filter is applied to both sides as an optimization
        assertNumStateRows(1000, 1000)
      )
    }
  }

  protected def getStreamStreamJoinQuery(inputStream: MemoryStream[(Int, Long)]): DataFrame = {
    val df = inputStream.toDS()
      .select(col("_1").as("value"), timestamp_seconds($"_2").as("timestamp"))

    val leftStream = df.select(col("value").as("leftId"), col("timestamp").as("leftTime"))

    val rightStream = df
      // Introduce misses for ease of debugging
      .where(col("value") % 2 === 0)
      .select(col("value").as("rightId"), col("timestamp").as("rightTime"))

    leftStream
      .withWatermark("leftTime", "5 seconds")
      .join(
        rightStream.withWatermark("rightTime", "5 seconds"),
        expr("rightId = leftId AND rightTime >= leftTime AND " +
          "rightTime <= leftTime + interval 5 seconds"),
        joinType = "inner")
      .select(col("leftId"), col("leftTime").cast("int"),
        col("rightId"), col("rightTime").cast("int"))
  }

  protected def runSessionWindowAggregationQuery(checkpointRoot: String): Unit = {
    val input = MemoryStream[(String, Long)]
    val sessionWindow = session_window($"eventTime", "10 seconds")

    val events = input.toDF()
      .select($"_1".as("value"), $"_2".as("timestamp"))
      .withColumn("eventTime", $"timestamp".cast("timestamp"))
      .withWatermark("eventTime", "30 seconds")
      .selectExpr("explode(split(value, ' ')) AS sessionId", "eventTime")

    val streamingDf = events
      .groupBy(sessionWindow as Symbol("session"), $"sessionId")
      .agg(count("*").as("numEvents"))
      .selectExpr("sessionId", "CAST(session.start AS LONG)", "CAST(session.end AS LONG)",
        "CAST(session.end AS LONG) - CAST(session.start AS LONG) AS durationMs",
        "numEvents")

    testStream(streamingDf, OutputMode.Complete())(
      StartStream(checkpointLocation = checkpointRoot),
      AddData(input,
        ("hello world spark streaming", 40L),
        ("world hello structured streaming", 41L)
      ),
      CheckNewAnswer(
        ("hello", 40, 51, 11, 2),
        ("world", 40, 51, 11, 2),
        ("streaming", 40, 51, 11, 2),
        ("spark", 40, 50, 10, 1),
        ("structured", 41, 51, 10, 1)
      ),
      StopStream
    )
  }
}

case class Event(sessionId: String, timestamp: Timestamp)

case class SessionInfo(
    numEvents: Int,
    startTimestampMs: Long,
    endTimestampMs: Long) {
  def durationMs: Long = endTimestampMs - startTimestampMs
}

case class SessionUpdate(
    id: String,
    durationMs: Long,
    numEvents: Int,
    expired: Boolean)
