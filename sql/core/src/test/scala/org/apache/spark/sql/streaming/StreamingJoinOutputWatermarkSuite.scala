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

import org.apache.logging.log4j.Level

import org.apache.spark.sql.{AnalysisException, DataFrame, Encoders, Row}
import org.apache.spark.sql.execution.streaming.operators.stateful.join.StreamingSymmetricHashJoinExec
import org.apache.spark.sql.execution.streaming.operators.stateful.join.StreamingSymmetricHashJoinHelper.{JoinSide, LeftSide, RightSide}
import org.apache.spark.sql.execution.streaming.runtime.{MemoryStream, StreamExecution}
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.functions.{col, count, expr, lit, session_window, timestamp_seconds, window}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.tags.SlowSQLTest

@SlowSQLTest
class StreamingJoinOutputWatermarkSuite extends StreamTest {
  import testImplicits._

  for (version <- Seq(2, 3, 4)) {
    test(s"unbounded join state can emit an old event time after watermarks advance, V$version") {
      withSQLConf(
          SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> version.toString,
          SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
          SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false") {
        val leftInput = MemoryStream[(Int, Long)]
        val rightInput = MemoryStream[(Int, Long)]
        val left = leftInput.toDF().toDF("key", "seconds")
          .select($"key", timestamp_seconds($"seconds").as("leftTime"))
          .withWatermark("leftTime", "10 seconds")
        val right = rightInput.toDF().toDF("key", "seconds")
          .select($"key", timestamp_seconds($"seconds").as("rightTime"))
          .withWatermark("rightTime", "10 seconds")
        val result = left.join(right, Seq("key"))
          .select($"key", $"leftTime")
          .dropDuplicatesWithinWatermark("key")
          .select($"key", $"leftTime".cast("long"))

        testStream(result)(
          MultiAddData((leftInput, Seq((1, 5L))), (rightInput, Seq((9, 5L)))),
          CheckNewAnswer(),
          MultiAddData((leftInput, Seq((2, 30L))), (rightInput, Seq((3, 30L)))),
          CheckNewAnswer(),
          // Process another batch so the join's output watermark reaches downstream operators.
          AddData(rightInput, (4, 40L)),
          CheckNewAnswer(),
          // The retained left key 1 can still join after both input watermarks reach 20 seconds.
          // The right event is newer than that watermark, but the joined left time is only 5.
          AddData(rightInput, (1, 100L)),
          CheckNewAnswer((1, 5L)),
          Execute { query =>
            val join = query.lastExecution.executedPlan.collectFirst {
              case j: StreamingSymmetricHashJoinExec => j
            }.get
            assert(join.eventTimeWatermarkForLateEvents.contains(20000L))
            assert(join.produceOutputWatermark(20000L).contains(0L))
          })
      }
    }
  }

  // Impressions are bounded by the click watermark; clicks have no lower bound on clickTime.
  private def oneDirectionalJoin(): (MemoryStream[(Int, Long)], MemoryStream[(Int, Long)],
      DataFrame) = {
    val impressionInput = MemoryStream[(Int, Long)]
    val clickInput = MemoryStream[(Int, Long)]
    val impressions = impressionInput.toDF().toDF("impressionAdId", "seconds")
      .select($"impressionAdId", timestamp_seconds($"seconds").as("impressionTime"))
      .withWatermark("impressionTime", "10 seconds")
    val clicks = clickInput.toDF().toDF("clickAdId", "seconds")
      .select($"clickAdId", timestamp_seconds($"seconds").as("clickTime"))
      .withWatermark("clickTime", "10 seconds")
    val joined = impressions.join(clicks,
      expr("clickAdId = impressionAdId AND clickTime <= impressionTime + INTERVAL 10 SECONDS"))
    (impressionInput, clickInput, joined)
  }

  private def windowCounts(
      df: DataFrame,
      timeColumn: String,
      slide: String = "10 seconds"): DataFrame = {
    df.groupBy(window(col(timeColumn), "10 seconds", slide)).agg(count("*").as("count"))
      .select($"window.start".cast("long"), $"count")
  }

  // The join that reads the inputs, below any stream-stream join after it.
  private def join(query: StreamExecution): StreamingSymmetricHashJoinExec = {
    query.lastExecution.executedPlan.collect {
      case j: StreamingSymmetricHashJoinExec
          if !j.children.exists(_.exists(_.isInstanceOf[StreamingSymmetricHashJoinExec])) => j
    }.head
  }

  private def runOneDirectionalJoin(
      impressionInput: MemoryStream[(Int, Long)],
      clickInput: MemoryStream[(Int, Long)],
      result: DataFrame,
      expectedRows: Seq[Row],
      expectedSides: Option[Set[JoinSide]],
      expectedOutputWatermark: Long): Unit = {
    withSQLConf(SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false") {
      testStream(result, OutputMode.Append())(
        MultiAddData((impressionInput, Seq((1, 5L), (2, 15L), (3, 25L))),
          (clickInput, Seq((1, 8L), (2, 20L)))),
        ProcessAllAvailable(),
        // The click at 30 seconds joins the retained impression at 25 seconds.
        MultiAddData((impressionInput, Seq((4, 60L))), (clickInput, Seq((3, 30L), (9, 70L)))),
        ProcessAllAvailable(),
        MultiAddData((impressionInput, Seq((5, 100L))), (clickInput, Seq((9, 110L)))),
        ProcessAllAvailable(),
        AddData(clickInput, (9, 150L)),
        ProcessAllAvailable(),
        AddData(impressionInput, (6, 200L)),
        ProcessAllAvailable(),
        CheckAnswer(expectedRows: _*),
        Execute { query =>
          val j = join(query)
          assert(j.outputWatermarkSides == expectedSides)
          assert(j.produceOutputWatermark(j.eventTimeWatermarkForEviction.get)
            .contains(expectedOutputWatermark))
          // No joined row arrives behind the watermark of the operator after the join.
          assert(query.recentProgress.flatMap(_.stateOperators)
            .map(_.numRowsDroppedByWatermark).sum == 0)
        })
    }
  }

  test("output watermark follows the bounded input when only its event time is used") {
    val (impressionInput, clickInput, joined) = oneDirectionalJoin()
    // The eviction watermark is 90 seconds: the impression input (100 seconds less its 10-second
    // delay) holds back the min-policy watermark, while the click input's own watermark is 140
    // seconds by then. The output watermark is that less 10 seconds and 1 ms, 79.999 seconds.
    runOneDirectionalJoin(impressionInput, clickInput,
      windowCounts(joined.select($"impressionTime"), "impressionTime"),
      Seq(Row(0L, 1L), Row(10L, 1L), Row(20L, 1L)), Some(Set(LeftSide)), 79999L)
  }

  Seq("clickTime", "clickTimeAlias").foreach { timeColumn =>
    test(s"output watermark is held back when the unbounded input's event time is used: " +
        timeColumn) {
      val (impressionInput, clickInput, joined) = oneDirectionalJoin()
      val clickTime = if (timeColumn == "clickTime") $"clickTime" else $"clickTime".as(timeColumn)
      // The start-time check rejects this query by default; run it to observe the held watermark.
      withSQLConf(SQLConf.STREAMING_JOIN_CHECK_FINALIZATION_ENABLED.key -> "false") {
        runOneDirectionalJoin(impressionInput, clickInput,
          windowCounts(joined.select(clickTime), timeColumn), Nil, Some(Set(RightSide)), 0L)
      }
    }
  }

  // Each recognized path must carry the operator's event-time column to the join input it comes
  // from. Over impressionTime the join narrows to the bounded input, so its output watermark is
  // that input's state watermark, 79.999 seconds by default; over clickTime it holds back at 0.
  // Mapping either column to the other input fails the side, watermark or late-row assertion.
  private def testTracedConsumer(
      path: String,
      impressionRows: Seq[Row],
      clickRows: Seq[Row],
      impressionWatermark: Long = 79999L,
      confs: Seq[(String, String)] = Nil)(consumer: (DataFrame, String) => DataFrame): Unit = {
    Seq(
      ("impressionTime", impressionRows, LeftSide, impressionWatermark),
      ("clickTime", clickRows, RightSide, 0L)
    ).foreach { case (timeColumn, rows, side, outputWatermark) =>
      test(s"output watermark through $path over $timeColumn") {
        // Over impressionTime the start-time check must let the query run. Over clickTime it
        // rejects an append-mode aggregation; disable it to observe the held watermark.
        val checkEnabled = SQLConf.STREAMING_JOIN_CHECK_FINALIZATION_ENABLED.key ->
          (side == LeftSide).toString
        withSQLConf((confs :+ checkEnabled): _*) {
          val (impressionInput, clickInput, joined) = oneDirectionalJoin()
          runOneDirectionalJoin(impressionInput, clickInput, consumer(joined, timeColumn), rows,
            Some(Set(side)), outputWatermark)
        }
      }
    }
  }

  // Another input with a later event time, so that it does not hold back the union. The key
  // comes first, so that a wrong position in the union maps the event time to the key.
  private def laterInput(): DataFrame = {
    val otherInput = MemoryStream[Long]
    otherInput.addData(1000L)
    otherInput.toDF().select(lit(0).as("id"), timestamp_seconds($"value").as("t"))
      .withWatermark("t", "10 seconds")
  }

  private val impressionWindows = Seq(Row(0L, 1L), Row(10L, 1L), Row(20L, 1L))

  testTracedConsumer("a union", impressionWindows, Nil) { (joined, timeColumn) =>
    windowCounts(joined.select($"impressionAdId", col(timeColumn).as("t")).union(laterInput()),
      "t")
  }

  // The union's output takes the attributes of its first input. Deduplication on both columns
  // reads them directly and keeps the key before the event time.
  testTracedConsumer("a union with the join second",
      Seq(1000L, 5L, 15L, 25L).map(Row(_)), Seq(1000L, 8L, 20L, 30L).map(Row(_))) {
    (joined, timeColumn) =>
      laterInput().union(joined.select($"impressionAdId", col(timeColumn).as("t")))
        .dropDuplicates("id", "t")
        .select($"t".cast("long"))
  }

  testTracedConsumer("a sliding window",
      Seq(0L, 5L, 10L, 15L, 20L, 25L).map(Row(_, 1L)), Nil) { (joined, timeColumn) =>
    windowCounts(joined.select(col(timeColumn)), timeColumn, slide = "5 seconds")
  }

  testTracedConsumer("a session window", Seq(Row(1, 5L, 1L), Row(2, 15L, 1L), Row(3, 25L, 1L)),
      Nil) { (joined, timeColumn) =>
    joined.select($"impressionAdId", col(timeColumn))
      .groupBy($"impressionAdId", session_window(col(timeColumn), "5 seconds"))
      .agg(count("*").as("count"))
      .select($"impressionAdId", $"session_window.start".cast("long"), $"count")
  }

  // Deduplication emits first occurrences without waiting for the watermark; the watermark
  // evicts its state.
  testTracedConsumer("deduplication",
      Seq(Row(1, 5L), Row(2, 15L), Row(3, 25L)), Seq(Row(1, 8L), Row(2, 20L), Row(3, 30L))) {
    (joined, timeColumn) =>
      joined.select($"impressionAdId", col(timeColumn).as("t"))
        .dropDuplicates("impressionAdId", "t").select($"impressionAdId", $"t".cast("long"))
  }

  testTracedConsumer("deduplication within the watermark",
      Seq(Row(1, 5L), Row(2, 15L), Row(3, 25L)), Seq(Row(1, 8L), Row(2, 20L), Row(3, 30L))) {
    (joined, timeColumn) =>
      joined.select($"impressionAdId", col(timeColumn).as("t"))
        .dropDuplicatesWithinWatermark("impressionAdId")
        .select($"impressionAdId", $"t".cast("long"))
  }

  // The other input's watermark, 18 seconds, holds back the query's watermark, so the bounded
  // input is evicted below 18 seconds less 10 seconds and 1 ms.
  testTracedConsumer("a stream-stream join", Seq(Row(1, 7L), Row(3, 28L)), Nil,
      impressionWatermark = 7999L) { (joined, timeColumn) =>
    val otherInput = MemoryStream[(Int, Long)]
    otherInput.addData((1, 7L), (3, 28L))
    val other = otherInput.toDF().toDF("otherAdId", "seconds")
      .select($"otherAdId", timestamp_seconds($"seconds").as("otherTime"))
      .withWatermark("otherTime", "10 seconds")
    joined.select($"impressionAdId", col(timeColumn).as("t"))
      .join(other, expr("otherAdId = impressionAdId AND " +
        "otherTime BETWEEN t AND t + INTERVAL 5 SECONDS"))
      .select($"impressionAdId", $"otherTime".cast("long"))
  }

  testTracedConsumer("flatMapGroupsWithState with event-time timeouts",
      Seq(Row(1, 1L), Row(2, 1L), Row(3, 1L)), Nil) { (joined, timeColumn) =>
    joined.select($"impressionAdId", col(timeColumn)).as[(Int, Timestamp)]
      .groupByKey(_._1)
      .flatMapGroupsWithState(OutputMode.Append(), GroupStateTimeout.EventTimeTimeout()) {
        (key: Int, rows: Iterator[(Int, Timestamp)], state: GroupState[Long]) =>
          if (state.hasTimedOut) {
            val result = Iterator.single((key, state.get))
            state.remove()
            result
          } else {
            val times = rows.map(_._2.getTime).toSeq
            state.update(state.getOption.getOrElse(0L) + times.size)
            state.setTimeoutTimestamp(times.max + 10000L)
            Iterator.empty
          }
      }.toDF("key", "count")
  }

  testTracedConsumer("transformWithState with event-time timers",
      Seq(Row(1, 1L), Row(2, 1L), Row(3, 1L)), Nil,
      confs = Seq(
        SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName)) {
    (joined, timeColumn) =>
      joined.select($"impressionAdId", col(timeColumn)).as[(Int, Timestamp)]
        .groupByKey(_._1)
        .transformWithState(new JoinOutputEventTimeCounter(), TimeMode.EventTime(),
          OutputMode.Append())
        .toDF("key", "count")
  }

  test("output watermark without a downstream stateful operator is unchanged") {
    val (impressionInput, clickInput, joined) = oneDirectionalJoin()
    runOneDirectionalJoin(impressionInput, clickInput,
      joined.select($"impressionAdId", $"impressionTime".cast("long")),
      Seq(Row(1, 5L), Row(2, 15L), Row(3, 25L)), None, 0L)
  }

  // An append-mode aggregation over the event time of an input that the join cannot bound would
  // never emit, so the query fails at start.
  private def unboundedEventTimeError(eventTime: String, side: String, requirement: String) = {
    ExpectFailure[AnalysisException](assertFailure = error => {
      checkError(error.asInstanceOf[AnalysisException],
        condition = "STREAMING_JOIN_UNBOUNDED_DOWNSTREAM_EVENT_TIME",
        parameters = Map(
          "eventTime" -> eventTime,
          "side" -> side,
          "requirement" -> requirement,
          "config" -> SQLConf.STREAMING_JOIN_CHECK_FINALIZATION_ENABLED.key))
    })
  }

  // A join on lk = rk AND lt = rt with a watermark on one input only: no watermark on the other
  // input bounds the watermarked input's state, so the join holds back its output watermark for
  // that input.
  private def oneWatermarkEqualityJoin(joinType: String, watermarkOnLeft: Boolean):
      (MemoryStream[(Int, Int)], MemoryStream[(Int, Int)], DataFrame) = {
    val leftInput = MemoryStream[(Int, Int)]
    val rightInput = MemoryStream[(Int, Int)]
    val left =
      leftInput.toDF().toDF("lk", "time").select($"lk", timestamp_seconds($"time").as("lt"))
    val right =
      rightInput.toDF().toDF("rk", "time").select($"rk", timestamp_seconds($"time").as("rt"))
    val (watermarkedLeft, watermarkedRight) = if (watermarkOnLeft) {
      (left.withWatermark("lt", "10 seconds"), right)
    } else {
      (left, right.withWatermark("rt", "10 seconds"))
    }
    (leftInput, rightInput,
      watermarkedLeft.join(watermarkedRight, expr("lk = rk AND lt = rt"), joinType))
  }

  // The start-time check uses the same tracer, so it also rejects these aggregations over the
  // unbounded clickTime.
  Seq[(String, DataFrame => DataFrame)](
    "a union" -> { joined =>
      windowCounts(joined.select($"impressionAdId", $"clickTime".as("t")).union(laterInput()),
        "t")
    },
    "a sliding window" -> { joined =>
      windowCounts(joined.select($"clickTime"), "clickTime", slide = "5 seconds")
    },
    "a session window" -> { joined =>
      joined.select($"impressionAdId", $"clickTime")
        .groupBy($"impressionAdId", session_window($"clickTime", "5 seconds")).count()
    }
  ).foreach { case (path, aggregation) =>
    test(s"reject an append-mode aggregation over the unbounded input through $path") {
      val (impressionInput, clickInput, joined) = oneDirectionalJoin()
      testStream(aggregation(joined), OutputMode.Append())(
        MultiAddData(impressionInput, (1, 5L))(clickInput, (1, 8L)),
        unboundedEventTimeError("clickTime", "right",
          "a time constraint that limits how late a matching left row can arrive"))
    }
  }

  test("reject an append-mode aggregation over an event time the join cannot bound") {
    val (leftInput, rightInput, joined) = oneWatermarkEqualityJoin("inner", watermarkOnLeft = true)
    testStream(windowCounts(joined, "lt"), OutputMode.Append())(
      MultiAddData(leftInput, (1, 5))(rightInput, (1, 5)),
      unboundedEventTimeError("lt", "left", "a watermark on the right input and a time " +
        "constraint that limits how late a matching right row can arrive"))
  }

  test("reject an append-mode aggregation over the unbounded input of an admitted outer join") {
    val (leftInput, rightInput, joined) =
      oneWatermarkEqualityJoin("left_outer", watermarkOnLeft = false)
    testStream(windowCounts(joined, "rt"), OutputMode.Append())(
      MultiAddData(leftInput, (1, 5))(rightInput, (1, 5)),
      unboundedEventTimeError("rt", "right", "a watermark on the left input and a time " +
        "constraint that limits how late a matching left row can arrive"))
  }

  test("run an update-mode aggregation over an event time the join cannot bound") {
    val (leftInput, rightInput, joined) = oneWatermarkEqualityJoin("inner", watermarkOnLeft = true)
    testStream(windowCounts(joined, "lt"), OutputMode.Update())(
      MultiAddData(leftInput, (1, 5))(rightInput, (1, 5)),
      CheckNewAnswer(Row(0L, 1L)))
  }

  test("run an append-mode aggregation over an unbounded event time with one global watermark") {
    val (leftInput, rightInput, joined) = oneWatermarkEqualityJoin("inner", watermarkOnLeft = true)
    withSQLConf(SQLConf.STATEFUL_OPERATOR_ALLOW_MULTIPLE.key -> "false") {
      testStream(windowCounts(joined, "lt"), OutputMode.Append())(
        MultiAddData(leftInput, (1, 5))(rightInput, (1, 5)),
        CheckNewAnswer(),
        AddData(leftInput, (2, 100)),
        CheckNewAnswer(Row(0L, 1L)))
    }
  }

  test("run an append-mode aggregation over an unbounded event time with the check disabled") {
    val (leftInput, rightInput, joined) = oneWatermarkEqualityJoin("inner", watermarkOnLeft = true)
    val appender = new LogAppender("unbounded downstream event time")
    withLogAppender(appender, level = Some(Level.WARN)) {
      withSQLConf(SQLConf.STREAMING_JOIN_CHECK_FINALIZATION_ENABLED.key -> "false") {
        testStream(windowCounts(joined, "lt"), OutputMode.Append())(
          MultiAddData(leftInput, (1, 5))(rightInput, (1, 5)),
          CheckNewAnswer(),
          AddData(leftInput, (2, 100)),
          CheckNewAnswer())
      }
    }
    assert(appender.loggingEvents.exists(_.getMessage.getFormattedMessage
      .contains("STREAMING_JOIN_UNBOUNDED_DOWNSTREAM_EVENT_TIME")))
  }
}

/** Counts each key's rows and emits the count once the watermark passes 10 seconds after them. */
class JoinOutputEventTimeCounter extends StatefulProcessor[Int, (Int, Timestamp), (Int, Long)] {
  @transient private var count: ValueState[Long] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    count = getHandle.getValueState[Long]("count", Encoders.scalaLong, TTLConfig.NONE)
  }

  override def handleInputRows(
      key: Int,
      rows: Iterator[(Int, Timestamp)],
      timerValues: TimerValues): Iterator[(Int, Long)] = {
    val times = rows.map(_._2.getTime).toSeq
    count.update((if (count.exists()) count.get() else 0L) + times.size)
    getHandle.registerTimer(times.max + 10000L)
    Iterator.empty
  }

  override def handleExpiredTimer(
      key: Int,
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[(Int, Long)] = {
    val result = Iterator.single((key, count.get()))
    count.clear()
    result
  }
}
