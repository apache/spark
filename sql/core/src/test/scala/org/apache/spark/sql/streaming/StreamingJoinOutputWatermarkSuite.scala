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
import java.sql.Timestamp

import org.apache.hadoop.fs.Path
import org.apache.logging.log4j.Level

import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, Row}
import org.apache.spark.sql.execution.streaming.checkpointing.{OffsetMap, OffsetSeq, OffsetSeqLog}
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf

/**
 * A stateful operator after a stream-stream join whose state of an input has no cutoff. The join's
 * output watermark ignores that input, so the operator would drop the old rows the join emits.
 */
class StreamingJoinOutputWatermarkSuite extends StreamTest {
  import testImplicits._

  private def withJoinConf(version: Int)(f: => Unit): Unit = withSQLConf(
    SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> version.toString,
    SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
    SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false")(f)

  private def input(side: String): (MemoryStream[(Int, Long)], DataFrame) = {
    val stream = MemoryStream[(Int, Long)]
    (stream, stream.toDF().toDF(s"${side}Key", "seconds")
      .select(col(s"${side}Key"), timestamp_seconds($"seconds").as(s"${side}Time"))
      .withWatermark(s"${side}Time", "10 seconds"))
  }

  // A window count over the join of the two inputs.
  private def countJoined(
      condition: String, time: String, joinType: String = "inner", where: String = "true")
      : (MemoryStream[(Int, Long)], MemoryStream[(Int, Long)], DataFrame) = {
    val (leftInput, left) = input("left")
    val (rightInput, right) = input("right")
    val result = left.join(right, expr(condition), joinType).where(where)
      .groupBy(window(col(time), "10 seconds")).count()
      .select($"window.start".cast("long"), $"count")
    (leftInput, rightInput, result)
  }

  private def windowCount(joined: DataFrame, time: String): DataFrame =
    joined.groupBy(window(col(time), "10 seconds")).count()
      .select($"window.start".cast("long"), $"count")

  private val keyOnly = "leftKey = rightKey"
  private val oneSidedRange = "leftKey = rightKey AND rightTime <= leftTime + INTERVAL 10 SECONDS"

  private def expectRejected(side: String): StreamAction = ExpectFailure[AnalysisException](
    e => assert(e.getMessage.contains(
      s"the state of the $side input of a stream-stream join has no cutoff")),
    typeIsSuperClass = true)

  for (version <- Seq(2, 4)) {
    test(s"reject a key-only join before a window aggregation, V$version") {
      withJoinConf(version) {
        val (leftInput, _, result) = countJoined(keyOnly, "leftTime")
        testStream(result)(AddData(leftInput, (1, 5L)), expectRejected("left"))
      }
    }

    test(s"reject a range join bounded on one side only before a window aggregation, V$version") {
      withJoinConf(version) {
        val (leftInput, _, result) = countJoined(oneSidedRange, "rightTime")
        testStream(result)(AddData(leftInput, (1, 5L)), expectRejected("right"))
      }
    }

    // Only the right input has no cutoff, and the window comes from the left event time, which
    // the output watermark bounds.
    test(s"accept a range join bounded on one side only before a window on the bounded side, " +
        s"V$version") {
      withJoinConf(version) {
        val (leftInput, rightInput, result) = countJoined(oneSidedRange, "leftTime")
        testStream(result)(
          MultiAddData(leftInput, (1, 5L))(rightInput, (1, 8L)), CheckNewAnswer(),
          MultiAddData(leftInput, (2, 40L))(rightInput, (2, 40L)), CheckNewAnswer(),
          AddData(rightInput, (2, 41L)), CheckNewAnswer((0L, 1L)),
          MultiAddData(leftInput, (3, 100L))(rightInput, (3, 100L)), CheckNewAnswer(),
          AddData(rightInput, (3, 101L)), CheckNewAnswer((40L, 2L)))
      }
    }

    // The check follows the event time of the aggregation, and only that column, through these
    // shapes, so each runs with the time of the bounded left input and fails with the right one.
    Seq[(String, (DataFrame, DataFrame, String) => DataFrame, Seq[Row], Seq[Row])](
      ("a repartition", (l, r, t) => windowCount(l.join(r, expr(oneSidedRange)).repartition(2), t),
        Seq(Row(0L, 1L)), Seq(Row(40L, 2L))),
      ("a join with a static table", (l, r, t) => windowCount(l.join(r, expr(oneSidedRange))
        .join(Seq(1, 2, 3).toDF("staticKey"), $"leftKey" === $"staticKey"), t),
        Seq(Row(0L, 1L)), Seq(Row(40L, 2L))),
      ("a MATERIALIZED CTE", (l, r, t) => {
        l.createOrReplaceTempView("l")
        r.createOrReplaceTempView("r")
        spark.sql(s"WITH j AS MATERIALIZED (SELECT * FROM l JOIN r ON $oneSidedRange) " +
          s"SELECT CAST(w.start AS BIGINT), count(*) FROM " +
          s"(SELECT window($t, '10 seconds') AS w FROM j) GROUP BY w")
      }, Seq(Row(0L, 1L)), Seq(Row(40L, 2L))),
      ("a sliding window", (l, r, t) => l.join(r, expr(oneSidedRange))
        .groupBy(window(col(t), "10 seconds", "5 seconds")).count()
        .select($"window.start".cast("long"), $"count"),
        Seq(Row(0L, 1L), Row(5L, 1L)), Seq(Row(35L, 2L), Row(40L, 2L))),
      ("an aggregate function of the other input's time", (l, r, t) => l.join(r,
        expr(oneSidedRange)).groupBy(window(col(t), "10 seconds"))
        .agg(count("*").as("count"), max($"rightTime").cast("long").as("maxRightTime"))
        .select($"window.start".cast("long"), $"count", $"maxRightTime"),
        Seq(Row(0L, 1L, 8L)), Seq(Row(40L, 2L, 41L)))
    ).foreach { case (shape, query, firstWindows, secondWindows) =>
      test(s"a range join bounded on one side only before $shape, V$version") {
        withJoinConf(version) {
          withTempView("l", "r") {
            val (leftInput, left) = input("left")
            val (rightInput, right) = input("right")
            testStream(query(left, right, "leftTime"))(
              MultiAddData(leftInput, (1, 5L))(rightInput, (1, 8L)), CheckNewAnswer(),
              MultiAddData(leftInput, (2, 40L))(rightInput, (2, 40L)), CheckNewAnswer(),
              AddData(rightInput, (2, 41L)), CheckNewAnswer(firstWindows: _*),
              MultiAddData(leftInput, (3, 100L))(rightInput, (3, 100L)), CheckNewAnswer(),
              AddData(rightInput, (3, 101L)), CheckNewAnswer(secondWindows: _*))
            val (otherLeftInput, otherLeft) = input("left")
            val (_, otherRight) = input("right")
            testStream(query(otherLeft, otherRight, "rightTime"))(
              AddData(otherLeftInput, (1, 5L)), expectRejected("right"))
          }
        }
      }
    }

    // A union traces each of its inputs that holds the join, so the second read of the CTE counts.
    test(s"a range join bounded on one side only read twice through a union of a MATERIALIZED " +
        s"CTE, V$version") {
      withJoinConf(version) {
        withTempView("l", "r") {
          def query(secondTime: String)
              : (MemoryStream[(Int, Long)], MemoryStream[(Int, Long)], DataFrame) = {
            val (leftInput, left) = input("left")
            val (rightInput, right) = input("right")
            left.createOrReplaceTempView("l")
            right.createOrReplaceTempView("r")
            (leftInput, rightInput, spark.sql(
              s"WITH j AS MATERIALIZED (SELECT * FROM l JOIN r ON $oneSidedRange) " +
              "SELECT CAST(w.start AS BIGINT), count(*) FROM " +
              "(SELECT window(t, '10 seconds') AS w FROM " +
              s"(SELECT leftTime AS t FROM j UNION ALL SELECT $secondTime AS t FROM j)) " +
              "GROUP BY w"))
          }
          val (leftInput, rightInput, accepted) = query("leftTime")
          testStream(accepted)(
            MultiAddData(leftInput, (1, 5L))(rightInput, (1, 8L)), CheckNewAnswer(),
            MultiAddData(leftInput, (2, 40L))(rightInput, (2, 40L)), CheckNewAnswer(),
            AddData(rightInput, (2, 41L)), CheckNewAnswer((0L, 2L)),
            MultiAddData(leftInput, (3, 100L))(rightInput, (3, 100L)), CheckNewAnswer(),
            AddData(rightInput, (3, 101L)), CheckNewAnswer((40L, 4L)))
          val (otherLeftInput, _, rejected) = query("rightTime")
          testStream(rejected)(AddData(otherLeftInput, (1, 5L)), expectRejected("right"))
        }
      }
    }

    test(s"a stream with its own watermark unioned with a range join bounded on one side only, " +
        s"V$version") {
      withJoinConf(version) {
        val (leftInput, left) = input("left")
        val (rightInput, right) = input("right")
        val (otherInput, other) = input("other")
        val result = windowCount(other.union(
          left.join(right, expr(oneSidedRange)).select($"leftKey", $"leftTime")), "otherTime")
        def addAll(other: Long, left: Long, right: Long): StreamAction =
          StreamProgressLockedActions(Seq(AddData(otherInput, (7, other)),
            AddData(leftInput, (left.toInt, left)), AddData(rightInput, (left.toInt, right))))
        testStream(result)(
          addAll(6L, 5L, 8L), CheckNewAnswer(),
          addAll(40L, 40L, 40L), CheckNewAnswer(),
          AddData(rightInput, (40, 41L)), CheckNewAnswer((0L, 2L)),
          addAll(100L, 100L, 100L), CheckNewAnswer(),
          AddData(rightInput, (100, 101L)), CheckNewAnswer((40L, 3L)))
      }
    }

    test(s"a range join bounded on one side only before a typed flatMapGroupsWithState with an " +
        s"event-time timeout, V$version") {
      withJoinConf(version) {
        def query(time: String)
            : (MemoryStream[(Int, Long)], MemoryStream[(Int, Long)], Dataset[(Int, Int)]) = {
          val (leftInput, left) = input("left")
          val (rightInput, right) = input("right")
          val joined = left.join(right, expr(oneSidedRange)).select($"leftKey", col(time))
          (leftInput, rightInput, joined.as[(Int, Timestamp)].groupByKey(_._1)
            .flatMapGroupsWithState(OutputMode.Append(), GroupStateTimeout.EventTimeTimeout()) {
              (key: Int, rows: Iterator[(Int, Timestamp)], _: GroupState[Int]) =>
                Iterator(key -> rows.size)
            })
        }
        val (leftInput, rightInput, accepted) = query("leftTime")
        testStream(accepted)(
          MultiAddData(leftInput, (1, 5L))(rightInput, (1, 8L)), CheckNewAnswer((1, 1)),
          MultiAddData(leftInput, (2, 40L))(rightInput, (2, 40L)), CheckNewAnswer((2, 1)),
          AddData(rightInput, (2, 41L)), CheckNewAnswer((2, 1)))
        val (otherLeftInput, _, rejected) = query("rightTime")
        testStream(rejected)(AddData(otherLeftInput, (1, 5L)), expectRejected("right"))
      }
    }

    test(s"accept a time-interval join before a window aggregation, V$version") {
      withJoinConf(version) {
        val (leftInput, rightInput, result) = countJoined(
          "leftKey = rightKey AND rightTime BETWEEN leftTime AND leftTime + INTERVAL 10 SECONDS",
          "leftTime")
        testStream(result)(
          MultiAddData(leftInput, (1, 5L))(rightInput, (1, 8L)), CheckNewAnswer(),
          MultiAddData(leftInput, (2, 40L))(rightInput, (2, 40L)), CheckNewAnswer(),
          AddData(rightInput, (2, 41L)), CheckNewAnswer((0L, 1L)))
      }
    }

    // The optimizer moves the range into the join condition, which eviction uses.
    test(s"accept a time-interval join with its range in a filter above it, V$version") {
      withJoinConf(version) {
        val (leftInput, rightInput, result) = countJoined(keyOnly, "leftTime",
          where = "rightTime BETWEEN leftTime AND leftTime + INTERVAL 10 SECONDS")
        testStream(result)(
          MultiAddData(leftInput, (1, 5L))(rightInput, (1, 8L)), CheckNewAnswer(),
          MultiAddData(leftInput, (2, 40L))(rightInput, (2, 40L)), CheckNewAnswer(),
          AddData(rightInput, (2, 41L)), CheckNewAnswer((0L, 1L)))
      }
    }

    test(s"accept a left semi join bounded on its left input before a window aggregation, " +
        s"V$version") {
      withJoinConf(version) {
        val (leftInput, rightInput, result) = countJoined(
          "leftKey = rightKey AND leftTime > rightTime - INTERVAL 5 SECONDS", "leftTime",
          joinType = "left_semi")
        testStream(result)(
          MultiAddData(leftInput, (1, 5L))(rightInput, (1, 8L)), CheckNewAnswer(),
          MultiAddData(leftInput, (2, 40L))(rightInput, (2, 40L)), CheckNewAnswer(),
          AddData(rightInput, (2, 41L)), CheckNewAnswer((0L, 1L)))
      }
    }

    test(s"accept a stream-static join before a window aggregation, V$version") {
      withJoinConf(version) {
        val (leftInput, left) = input("left")
        val result = left.join(Seq(1, 2).toDF("rightKey"), expr(keyOnly))
          .groupBy(window($"leftTime", "10 seconds")).count()
          .select($"window.start".cast("long"), $"count")
        testStream(result)(
          AddData(leftInput, (1, 5L)), CheckNewAnswer(),
          AddData(leftInput, (2, 40L)), CheckNewAnswer(),
          AddData(leftInput, (3, 41L)), CheckNewAnswer((0L, 1L)))
      }
    }

    // flatMapGroupsWithState drops late rows only with an event-time timeout.
    test(s"accept a key-only join before flatMapGroupsWithState without a timeout, V$version") {
      withJoinConf(version) {
        val (leftInput, left) = input("left")
        val (rightInput, right) = input("right")
        val result = left.join(right, expr(keyOnly)).select($"leftKey", $"leftTime")
          .as[(Int, Timestamp)].groupByKey(_._1)
          .flatMapGroupsWithState(OutputMode.Append(), GroupStateTimeout.NoTimeout()) {
            (key: Int, rows: Iterator[(Int, Timestamp)], _: GroupState[Int]) =>
              Iterator(key -> rows.size)
          }
        testStream(result)(
          MultiAddData(leftInput, (1, 5L), (2, 5L))(rightInput, (2, 5L)), CheckNewAnswer((2, 1)),
          MultiAddData(leftInput, (3, 30L))(rightInput, (8, 30L)), CheckNewAnswer(),
          // Not late for the join, and joins the stored left (1, 5).
          AddData(rightInput, (1, 40L)), CheckNewAnswer((1, 1)))
      }
    }

    test(s"reject a key-only join before a deduplication without an event-time key, V$version") {
      withJoinConf(version) {
        val (leftInput, left) = input("left")
        val (_, right) = input("right")
        val result = left.join(right, expr(keyOnly)).select($"leftKey", $"leftTime")
          .dropDuplicates("leftKey")
        testStream(result)(AddData(leftInput, (1, 5L)), expectRejected("left"))
      }
    }

    test(s"reject a key-only join in a SQL CTE before a window aggregation, V$version") {
      withJoinConf(version) {
        val (leftInput, left) = input("left")
        val (_, right) = input("right")
        withTempView("l", "r") {
          left.createOrReplaceTempView("l")
          right.createOrReplaceTempView("r")
          val result = spark.sql("WITH j AS (SELECT * FROM l JOIN r ON leftKey = rightKey) " +
            "SELECT count(*) FROM j GROUP BY window(leftTime, '10 seconds')")
          testStream(result)(AddData(leftInput, (1, 5L)), expectRejected("left"))
        }
      }
    }

    test(s"accept a window-key join before a window aggregation, V$version") {
      withJoinConf(version) {
        val (leftInput, left) = input("left")
        val (rightInput, right) = input("right")
        val result = left.select($"leftKey".as("key"), window($"leftTime", "10 seconds"))
          .join(right.select($"rightKey".as("key"), window($"rightTime", "10 seconds")),
            Seq("key", "window"))
          .groupBy("window").count()
          .select($"window.start".cast("long"), $"count")
        testStream(result)(
          MultiAddData(leftInput, (1, 5L))(rightInput, (1, 8L)), CheckNewAnswer(),
          MultiAddData(leftInput, (2, 40L))(rightInput, (2, 40L)), CheckNewAnswer(),
          AddData(rightInput, (2, 41L)), CheckNewAnswer((0L, 1L)))
      }
    }
  }

  // The join evicts the left input by expiry, so its state keeps rows of any leftTime.
  private def countJoinedByExpiry(joinType: String = "inner")
      : (MemoryStream[(Int, Long)], MemoryStream[(Int, Long)], DataFrame) = {
    val (leftInput, left) = input("left")
    val (rightInput, right) = input("right")
    val result = left.withColumn("expiry", expr("leftTime + INTERVAL 10 MINUTES"))
      .join(right, expr("leftKey = rightKey AND rightTime BETWEEN leftTime AND expiry"), joinType)
      .groupBy(window($"leftTime", "10 seconds")).count()
      .select($"window.start".cast("long"), $"count")
    (leftInput, rightInput, result)
  }

  for (version <- Seq(2, 4)) {
    test(s"reject a join evicted by another column before a window aggregation, V$version") {
      withJoinConf(version) {
        val (leftInput, _, result) = countJoinedByExpiry()
        testStream(result)(AddData(leftInput, (1, 5L)), expectRejected("left"))
      }
    }
  }

  // The left input is bounded only through a DATE column cast to TIMESTAMP, so the join keeps
  // evicting it by its event time.
  for (version <- Seq(2, 4)) {
    test(s"accept a join bounded through a cast column before a window aggregation, V$version") {
      withJoinConf(version) {
        val (leftInput, left) = input("left")
        val (rightInput, right) = input("right")
        val result = left.withColumn("leftDate", expr("date_add(to_date(leftTime), 1)"))
          .join(right, expr("leftKey = rightKey AND " +
            "rightTime BETWEEN leftTime AND CAST(leftDate AS TIMESTAMP)"))
          .groupBy(window($"leftTime", "10 seconds")).count()
          .select($"window.start".cast("long"), $"count")
        testStream(result)(
          MultiAddData(leftInput, (1, 5L))(rightInput, (1, 8L)), CheckNewAnswer(),
          MultiAddData(leftInput, (2, 40L))(rightInput, (2, 40L)), CheckNewAnswer(),
          AddData(rightInput, (2, 41L)), CheckNewAnswer((0L, 1L)))
      }
    }
  }

  // An earlier version evicted the left input by leftTime and emitted its outer row at the
  // watermark 390 s, which the aggregation counted. A restarted query keeps that eviction.
  test("a join evicted by another column keeps the earlier eviction from an older checkpoint") {
    withJoinConf(2) {
      withTempDir { checkpoint =>
        val (leftInput, rightInput, result) = countJoinedByExpiry("left_outer")
        val start = StartStream(checkpointLocation = checkpoint.getCanonicalPath)
        withSQLConf(SQLConf.STATEFUL_OPERATOR_CHECK_CORRECTNESS_ENABLED.key -> "false") {
          testStream(result)(start,
            MultiAddData(leftInput, (1, 100L))(rightInput, (9, 100L)), CheckNewAnswer(),
            StopStream)
        }
        removeCheckJoinOutputWatermark(checkpoint)
        val logAppender = new LogAppender("join output watermark")
        withLogAppender(logAppender, level = Some(Level.WARN)) {
          testStream(result)(start,
            MultiAddData(leftInput, (8, 400L))(rightInput, (9, 400L)), CheckNewAnswer(),
            AddData(rightInput, (9, 400L)), CheckNewAnswer((100L, 1L)),
            AddData(rightInput, (1, 460L)), CheckNewAnswer())
        }
        assert(logAppender.loggingEvents.exists(_.getMessage.getFormattedMessage.contains(
          "the state of the left input of a stream-stream join has no cutoff on its event time")))
      }
    }
  }

  test("a join evicted by another column before a window aggregation runs with the check " +
      "disabled") {
    withJoinConf(4) {
      withSQLConf(SQLConf.STATEFUL_OPERATOR_CHECK_CORRECTNESS_ENABLED.key -> "false") {
        val (leftInput, rightInput, result) = countJoinedByExpiry()
        testStream(result)(
          MultiAddData(leftInput, (1, 5L))(rightInput, (1, 8L)), CheckNewAnswer())
      }
    }
  }

  test("a key-only join before a window aggregation only warns with the check disabled") {
    withJoinConf(4) {
      withSQLConf(SQLConf.STATEFUL_OPERATOR_CHECK_CORRECTNESS_ENABLED.key -> "false") {
        val (leftInput, rightInput, result) = countJoined(keyOnly, "leftTime")
        val logAppender = new LogAppender("join output watermark")
        withLogAppender(logAppender, level = Some(Level.WARN)) {
          testStream(result)(
            MultiAddData(leftInput, (1, 5L))(rightInput, (1, 5L)), CheckNewAnswer())
        }
        assert(logAppender.loggingEvents.exists(_.getMessage.getFormattedMessage.contains(
          "the state of the left input of a stream-stream join has no cutoff")))
      }
    }
  }

  test("a key-only join before a window aggregation keeps running from an older checkpoint") {
    withJoinConf(4) {
      withTempDir { checkpoint =>
        val (leftInput, rightInput, result) = countJoined(keyOnly, "leftTime")
        val start = StartStream(checkpointLocation = checkpoint.getCanonicalPath)
        withSQLConf(SQLConf.STATEFUL_OPERATOR_CHECK_CORRECTNESS_ENABLED.key -> "false") {
          testStream(result)(start,
            MultiAddData(leftInput, (1, 5L))(rightInput, (1, 5L)), CheckNewAnswer(), StopStream)
        }
        // The checkpoint of this version is checked when the query restarts.
        testStream(result)(start,
          MultiAddData(leftInput, (2, 40L))(rightInput, (2, 40L)), expectRejected("left"))
        removeCheckJoinOutputWatermark(checkpoint)
        val logAppender = new LogAppender("join output watermark")
        withLogAppender(logAppender, level = Some(Level.WARN)) {
          testStream(result)(start,
            MultiAddData(leftInput, (3, 50L))(rightInput, (3, 50L)), ProcessAllAvailable())
        }
        assert(logAppender.loggingEvents.exists(_.getMessage.getFormattedMessage.contains(
          "the state of the left input of a stream-stream join has no cutoff")))
      }
    }
  }

  // Removes the conf from the latest offset log entry, as versions without it wrote it.
  private def removeCheckJoinOutputWatermark(checkpoint: File): Unit = {
    val key = SQLConf.STATEFUL_OPERATOR_CHECK_JOIN_OUTPUT_WATERMARK.key
    val offsetsPath = new File(checkpoint, "offsets").getCanonicalPath
    val log = new OffsetSeqLog(spark, offsetsPath)
    val (batchId, offsets) = log.getLatest().get
    val updated = offsets match {
      case o: OffsetSeq =>
        assert(o.metadataOpt.exists(_.conf.contains(key)))
        o.copy(metadataOpt = o.metadataOpt.map(m => m.copy(conf = m.conf - key)))
      case o: OffsetMap =>
        assert(o.metadata.conf.contains(key))
        o.copy(metadata = o.metadata.copy(conf = o.metadata.conf - key))
    }
    val path = new Path(offsetsPath, batchId.toString)
    assert(path.getFileSystem(spark.sessionState.newHadoopConf()).delete(path, false))
    assert(log.add(batchId, updated))
  }
}
