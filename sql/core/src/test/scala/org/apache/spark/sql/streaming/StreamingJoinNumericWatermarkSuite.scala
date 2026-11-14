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

import org.apache.logging.log4j.Level

import org.apache.spark.sql.{AnalysisException, DataFrame, Row}
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf

class StreamingJoinNumericWatermarkSuite extends StreamTest {
  import testImplicits._

  private val timestampRule = "A time range condition must bound a TIMESTAMP column"

  // Analysis rejects the plan before any state format is used. Each bound below is exact, with
  // no truncating cast and no whole days, so only the TIMESTAMP-column rule rejects it.
  test("left outer join cannot use a numeric state field for range eviction") {
    val leftInput = MemoryStream[(Int, Long, Long)]
    val rightInput = MemoryStream[(Int, Long)]
    val left = leftInput.toDF().toDF("leftKey", "n", "time")
      .select($"leftKey", $"n", timestamp_seconds($"time").as("lt"))
      .withWatermark("lt", "1 second")
    val right = rightInput.toDF().toDF("rightKey", "time")
      .select($"rightKey", timestamp_seconds($"time").as("rt"))
      .withWatermark("rt", "1 second")
    val joined = left.join(right,
      expr("leftKey = rightKey AND n > CAST(rt AS DOUBLE)"), "left_outer")
      .select($"leftKey", $"n", $"lt")

    val error = intercept[AnalysisException] {
      testStream(joined)(
        AddData(leftInput, (1, 20L, 20L)),
        CheckNewAnswer())
    }
    assert(error.getMessage.contains("a watermark on the right input and a time constraint"))
    assert(error.getMessage.contains(timestampRule))
  }

  // Earlier versions admitted this join, so disabling the finalization check runs it. The left
  // state is never evicted, so the unmatched left row (2, 50) is never output with nulls, which
  // evicting it by the cutoff solved for the numeric field would do.
  test("left outer join on a numeric state field runs with the finalization check disabled") {
    val leftInput = MemoryStream[(Int, Long, Long)]
    val rightInput = MemoryStream[(Int, Long)]
    val left = leftInput.toDF().toDF("leftKey", "n", "time")
      .select($"leftKey", $"n", timestamp_seconds($"time").as("lt"))
      .withWatermark("lt", "1 second")
    val right = rightInput.toDF().toDF("rightKey", "time")
      .select($"rightKey", timestamp_seconds($"time").as("rt"))
      .withWatermark("rt", "1 second")
    val joined = left.join(right,
      expr("leftKey = rightKey AND n > CAST(rt AS DOUBLE)"), "left_outer")
      .select($"leftKey", $"n", $"rt".cast("long"))

    val appender = new LogAppender("finalization check disabled")
    withLogAppender(appender, level = Some(Level.WARN)) {
      withSQLConf(SQLConf.STREAMING_JOIN_CHECK_FINALIZATION_ENABLED.key -> "false") {
        testStream(joined)(
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (1, 20L, 20L)), AddData(rightInput, (1, 10L)))),
          CheckNewAnswer(Row(1, 20L, 10L)),
          AddData(rightInput, (2, 100L)),
          CheckNewAnswer(),
          // The left row (2, 50) does not match (2, 100). Its time is below the eviction
          // watermark of 99 seconds in the next batch, yet the left state keeps the row.
          AddData(leftInput, (2, 50L, 50L), (3, 200L, 200L)),
          CheckNewAnswer(),
          AddData(rightInput, (4, 120L)),
          CheckNewAnswer())
      }
    }
    assert(appender.loggingEvents.exists { event =>
      val message = event.getMessage.getFormattedMessage
      message.contains("allow unmatched state to be finalized") &&
        message.contains("keeps that state without bound")
    })
  }

  // Comparing with the TIMESTAMP event time casts these columns, but eviction compares the
  // cutoff with the column itself.
  for (columnType <- Seq("date", "timestamp_ntz")) {
    test(s"left outer join cannot use a $columnType state field for range eviction") {
      val leftInput = MemoryStream[(Int, Long)]
      val rightInput = MemoryStream[(Int, Long)]
      val left = leftInput.toDF().toDF("leftKey", "time")
        .select($"leftKey", timestamp_seconds($"time").cast(columnType).as("orderDate"))
      val right = rightInput.toDF().toDF("rightKey", "time")
        .select($"rightKey", timestamp_seconds($"time").as("rt"))
        .withWatermark("rt", "1 second")
      val joined = left.join(right,
        expr("leftKey = rightKey AND orderDate >= rt - INTERVAL 7 HOURS"), "left_outer")

      val error = intercept[AnalysisException] {
        testStream(joined)(
          AddData(leftInput, (1, 20L)),
          CheckNewAnswer())
      }
      assert(error.getMessage.contains(timestampRule))
    }
  }

  private def withJoinConf(version: Int)(f: => Unit): Unit = {
    withSQLConf(
        SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> version.toString,
        SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
        SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false")(f)
  }

  // window() over a watermarked time column. The window is the input's only event-time column.
  private def windowed(
      input: MemoryStream[(Int, Long)],
      key: String,
      windowName: String,
      duration: String,
      slide: String): DataFrame = {
    input.toDF().toDF(key, "time")
      .select(col(key), timestamp_seconds($"time").as("t"))
      .withWatermark("t", "0 seconds")
      .select(col(key), window($"t", duration, slide).as(windowName))
  }

  // An evicted left window starts at least its own duration before the watermark, and a later
  // right window starts less than its own duration before it, so with left windows at least as
  // long a later right window orders above every evicted left window.
  for {
    version <- Seq(2, 3, 4)
    leftDuration <- Seq("10 seconds", "20 seconds")
  } {
    test(s"left outer join finalizes $leftDuration windows by a whole-window bound, V$version") {
      withJoinConf(version) {
        val leftInput = MemoryStream[(Int, Long)]
        val rightInput = MemoryStream[(Int, Long)]
        val left = windowed(leftInput, "leftKey", "lw", leftDuration, leftDuration)
        val right = windowed(rightInput, "rightKey", "rw", "10 seconds", "10 seconds")
        val joined = left.join(right, $"leftKey" === $"rightKey" && $"lw" > $"rw", "left_outer")
          .select($"leftKey", $"lw.start".cast("long"), $"rw.start".cast("long"))
        val matchedStart = if (leftDuration == "10 seconds") 10L else 0L

        testStream(joined)(
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (1, 5L), (5, 15L)), AddData(rightInput, (2, 5L), (5, 5L)))),
          CheckNewAnswer(Row(5, matchedStart, 0L)),
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (3, 45L)), AddData(rightInput, (2, 45L)))),
          CheckNewAnswer(),
          // The eviction watermark is 45 seconds, past the end of key 1's left window.
          AddData(rightInput, (2, 46L)),
          CheckNewAnswer(Row(1, 0L, null)))
      }
    }
  }

  // The left input is watermarked on its event time lt, which the join condition does not use,
  // and holds a window over the unwatermarked column ot. Eviction must try the window after lt
  // finds no bound, or the left state is never finalized.
  for (version <- Seq(2, 4)) {
    test(s"left outer join finalizes a window over an unwatermarked column, V$version") {
      withJoinConf(version) {
        val leftInput = MemoryStream[(Int, Long)]
        val rightInput = MemoryStream[(Int, Long)]
        val left = leftInput.toDF().toDF("leftKey", "time")
          .select($"leftKey", timestamp_seconds($"time").as("lt"),
            timestamp_seconds($"time").as("ot"))
          .withWatermark("lt", "0 seconds")
          .select($"leftKey", $"lt", window($"ot", "10 seconds").as("lw"))
        val right = windowed(rightInput, "rightKey", "rw", "10 seconds", "10 seconds")
        val joined = left.join(right, $"leftKey" === $"rightKey" && $"lw" > $"rw", "left_outer")
          .select($"leftKey", $"lw.start".cast("long"), $"rw.start".cast("long"))

        testStream(joined)(
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (1, 5L), (5, 15L)), AddData(rightInput, (2, 5L), (5, 5L)))),
          CheckNewAnswer(Row(5, 10L, 0L)),
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (3, 45L)), AddData(rightInput, (2, 45L)))),
          CheckNewAnswer(),
          // The eviction watermark is 45 seconds, past the end of key 1's left window.
          AddData(rightInput, (2, 46L)),
          CheckNewAnswer(Row(1, 0L, null)))
      }
    }
  }

  test("right outer and left semi joins finalize windows by a whole-window bound") {
    Seq("right_outer", "left_semi").foreach { joinType =>
      withClue(joinType) {
        withJoinConf(2) {
          val leftInput = MemoryStream[(Int, Long)]
          val rightInput = MemoryStream[(Int, Long)]
          val left = windowed(leftInput, "leftKey", "lw", "10 seconds", "10 seconds")
          val right = windowed(rightInput, "rightKey", "rw", "10 seconds", "10 seconds")
          val condition = $"leftKey" === $"rightKey" && $"lw" > $"rw"
          // The right outer join finalizes the same left windows.
          val (joined, unmatched) = if (joinType == "right_outer") {
            (right.join(left, condition, joinType)
              .select($"leftKey", $"lw.start".cast("long"), $"rw.start".cast("long")),
              Seq(Row(1, 0L, null)))
          } else {
            (left.join(right, condition, joinType)
              .select($"leftKey", $"lw.start".cast("long")), Seq.empty)
          }
          val matched = if (joinType == "right_outer") Row(5, 10L, 0L) else Row(5, 10L)

          testStream(joined)(
            StreamProgressLockedActions(Seq(
              AddData(leftInput, (1, 5L), (5, 15L)), AddData(rightInput, (2, 5L), (5, 5L)))),
            CheckNewAnswer(matched),
            StreamProgressLockedActions(Seq(
              AddData(leftInput, (3, 25L)), AddData(rightInput, (2, 25L)))),
            CheckNewAnswer(),
            AddData(rightInput, (2, 26L)),
            CheckNewAnswer(unmatched: _*))
        }
      }
    }
  }

  // A 20-second right window that ends above the watermark can start before a 10-second left
  // window that ends at or below it, so these left windows cannot be finalized.
  test("outer and semi joins cannot finalize windows shorter than the other input's") {
    Seq("left_outer", "left_semi", "right_outer").foreach { joinType =>
      withClue(joinType) {
        val leftInput = MemoryStream[(Int, Long)]
        val rightInput = MemoryStream[(Int, Long)]
        val left = windowed(leftInput, "leftKey", "lw", "10 seconds", "10 seconds")
        val right = windowed(rightInput, "rightKey", "rw", "20 seconds", "1 second")
        val condition = $"leftKey" === $"rightKey" && $"lw" > $"rw"
        val joined = if (joinType == "right_outer") {
          right.join(left, condition, joinType)
        } else {
          left.join(right, condition, joinType)
        }

        val error = intercept[AnalysisException] {
          testStream(joined)(
            AddData(leftInput, (1, 5L)),
            CheckNewAnswer())
        }
        assert(error.getMessage.contains(timestampRule))
      }
    }
  }

  for (version <- Seq(2, 3, 4)) {
    test(s"inner join evicts windows by a whole-window bound, V$version") {
      withJoinConf(version) {
        val leftInput = MemoryStream[(Int, Long)]
        val rightInput = MemoryStream[(Int, Long)]
        val left = windowed(leftInput, "leftKey", "lw", "10 seconds", "10 seconds")
        val right = windowed(rightInput, "rightKey", "rw", "10 seconds", "10 seconds")
        val joined = left.join(right, $"leftKey" === $"rightKey" && $"lw" > $"rw")
          .select($"leftKey", $"lw.start".cast("long"), $"rw.start".cast("long"))

        testStream(joined)(
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (1, 5L)), AddData(rightInput, (2, 5L)))),
          CheckNewAnswer(),
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (3, 25L)), AddData(rightInput, (4, 25L)))),
          CheckNewAnswer(),
          // Distinct keys: V4 groups rows with the same key and time into one entry.
          AddData(rightInput, (6, 26L)),
          CheckNewAnswer(),
          Execute { query =>
            // Left window [0, 10) is evicted. Left [20, 30) and the three right rows remain,
            // because the bound gives right windows no cutoff.
            val total = query.lastProgress.stateOperators.head.numRowsTotal
            assert(total == 4, s"numRowsTotal = $total")
          })
      }
    }
  }

  // Ordering windows compares their starts first. A 20-second right window that ends above the
  // watermark can start before a 10-second left window that ends at or below it, so a bound that
  // compares whole windows must not evict left windows.
  for (version <- Seq(2, 3, 4)) {
    test(s"inner join keeps windows that a longer opposite window can still match, V$version") {
      withJoinConf(version) {
        val leftInput = MemoryStream[(Int, Long)]
        val rightInput = MemoryStream[(Int, Long)]
        val left = windowed(leftInput, "leftKey", "lw", "10 seconds", "10 seconds")
        val right = windowed(rightInput, "rightKey", "rw", "20 seconds", "1 second")
        val joined = left.join(right, $"leftKey" === $"rightKey" && $"lw" > $"rw")
          .select($"leftKey", $"lw.start".cast("long"), $"rw.start".cast("long"))

        testStream(joined)(
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (1, 5L)), AddData(rightInput, (9, 5L)))),
          CheckNewAnswer(),
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (8, 15L)), AddData(rightInput, (9, 15L)))),
          CheckNewAnswer(),
          // The watermark reaches 15 seconds, past the end of left window [0, 10).
          AddData(rightInput, (9, 16L)),
          CheckNewAnswer(),
          // Time 18 is in right window [-1, 19), which is not late and orders below [0, 10).
          AddData(rightInput, (1, 18L)),
          CheckNewAnswer(Row(1, 0L, -1L)))
      }
    }
  }

  // A union's column keeps its first child's metadata, so with children of different window
  // lengths it records none, and a whole-window bound evicts neither union's windows nor the
  // windows compared with them: a 20-second window can start before a 10-second window that ends
  // at or below the watermark.
  test("outer joins cannot finalize windows compared with a union of window lengths") {
    Seq("right", "left").foreach { unionSide =>
      withClue(s"$unionSide union") {
        val leftInput = MemoryStream[(Int, Long)]
        val otherLeftInput = MemoryStream[(Int, Long)]
        val rightInput = MemoryStream[(Int, Long)]
        val otherRightInput = MemoryStream[(Int, Long)]
        val (left, right) = if (unionSide == "right") {
          (windowed(leftInput, "leftKey", "lw", "10 seconds", "10 seconds"),
            windowed(rightInput, "rightKey", "rw", "10 seconds", "10 seconds")
              .union(windowed(otherRightInput, "rightKey", "rw", "20 seconds", "1 second")))
        } else {
          (windowed(leftInput, "leftKey", "lw", "20 seconds", "20 seconds")
              .union(windowed(otherLeftInput, "leftKey", "lw", "10 seconds", "10 seconds")),
            windowed(rightInput, "rightKey", "rw", "20 seconds", "1 second"))
        }
        val joined = left.join(right, $"leftKey" === $"rightKey" && $"lw" > $"rw", "left_outer")

        val error = intercept[AnalysisException] {
          testStream(joined)(
            AddData(leftInput, (1, 5L)),
            CheckNewAnswer())
        }
        assert(error.getMessage.contains(timestampRule))
      }
    }
  }

  for (version <- Seq(2, 3, 4)) {
    test(s"inner join keeps windows compared with a union of window lengths, V$version") {
      withJoinConf(version) {
        val leftInput = MemoryStream[(Int, Long)]
        val tumblingInput = MemoryStream[(Int, Long)]
        val slidingInput = MemoryStream[(Int, Long)]
        val left = windowed(leftInput, "leftKey", "lw", "10 seconds", "10 seconds")
        val right = windowed(tumblingInput, "rightKey", "rw", "10 seconds", "10 seconds")
          .union(windowed(slidingInput, "rightKey", "rw", "20 seconds", "1 second"))
        val joined = left.join(right, $"leftKey" === $"rightKey" && $"lw" > $"rw")
          .select($"leftKey", $"lw.start".cast("long"), $"rw.start".cast("long"))

        testStream(joined)(
          StreamProgressLockedActions(Seq(AddData(leftInput, (1, 5L)),
            AddData(tumblingInput, (9, 5L)), AddData(slidingInput, (9, 5L)))),
          CheckNewAnswer(),
          StreamProgressLockedActions(Seq(AddData(leftInput, (8, 15L)),
            AddData(tumblingInput, (9, 15L)), AddData(slidingInput, (9, 15L)))),
          CheckNewAnswer(),
          // The watermark reaches 15 seconds, past the end of left window [0, 10).
          AddData(tumblingInput, (9, 16L)),
          CheckNewAnswer(),
          // Time 18 is in sliding right window [-1, 19), which is not late and orders below
          // [0, 10).
          AddData(slidingInput, (1, 18L)),
          CheckNewAnswer(Row(1, 0L, -1L)))
      }
    }
  }

  // Both directions give a cutoff, so V4 would compute range-scan offsets, which read the event
  // time as a long. A time-window event time must not use them.
  test("V4 two-sided whole-window bound does not range-scan by window") {
    withJoinConf(4) {
      val leftInput = MemoryStream[(Int, Long)]
      val rightInput = MemoryStream[(Int, Long)]
      val left = windowed(leftInput, "leftKey", "lw", "10 seconds", "10 seconds")
      val right = windowed(rightInput, "rightKey", "rw", "10 seconds", "10 seconds")
      val joined = left.join(right,
          $"leftKey" === $"rightKey" && $"lw" >= $"rw" && $"lw" <= $"rw")
        .select($"leftKey", $"lw.start".cast("long"), $"rw.start".cast("long"))

      testStream(joined)(
        StreamProgressLockedActions(Seq(
          AddData(leftInput, (1, 5L), (2, 15L)), AddData(rightInput, (1, 7L)))),
        CheckNewAnswer(Row(1, 0L, 0L)),
        AddData(rightInput, (2, 12L)),
        CheckNewAnswer(Row(2, 10L, 10L)))
    }
  }

  test("whole-window join restarts from its checkpoint and keeps evicting") {
    withJoinConf(2) {
      withTempDir { checkpointDir =>
        val leftInput = MemoryStream[(Int, Long)]
        val rightInput = MemoryStream[(Int, Long)]
        val left = windowed(leftInput, "leftKey", "lw", "10 seconds", "10 seconds")
        val right = windowed(rightInput, "rightKey", "rw", "10 seconds", "10 seconds")
        val joined = left.join(right, $"leftKey" === $"rightKey" && $"lw" > $"rw", "left_outer")
          .select($"leftKey", $"lw.start".cast("long"), $"rw.start".cast("long"))
        val checkpoint = checkpointDir.getCanonicalPath

        testStream(joined)(
          StartStream(checkpointLocation = checkpoint),
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (1, 5L)), AddData(rightInput, (2, 5L)))),
          CheckNewAnswer(),
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (3, 25L)), AddData(rightInput, (2, 25L)))),
          CheckNewAnswer(),
          StopStream,
          StartStream(checkpointLocation = checkpoint),
          AddData(rightInput, (2, 26L)),
          CheckNewAnswer(Row(1, 0L, null)))
      }
    }
  }
}
