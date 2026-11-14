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

import org.apache.spark.sql.{AnalysisException, DataFrame, Row}
import org.apache.spark.sql.catalyst.analysis.StreamingJoinHelper
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Join, JoinHint, Union}
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf

/**
 * With spark.sql.streaming.statefulOperator.allowMultiple=false, an input filters late rows by
 * its first watermarked column. An equality key on a later watermarked column bounds the other
 * input's state only when the input builds it from that first column, as a copy or as a
 * tumbling or sliding time window.
 */
class StreamingJoinLegacyEventTimeKeySuite extends StreamTest {
  import testImplicits._

  private val legacyMode = SQLConf.STATEFUL_OPERATOR_ALLOW_MULTIPLE.key -> "false"

  /** Adds the key column `<side>w` to an input whose event time is `<side>t`. */
  private type KeyBuilder = (DataFrame, String) => DataFrame

  private val tumbling: KeyBuilder =
    (df, side) => df.withColumn(s"${side}w", window(col(s"${side}t"), "10 seconds"))
  private val sliding: KeyBuilder =
    (df, side) => df.withColumn(s"${side}w", window(col(s"${side}t"), "10 seconds", "5 seconds"))
  private val copy: KeyBuilder = (df, side) => df.withColumn(s"${side}w", col(s"${side}t"))
  private val copyThenWindow: KeyBuilder = (df, side) =>
    df.withColumn(s"${side}c", col(s"${side}t"))
      .withColumn(s"${side}w", window(col(s"${side}c"), "10 seconds"))
  private val renamedWindow: KeyBuilder = (df, side) =>
    df.withColumn(s"${side}x", window(col(s"${side}t"), "10 seconds"))
      .filter(col(s"${side}k") >= 0)
      .withColumnRenamed(s"${side}x", s"${side}w")
  // A window over the event time less 20 seconds, in a column that keeps the event time's
  // watermark metadata. Its end can be at or below the watermark while the event time is above.
  private val shiftedWindow: KeyBuilder = (df, side) => {
    val time = s"${side}t"
    df.select(col("*"),
        (col(time) - expr("INTERVAL 20 SECONDS")).as(s"${side}s", df.schema(time).metadata))
      .withColumn(s"${side}w", window(col(s"${side}s"), "10 seconds"))
  }
  private val sessionWindow: KeyBuilder =
    (df, side) => df.withColumn(s"${side}w", session_window(col(s"${side}t"), "10 seconds"))

  private def eventTimeInput(memory: MemoryStream[(Int, Int)], side: String): DataFrame =
    memory.toDF().toDF(s"${side}k", "time")
      .select(col(s"${side}k"), timestamp_seconds($"time").as(s"${side}t"))
      .withWatermark(s"${side}t", "1 second")

  private def keyed(memory: MemoryStream[(Int, Int)], side: String, key: KeyBuilder): DataFrame =
    key(eventTimeInput(memory, side), side)
      .select(col(s"${side}k"), col(s"${side}t"), col(s"${side}w"))

  /** The (key, event time) pairs that the shared rule accepts, by column name. */
  private def eventTimeCopies(df: DataFrame): Set[(String, String)] = {
    val plan = df.queryExecution.analyzed
    val names = plan.flatMap(_.output).map(a => a.exprId -> a.name).toMap
    StreamingJoinHelper.findEventTimeCopies(plan).pairs.map { case (key, eventTime) =>
      (names(key), names(eventTime))
    }
  }

  private def joinWithRight(left: DataFrame, key: KeyBuilder = tumbling): DataFrame =
    left.join(keyed(MemoryStream[(Int, Int)], "r", key), expr("lk = rk AND lw = rw"))

  Seq(
    "tumbling window" -> tumbling,
    "sliding window" -> sliding,
    "copy" -> copy,
    "window over a copy" -> copyThenWindow,
    "renamed window after a filter" -> renamedWindow).foreach { case (name, key) =>
    test(s"$name of the event time after it is accepted") {
      val copies = eventTimeCopies(joinWithRight(keyed(MemoryStream[(Int, Int)], "l", key), key))
      assert(copies.contains(("lw", "lt")))
      assert(copies.contains(("rw", "rt")))
    }
  }

  test("window over a shifted time is not accepted") {
    val left = shiftedWindow(eventTimeInput(MemoryStream[(Int, Int)], "l"), "l")
    val copies = eventTimeCopies(joinWithRight(left))
    assert(!copies.contains(("lw", "lt")))
    assert(!copies.contains(("ls", "lt")))
    // The window is over ls itself, which late filtering does not use while lt comes first.
    assert(copies.contains(("lw", "ls")))
  }

  test("session window is not accepted") {
    val copies = eventTimeCopies(joinWithRight(keyed(MemoryStream[(Int, Int)], "l", sessionWindow)))
    assert(!copies.exists(_._1 == "lw"))
  }

  test("window over another input's event time is not accepted") {
    val a = MemoryStream[(Int, Int)]
    val b = MemoryStream[(Int, Int)]
    val left = eventTimeInput(a, "a").join(eventTimeInput(b, "b"), expr("ak = bk AND at = bt"))
      .withColumn("lk", $"ak")
      .withColumn("lw", window($"bt", "10 seconds"))
    val copies = eventTimeCopies(joinWithRight(left))
    // The left input filters late rows by at, but the window is over bt.
    assert(!copies.contains(("lw", "at")))
    assert(copies.contains(("lw", "bt")))
  }

  test("union is accepted only when every input builds the key from the same position") {
    def union(first: KeyBuilder, second: KeyBuilder): DataFrame =
      keyed(MemoryStream[(Int, Int)], "l", first)
        .union(keyed(MemoryStream[(Int, Int)], "l", second))
    assert(eventTimeCopies(joinWithRight(union(tumbling, sliding))).contains(("lw", "lt")))
    assert(!eventTimeCopies(joinWithRight(union(tumbling, shiftedWindow))).contains(("lw", "lt")))
    assert(!eventTimeCopies(joinWithRight(union(shiftedWindow, tumbling))).contains(("lw", "lt")))
  }

  test("a pair rejected by one join input is rejected for every join") {
    // The second join's left input is a union whose first input is the first join's input, so
    // both inputs output the same expression IDs, but only the first builds the window from lt.
    val windowed = keyed(MemoryStream[(Int, Int)], "l", tumbling).queryExecution.analyzed
    val shifted = keyed(MemoryStream[(Int, Int)], "l", shiftedWindow).queryExecution.analyzed
    val right = keyed(MemoryStream[(Int, Int)], "r", tumbling).queryExecution.analyzed
    val first = Join(windowed, right, Inner, None, JoinHint.NONE)
    val second = Join(Union(Seq(windowed, shifted)), right, Inner, None, JoinHint.NONE)
    val Seq(_, lt, lw) = windowed.output
    val pair = (lw.exprId, lt.exprId)
    assert(StreamingJoinHelper.findEventTimeCopies(first).pairs.contains(pair))
    assert(!StreamingJoinHelper.findEventTimeCopies(second).pairs.contains(pair))
    assert(!StreamingJoinHelper.findEventTimeCopies(Union(Seq(first, second))).pairs
      .contains(pair))
  }

  // Batches of left and right rows of (key, seconds), with 10-second windows. The watermark is
  // 30 seconds in the third batch, so it finalizes the rows at 5 and 6 seconds, whose windows end
  // by 15 seconds, and keeps those at 31 and 40 seconds.
  private def legacyJoin(
      joinType: String,
      key: KeyBuilder,
      windowsPerRow: Int,
      version: Int,
      restart: Boolean): Unit = {
    withSQLConf(legacyMode,
        SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> version.toString,
        SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
        SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false") {
      val leftInput = MemoryStream[(Int, Int)]
      val rightInput = MemoryStream[(Int, Int)]
      val joined = keyed(leftInput, "l", key)
        .join(keyed(rightInput, "r", key), expr("lk = rk AND lw = rw"), joinType)
      val (result, matched, unmatchedLeft) = if (joinType == "left_semi") {
        (joined.select($"lk", $"lt".cast("int")), Row(1, 5), Nil)
      } else {
        val unmatched = if (joinType == "left_outer" || joinType == "full_outer") {
          Seq(Row(2, 6, null, null))
        } else {
          Nil
        }
        (joined.select($"lk", $"lt".cast("int"), $"rk", $"rt".cast("int")), Row(1, 5, 1, 5),
          unmatched)
      }
      def times(rows: Seq[Row]): Seq[Row] = rows.flatMap(Seq.fill(windowsPerRow)(_))
      withTempDir { checkpoint =>
        val start = StartStream(checkpointLocation = checkpoint.getCanonicalPath)
        val restartActions = if (restart) Seq(StopStream, start) else Nil
        testStream(result)((Seq(
          start,
          MultiAddData(leftInput, (1, 5), (2, 6))(rightInput, (1, 5)),
          CheckNewAnswer(times(Seq(matched)): _*),
          MultiAddData(leftInput, (3, 31))(rightInput, (9, 31)),
          CheckNewAnswer()) ++ restartActions ++ Seq(
          AddData(rightInput, (9, 40)),
          CheckNewAnswer(times(unmatchedLeft): _*),
          // Only the rows at 31 and 40 seconds stay, as in Spark 4.3. Without the bound, all
          // six rows would stay in state.
          Execute { query =>
            assert(query.lastProgress.stateOperators.head.numRowsTotal === 3 * windowsPerRow)
          })): _*)
      }
    }
  }

  for {
    (keyName, key, windowsPerRow) <- Seq(
      ("tumbling window", tumbling, 1), ("sliding window", sliding, 2), ("copy", copy, 1))
    joinType <- Seq("inner", "left_outer", "right_outer", "full_outer", "left_semi")
    version <- Seq(2, 3)
  } {
    test(s"legacy-mode $joinType join on a $keyName after the event time evicts by it, " +
        s"V$version") {
      legacyJoin(joinType, key, windowsPerRow, version, restart = false)
    }
  }

  for {
    joinType <- Seq("left_outer", "full_outer")
    version <- Seq(2, 3)
  } {
    test(s"legacy-mode $joinType join on a window after the event time evicts after a " +
        s"restart, V$version") {
      legacyJoin(joinType, tumbling, 1, version, restart = true)
    }
  }

  // The V4 state manager accepts one event-time column per input, as in Spark 4.3, so these
  // inputs keep failing there whichever key bounds them.
  test("legacy-mode V4 join on an input with a window after the event time still fails") {
    withSQLConf(legacyMode,
        SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> "4",
        SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName) {
      val leftInput = MemoryStream[(Int, Int)]
      val rightInput = MemoryStream[(Int, Int)]
      val joined = keyed(leftInput, "l", tumbling)
        .join(keyed(rightInput, "r", tumbling), expr("lk = rk AND lw = rw"))
      testStream(joined)(
        MultiAddData(leftInput, (1, 5))(rightInput, (1, 5)),
        ExpectFailure[AnalysisException](assertFailure = error =>
          assert(error.getMessage.contains("MULTIPLE_EVENT_TIME_COLUMNS"))))
    }
  }

  for (version <- Seq(2, 3)) {
    test(s"legacy-mode inner join on a window over a shifted time keeps rows that can still " +
        s"match, V$version") {
      withSQLConf(legacyMode,
          SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> version.toString,
          SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
          SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false") {
        val leftInput = MemoryStream[(Int, Int)]
        val rightInput = MemoryStream[(Int, Int)]
        val joined = keyed(leftInput, "l", shiftedWindow)
          .join(keyed(rightInput, "r", shiftedWindow), expr("lk = rk AND lw = rw"))
          .select($"lk", $"lt".cast("int"), $"rk", $"rt".cast("int"))
        testStream(joined)(
          MultiAddData(leftInput, (1, 55))(rightInput, (9, 50)),
          CheckNewAnswer(),
          // The watermark is 49 seconds. The window of (1, 55) is [30, 40), so evicting by the
          // window end would remove it here.
          AddData(rightInput, (9, 51)),
          CheckNewAnswer(),
          // (1, 56) is above the watermark of 50 seconds and has the same window.
          AddData(rightInput, (1, 56)),
          CheckNewAnswer(Row(1, 55, 1, 56)))
      }
    }
  }

  private def assertLegacyWindowHint(left: DataFrame, right: DataFrame): Unit = {
    withSQLConf(legacyMode) {
      val error = intercept[AnalysisException] {
        testStream(left.join(right, expr("lk = rk AND lw = rw"), "left_outer"))(
          StartStream(), CheckNewAnswer())
      }
      assert(error.getMessage.contains("the right input filters late rows by its first " +
        "watermarked column, rt, so its window key rw is a time constraint only if that input " +
        "computes it as a tumbling or sliding window over rt itself"))
    }
  }

  test("legacy-mode left outer join on a window over a shifted time fails analysis") {
    assertLegacyWindowHint(keyed(MemoryStream[(Int, Int)], "l", tumbling),
      keyed(MemoryStream[(Int, Int)], "r", shiftedWindow))
  }

  test("legacy-mode left outer join on a session window fails analysis") {
    assertLegacyWindowHint(keyed(MemoryStream[(Int, Int)], "l", tumbling),
      keyed(MemoryStream[(Int, Int)], "r", sessionWindow))
  }

  test("legacy-mode left outer join on a union of different windows fails analysis") {
    val right = keyed(MemoryStream[(Int, Int)], "r", tumbling)
      .union(keyed(MemoryStream[(Int, Int)], "r", shiftedWindow))
    assertLegacyWindowHint(keyed(MemoryStream[(Int, Int)], "l", tumbling), right)
  }

  test("default-mode left outer join on a window after the event time still fails analysis") {
    val error = intercept[AnalysisException] {
      val joined = keyed(MemoryStream[(Int, Int)], "l", tumbling)
        .join(keyed(MemoryStream[(Int, Int)], "r", tumbling), expr("lk = rk AND lw = rw"),
          "left_outer")
      testStream(joined)(StartStream(), CheckNewAnswer())
    }
    assert(error.getMessage.contains("allow unmatched state to be finalized"))
    assert(!error.getMessage.contains(SQLConf.STATEFUL_OPERATOR_ALLOW_MULTIPLE.key))
  }

  test("legacy-mode join fails at start when the planned input filters late rows by " +
      "another column") {
    withSQLConf(legacyMode, SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false") {
      val leftInput = MemoryStream[(Int, Int)]
      val rightInput = MemoryStream[(Int, Int)]
      def withCopy(input: MemoryStream[(Int, Int)], side: String): DataFrame =
        tumbling(eventTimeInput(input, side), side).withColumn(s"${side}c", col(s"${side}t"))
      // The analyzed left input is [lk, lt, lw, lc], where lc copies lt, so lc bounds the right
      // state. Nothing after the join reads lt, so the optimizer prunes it, and the planned left
      // input filters late rows by its window lw, which lc is not built from.
      val joined = withCopy(leftInput, "l")
        .join(withCopy(rightInput, "r"), expr("lk = rk AND lc = rc"), "right_outer")
        .select($"lk", $"lw", $"rk", $"rt".cast("int"))
      testStream(joined)(
        MultiAddData(leftInput, (1, 5))(rightInput, (1, 5)),
        ExpectFailure[AnalysisException](assertFailure = error => {
          checkError(error.asInstanceOf[AnalysisException],
            condition = "STREAMING_JOIN_EVENT_TIME_KEY_NOT_PLANNED",
            parameters = Map(
              "joinType" -> "RightOuter",
              "side" -> "right",
              "otherSide" -> "left",
              "key" -> "lc",
              "lateFilteredColumn" -> "lw",
              "config" -> SQLConf.STREAMING_JOIN_CHECK_FINALIZATION_ENABLED.key))
        }))
    }
  }
}
