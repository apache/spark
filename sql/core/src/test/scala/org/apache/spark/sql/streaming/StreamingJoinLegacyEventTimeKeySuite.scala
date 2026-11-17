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

import org.apache.spark.sql.{AnalysisException, DataFrame, Row}
import org.apache.spark.sql.catalyst.analysis.{EventTimeLag, StreamingJoinHelper}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Join, JoinHint, LogicalPlan, Union}
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf

/**
 * With spark.sql.streaming.statefulOperator.allowMultiple=false, an input filters late rows by
 * its first watermarked column. An equality key on a later watermarked column bounds the other
 * input's state, as in Spark 4.3, lowered by how far the plan shows that some rows of the input
 * can hold the key below a watermarked column before it, and not at all when the plan shows no
 * bound on that.
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
  // Each session is [t, t + 10 seconds), as no aggregation merges sessions.
  private val sessionWindow: KeyBuilder =
    (df, side) => df.withColumn(s"${side}w", session_window(col(s"${side}t"), "10 seconds"))
  // Adds the event time less 20 seconds, in a column that keeps the event time's watermark.
  private def withShifted(df: DataFrame, side: String): DataFrame = {
    val time = s"${side}t"
    df.select(col("*"),
      (col(time) - expr("INTERVAL 20 SECONDS")).as(s"${side}s", df.schema(time).metadata))
  }
  // A window over the shifted time. Its end can be at or below the watermark while the event
  // time is above.
  private val shiftedWindow: KeyBuilder = (df, side) =>
    withShifted(df, side).withColumn(s"${side}w", window(col(s"${side}s"), "10 seconds"))
  private val shiftedCopy: KeyBuilder = (df, side) =>
    withShifted(df, side).withColumnRenamed(s"${side}s", s"${side}w")
  // A window over the shifted time without a watermark, which a window over an expression drops.
  private val unwatermarkedWindow: KeyBuilder = (df, side) =>
    df.withColumn(s"${side}w",
      window(col(s"${side}t") - expr("INTERVAL 20 SECONDS"), "10 seconds"))

  /** Puts a node that keeps the input's columns between an input and the join. */
  private type Through = (DataFrame, String) => DataFrame

  private val throughNodes: Seq[(String, Through)] = Seq(
    "repartition" -> ((df, _) => df.repartition(3)),
    "temporary view read from SQL" -> { (df, side) =>
      df.createOrReplaceTempView(s"legacy_$side")
      spark.sql(s"SELECT * FROM legacy_$side")
    },
    "typed filter" -> ((df, _) => df.filter((r: Row) => !r.isNullAt(0))),
    "observe" -> ((df, side) => df.observe(s"legacy_$side", count(lit(1)))),
    "hint" -> ((df, _) => df.hint("broadcast")),
    "stream-static join" -> { (df, side) =>
      df.join(Seq(1, 2, 3, 9).toDF("sk"), col(s"${side}k") === $"sk")
        .select(col(s"${side}k"), col(s"${side}t"), col(s"${side}w"))
    })

  private def eventTimeInput(memory: MemoryStream[(Int, Int)], side: String): DataFrame =
    memory.toDF().toDF(s"${side}k", "time")
      .select(col(s"${side}k"), timestamp_seconds($"time").as(s"${side}t"))
      .withWatermark(s"${side}t", "1 second")

  private def keyed(memory: MemoryStream[(Int, Int)], side: String, key: KeyBuilder): DataFrame =
    key(eventTimeInput(memory, side), side)
      .select(col(s"${side}k"), col(s"${side}t"), col(s"${side}w"))

  /** The lags of the join inputs' watermarked columns other than lag zero, by column name. */
  private def lags(df: DataFrame, planned: Boolean = false): Map[String, EventTimeLag] = {
    val plan = if (planned) df.queryExecution.optimizedPlan else df.queryExecution.analyzed
    val names = plan.flatMap(_.output).map(a => a.exprId -> a.name).toMap
    StreamingJoinHelper.findEventTimeLags(plan, planned).lags
      .filter(_._2 != EventTimeLag.zero).map { case (id, lag) => names(id) -> lag }
  }

  private def lagOf(seconds: Int): EventTimeLag =
    EventTimeLag(seconds * 1000000L, unrelated = false, unbounded = false)

  private def joinWithRight(left: DataFrame, key: KeyBuilder = tumbling): DataFrame =
    left.join(keyed(MemoryStream[(Int, Int)], "r", key), expr("lk = rk AND lw = rw"))

  Seq(
    "tumbling window" -> tumbling,
    "sliding window" -> sliding,
    "session window" -> sessionWindow,
    "copy" -> copy,
    "window over a copy" -> copyThenWindow,
    "renamed window after a filter" -> renamedWindow).foreach { case (name, key) =>
    test(s"$name of the event time after it has lag zero") {
      assert(lags(joinWithRight(keyed(MemoryStream[(Int, Int)], "l", key), key)).isEmpty)
    }
  }

  throughNodes.foreach { case (name, through) =>
    test(s"window of the event time behind a $name has lag zero") {
      withTempView("legacy_l", "legacy_r") {
        val left = through(keyed(MemoryStream[(Int, Int)], "l", tumbling), "l")
        val right = through(keyed(MemoryStream[(Int, Int)], "r", tumbling), "r")
        assert(lags(left.join(right, expr("lk = rk AND lw = rw"))).isEmpty)
      }
    }
  }

  test("window or copy of a time shifted backward has the shift as its lag") {
    val left = shiftedWindow(eventTimeInput(MemoryStream[(Int, Int)], "l"), "l")
    // ls is 20 seconds below lt in every row, and lw ends after ls.
    assert(lags(joinWithRight(left)) === Map("lw" -> lagOf(20), "ls" -> lagOf(20)))
    assert(lags(joinWithRight(keyed(MemoryStream[(Int, Int)], "l", shiftedCopy), copy))
      === Map("lw" -> lagOf(20)))
  }

  private val shiftedSlidingWindow: KeyBuilder = (df, side) =>
    withShifted(df, side).withColumn(s"${side}w",
      window(col(s"${side}s"), "10 seconds", "5 seconds"))

  test("sliding window over a time shifted backward has the shift as its lag") {
    // The walk follows the key through the expansion of the sliding window, to its source ls.
    val left = shiftedSlidingWindow(eventTimeInput(MemoryStream[(Int, Int)], "l"), "l")
    assert(lags(joinWithRight(left)) === Map("lw" -> lagOf(20), "ls" -> lagOf(20)))
  }

  test("window over a time shifted backward has the shift as its lag behind a stream-static " +
      "join") {
    // The walk follows both columns into the streaming input of the join.
    val left = throughNodes.toMap.apply("stream-static join")(
      keyed(MemoryStream[(Int, Int)], "l", shiftedWindow), "l")
    assert(lags(joinWithRight(left)) === Map("lw" -> lagOf(20)))
  }

  // The shifted copy ls is 20 seconds below the event time lt in every row. The column before it
  // is a window over lt, which ends up to 10 seconds after lt, so ls is up to 30 seconds below
  // it. The walk follows that window through each way that the rewrite builds one.
  Seq(
    "tumbling" -> ((df: DataFrame) => df.withColumn("lw", window($"lt", "10 seconds"))),
    "sliding" -> ((df: DataFrame) => df.withColumn("lw", window($"lt", "10 seconds", "5 seconds"))),
    "session" -> ((df: DataFrame) => df.withColumn("lw", session_window($"lt", "10 seconds")))
  ).foreach { case (kind, addWindow) =>
    test(s"copy of a time shifted backward after a $kind window over the event time has the " +
        "shift and the window's length as its lag") {
      val left = withShifted(addWindow(eventTimeInput(MemoryStream[(Int, Int)], "l")), "l")
        .select($"lk", $"lw", $"ls")
      val right = keyed(MemoryStream[(Int, Int)], "r", copy)
      assert(lags(left.join(right, expr("lk = rk AND ls = rw"))) === Map("ls" -> lagOf(30)))
    }
  }

  test("window over a time shifted forward has lag zero") {
    def forwardWindow(df: DataFrame, side: String): DataFrame = {
      val forward = df.select(col("*"), (col(s"${side}t") + expr("INTERVAL 5 SECONDS"))
        .as(s"${side}f", df.schema(s"${side}t").metadata))
      forward.withColumn(s"${side}w", window(col(s"${side}f"), "10 seconds"))
    }
    val left = forwardWindow(eventTimeInput(MemoryStream[(Int, Int)], "l"), "l")
    assert(lags(joinWithRight(left)).isEmpty)
  }

  test("window over a time shifted backward has the shift as its lag behind a repartition") {
    val left = keyed(MemoryStream[(Int, Int)], "l", shiftedWindow).repartition(3)
    assert(lags(joinWithRight(left)) === Map("lw" -> lagOf(20)))
  }

  test("copy of a time shifted forward has lag zero") {
    def forward(df: DataFrame, side: String): DataFrame = df.select(col("*"),
      (col(s"${side}t") + expr("INTERVAL 1 DAY")).as(s"${side}w", df.schema(s"${side}t").metadata))
    assert(lags(joinWithRight(keyed(MemoryStream[(Int, Int)], "l", forward), copy)).isEmpty)
  }

  test("copy of a time shifted backward by a whole day has no bound") {
    def backward(df: DataFrame, side: String): DataFrame = df.select(col("*"),
      (col(s"${side}t") - expr("INTERVAL 1 DAY")).as(s"${side}w", df.schema(s"${side}t").metadata))
    assert(lags(joinWithRight(keyed(MemoryStream[(Int, Int)], "l", backward), copy)) ===
      Map("lw" -> EventTimeLag.unbounded))
  }

  test("copy of the event time after a window over it has the window's length as its lag") {
    // The planned input filters late rows by lw if the optimizer prunes lt, and lc is then
    // above the watermark less the 10 seconds of lw.
    Seq(tumbling -> 10, sliding -> 10, sessionWindow -> 10).foreach { case (window, length) =>
      val left = window(eventTimeInput(MemoryStream[(Int, Int)], "l"), "l")
        .withColumn("lc", $"lt")
      assert(lags(joinWithRight(left)) === Map("lc" -> lagOf(length)))
    }
    // A session ends its gap after the event time, which is not bounded for a gap from a column.
    val dynamicGap = eventTimeInput(MemoryStream[(Int, Int)], "l")
      .withColumn("lw", session_window($"lt", when($"lk" > 0, "10 seconds").otherwise("1 hour")))
      .withColumn("lc", $"lt")
    assert(lags(joinWithRight(dynamicGap)) === Map("lc" -> EventTimeLag.unbounded))
  }

  test("window after a longer window has the longer window's length as its lag") {
    val left = eventTimeInput(MemoryStream[(Int, Int)], "l")
      .withColumn("lh", window($"lt", "60 seconds"))
      .withColumn("lw", window($"lt", "10 seconds"))
    assert(lags(joinWithRight(left)) === Map("lw" -> lagOf(60)))
  }

  test("planned lags are against the column that the planned input filters late rows by") {
    def withCopy(memory: MemoryStream[(Int, Int)], side: String): DataFrame =
      tumbling(eventTimeInput(memory, side), side).withColumn(s"${side}c", col(s"${side}t"))
    val leftInput = MemoryStream[(Int, Int)]
    val rightInput = MemoryStream[(Int, Int)]
    val joined = withCopy(leftInput, "l")
      .join(withCopy(rightInput, "r"), expr("lk = rk AND lc = rc"))
    def plannedLags(times: Seq[String]): Map[String, EventTimeLag] = {
      var result: Map[String, EventTimeLag] = Map.empty
      withSQLConf(legacyMode) {
        testStream(joined.select((Seq("lk", "rk") ++ times).map(col): _*))(
          AddData(leftInput, (1, 5)),
          CheckNewAnswer(),
          Execute { query =>
            val plan = query.lastExecution.optimizedPlan
            val names = plan.flatMap(_.output).map(a => a.exprId -> a.name).toMap
            result = StreamingJoinHelper.findEventTimeLags(plan, planned = true).lags
              .filter(_._2 != EventTimeLag.zero).map { case (id, lag) => names(id) -> lag }
          })
      }
      result
    }
    // With the event times read after the join, they filter late rows, so the copies have lag
    // zero. Without them, the windows do, and the copies are up to 10 seconds below them.
    assert(plannedLags(Seq("lt", "rt")).isEmpty)
    assert(plannedLags(Seq("lw", "rw")) === Map("lc" -> lagOf(10), "rc" -> lagOf(10)))
  }

  // The earlier join outputs at from its left input and bt from its right one. Its condition
  // relates them where every row that it outputs with a non-NULL bt satisfies at = bt: in an
  // inner join, and in a left outer join, which pads bt's input. Otherwise the key keeps its
  // bound, as in Spark 4.3, but a range condition gets none from it.
  Seq(
    ("ak = bk AND at = bt", "inner", true),
    ("ak = bk AND at = bt", "left_outer", true),
    ("ak = bk AND at = bt", "right_outer", false),
    ("ak = bk AND at = bt", "full_outer", false),
    ("ak = bk", "inner", false),
    ("ak = bk AND at <= bt", "inner", false)
  ).foreach { case (condition, joinType, related) =>
    val outcome = if (related) "has lag zero" else "is unrelated"
    test(s"window over another input's event time after a $joinType join on $condition " +
        outcome) {
      val left = eventTimeInput(MemoryStream[(Int, Int)], "a")
        .join(eventTimeInput(MemoryStream[(Int, Int)], "b"), expr(condition), joinType)
        .withColumn("lk", $"ak")
        .withColumn("lw", window($"bt", "10 seconds"))
      val expected = if (related) {
        Map.empty
      } else {
        Map("bt" -> EventTimeLag.unrelated, "lw" -> EventTimeLag.unrelated)
      }
      assert(lags(joinWithRight(left)) === expected)
    }
  }

  test("union takes the largest lag of its inputs") {
    def union(first: KeyBuilder, second: KeyBuilder): DataFrame =
      keyed(MemoryStream[(Int, Int)], "l", first)
        .union(keyed(MemoryStream[(Int, Int)], "l", second))
    assert(lags(joinWithRight(union(tumbling, sliding))).isEmpty)
    assert(lags(joinWithRight(union(tumbling, shiftedWindow))) === Map("lw" -> lagOf(20)))
    assert(lags(joinWithRight(union(shiftedWindow, tumbling))) === Map("lw" -> lagOf(20)))
    // The union takes its first input's metadata, but the second does not watermark lw.
    assert(lags(joinWithRight(union(tumbling, unwatermarkedWindow))) ===
      Map("lw" -> EventTimeLag.unbounded))
  }

  test("a column takes its largest lag over every join input") {
    // The second join's left input is a union whose first input is the first join's input, so
    // both inputs output the same expression IDs, but only the first builds the window from lt.
    val windowed = keyed(MemoryStream[(Int, Int)], "l", tumbling).queryExecution.analyzed
    val shifted = keyed(MemoryStream[(Int, Int)], "l", shiftedWindow).queryExecution.analyzed
    val right = keyed(MemoryStream[(Int, Int)], "r", tumbling).queryExecution.analyzed
    val first = Join(windowed, right, Inner, None, JoinHint.NONE)
    val second = Join(Union(Seq(windowed, shifted)), right, Inner, None, JoinHint.NONE)
    val lw = windowed.output(2).exprId
    def lag(plan: LogicalPlan): Option[EventTimeLag] =
      StreamingJoinHelper.findEventTimeLags(plan, planned = false).lags.get(lw)
    assert(lag(first) === Some(EventTimeLag.zero))
    assert(lag(second) === Some(lagOf(20)))
    assert(lag(Union(Seq(first, second))) === Some(lagOf(20)))
  }

  // Batches of left and right rows of (key, seconds), with 10-second windows. The watermark is
  // 30 seconds in the third batch, so it finalizes the rows at 5 and 6 seconds, whose windows end
  // by 16 seconds, and keeps those at 31 and 40 seconds.
  private def legacyJoin(
      joinType: String,
      key: KeyBuilder,
      windowsPerRow: Int,
      version: Int,
      restart: Boolean,
      through: Through = (df, _) => df): Unit = {
    withSQLConf(legacyMode,
        SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> version.toString,
        SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
        SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false") {
      val leftInput = MemoryStream[(Int, Int)]
      val rightInput = MemoryStream[(Int, Int)]
      val joined = through(keyed(leftInput, "l", key), "l")
        .join(through(keyed(rightInput, "r", key), "r"), expr("lk = rk AND lw = rw"), joinType)
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

  private val joinTypes = Seq("inner", "left_outer", "right_outer", "full_outer", "left_semi")

  for {
    (keyName, key, windowsPerRow) <- Seq(("tumbling window", tumbling, 1),
      ("sliding window", sliding, 2), ("session window", sessionWindow, 1), ("copy", copy, 1))
    joinType <- joinTypes
    version <- Seq(2, 3)
  } {
    test(s"legacy-mode $joinType join on a $keyName after the event time evicts by it, " +
        s"V$version") {
      legacyJoin(joinType, key, windowsPerRow, version, restart = false)
    }
  }

  for {
    (name, through) <- throughNodes
    joinType <- joinTypes
  } {
    test(s"legacy-mode $joinType join on a window behind a $name evicts by it, V3") {
      withTempView("legacy_l", "legacy_r") {
        legacyJoin(joinType, tumbling, 1, 3, restart = false, through)
      }
    }
  }

  for ((name, through) <- throughNodes) {
    test(s"legacy-mode left_outer join on a window behind a $name evicts by it, V2") {
      withTempView("legacy_l", "legacy_r") {
        legacyJoin("left_outer", tumbling, 1, 2, restart = false, through)
      }
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

  for (joinType <- Seq("inner", "left_outer")) {
    test(s"legacy-mode $joinType self-join on a window after the event time evicts by it") {
      withSQLConf(legacyMode,
          SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
          SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false") {
        val input = MemoryStream[(Int, Int)]
        val left = keyed(input, "l", tumbling)
        val joined = left.join(left.toDF("rk", "rt", "rw"), expr("lk = rk AND lw = rw"), joinType)
          .select($"lk", $"lt".cast("int"), $"rk", $"rt".cast("int"))
        testStream(joined)(
          AddData(input, (1, 5), (2, 6)),
          CheckNewAnswer(Row(1, 5, 1, 5), Row(2, 6, 2, 6)),
          AddData(input, (3, 31)),
          CheckNewAnswer(Row(3, 31, 3, 31)),
          AddData(input, (9, 40)),
          CheckNewAnswer(Row(9, 40, 9, 40)),
          // The watermark is 30 seconds, so each input keeps its rows at 31 and 40 seconds.
          Execute { query =>
            assert(query.lastProgress.stateOperators.head.numRowsTotal === 4)
          })
      }
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

    test(s"legacy-mode inner join on a copy of the event time after a window over it keeps " +
        s"rows that can still match, V$version") {
      withSQLConf(legacyMode,
          SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> version.toString,
          SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
          SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false") {
        val leftInput = MemoryStream[(Int, Int)]
        val rightInput = MemoryStream[(Int, Int)]
        def withCopy(input: MemoryStream[(Int, Int)], side: String): DataFrame =
          tumbling(eventTimeInput(input, side), side).withColumn(s"${side}c", col(s"${side}t"))
        // Nothing after the join reads lt, so the optimizer prunes it, and the planned left input
        // filters late rows by its window lw.
        val joined = withCopy(leftInput, "l")
          .join(withCopy(rightInput, "r"), expr("lk = rk AND lc = rc"))
          .select($"lk", $"lw", $"rk", $"rt".cast("int"))
        val window = Row(new Timestamp(0), new Timestamp(10000))
        testStream(joined)(
          AddData(rightInput, (1, 5)),
          CheckNewAnswer(),
          MultiAddData(leftInput, (9, 7))(rightInput, (9, 7)),
          CheckNewAnswer(Row(9, window, 9, 7)),
          // The watermark is 6 seconds, so evicting by lc would remove the right row at 5
          // seconds here.
          AddData(rightInput, (8, 7)),
          CheckNewAnswer(),
          // The left row at 5 seconds passes late filtering, as its window [0, 10) ends after
          // the watermark.
          AddData(leftInput, (1, 5)),
          CheckNewAnswer(Row(1, window, 1, 5)))
      }
    }
  }

  private def legacyLeftOuter(left: DataFrame, right: DataFrame): DataFrame =
    left.join(right, expr("lk = rk AND lw = rw"), "left_outer")

  private def assertLegacyKeyHint(left: DataFrame, right: DataFrame): Unit = {
    withSQLConf(legacyMode) {
      val error = intercept[AnalysisException] {
        testStream(legacyLeftOuter(left, right))(StartStream(), CheckNewAnswer())
      }
      assert(error.getMessage.contains("the right input filters late rows by its first " +
        "watermarked column, rt, and some of its rows can hold the key rw below a watermarked " +
        "column before it by an amount that analysis cannot bound"))
      // Legacy mode has one global watermark, so nothing after the join waits for this one.
      assert(!error.getMessage.contains("stateful operator after the join"))
    }
  }

  private def assertAdmitted(left: DataFrame, right: DataFrame): Unit = {
    withSQLConf(legacyMode) {
      testStream(legacyLeftOuter(left, right))(StartStream(), CheckNewAnswer())
    }
  }

  // Each of these keys bounds the left state at the watermark less its lag.
  test("legacy-mode left outer join on a window over a shifted time passes analysis") {
    assertAdmitted(keyed(MemoryStream[(Int, Int)], "l", tumbling),
      keyed(MemoryStream[(Int, Int)], "r", shiftedWindow))
  }

  test("legacy-mode left outer join on a copy of a shifted time passes analysis") {
    assertAdmitted(keyed(MemoryStream[(Int, Int)], "l", copy),
      keyed(MemoryStream[(Int, Int)], "r", shiftedCopy))
  }

  test("legacy-mode left outer join on a shifted window behind a repartition passes analysis") {
    assertAdmitted(keyed(MemoryStream[(Int, Int)], "l", tumbling),
      keyed(MemoryStream[(Int, Int)], "r", shiftedWindow).repartition(3))
  }

  test("legacy-mode left outer join on a union of a window and a shifted window passes " +
      "analysis") {
    val right = keyed(MemoryStream[(Int, Int)], "r", tumbling)
      .union(keyed(MemoryStream[(Int, Int)], "r", shiftedWindow))
    assertAdmitted(keyed(MemoryStream[(Int, Int)], "l", tumbling), right)
  }

  test("legacy-mode left outer join on a union with an unwatermarked window fails analysis") {
    val right = keyed(MemoryStream[(Int, Int)], "r", tumbling)
      .union(keyed(MemoryStream[(Int, Int)], "r", unwatermarkedWindow))
    assertLegacyKeyHint(keyed(MemoryStream[(Int, Int)], "l", tumbling), right)
  }

  test("legacy-mode left outer join on a copy of a time shifted back by a day fails analysis") {
    def backward(df: DataFrame, side: String): DataFrame = df.select(col("*"),
      (col(s"${side}t") - expr("INTERVAL 1 DAY")).as(s"${side}w", df.schema(s"${side}t").metadata))
    assertLegacyKeyHint(keyed(MemoryStream[(Int, Int)], "l", copy),
      keyed(MemoryStream[(Int, Int)], "r", backward))
  }

  test("legacy-mode left outer join on a copy of the event time after a session window with a " +
      "gap from a column fails analysis") {
    def copyAfterSession(df: DataFrame, side: String): DataFrame =
      df.withColumn(s"${side}x", session_window(col(s"${side}t"),
          when(col(s"${side}k") > 0, "10 seconds").otherwise("1 hour")))
        .withColumn(s"${side}w", col(s"${side}t"))
    val right = copyAfterSession(eventTimeInput(MemoryStream[(Int, Int)], "r"), "r")
    assertLegacyKeyHint(keyed(MemoryStream[(Int, Int)], "l", copy), right)
  }

  test("default-mode left outer join on a window after the event time, with the event time " +
      "kept, fails at start") {
    val leftInput = MemoryStream[(Int, Int)]
    val rightInput = MemoryStream[(Int, Int)]
    // Any watermarked key bounds state at analysis, as the planned input keeps one distinct
    // watermarked column, or fails at start, as here, where the query reads both.
    val joined = keyed(leftInput, "l", tumbling)
      .join(keyed(rightInput, "r", tumbling), expr("lk = rk AND lw = rw"), "left_outer")
    testStream(joined)(
      MultiAddData(leftInput, (1, 5))(rightInput, (1, 5)),
      ExpectFailure[AnalysisException](assertFailure = error =>
        assert(error.getMessage.contains("MULTIPLE_EVENT_TIME_COLUMNS"))))
  }
}
