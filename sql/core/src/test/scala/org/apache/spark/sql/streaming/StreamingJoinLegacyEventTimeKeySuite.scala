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
 * input's state, as in Spark 4.3, unless the plan shows that some rows of the input hold the key
 * below a watermarked column before it.
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

  /** The keys that the shared rule rejects, by column name. */
  private def rejectedKeys(df: DataFrame): Set[String] = {
    val plan = df.queryExecution.analyzed
    val names = plan.flatMap(_.output).map(a => a.exprId -> a.name).toMap
    StreamingJoinHelper.findLegacyEventTimeKeys(plan).rejected.map(names)
  }

  private def joinWithRight(left: DataFrame, key: KeyBuilder = tumbling): DataFrame =
    left.join(keyed(MemoryStream[(Int, Int)], "r", key), expr("lk = rk AND lw = rw"))

  Seq(
    "tumbling window" -> tumbling,
    "sliding window" -> sliding,
    "session window" -> sessionWindow,
    "copy" -> copy,
    "window over a copy" -> copyThenWindow,
    "renamed window after a filter" -> renamedWindow).foreach { case (name, key) =>
    test(s"$name of the event time after it is not rejected") {
      assert(rejectedKeys(joinWithRight(keyed(MemoryStream[(Int, Int)], "l", key), key)).isEmpty)
    }
  }

  throughNodes.foreach { case (name, through) =>
    test(s"window of the event time behind a $name is not rejected") {
      withTempView("legacy_l", "legacy_r") {
        val left = through(keyed(MemoryStream[(Int, Int)], "l", tumbling), "l")
        val right = through(keyed(MemoryStream[(Int, Int)], "r", tumbling), "r")
        assert(rejectedKeys(left.join(right, expr("lk = rk AND lw = rw"))).isEmpty)
      }
    }
  }

  test("window or copy of a time shifted backward is rejected") {
    val left = shiftedWindow(eventTimeInput(MemoryStream[(Int, Int)], "l"), "l")
    // ls is below lt in every row, and lw ends at most 10 seconds after ls.
    assert(rejectedKeys(joinWithRight(left)) === Set("lw", "ls"))
    assert(rejectedKeys(joinWithRight(keyed(MemoryStream[(Int, Int)], "l", shiftedCopy), copy))
      === Set("lw"))
  }

  test("window over a time shifted backward is rejected behind a repartition") {
    val left = keyed(MemoryStream[(Int, Int)], "l", shiftedWindow).repartition(3)
    assert(rejectedKeys(joinWithRight(left)) === Set("lw"))
  }

  test("copy of the event time after a window over it is rejected") {
    // The planned input filters late rows by lw if the optimizer prunes lt, and lc can be at or
    // below the watermark while lw ends after it.
    val left = tumbling(eventTimeInput(MemoryStream[(Int, Int)], "l"), "l")
      .withColumn("lc", $"lt")
    assert(rejectedKeys(joinWithRight(left)) === Set("lc"))
  }

  test("window over another input's event time is not rejected") {
    val a = MemoryStream[(Int, Int)]
    val b = MemoryStream[(Int, Int)]
    val left = eventTimeInput(a, "a").join(eventTimeInput(b, "b"), expr("ak = bk AND at = bt"))
      .withColumn("lk", $"ak")
      .withColumn("lw", window($"bt", "10 seconds"))
    // The walk does not compare columns of different join inputs, so it keeps the key, as
    // Spark 4.3 did; here at = bt makes the window a window over at.
    assert(rejectedKeys(joinWithRight(left)).isEmpty)
  }

  test("union is rejected when an input builds the key differently") {
    def union(first: KeyBuilder, second: KeyBuilder): DataFrame =
      keyed(MemoryStream[(Int, Int)], "l", first)
        .union(keyed(MemoryStream[(Int, Int)], "l", second))
    assert(rejectedKeys(joinWithRight(union(tumbling, sliding))).isEmpty)
    assert(rejectedKeys(joinWithRight(union(tumbling, shiftedWindow))) === Set("lw"))
    assert(rejectedKeys(joinWithRight(union(shiftedWindow, tumbling))) === Set("lw"))
    // The union takes its first input's metadata, but the second does not watermark lw.
    assert(rejectedKeys(joinWithRight(union(tumbling, unwatermarkedWindow))) === Set("lw"))
  }

  test("a key rejected by one join input is rejected for every join") {
    // The second join's left input is a union whose first input is the first join's input, so
    // both inputs output the same expression IDs, but only the first builds the window from lt.
    val windowed = keyed(MemoryStream[(Int, Int)], "l", tumbling).queryExecution.analyzed
    val shifted = keyed(MemoryStream[(Int, Int)], "l", shiftedWindow).queryExecution.analyzed
    val right = keyed(MemoryStream[(Int, Int)], "r", tumbling).queryExecution.analyzed
    val first = Join(windowed, right, Inner, None, JoinHint.NONE)
    val second = Join(Union(Seq(windowed, shifted)), right, Inner, None, JoinHint.NONE)
    val lw = windowed.output(2).exprId
    assert(!StreamingJoinHelper.findLegacyEventTimeKeys(first).rejected.contains(lw))
    assert(StreamingJoinHelper.findLegacyEventTimeKeys(second).rejected.contains(lw))
    assert(StreamingJoinHelper.findLegacyEventTimeKeys(Union(Seq(first, second))).rejected
      .contains(lw))
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

  private def assertLegacyKeyHint(left: DataFrame, right: DataFrame): Unit = {
    withSQLConf(legacyMode) {
      val error = intercept[AnalysisException] {
        testStream(left.join(right, expr("lk = rk AND lw = rw"), "left_outer"))(
          StartStream(), CheckNewAnswer())
      }
      assert(error.getMessage.contains("the right input filters late rows by its first " +
        "watermarked column, rt, and some of its rows can hold the key rw below a watermarked " +
        "column before it"))
      // Legacy mode has one global watermark, so nothing after the join waits for this one.
      assert(!error.getMessage.contains("stateful operator after the join"))
    }
  }

  test("legacy-mode left outer join on a window over a shifted time fails analysis") {
    assertLegacyKeyHint(keyed(MemoryStream[(Int, Int)], "l", tumbling),
      keyed(MemoryStream[(Int, Int)], "r", shiftedWindow))
  }

  test("legacy-mode left outer join on a copy of a shifted time fails analysis") {
    assertLegacyKeyHint(keyed(MemoryStream[(Int, Int)], "l", copy),
      keyed(MemoryStream[(Int, Int)], "r", shiftedCopy))
  }

  test("legacy-mode left outer join on a shifted window behind a repartition fails analysis") {
    assertLegacyKeyHint(keyed(MemoryStream[(Int, Int)], "l", tumbling),
      keyed(MemoryStream[(Int, Int)], "r", shiftedWindow).repartition(3))
  }

  test("legacy-mode left outer join on a union of different windows fails analysis") {
    Seq(shiftedWindow, unwatermarkedWindow).foreach { second =>
      val right = keyed(MemoryStream[(Int, Int)], "r", tumbling)
        .union(keyed(MemoryStream[(Int, Int)], "r", second))
      assertLegacyKeyHint(keyed(MemoryStream[(Int, Int)], "l", tumbling), right)
    }
  }

  test("legacy-mode right outer join on a copy of the event time after a window over it " +
      "fails analysis") {
    withSQLConf(legacyMode) {
      def withCopy(side: String): DataFrame =
        tumbling(eventTimeInput(MemoryStream[(Int, Int)], side), side)
          .withColumn(s"${side}c", col(s"${side}t"))
      val error = intercept[AnalysisException] {
        testStream(withCopy("l").join(withCopy("r"), expr("lk = rk AND lc = rc"), "right_outer"))(
          StartStream(), CheckNewAnswer())
      }
      assert(error.getMessage.contains("the left input filters late rows by its first " +
        "watermarked column, lt, and some of its rows can hold the key lc below a watermarked " +
        "column before it"))
    }
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
}
