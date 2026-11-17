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

import org.apache.spark.sql.{AnalysisException, Column, DataFrame, Row}
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf

/**
 * Stream-stream joins whose bound must hold whatever column pruning leaves in the planned
 * inputs: keys and range conditions on a watermarked column that follows the input's event time,
 * and keys that shift the event time by several intervals.
 */
class StreamingJoinPlanRobustBoundSuite extends StreamTest {
  import testImplicits._

  private val legacyMode = SQLConf.STATEFUL_OPERATOR_ALLOW_MULTIPLE.key -> "false"
  private val defaultMode = SQLConf.STATEFUL_OPERATOR_ALLOW_MULTIPLE.key -> "true"

  /** An input of (key, seconds) with the event time `<side>t` and a watermark delay of 1 s. */
  private def eventTimeInput(memory: MemoryStream[(Int, Int)], side: String): DataFrame =
    memory.toDF().toDF(s"${side}k", "time")
      .select(col(s"${side}k"), timestamp_seconds($"time").as(s"${side}t"))
      .withWatermark(s"${side}t", "1 second")

  /** Adds `<side>w`, a 10-second window over the event time. */
  private def windowed(memory: MemoryStream[(Int, Int)], side: String): DataFrame =
    eventTimeInput(memory, side).withColumn(s"${side}w", window(col(s"${side}t"), "10 seconds"))

  /** Adds a watermarked column `<side>s` that holds the event time shifted by `shift`. */
  private def withShifted(df: DataFrame, side: String, shift: String): DataFrame = {
    val time = s"${side}t"
    df.select(col("*"), (col(time) + expr(shift)).as(s"${side}s", df.schema(time).metadata))
  }

  private def testJoin(conf: (String, String)*)(result: DataFrame)(
      actions: StreamAction*): Unit = {
    withSQLConf(Seq(
        SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
        SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false") ++ conf: _*) {
      testStream(result)(StartStream() +: actions: _*)
    }
  }

  private def restartActions(restart: Boolean): Seq[StreamAction] =
    if (restart) Seq(StopStream, StartStream()) else Nil

  private def assertStateRows(rows: Long): StreamAction = Execute { query =>
    assert(query.lastProgress.stateOperators.head.numRowsTotal === rows)
  }

  /**
   * Checks the state rows. A left semi join does not store a left row that a right row matches in
   * its batch, so it can hold fewer rows than the joins that keep every row until eviction.
   */
  private def assertStateRows(joinType: String, rows: Long, semiRows: Long): Seq[StreamAction] =
    Seq(assertStateRows(if (joinType == "left_semi") semiRows else rows))

  private def version(v: Int): (String, String) =
    SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> v.toString

  /** The window of `seconds` seconds that starts at `start` seconds. */
  private def windowRow(start: Int, seconds: Int = 10): Row =
    Row(new Timestamp(start * 1000L), new Timestamp((start + seconds) * 1000L))

  // With multiple stateful operators allowed, the planned input keeps one watermarked column or
  // the query fails at start. Here pruning removes the event time, so the window filters late
  // rows, and Spark 4.3 evicted both inputs at the window end.
  for {
    joinType <- Seq("left_outer", "left_semi", "full_outer")
    v <- Seq(2, 3, 4)
    restart <- Seq(false, true)
  } {
    test(s"default-mode $joinType join on a window after the event time, with the event time " +
        s"pruned, evicts at the window end, V$v${if (restart) ", restart" else ""}") {
      val leftInput = MemoryStream[(Int, Int)]
      val rightInput = MemoryStream[(Int, Int)]
      val joined = windowed(leftInput, "l")
        .join(windowed(rightInput, "r"), expr("lk = rk AND lw = rw"), joinType)
      val result = if (joinType == "left_semi") {
        joined.select($"lk", $"lw.start".cast("int"))
      } else {
        joined.select($"lk", $"lw.start".cast("int"), $"rk", $"rw.start".cast("int"))
      }
      val (matched, unmatched) = joinType match {
        case "left_semi" => (Row(1, 0), Nil)
        case _ => (Row(1, 0, 1, 0), Seq(Row(2, 0, null, null)))
      }
      testJoin(defaultMode, version(v))(result)(Seq(
        MultiAddData(leftInput, (1, 5), (2, 6))(rightInput, (1, 5)),
        CheckNewAnswer(matched),
        // The watermark is 30 seconds from the next batch on.
        MultiAddData(leftInput, (3, 31))(rightInput, (9, 31)),
        CheckNewAnswer()) ++ restartActions(restart) ++ Seq(
        AddData(rightInput, (9, 40)),
        // The windows [0, 10) end below the watermark, so their unmatched rows are output.
        CheckNewAnswer(unmatched: _*)) ++ assertStateRows(joinType, 3, 3): _*)
    }
  }

  // A range condition on a copy of the right event time placed after it. Without rt in the
  // output, the planned right input keeps only the copy, which then filters late rows itself.
  for {
    (modeName, mode) <- Seq("default" -> defaultMode, "legacy" -> legacyMode)
    joinType <- Seq("left_outer", "left_semi")
    v <- Seq(2, 3)
  } {
    test(s"$modeName-mode $joinType join with a range on a copy of the event time evicts by " +
        s"it, V$v") {
      val leftInput = MemoryStream[(Int, Int)]
      val rightInput = MemoryStream[(Int, Int)]
      val right = eventTimeInput(rightInput, "r").withColumn("rc", $"rt")
      val joined = eventTimeInput(leftInput, "l").join(right,
        expr("lk = rk AND lt BETWEEN rc - INTERVAL 5 SECONDS AND rc + INTERVAL 5 SECONDS"),
        joinType)
      val result = if (joinType == "left_semi") {
        joined.select($"lk", $"lt".cast("int"))
      } else {
        joined.select($"lk", $"lt".cast("int"), $"rk")
      }
      val (matched, unmatched) = if (joinType == "left_semi") {
        (Row(1, 10), Nil)
      } else {
        (Row(1, 10, 1), Seq(Row(2, 12, null)))
      }
      testJoin(mode, version(v))(result)(Seq(
        MultiAddData(leftInput, (1, 10), (2, 12))(rightInput, (1, 12)),
        CheckNewAnswer(matched),
        MultiAddData(leftInput, (3, 40))(rightInput, (9, 40)),
        CheckNewAnswer(),
        // At the watermark of 39 seconds, a right row has rc above 39 seconds, so left rows
        // below 34 seconds can no longer match.
        AddData(rightInput, (9, 41)),
        CheckNewAnswer(unmatched: _*)) ++ assertStateRows(joinType, 3, 3): _*)
    }
  }

  // Legacy mode: a copy of the event time placed after a window over it. The planned input
  // filters late rows by the event time when the query reads it after the join, and by the
  // window otherwise, so a row's copy is above the watermark less the window's 10 seconds.
  for {
    joinType <- Seq("inner", "left_outer", "right_outer", "full_outer", "left_semi")
    v <- Seq(2, 3)
  } {
    def withCopy(memory: MemoryStream[(Int, Int)], side: String): DataFrame =
      windowed(memory, side).withColumn(s"${side}c", col(s"${side}t"))
    def expected(joinType: String, rows: Row*): Seq[Row] = joinType match {
      case "left_semi" => rows.map(r => Row(r.get(0), r.get(1)))
      case _ => rows
    }
    def project(joined: DataFrame, joinType: String, time: String => Column): DataFrame =
      if (joinType == "left_semi") {
        joined.select($"lk", time("l"))
      } else {
        joined.select($"lk", time("l"), $"rk", time("r"))
      }
    val outer = joinType == "left_outer" || joinType == "full_outer"

    test(s"legacy-mode $joinType join on a copy of the event time after a window over it, " +
        s"with the event time kept, evicts at the watermark, V$v") {
      val leftInput = MemoryStream[(Int, Int)]
      val rightInput = MemoryStream[(Int, Int)]
      val joined = withCopy(leftInput, "l")
        .join(withCopy(rightInput, "r"), expr("lk = rk AND lc = rc"), joinType)
      val result = project(joined, joinType, side => col(s"${side}t").cast("int"))
      testJoin(legacyMode, version(v))(result)(Seq(
        MultiAddData(leftInput, (1, 5), (2, 25))(rightInput, (1, 5)),
        CheckNewAnswer(expected(joinType, Row(1, 5, 1, 5)): _*),
        MultiAddData(leftInput, (9, 31))(rightInput, (9, 31)),
        CheckNewAnswer(expected(joinType, Row(9, 31, 9, 31)): _*),
        // The planned inputs filter late rows by their event times, so at the watermark of 30
        // seconds no later row matches the left row at 25 seconds or the right row at 5 seconds.
        AddData(rightInput, (8, 32)),
        CheckNewAnswer((if (outer) Seq(Row(2, 25, null, null)) else Nil): _*)) ++
        assertStateRows(joinType, 3, 2): _*)
    }

    // The left semi join outputs only left columns, so its right input is pruned to rk and rc,
    // and rc filters late rows itself: the left state is evicted at the watermark, not less the
    // window.
    val evictsAt = if (joinType == "left_semi") "the watermark" else "the watermark less the window"
    test(s"legacy-mode $joinType join on a copy of the event time after a window over it, " +
        s"with the event time pruned, evicts at $evictsAt, V$v") {
      val leftInput = MemoryStream[(Int, Int)]
      val rightInput = MemoryStream[(Int, Int)]
      val joined = withCopy(leftInput, "l")
        .join(withCopy(rightInput, "r"), expr("lk = rk AND lc = rc"), joinType)
      // The query reads the whole windows: reading only a field of one lets the optimizer prune
      // the watermarked window, and the copy then filters late rows itself.
      val result = project(joined, joinType, side => col(s"${side}w"))
      testJoin(legacyMode, version(v))(result)(Seq(
        MultiAddData(leftInput, (1, 5), (2, 25))(rightInput, (1, 5)),
        CheckNewAnswer(expected(joinType, Row(1, windowRow(0), 1, windowRow(0))): _*),
        MultiAddData(leftInput, (9, 31))(rightInput, (9, 31)),
        CheckNewAnswer(expected(joinType, Row(9, windowRow(30), 9, windowRow(30))): _*),
        // A left row passes late filtering only when its window ends after the 30-second
        // watermark, so its copy lc is above 20 seconds and the right state keeps rows above that.
        AddData(rightInput, (8, 32)),
        CheckNewAnswer(),
        MultiAddData(leftInput, (7, 41))(rightInput, (7, 41)),
        CheckNewAnswer(expected(joinType, Row(7, windowRow(40), 7, windowRow(40))): _*),
        // At the watermark of 40 seconds, each input's state keeps rows above 30 seconds.
        AddData(rightInput, (6, 42)),
        CheckNewAnswer((if (outer) Seq(Row(2, windowRow(20), null, null)) else Nil): _*)) ++
        assertStateRows(joinType, 6, 4): _*)
    }
  }

  // Legacy mode: each input keeps a 60-second window before the 10-second window it joins on,
  // so the planned input filters late rows by the longer window once pruning removes the event
  // time.
  for (v <- Seq(2, 3)) {
    test(s"legacy-mode inner join on a window after a longer window keeps rows that can still " +
        s"match, V$v") {
      def input(memory: MemoryStream[(Int, Int)], side: String): DataFrame =
        eventTimeInput(memory, side)
          .withColumn(s"${side}h", window(col(s"${side}t"), "60 seconds"))
          .withColumn(s"${side}w", window(col(s"${side}t"), "10 seconds"))
      val leftInput = MemoryStream[(Int, Int)]
      val rightInput = MemoryStream[(Int, Int)]
      // The query reads the whole 60-second window, so the optimizer keeps it.
      val result = input(leftInput, "l").join(input(rightInput, "r"), expr("lk = rk AND lw = rw"))
        .select($"lk", $"lh", $"rk")
      testJoin(legacyMode, version(v))(result)(
        AddData(rightInput, (1, 45)),
        CheckNewAnswer(),
        MultiAddData(leftInput, (8, 57))(rightInput, (9, 57)),
        CheckNewAnswer(),
        // The watermark is 56 seconds. The right row's window [40, 50) has ended, but a left row
        // at 45 seconds still passes late filtering, as its window [0, 60) has not.
        AddData(rightInput, (7, 58)),
        CheckNewAnswer(),
        AddData(leftInput, (1, 45)),
        CheckNewAnswer(Row(1, windowRow(0, 60), 1)))
    }
  }

  // Legacy mode: the left key ls is the event time less 20 seconds in a column that keeps its
  // watermark, so a left row keys the right state 20 seconds below its event time. The toDF
  // above the repartition aliases every column, and the optimizer removes those aliases.
  for {
    joinType <- Seq("inner", "left_outer")
    v <- Seq(2, 3)
  } {
    test(s"legacy-mode $joinType join on a shifted key that the optimizer renames keeps rows " +
        s"that can still match, V$v") {
      val leftInput = MemoryStream[(Int, Int)]
      val rightInput = MemoryStream[(Int, Int)]
      val left = withShifted(eventTimeInput(leftInput, "l"), "l", "- INTERVAL 20 SECONDS")
        .repartition(3).toDF("lk", "lt", "ls")
      val result = left.join(eventTimeInput(rightInput, "r"), expr("lk = rk AND ls = rt"),
        joinType).select($"lk", $"lt".cast("int"), $"rk", $"rt".cast("int"))
      testJoin(legacyMode, version(v))(result)(
        AddData(rightInput, (1, 30)),
        CheckNewAnswer(),
        MultiAddData(leftInput, (8, 80))(rightInput, (9, 46)),
        CheckNewAnswer(),
        // The watermark is 45 seconds. A later left row at 50 seconds has ls = 30 seconds.
        AddData(rightInput, (7, 47)),
        CheckNewAnswer(),
        AddData(leftInput, (1, 50)),
        CheckNewAnswer(Row(1, 50, 1, 30)))
    }
  }

  // Legacy mode: rs is the right event time less 20 seconds in a column that keeps its
  // watermark, and the range condition bounds lt by it.
  for {
    joinType <- Seq("inner", "left_outer")
    v <- Seq(2, 3)
  } {
    test(s"legacy-mode $joinType join with a range on a shifted watermarked column keeps rows " +
        s"that can still match, V$v") {
      val leftInput = MemoryStream[(Int, Int)]
      val rightInput = MemoryStream[(Int, Int)]
      val right = withShifted(eventTimeInput(rightInput, "r"), "r", "- INTERVAL 20 SECONDS")
      val result = eventTimeInput(leftInput, "l").join(right,
        expr("lk = rk AND lt BETWEEN rs AND rs + INTERVAL 1 HOUR"), joinType)
        .select($"lk", $"lt".cast("int"), $"rk", $"rt".cast("int"))
      testJoin(legacyMode, version(v))(result)(
        AddData(leftInput, (1, 35)),
        CheckNewAnswer(),
        MultiAddData(leftInput, (8, 46))(rightInput, (9, 66)),
        CheckNewAnswer(),
        // The watermark is 45 seconds. A later right row at 55 seconds has rs = 35 seconds.
        AddData(rightInput, (7, 67)),
        CheckNewAnswer(),
        AddData(rightInput, (1, 55)),
        CheckNewAnswer(Row(1, 35, 1, 55)))
    }
  }

  // Legacy mode: B is the padded left input of an earlier right outer join with A, and bs is bt
  // less 20 seconds in a column that keeps its watermark. Every non-NULL bt equals at, which
  // filters late rows, so the range on bs bounds C's state at the watermark less 20 seconds. The
  // analysis check needs that bound for a right outer join with C, and C's rows stay until then.
  for (v <- Seq(2, 3)) {
    test(s"legacy-mode right outer join with a range on a shifted column of the padded input " +
        s"of an earlier right outer join keeps rows that can still match, V$v") {
      val a = MemoryStream[(Int, Int)]
      val b = MemoryStream[(Int, Int)]
      val c = MemoryStream[(Int, Int)]
      val ab = withShifted(eventTimeInput(b, "b").join(eventTimeInput(a, "a"),
          expr("ak = bk AND at = bt"), "right_outer").select($"ak", $"at", $"bk", $"bt"),
        "b", "- INTERVAL 20 SECONDS")
      val result = ab.join(eventTimeInput(c, "c"),
          expr("ak = ck AND ct BETWEEN bs AND bs + INTERVAL 1 HOUR"), "right_outer")
        .select($"ak", $"at".cast("int"), $"bt".cast("int"), $"ck", $"ct".cast("int"))
      testJoin(legacyMode, version(v))(result)(
        AddData(c, (1, 30)),
        CheckNewAnswer(),
        MultiAddData(a, (8, 80))(b, (8, 80)),
        CheckNewAnswer(),
        AddData(c, (9, 46)),
        CheckNewAnswer(),
        // The watermark is 45 seconds. A later AB row at 50 seconds has bs = 30 seconds.
        AddData(c, (7, 47)),
        CheckNewAnswer(),
        MultiAddData(a, (1, 50))(b, (1, 50)),
        CheckNewAnswer(Row(1, 50, 50, 1, 30)))
    }
  }

  // The inputs of AB = A.join(B, condition, joinType), followed by a join of AB with C on
  // ak = ck AND ct BETWEEN bt AND bt + INTERVAL 1 HOUR. In legacy mode AB filters late rows by at,
  // which the query keeps. Every row of an inner or left outer AB with a non-NULL bt has bt = at,
  // or bt >= at for a BETWEEN tie, so C's state can be evicted at the watermark, as Spark 4.3 did.
  private def chainedRangeJoin(condition: String, joinType: String, secondJoinType: String):
      (MemoryStream[(Int, Int)], MemoryStream[(Int, Int)], MemoryStream[(Int, Int)],
        DataFrame) = {
    val a = MemoryStream[(Int, Int)]
    val b = MemoryStream[(Int, Int)]
    val c = MemoryStream[(Int, Int)]
    val ab = eventTimeInput(a, "a").join(eventTimeInput(b, "b"), expr(condition), joinType)
    val result = ab.join(eventTimeInput(c, "c"),
        expr("ak = ck AND ct BETWEEN bt AND bt + INTERVAL 1 HOUR"), secondJoinType)
      .select($"ck", $"ct".cast("int"), $"ak", $"at".cast("int"), $"bt".cast("int"))
    (a, b, c, result)
  }

  for {
    (tie, condition) <- Seq("equates" -> "ak = bk AND at = bt",
      "bounds by the event time" -> "ak = bk AND bt BETWEEN at AND at + INTERVAL 10 MINUTES")
    joinType <- Seq("inner", "left_outer")
    secondJoinType <- Seq("right_outer", "inner")
    v <- Seq(2, 3)
  } {
    test(s"legacy-mode $secondJoinType join with a range on a column that an earlier $joinType " +
        s"join $tie evicts at the watermark, V$v") {
      val (a, b, c, result) = chainedRangeJoin(condition, joinType, secondJoinType)
      val unmatched =
        if (secondJoinType == "right_outer") Seq(Row(2, 21, null, null, null)) else Nil
      testJoin(legacyMode, version(v))(result)(
        MultiAddData((a, Seq((1, 10))), (b, Seq((1, 10))), (c, Seq((1, 20), (2, 21)))),
        CheckNewAnswer(Row(1, 20, 1, 10, 10)),
        MultiAddData((a, Seq((3, 50))), (b, Seq((3, 50))), (c, Seq((9, 50)))),
        CheckNewAnswer(),
        // The watermark is 49 seconds, so C's rows at 20 and 21 seconds are evicted.
        AddData(c, (9, 60)),
        CheckNewAnswer(unmatched: _*),
        // Both AB rows stay, as a later C row can match them within the hour.
        assertStateRows(4))
    }
  }

  // After a key-only earlier join, a new A row can join an old B row, so bt can be far below at,
  // and after a full outer join, bt can be on a row without at. Neither bounds C's state.
  Seq(("ak = bk", "inner"), ("ak = bk AND at = bt", "full_outer")).foreach {
    case (condition, joinType) =>
      test(s"legacy-mode right outer join with a range on a column of an earlier $joinType join " +
          s"on $condition fails analysis") {
        val (_, _, _, result) = chainedRangeJoin(condition, joinType, "right_outer")
        withSQLConf(legacyMode) {
          val error = intercept[AnalysisException] {
            testStream(result)(StartStream(), CheckNewAnswer())
          }
          assert(error.getMessage.contains("allow unmatched state to be finalized"))
        }
      }
  }

  // bt is at least at less an hour, so C's state is evicted at the watermark less an hour, which
  // keeps the match of a C row with an AB row whose bt is up to an hour below at.
  for (v <- Seq(2, 3)) {
    test(s"legacy-mode inner join with a range on a column that an earlier join bounds an hour " +
        s"below the event time keeps rows that can still match, V$v") {
      val (a, b, c, result) =
        chainedRangeJoin("ak = bk AND bt >= at - INTERVAL 1 HOUR", "inner", "inner")
      testJoin(legacyMode, version(v))(result)(
        MultiAddData((b, Seq((1, 1000))), (c, Seq((1, 1000)))),
        CheckNewAnswer(),
        // The watermark becomes 1500 seconds, above the C row at 1000 seconds.
        MultiAddData((a, Seq((9, 1501))), (b, Seq((9, 1501))), (c, Seq((9, 1501)))),
        CheckNewAnswer(Row(9, 1501, 9, 1501, 1501)),
        AddData(c, (7, 1502)),
        CheckNewAnswer(),
        // A new A row at 1600 seconds joins the B row at 1000 seconds, which is at - 600 seconds.
        AddData(a, (1, 1600)),
        CheckNewAnswer(Row(1, 1000, 1, 1600, 1000)))
    }
  }

  test("legacy-mode inner join with a range on a column of an earlier key-only join keeps rows " +
      "that can still match") {
    val (a, b, c, result) = chainedRangeJoin("ak = bk", "inner", "inner")
    testJoin(legacyMode, version(2))(result)(
      MultiAddData((a, Seq((2, 30))), (b, Seq((1, 10), (2, 30))), (c, Seq((1, 15), (9, 30)))),
      CheckNewAnswer(),
      MultiAddData((a, Seq((3, 60))), (b, Seq((3, 60))), (c, Seq((9, 60)))),
      CheckNewAnswer(),
      // The watermark is 59 seconds. The new A row joins the B row at 10 seconds, whose bt
      // matches the C row at 15 seconds.
      AddData(a, (1, 100)),
      CheckNewAnswer(Row(1, 15, 1, 100, 10)))
  }

  // Legacy mode: R = A.join(B, ak = bk) keeps at, which filters its late rows. as is at less 20
  // seconds in a column that keeps its watermark, and the key-only join leaves bt unrelated to at:
  // a new A row can join an old B row. The left state is bounded by the as key at the watermark
  // less 20 seconds; the bt key, whose cutoff the plan does not show to be safe, must not win.
  for {
    joinType <- Seq("inner", "left_outer")
    v <- Seq(2, 3)
  } {
    test(s"legacy-mode $joinType join prefers a key with a known lag over an unrelated key, V$v") {
      val leftInput = MemoryStream[(Int, Int)]
      val a = MemoryStream[(Int, Int)]
      val b = MemoryStream[(Int, Int)]
      // lu has no watermark, so the optimizer cannot tie as to bt through the left input.
      val left = leftInput.toDF().toDF("lk", "time")
        .select($"lk", timestamp_seconds($"time").as("lx"), timestamp_seconds($"time").as("lu"))
        .withWatermark("lx", "1 second")
      val r = withShifted(eventTimeInput(a, "a"), "a", "- INTERVAL 20 SECONDS")
        .join(eventTimeInput(b, "b"), expr("ak = bk"))
      val result = left.join(r, expr("lk = ak AND lx = `as` AND lu = bt"), joinType)
        .select($"lk", $"lx".cast("int"), $"at".cast("int"), $"bt".cast("int"))
      testJoin(legacyMode, version(v))(result)(
        MultiAddData((leftInput, Seq((1, 30))), (b, Seq((1, 30)))),
        CheckNewAnswer(),
        MultiAddData((leftInput, Seq((9, 46))), (a, Seq((9, 46))), (b, Seq((9, 46)))),
        CheckNewAnswer(),
        // The watermark is 45 seconds: the left row keyed by lu = 30 seconds is below it, but
        // its lx = 30 seconds is above the as cutoff of 25 seconds.
        AddData(leftInput, (7, 47)),
        CheckNewAnswer(),
        // A new A row at 50 seconds has as = 30 seconds and joins the B row at 30 seconds.
        AddData(a, (1, 50)),
        CheckNewAnswer(Row(1, 30, 50, 30)))
    }
  }

  // Legacy mode: A keeps as, at less a whole day, in a column that keeps its watermark, and
  // AB = A.join(B, ak = bk AND as = bt) keeps at, which filters its late rows. A day can last 23
  // to 25 hours, so the equality bounds bt by nothing below at, as it bounds as: C's state has no
  // key cutoff, and evicting it at the watermark would lose the match below.
  private def dayShiftedChain(joinType: String): (MemoryStream[(Int, Int)],
      MemoryStream[(Int, Int)], MemoryStream[(Int, Int)], DataFrame) = {
    val a = MemoryStream[(Int, Int)]
    val b = MemoryStream[(Int, Int)]
    val c = MemoryStream[(Int, Int)]
    val ab = withShifted(eventTimeInput(a, "a"), "a", "- INTERVAL 1 DAY")
      .join(eventTimeInput(b, "b"), expr("ak = bk AND `as` = bt"))
    val result = ab.join(eventTimeInput(c, "c"), expr("ak = ck AND bt = ct"), joinType)
      .select($"ck", $"ct".cast("int"), $"at".cast("int"), $"bt".cast("int"))
    (a, b, c, result)
  }

  for (v <- Seq(2, 3)) {
    test(s"legacy-mode inner join on a column that an earlier join ties to a day shift keeps " +
        s"rows that can still match, V$v") {
      val (a, b, c, result) = dayShiftedChain("inner")
      testJoin(legacyMode, version(v))(result)(
        MultiAddData((b, Seq((1, 35))), (c, Seq((1, 35)))),
        CheckNewAnswer(),
        // The watermark becomes 70000 seconds, far above the C row at 35 seconds.
        MultiAddData((a, Seq((9, 70001))), (b, Seq((9, 70001))), (c, Seq((9, 70001)))),
        CheckNewAnswer(),
        AddData(c, (7, 70002)),
        CheckNewAnswer(),
        // A new A row at 86435 seconds has as = 35 seconds, which joins the B row at 35 seconds.
        AddData(a, (1, 86435)),
        CheckNewAnswer(Row(1, 35, 86435, 35)))
    }
  }

  test("legacy-mode right outer join on a column that an earlier join ties to a day shift " +
      "fails analysis") {
    val (_, _, _, result) = dayShiftedChain("right_outer")
    withSQLConf(legacyMode) {
      val error = intercept[AnalysisException] {
        testStream(result)(StartStream(), CheckNewAnswer())
      }
      assert(error.getMessage.contains("allow unmatched state to be finalized"))
    }
  }

  // Legacy mode: the right input is a union whose second input has no watermark and builds rc
  // from its own column, so rc has no bound below rt, which filters the union's late rows. In a
  // batch where that input has no data, the optimizer removes it from the plan, and the left
  // state must still not be evicted by rc.
  for {
    (name, condition) <- Seq("an equality key" -> "lk = rk AND lt = rc",
      "a range" -> "lk = rk AND lt BETWEEN rc AND rc + INTERVAL 1 HOUR")
    v <- Seq(2, 3)
  } {
    test(s"legacy-mode inner join with $name on a column that an unwatermarked union input " +
        s"builds keeps rows after a batch without its data, V$v") {
      val leftInput = MemoryStream[(Int, Int)]
      val r1 = MemoryStream[(Int, Int)]
      val r2 = MemoryStream[(Int, Int)]
      val watermarked = eventTimeInput(r1, "r").withColumn("rc", $"rt")
      val unwatermarked = r2.toDF().toDF("rk", "time").select($"rk",
        timestamp_seconds($"time").as("rt"), timestamp_seconds($"time" - 70).as("rc"))
      val result = eventTimeInput(leftInput, "l")
        .join(watermarked.union(unwatermarked), expr(condition))
        .select($"lk", $"lt".cast("int"), $"rt".cast("int"), $"rc".cast("int"))
      testJoin(legacyMode, version(v))(result)(
        MultiAddData((leftInput, Seq((1, 10))), (r2, Seq((5, 5)))),
        CheckNewAnswer(),
        MultiAddData((leftInput, Seq((9, 70))), (r1, Seq((9, 70)))),
        CheckNewAnswer(Row(9, 70, 70, 70)),
        // The watermark is 69 seconds, and the unwatermarked input has no data in this batch.
        AddData(leftInput, (7, 71)),
        CheckNewAnswer(),
        // A later row of that input at 80 seconds has rc = 10 seconds.
        AddData(r2, (1, 80)),
        CheckNewAnswer(Row(1, 10, 80, 10)))
    }
  }

  // Legacy mode: rs is the right event time less a whole day in a column that keeps its
  // watermark. A day can last 23 to 25 hours, so the plan gives rs no bound below rt, and the
  // range condition on it bounds no left row: evicting the left state at the watermark would lose
  // the match below, which the join must keep.
  for (v <- Seq(2, 3)) {
    test(s"legacy-mode inner join with a range on a column shifted back by a day keeps all " +
        s"left rows, V$v") {
      val leftInput = MemoryStream[(Int, Int)]
      val rightInput = MemoryStream[(Int, Int)]
      val right = withShifted(eventTimeInput(rightInput, "r"), "r", "- INTERVAL 1 DAY")
      val result = eventTimeInput(leftInput, "l").join(right,
        expr("lk = rk AND lt BETWEEN rs AND rs + INTERVAL 1 HOUR"))
        .select($"lk", $"lt".cast("int"), $"rk", $"rt".cast("int"))
      testJoin(legacyMode, version(v))(result)(
        AddData(leftInput, (1, 35)),
        CheckNewAnswer(),
        // The watermark is 69999 seconds, far above the left row at 35 seconds.
        MultiAddData(leftInput, (8, 70000))(rightInput, (9, 80000)),
        CheckNewAnswer(),
        AddData(rightInput, (7, 80001)),
        CheckNewAnswer(),
        // A later right row at 86435 seconds has rs = 35 seconds, which matches the left row.
        AddData(rightInput, (1, 86435)),
        CheckNewAnswer(Row(1, 35, 1, 86435)))
    }
  }

  // A key that adds a whole day and subtracts an hour never moves a time backward: a day is at
  // least 23 hours. Spark 4.3 admitted the join through lt and evicted both inputs at the
  // watermark, which this keeps.
  for {
    (modeName, mode, versions) <- Seq(("default", defaultMode, Seq(2, 3, 4)),
      ("legacy", legacyMode, Seq(3)))
    v <- versions
  } {
    test(s"$modeName-mode left outer join on a net forward compound shift evicts at the " +
        s"watermark, V$v") {
      val leftInput = MemoryStream[(Int, Int)]
      val rightInput = MemoryStream[(Int, Int)]
      val result = eventTimeInput(leftInput, "l").join(eventTimeInput(rightInput, "r"),
        expr("lk = rk AND lt = rt + INTERVAL 1 DAY - INTERVAL 1 HOUR"), "left_outer")
        .select($"lk", $"lt".cast("int"), $"rk", $"rt".cast("int"))
      testJoin(mode, version(v), SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC")(result)(
        MultiAddData(leftInput, (1, 82810), (2, 82820))(rightInput, (1, 10)),
        CheckNewAnswer(Row(1, 82810, 1, 10)),
        MultiAddData(leftInput, (3, 90000))(rightInput, (9, 90000)),
        CheckNewAnswer(),
        // The watermark is 89999 seconds, and every later right row keys above it.
        AddData(rightInput, (9, 90001)),
        CheckNewAnswer(Row(2, 82820, null, null)),
        assertStateRows(3))
    }
  }

  test("left outer join on a net backward compound shift fails analysis") {
    val joined = eventTimeInput(MemoryStream[(Int, Int)], "l")
      .join(eventTimeInput(MemoryStream[(Int, Int)], "r"),
        expr("lk = rk AND lt = rt - INTERVAL 1 DAY + INTERVAL 1 HOUR"), "left_outer")
    val error = intercept[AnalysisException] {
      testStream(joined)(StartStream(), CheckNewAnswer())
    }
    assert(error.getMessage.contains("allow unmatched state to be finalized"))
  }
}
