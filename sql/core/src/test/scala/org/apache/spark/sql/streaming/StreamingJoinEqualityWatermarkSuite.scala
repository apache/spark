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

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.streaming.operators.stateful.join.StreamingSymmetricHashJoinExec
import org.apache.spark.sql.execution.streaming.operators.stateful.join.StreamingSymmetricHashJoinHelper.{JoinStateKeyWatermarkPredicate, JoinStateValueWatermarkPredicate}
import org.apache.spark.sql.execution.streaming.runtime.{MemoryStream, StreamExecution}
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf

class StreamingJoinEqualityWatermarkSuite extends StreamTest {
  import testImplicits._

  for {
    version <- Seq(2, 3, 4)
    restart <- Seq(false, true)
  } {
    test(s"equality eviction follows the opposite watermark, V$version restart=$restart") {
      withSQLConf(
        SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> version.toString,
        SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
        SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false") {
        val leftInput = MemoryStream[(Int, Int)]
        val rightInput = MemoryStream[(Int, Int)]
        val left = leftInput.toDF().toDF("a", "b")
          .select(timestamp_seconds($"a").as("la"), timestamp_seconds($"b").as("lb"))
          .withWatermark("la", "10 seconds")
        val right = rightInput.toDF().toDF("a", "b")
          .select(timestamp_seconds($"a").as("ra"), timestamp_seconds($"b").as("rb"))
          .withWatermark("rb", "10 seconds")
        val joined = left.join(right, expr("la = ra AND lb = rb"), "full_outer")
          .select($"la".cast("int"), $"lb".cast("int"),
            $"ra".cast("int"), $"rb".cast("int"))
        withTempDir { checkpoint =>
          val restartActions = if (restart) {
            Seq(StopStream, StartStream(checkpointLocation = checkpoint.getCanonicalPath))
          } else {
            Nil
          }
          testStream(joined)((Seq(
            StartStream(checkpointLocation = checkpoint.getCanonicalPath),
            MultiAddData(leftInput, (5, 50))(rightInput, (80, 50)),
            CheckNewAnswer(),
            MultiAddData(leftInput, (60, 100))(rightInput, (70, 50)),
            CheckNewAnswer(),
            AddData(leftInput, (70, 100)),
            // At watermark 40, left (5, 50) can still match fresh right input on rb=50.
            CheckNewAnswer()) ++ restartActions ++ Seq(
            AddData(rightInput, (5, 50)),
            CheckNewAnswer(Row(5, 50, 5, 50)),
            // Fresh on la, but already expired on lb: V4 must visit it below the old cutoff.
            AddData(leftInput, (90, 10)),
            CheckNewAnswer(Row(90, 10, null, null)),
            MultiAddData(leftInput, (200, 200))(rightInput, (200, 200)),
            CheckNewAnswer(Row(200, 200, 200, 200)),
            MultiAddData(leftInput, (210, 210))(rightInput, (210, 210)),
            CheckNewAnswer(Row(210, 210, 210, 210), Row(60, 100, null, null),
              Row(70, 100, null, null), Row(null, null, 80, 50), Row(null, null, 70, 50))
          )): _*)
        }
      }
    }
  }

  test("V4 window equality retains state for future unwatermarked input") {
    withSQLConf(
      SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> "4",
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName) {
      val leftInput = MemoryStream[Int]
      val rightInput = MemoryStream[Int]
      val left = leftInput.toDF()
        .select($"value".as("key"), timestamp_seconds($"value").as("time"))
        .withWatermark("time", "10 seconds")
        .select($"key", window($"time", "10 seconds"))
      val right = rightInput.toDF()
        .select($"value".as("key"), timestamp_seconds($"value").as("time"))
        .select($"key", window($"time", "10 seconds"))
      val joined = left.join(right, Seq("key", "window"))
        .select($"key", $"window.end".cast("long"))
      testStream(joined)(
        AddData(leftInput, 1), CheckNewAnswer(),
        AddData(rightInput, 1), CheckNewAnswer(Row(1, 10L)),
        AddData(leftInput, 25), CheckNewAnswer(),
        StopStream, StartStream(),
        // The right input has no late filter; its old window must still find left state.
        AddData(rightInput, 1), CheckNewAnswer(Row(1, 10L)),
        AddData(leftInput, 5), CheckNewAnswer())
    }
  }

  private def joinExec(query: StreamExecution): StreamingSymmetricHashJoinExec =
    query.lastExecution.executedPlan.collectFirst {
      case j: StreamingSymmetricHashJoinExec => j
    }.get

  /** Where the V4 eviction scan of the left key predicate starts in the last batch. */
  private def leftPreviousScanBound(query: StreamExecution): Option[Long] =
    joinExec(query).stateWatermarkPredicates.left.collect {
      case p: JoinStateKeyWatermarkPredicate => p.prevStateWatermark
    }.get

  for {
    version <- Seq(2, 3, 4)
    offset <- Seq(10, -10)
  } {
    test(s"equality on a shifted opposite event time evicts state, V$version offset=$offset") {
      withSQLConf(
        SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> version.toString,
        SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
        SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false") {
        val leftInput = MemoryStream[(Int, Int)]
        val rightInput = MemoryStream[(Int, Int)]
        val left = leftInput.toDF().toDF("lk", "lt")
          .select($"lk", timestamp_seconds($"lt").as("lt"))
          .withWatermark("lt", "10 seconds")
        val right = rightInput.toDF().toDF("rk", "rt")
          .select($"rk", timestamp_seconds($"rt").as("rt"))
          .withWatermark("rt", "10 seconds")
        // A left row matches the right row whose time is `offset` seconds earlier.
        val joined = left.join(right,
          expr(s"lk = rk AND lt = rt + INTERVAL $offset SECONDS"), "left_outer")
          .select($"lk", $"lt".cast("int"), $"rk", $"rt".cast("int"))
        testStream(joined)(
          MultiAddData(leftInput, (1, 100 + offset), (2, 90 + offset), (3, 145 + offset))(
            rightInput, (9, 50)),
          CheckNewAnswer(),
          // The first batch has no previous eviction to start the V4 scan after.
          Execute { query => assert(leftPreviousScanBound(query).isEmpty) },
          AddData(rightInput, (1, 100)),
          CheckNewAnswer(Row(1, 100 + offset, 1, 100)),
          MultiAddData(leftInput, (9, 300))(rightInput, (9, 150)),
          // At watermark 90, the left key cutoff is (90 + offset) s less the 1 ms the solver
          // subtracts for >=, so (2, 90 + offset) stays in state for one more batch.
          CheckNewAnswer(),
          // (4, 95) passes late filtering at 90 s. For offset 10 it is below the previous
          // cutoff, 99.999 s, so the V4 scan must start after 90 s, not after that cutoff.
          MultiAddData(leftInput, (4, 95))(rightInput, (9, 151)),
          // At watermark 140, the left key cutoff is (140 + offset) s less 1 ms.
          CheckNewAnswer(Row(2, 90 + offset, null, null), Row(4, 95, null, null)),
          Execute { query =>
            val shiftedLateCutoff = 90000L + offset * 1000L - 1
            assert(leftPreviousScanBound(query) === Some(math.min(shiftedLateCutoff, 90000L)))
            // Retained left rows are above 140 + offset, retained right rows above 140 - offset.
            assert(joinExec(query).produceOutputWatermark(140000L).contains(129999L))
          },
          // Right time 145 is above watermark 140, so (3, 145 + offset) must still be in state.
          AddData(rightInput, (3, 145)),
          CheckNewAnswer(Row(3, 145 + offset, 3, 145)))
      }
    }
  }

  test("shifted key eviction scans from the start in legacy mode") {
    withSQLConf(
      SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> "4",
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
      SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false",
      SQLConf.STATEFUL_OPERATOR_ALLOW_MULTIPLE.key -> "false") {
      val leftInput = MemoryStream[(Int, Int)]
      val rightInput = MemoryStream[(Int, Int)]
      val left = leftInput.toDF().toDF("lk", "lt")
        .select($"lk", timestamp_seconds($"lt").as("lt"))
        .withWatermark("lt", "10 seconds")
      val right = rightInput.toDF().toDF("rk", "rt")
        .select($"rk", timestamp_seconds($"rt").as("rt"))
        .withWatermark("rt", "10 seconds")
      val joined = left.join(right, expr("lk = rk AND lt = rt + INTERVAL 10 SECONDS"), "left_outer")
        .select($"lk", $"lt".cast("int"), $"rk", $"rt".cast("int"))
      testStream(joined)(
        MultiAddData(leftInput, (1, 95), (8, 300))(rightInput, (9, 100)),
        CheckNewAnswer(),
        AddData(rightInput, (9, 120)),
        // Legacy mode filters late rows and evicts at the same watermark, so it gives no earlier
        // cutoff to start the V4 scan after.
        CheckNewAnswer(Row(1, 95, null, null)),
        Execute { query => assert(leftPreviousScanBound(query).isEmpty) })
    }
  }

  for (version <- Seq(2, 3, 4)) {
    test(s"key eviction on one side and value eviction on the other keep a match, V$version") {
      withSQLConf(
        SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> version.toString,
        SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
        SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false") {
        val leftInput = MemoryStream[(Int, Long)]
        val rightInput = MemoryStream[(Int, Long, Long)]
        val left = leftInput.toDF().toDF("leftId", "seconds")
          .select($"leftId", timestamp_seconds($"seconds").as("leftTime"))
          .withWatermark("leftTime", "1 hour")
        val right = rightInput.toDF().toDF("rightId", "keySeconds", "seconds")
          .select($"rightId", timestamp_seconds($"keySeconds").as("rightKeyTime"),
            timestamp_seconds($"seconds").as("rightTime"))
          .withWatermark("rightTime", "1 hour")
        // The leftTime watermark bounds rightKeyTime, so the right side evicts by key, while the
        // left side evicts on leftTime. The V4 left joiner derives a time window for its lookups
        // into the right state, which must use the exact key timestamp instead.
        val joined = left.join(right, expr(
          "leftId = rightId AND leftTime = rightKeyTime AND " +
            "rightTime >= leftTime + INTERVAL 10 SECONDS AND " +
            "rightTime <= leftTime + INTERVAL 20 SECONDS"))
          .select($"leftId", $"leftTime".cast("long"), $"rightTime".cast("long"))
        testStream(joined)(
          AddData(rightInput, (1, 100L, 115L)),
          CheckNewAnswer(),
          Execute { query =>
            val join = query.lastExecution.executedPlan.collectFirst {
              case j: StreamingSymmetricHashJoinExec => j
            }.get
            assert(join.stateWatermarkPredicates.left.exists(
              _.isInstanceOf[JoinStateValueWatermarkPredicate]))
            assert(join.stateWatermarkPredicates.right.exists(
              _.isInstanceOf[JoinStateKeyWatermarkPredicate]))
          },
          AddData(leftInput, (1, 100L)),
          CheckNewAnswer(Row(1, 100L, 115L)))
      }
    }
  }

  for (version <- Seq(2, 3, 4)) {
    test(s"equality on a truncated opposite event time keeps matchable state, V$version") {
      withSQLConf(
        SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> version.toString,
        SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
        SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false") {
        val leftInput = MemoryStream[(Int, Long)]
        val rightInput = MemoryStream[(Int, Long)]
        val left = leftInput.toDF().toDF("lk", "ms")
          .select($"lk", timestamp_millis($"ms").as("lt"))
          .withWatermark("lt", "10 seconds")
        val right = rightInput.toDF().toDF("rk", "ms")
          .select($"rk", timestamp_millis($"ms").as("rt"))
          .withWatermark("rt", "10 seconds")
        val joined = left.join(right,
          expr("lk = rk AND lt = CAST(CAST(rt AS BIGINT) AS TIMESTAMP)"))
          .select($"lk", $"rk")
        testStream(joined)(
          MultiAddData(leftInput, (1, 25000L))(rightInput, (9, 25000L)),
          CheckNewAnswer(),
          MultiAddData(leftInput, (8, 35500L))(rightInput, (9, 35500L)),
          CheckNewAnswer(),
          AddData(rightInput, (9, 35600L)),
          CheckNewAnswer(),
          // rt = 25.7 seconds is above the watermark 25.5 seconds but truncates to lt = 25.
          AddData(rightInput, (1, 25700L)),
          CheckNewAnswer(Row(1, 1)))
      }
    }
  }

  // (shift, watermark row, evicting row, right time, left time by zone, other left rows)
  // Each left time is the right time shifted in that zone. In Los Angeles a day across the
  // spring-forward change is 23 hours and across the fall-back change 25 hours.
  private val dayShifts = Seq(
    // One day after 2024-03-09T20:00Z.
    ("+ INTERVAL 1 DAY", 1710012600000L, 1710012660000L, 1710014400000L,
      Map("UTC" -> 1710100800000L, "America/Los_Angeles" -> 1710097200000L), Nil),
    // One day and 36 hours before 2024-11-03T20:00Z. The other left row keeps the left
    // watermark above the right one.
    ("- INTERVAL 1 DAY", 1730662200000L, 1730662260000L, 1730664000000L,
      Map("UTC" -> 1730577600000L, "America/Los_Angeles" -> 1730574000000L),
      Seq(1730678400000L)),
    ("- INTERVAL 36 HOURS", 1730662200000L, 1730662260000L, 1730664000000L,
      Map("UTC" -> 1730534400000L, "America/Los_Angeles" -> 1730530800000L),
      Seq(1730678400000L)))

  for {
    version <- Seq(2, 3, 4)
    zone <- Seq("America/Los_Angeles", "UTC")
    (shift, watermarkRow, evictingRow, rightTime, leftTimes, otherLeftRows) <- dayShifts
  } {
    test(s"equality on an opposite event time $shift across DST, V$version $zone") {
      withSQLConf(
        SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> version.toString,
        SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
        SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> zone) {
        val leftInput = MemoryStream[(Int, Long)]
        val rightInput = MemoryStream[(Int, Long)]
        val left = leftInput.toDF().toDF("lk", "ms")
          .select($"lk", timestamp_millis($"ms").as("lt"))
          .withWatermark("lt", "0 seconds")
        val right = rightInput.toDF().toDF("rk", "ms")
          .select($"rk", timestamp_millis($"ms").as("rt"))
          .withWatermark("rt", "0 seconds")
        val joined = left.join(right, expr(s"lk = rk AND lt = rt $shift"))
          .select($"lk", $"rk")
        val leftRows = (1, leftTimes(zone)) +: otherLeftRows.map(time => (8, time))
        testStream(joined)(
          StreamProgressLockedActions(Seq(
            AddData(leftInput, leftRows: _*), AddData(rightInput, (9, watermarkRow)))),
          CheckNewAnswer(),
          // Evicts at the right watermark, which is below rightTime.
          AddData(rightInput, (9, evictingRow)),
          CheckNewAnswer(),
          AddData(rightInput, (1, rightTime)),
          CheckNewAnswer(Row(1, 1)))
      }
    }
  }

  // A V4 input whose key is not its own event time is stored at the key timestamp only when the
  // opposite key bounds that key. A restart may change the session time zone, which the offset
  // log does not keep, so that choice must not depend on it: otherwise rows stored before the
  // restart are looked up at another timestamp and no longer match.
  for {
    (shift, keyOffset) <- Seq(("+ INTERVAL 1 DAY", 86400000L), ("- INTERVAL 1 DAY", -86400000L))
    (firstZone, secondZone) <- Seq(("America/Los_Angeles", "UTC"), ("UTC", "America/Los_Angeles"))
  } {
    test(s"V4 keeps matching k = rt $shift after a restart from $firstZone to $secondZone") {
      val leftInput = MemoryStream[(Int, Long)]
      val rightInput = MemoryStream[(Int, Long)]
      def joined(zone: String): DataFrame = {
        var df: DataFrame = null
        withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> zone) {
          val left = leftInput.toDF().toDF("lk", "ms")
            .select($"lk", timestamp_millis($"ms").as("k"))
          val right = rightInput.toDF().toDF("rk", "ms")
            .select($"rk", timestamp_millis($"ms").as("rt"))
            .withWatermark("rt", "0 seconds")
          df = left.join(right, expr(s"lk = rk AND k = rt $shift")).select($"lk", $"rk")
        }
        df
      }
      // 2024-01-15T00:00Z. January has no daylight saving change, so a day is 24 hours in both
      // zones.
      val rightTime = 1705276800000L
      def run(zone: String, checkpoint: String)(actions: StreamAction*): Unit =
        withSQLConf(
          SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> "4",
          SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> zone) {
          testStream(joined(zone))(StartStream(checkpointLocation = checkpoint) +: actions: _*)
        }
      withTempDir { dir =>
        val checkpoint = dir.getCanonicalPath
        run(firstZone, checkpoint)(
          AddData(leftInput, (1, rightTime + keyOffset)), CheckNewAnswer(), StopStream)
        run(secondZone, checkpoint)(AddData(rightInput, (1, rightTime)), CheckNewAnswer(Row(1, 1)))
      }
    }
  }
}
