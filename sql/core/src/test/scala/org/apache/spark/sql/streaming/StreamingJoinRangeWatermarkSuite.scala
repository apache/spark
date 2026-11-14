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
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.tags.SlowSQLTest

@SlowSQLTest
class StreamingJoinRangeWatermarkSuite extends StreamTest {
  import testImplicits._

  private def withJoinConf(version: Int)(f: => Unit): Unit = {
    withSQLConf(
      SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> version.toString,
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
      SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false")(f)
  }

  // StreamingJoinRangeEvictionSuite covers the other range shapes. Here leftTime is not
  // selected, so it is pruned from the join input and only the right watermark bounds expiry.
  for {
    version <- Seq(2, 3, 4)
    joinType <- Seq("inner", "left_outer")
  } {
    test(s"expiry-column eviction with a pruned event-time column, V$version $joinType") {
      withJoinConf(version) {
        val leftInput = MemoryStream[(Int, Long, Long)]
        val rightInput = MemoryStream[(Int, Long)]
        val left = leftInput.toDF().toDF("leftKey", "start", "end")
          .select($"leftKey", timestamp_seconds($"start").as("leftTime"),
            timestamp_seconds($"end").as("expiry"))
          .withWatermark("leftTime", "60 seconds")
        val right = rightInput.toDF().toDF("rightKey", "time")
          .select($"rightKey", timestamp_seconds($"time").as("rightTime"))
          .withWatermark("rightTime", "60 seconds")
        val result = left.join(right, expr("leftKey = rightKey AND rightTime <= expiry"), joinType)
          .select($"leftKey", $"rightTime".cast("long"))
        testStream(result)(
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (1, 100L, 700L), (2, 100L, 700L)), AddData(rightInput, (9, 100L)))),
          CheckNewAnswer(),
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (8, 400L, 1000L)), AddData(rightInput, (9, 400L)))),
          CheckNewAnswer(),
          AddData(rightInput, (9, 400L)),
          // The watermark has passed the start time, but the interval has not expired.
          CheckNewAnswer(),
          Execute { query =>
            val join = query.lastExecution.executedPlan.collectFirst {
              case j: StreamingSymmetricHashJoinExec => j
            }.get
            assert(join.eventTimeWatermarkForEviction.contains(340000L))
            assert(!join.left.output.exists(_.name == "leftTime"))
          },
          AddData(rightInput, (1, 460L)),
          CheckNewAnswer(Row(1, 460L)),
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (8, 1000L, 1600L)), AddData(rightInput, (9, 1000L)))),
          CheckNewAnswer(),
          AddData(rightInput, (9, 1100L)),
          CheckNewAnswer((if (joinType == "left_outer") Seq(Row(2, null)) else Seq.empty): _*)
        )
      }
    }
  }

  for {
    version <- Seq(2, 3, 4)
    retainedSide <- Seq("left", "right")
  } {
    test(s"crossed intervals retain $retainedSide state until expiry, V$version") {
      withJoinConf(version) {
        val leftInput = MemoryStream[(Int, Long, Long)]
        val rightInput = MemoryStream[(Int, Long, Long)]
        val left = leftInput.toDF().toDF("leftKey", "time", "other")
          .select($"leftKey", timestamp_seconds($"time").as("leftTime"),
            timestamp_seconds($"other").as("leftOther"))
          .withWatermark("leftTime", "60 seconds")
        val right = rightInput.toDF().toDF("rightKey", "time", "other")
          .select($"rightKey", timestamp_seconds($"time").as("rightTime"),
            timestamp_seconds($"other").as("rightOther"))
          .withWatermark("rightTime", "60 seconds")
        val joined = left.join(right, expr(
          "leftKey = rightKey AND " +
            "leftOther BETWEEN rightTime - INTERVAL 10 SECONDS AND " +
            "rightTime + INTERVAL 10 SECONDS AND " +
            "rightOther BETWEEN leftTime - INTERVAL 10 SECONDS AND " +
            "leftTime + INTERVAL 10 SECONDS"), "full_outer")
          .select(coalesce($"leftKey", $"rightKey").as("key"),
            $"leftTime".cast("long"), $"rightTime".cast("long"))
          .where($"key".isin(1, 2))
        val (retained, arriving, matched, unmatched) = if (retainedSide == "left") {
          (leftInput, rightInput, Row(1, 100L, 460L), Row(2, 100L, null))
        } else {
          (rightInput, leftInput, Row(1, 460L, 100L), Row(2, null, 100L))
        }
        testStream(joined)(
          StreamProgressLockedActions(Seq(
            AddData(retained, (1, 100L, 460L), (2, 100L, 460L)),
            AddData(arriving, (9, 100L, 100L)))),
          CheckNewAnswer(),
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (8, 400L, 800L)), AddData(rightInput, (9, 400L, 800L)))),
          CheckNewAnswer(),
          AddData(arriving, (9, 400L, 800L)),
          CheckNewAnswer(),
          Execute { query =>
            val join = query.lastExecution.executedPlan.collectFirst {
              case j: StreamingSymmetricHashJoinExec => j
            }.get
            assert(join.eventTimeWatermarkForEviction.contains(340000L))
            assert(join.stateWatermarkPredicates.left.get.expr.references.map(_.name).toSet ==
              Set("leftOther"))
            assert(join.stateWatermarkPredicates.right.get.expr.references.map(_.name).toSet ==
              Set("rightOther"))
          },
          AddData(arriving, (1, 460L, 100L)),
          CheckNewAnswer(matched),
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (8, 1000L, 1600L)), AddData(rightInput, (9, 1000L, 1600L)))),
          CheckNewAnswer(),
          AddData(arriving, (9, 1100L, 1600L)),
          CheckNewAnswer(unmatched))
      }
    }
  }

  // Whether a column gets a cutoff decides which column V4 stores, indexes and finds rows by. A
  // restart may change the session time zone, which the offset log does not keep, so a bound of
  // a whole day must give the same answer in every zone. Otherwise rows stored before the
  // restart are looked up or evicted by another column.
  for ((firstZone, secondZone) <- Seq(("UTC", "America/Los_Angeles"),
      ("America/Los_Angeles", "UTC"))) {
    test(s"V4 keeps matching a whole-day bound after a restart from $firstZone to $secondZone") {
      val leftInput = MemoryStream[(Int, Long, Long)]
      val rightInput = MemoryStream[(Int, Long, Long)]
      def joined(zone: String): DataFrame = {
        var df: DataFrame = null
        withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> zone) {
          val left = leftInput.toDF().toDF("lk", "time", "expiryMs")
            .select($"lk", timestamp_millis($"time").as("leftTime"),
              timestamp_millis($"expiryMs").as("expiry"))
            .withWatermark("leftTime", "0 seconds")
          val right = rightInput.toDF().toDF("rk", "refMs", "time")
            .select($"rk", timestamp_millis($"refMs").as("refTime"),
              timestamp_millis($"time").as("rightTime"))
            .withWatermark("rightTime", "0 seconds")
          // refTime has no watermark, so only the bound on expiry can finalize left rows.
          df = left.join(right, expr("lk = rk AND leftTime = refTime AND " +
            "rightTime <= expiry + INTERVAL 1 DAY")).select($"lk", $"rk")
        }
        df
      }
      // 2024-01-15T00:00Z. January has no daylight saving change.
      val t = 1705276800000L
      val day = 86400000L
      def run(zone: String, checkpoint: String)(actions: StreamAction*): Unit =
        withSQLConf(
          SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> "4",
          SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
          SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false",
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> zone) {
          testStream(joined(zone))(StartStream(checkpointLocation = checkpoint) +: actions: _*)
        }
      withTempDir { dir =>
        val checkpoint = dir.getCanonicalPath
        run(firstZone, checkpoint)(
          AddData(leftInput, (1, t, t + 10 * day)), CheckNewAnswer(), StopStream)
        run(secondZone, checkpoint)(
          MultiAddData(leftInput, (7, t + 5 * day, t + 20 * day))(
            rightInput, (9, t + 2 * day, t + 2 * day)),
          CheckNewAnswer(),
          // Eviction watermark t + 2 days. The left row (1, t) can match until its expiry plus
          // a day, so it must be neither evicted nor indexed where lookups miss it.
          AddData(rightInput, (8, t + 2 * day, t + 2 * day + 60000L)),
          CheckNewAnswer(),
          AddData(rightInput, (1, t, t + 3 * day)),
          CheckNewAnswer(Row(1, 1)))
      }
    }
  }
}
