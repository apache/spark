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
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf

class StreamingJoinOneSidedWatermarkSuite extends StreamTest {
  import testImplicits._

  for {
    version <- Seq(2, 3, 4)
    joinType <- Seq("left_outer", "right_outer")
  } {
    test(s"range eviction needs only the opposite watermark, V$version $joinType") {
      withSQLConf(
        SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> version.toString,
        SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
        SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false") {
        val leftInput = MemoryStream[(Int, Int)]
        val rightInput = MemoryStream[(Int, Int)]
        val leftData = leftInput.toDF().toDF("leftKey", "time")
          .select($"leftKey", timestamp_seconds($"time").as("leftTime"))
        val rightData = rightInput.toDF().toDF("rightKey", "time")
          .select($"rightKey", timestamp_seconds($"time").as("rightTime"))
        val left = if (joinType == "right_outer") {
          leftData.withWatermark("leftTime", "10 seconds")
        } else {
          leftData
        }
        val right = if (joinType == "left_outer") {
          rightData.withWatermark("rightTime", "10 seconds")
        } else {
          rightData
        }
        val joined = left.join(right, expr("leftKey = rightKey AND " +
          "leftTime BETWEEN rightTime - INTERVAL 5 SECONDS AND " +
          "rightTime + INTERVAL 5 SECONDS"), joinType)
          .select($"leftKey", $"rightKey", $"leftTime".cast("int"), $"rightTime".cast("int"))
        val stateInput = if (joinType == "left_outer") leftInput else rightInput
        val watermarkInput = if (joinType == "left_outer") rightInput else leftInput
        def unmatched(key: Int): Row = {
          if (joinType == "left_outer") Row(key, null, 5, null) else Row(null, key, null, 5)
        }
        withTempDir { checkpoint =>
          testStream(joined)(
            StartStream(checkpointLocation = checkpoint.getCanonicalPath),
            AddData(stateInput, (1, 5)), CheckNewAnswer(),
            AddData(watermarkInput, (2, 30)), CheckNewAnswer(),
            AddData(watermarkInput, (2, 40)), CheckNewAnswer(unmatched(1)),
            StopStream,
            StartStream(checkpointLocation = checkpoint.getCanonicalPath),
            // The unwatermarked input can introduce rows below an earlier eviction cutoff.
            AddData(stateInput, (3, 5)), CheckNewAnswer(unmatched(3)),
            AddData(watermarkInput, (3, 5)), CheckNewAnswer(),
            AddData(watermarkInput, (4, 50)), CheckNewAnswer(),
            // Opposite-side state must survive to match arbitrarily old unwatermarked input.
            AddData(stateInput, (2, 30)), CheckNewAnswer(Row(2, 2, 30, 30)))
        }
      }
    }
  }

  // Impression 2024-11-02T19:00Z, 12:00 PDT. One local day later is 2024-11-03T20:00Z, 25 hours
  // later, because clocks fall back on 2024-11-03 in America/Los_Angeles.
  private val impressionMs = 1730574000000L
  private val watermarkMs = 1730661000000L // 2024-11-03T19:10Z
  private val lateClickMs = 1730662200000L // 2024-11-03T19:30Z, inside the 25-hour day

  private def impressionsJoinClicks(
      impressions: MemoryStream[(Int, Long)],
      clicks: MemoryStream[(Int, Long)],
      joinType: String): DataFrame = {
    val left = impressions.toDF().toDF("adId", "ms")
      .select($"adId", timestamp_millis($"ms").as("impressionTime"))
    val right = clicks.toDF().toDF("clickAdId", "ms")
      .select($"clickAdId", timestamp_millis($"ms").as("clickTime"))
      .withWatermark("clickTime", "0 seconds")
    left.join(right, expr("adId = clickAdId AND " +
      "clickTime BETWEEN impressionTime AND impressionTime + INTERVAL 1 DAY"), joinType)
      .select($"adId", expr("unix_millis(impressionTime)"), expr("unix_millis(clickTime)"))
  }

  for {
    version <- Seq(2, 3, 4)
    zone <- Seq("America/Los_Angeles", "UTC")
  } {
    test(s"one-sided range bound of a whole day gives no cutoff, V$version $zone") {
      withSQLConf(
        SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> version.toString,
        SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
        SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> zone) {
        val impressions = MemoryStream[(Int, Long)]
        val clicks = MemoryStream[(Int, Long)]
        testStream(impressionsJoinClicks(impressions, clicks, "inner"))(
          MultiAddData(impressions, (1, impressionMs))(clicks, (9, watermarkMs)),
          CheckNewAnswer(),
          // Eviction watermark 19:10Z. The impression stays in state in every zone.
          AddData(clicks, (8, watermarkMs + 60000L)),
          CheckNewAnswer(),
          AddData(clicks, (1, lateClickMs)),
          // The impression matches clicks until 20:00Z in America/Los_Angeles and 19:00Z in UTC.
          if (zone == "UTC") {
            CheckNewAnswer()
          } else {
            CheckNewAnswer(Row(1, impressionMs, lateClickMs))
          })
        // With no cutoff, an outer join could never emit its unmatched impressions.
        val e = intercept[AnalysisException] {
          impressionsJoinClicks(impressions, clicks, "left_outer").writeStream
            .format("memory").queryName("unfinalizedOuterJoin").start()
        }
        assert(e.getMessage.contains("requires a watermark on the right input"))
        assert(e.getMessage.contains("through an interval of a day or more"))
      }
    }
  }

  for (version <- Seq(2, 3, 4)) {
    test(s"one-sided range bound through a truncating cast gives no cutoff, V$version") {
      withSQLConf(
        SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> version.toString,
        SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
        SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false") {
        val leftInput = MemoryStream[(Int, Long)]
        val rightInput = MemoryStream[(Int, Long)]
        val left = leftInput.toDF().toDF("lk", "ms")
          .select($"lk", timestamp_millis($"ms").as("lt"))
        val right = rightInput.toDF().toDF("rk", "ms")
          .select($"rk", timestamp_millis($"ms").as("rt"))
          .withWatermark("rt", "0 seconds")
        val joined = left.join(right,
          expr("lk = rk AND CAST(lt AS BIGINT) >= CAST(rt AS BIGINT) + 5"))
          .select($"lk", expr("unix_millis(lt)"), expr("unix_millis(rt)"))
        testStream(joined)(
          MultiAddData(leftInput, (1, 15500L))(rightInput, (9, 10900L)),
          CheckNewAnswer(),
          AddData(rightInput, (8, 10910L)),
          CheckNewAnswer(),
          // CAST(15.5 s AS BIGINT) = 15 >= CAST(10.95 s AS BIGINT) + 5, and 10.95 s is above the
          // 10.9 s watermark, so the left row must still be in state.
          AddData(rightInput, (1, 10950L)),
          CheckNewAnswer(Row(1, 15500L, 10950L)))
      }
    }
  }
}
