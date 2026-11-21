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

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.tags.SlowSQLTest

@SlowSQLTest
class StreamingJoinOneSidedWatermarkSuite extends StateStoreMetricsTest {
  import testImplicits._

  private def withJoinConf(version: Int)(f: => Unit): Unit = {
    withSQLConf(
      SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> version.toString,
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
      SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false")(f)
  }

  for {
    version <- Seq(2, 3, 4)
    joinType <- Seq("left_outer", "right_outer", "full_outer")
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
        // Only one input has a watermark: the one whose rows are not preserved, and the right
        // input of a full outer join, whose unmatched rows are then never emitted.
        val watermarkOnLeft = joinType == "right_outer"
        val left = if (watermarkOnLeft) leftData.withWatermark("leftTime", "10 seconds") else {
          leftData
        }
        val right = if (watermarkOnLeft) rightData else {
          rightData.withWatermark("rightTime", "10 seconds")
        }
        val joined = left.join(right, expr("leftKey = rightKey AND " +
          "leftTime BETWEEN rightTime - INTERVAL 5 SECONDS AND " +
          "rightTime + INTERVAL 5 SECONDS"), joinType)
          .select($"leftKey", $"rightKey", $"leftTime".cast("int"), $"rightTime".cast("int"))
        val stateInput = if (watermarkOnLeft) rightInput else leftInput
        val watermarkInput = if (watermarkOnLeft) leftInput else rightInput
        def unmatched(key: Int): Row = {
          if (watermarkOnLeft) Row(null, key, null, 5) else Row(key, null, 5, null)
        }
        withTempDir { checkpoint =>
          testStream(joined)(
            StartStream(checkpointLocation = checkpoint.getCanonicalPath),
            AddData(stateInput, (1, 5)), CheckNewAnswer(),
            AddData(watermarkInput, (2, 30)), CheckNewAnswer(),
            AddData(watermarkInput, (2, 40)), CheckNewAnswer(unmatched(1)),
            StopStream,
            StartStream(checkpointLocation = checkpoint.getCanonicalPath),
            // The input without a watermark can add rows below an earlier eviction cutoff.
            AddData(stateInput, (3, 5)), CheckNewAnswer(unmatched(3)),
            AddData(watermarkInput, (3, 5)), CheckNewAnswer(),
            AddData(watermarkInput, (4, 50)), CheckNewAnswer(),
            // The watermarked input keeps its state for arbitrarily old rows of the other input.
            AddData(stateInput, (2, 30)), CheckNewAnswer(Row(2, 2, 30, 30)))
        }
      }
    }
  }

  // The input without a watermark is evicted, and its matches within the range are kept.
  for {
    version <- Seq(2, 4)
    (joinType, watermarkOnLeft) <- Seq(("inner", false), ("inner", true), ("left_semi", false))
  } {
    test(s"range eviction needs only the opposite watermark, V$version $joinType " +
        s"watermarkOnLeft=$watermarkOnLeft") {
      withJoinConf(version) {
        val leftInput = MemoryStream[(Int, Int)]
        val rightInput = MemoryStream[(Int, Int)]
        val leftData = leftInput.toDF().toDF("leftKey", "time")
          .select($"leftKey", timestamp_seconds($"time").as("leftTime"))
        val rightData = rightInput.toDF().toDF("rightKey", "time")
          .select($"rightKey", timestamp_seconds($"time").as("rightTime"))
        val left =
          if (watermarkOnLeft) leftData.withWatermark("leftTime", "10 seconds") else leftData
        val right =
          if (watermarkOnLeft) rightData else rightData.withWatermark("rightTime", "10 seconds")
        val joined = left.join(right, expr("leftKey = rightKey AND " +
          "leftTime BETWEEN rightTime - INTERVAL 5 SECONDS AND " +
          "rightTime + INTERVAL 5 SECONDS"), joinType)
        val result = if (joinType == "left_semi") {
          joined.select($"leftKey", $"leftTime".cast("int"))
        } else {
          joined.select($"leftKey", $"rightKey", $"leftTime".cast("int"), $"rightTime".cast("int"))
        }
        val stateInput = if (watermarkOnLeft) rightInput else leftInput
        val watermarkInput = if (watermarkOnLeft) leftInput else rightInput
        // A match of a row of stateInput at time s with a row of watermarkInput at time w.
        def matched(key: Int, s: Int, w: Int): Row = {
          if (joinType == "left_semi") {
            Row(key, s)
          } else if (watermarkOnLeft) {
            Row(key, key, w, s)
          } else {
            Row(key, key, s, w)
          }
        }
        withTempDir { checkpoint =>
          testStream(result)(
            StartStream(checkpointLocation = checkpoint.getCanonicalPath),
            // The row with key 4 never matches.
            AddData(stateInput, (1, 5), (4, 5)), CheckNewAnswer(),
            AddData(watermarkInput, (1, 8)), CheckNewAnswer(matched(1, 5, 8)),
            AddData(watermarkInput, (2, 30)), CheckNewAnswer(),
            // At watermark 20 the rows at 5 s of the input without a watermark are evicted.
            AddData(watermarkInput, (2, 40)), CheckNewAnswer(),
            Execute { q =>
              val numRows = q.lastProgress.stateOperators.head.numRowsTotal
              assert(numRows === 3)
            },
            StopStream,
            StartStream(checkpointLocation = checkpoint.getCanonicalPath),
            AddData(stateInput, (3, 5)), CheckNewAnswer(),
            AddData(stateInput, (2, 30)), CheckNewAnswer(matched(2, 30, 30)))
        }
      }
    }
  }

  // An earlier version, like the legacy mode, kept every row of such an input, including one
  // whose bounded column is NULL.
  test("a stored row of an input without its own watermark whose column is NULL is evicted") {
    withJoinConf(2) {
      val leftInput = MemoryStream[(Int, Int)]
      val rightInput = MemoryStream[(Int, Int)]
      val left = leftInput.toDF().toDF("leftKey", "time")
        .select($"leftKey", when($"time" >= 0, timestamp_seconds($"time")).as("leftTime"))
      val right = rightInput.toDF().toDF("rightKey", "time")
        .select($"rightKey", timestamp_seconds($"time").as("rightTime"))
        .withWatermark("rightTime", "10 seconds")
      val joined = left.join(right, expr("leftKey = rightKey AND " +
        "leftTime BETWEEN rightTime - INTERVAL 5 SECONDS AND rightTime + INTERVAL 5 SECONDS"),
        "left_outer")
        .select($"leftKey", $"rightKey", $"leftTime".cast("int"), $"rightTime".cast("int"))
      withTempDir { checkpoint =>
        withSQLConf(SQLConf.STATEFUL_OPERATOR_ALLOW_MULTIPLE.key -> "false") {
          testStream(joined)(
            StartStream(checkpointLocation = checkpoint.getCanonicalPath),
            MultiAddData(leftInput, (1, -1), (2, 5))(rightInput, (9, 30)),
            CheckNewAnswer(),
            StopStream)
        }
        testStream(joined)(
          StartStream(checkpointLocation = checkpoint.getCanonicalPath),
          AddData(rightInput, (9, 40)),
          CheckNewAnswer(Row(1, null, null, null), Row(2, null, 5, null)))
      }
    }
  }
}
