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
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf

/**
 * Range conditions of stream-stream joins only bound the state of one input when the other
 * input's watermark appears in them with a positive sign.
 */
class StreamingJoinRangeCutoffSuite extends StreamTest {
  import testImplicits._

  private def withJoinStateVersion(version: Int)(body: => Unit): Unit = {
    withSQLConf(
        SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> version.toString,
        SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
        SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false") {
      body
    }
  }

  private def inputs(): (MemoryStream[(Int, Long)], MemoryStream[(Int, Long)], DataFrame,
      DataFrame) = {
    val leftInput = MemoryStream[(Int, Long)]
    val rightInput = MemoryStream[(Int, Long)]
    val left = leftInput.toDF().toDF("leftKey", "time")
      .select($"leftKey", timestamp_seconds($"time").as("lt"))
      .withWatermark("lt", "1 second")
    val right = rightInput.toDF().toDF("rightKey", "time")
      .select($"rightKey", timestamp_seconds($"time").as("rt"))
      .withWatermark("rt", "1 second")
    (leftInput, rightInput, left, right)
  }

  Seq(2, 3, 4).foreach { version =>
    test(s"negative watermark term does not evict state that can still match, V$version") {
      withJoinStateVersion(version) {
        val (leftInput, rightInput, left, right) = inputs()
        val joined = left
          .join(right, expr("leftKey = rightKey AND CAST(lt AS LONG) + CAST(rt AS LONG) > 100"))
          .select($"leftKey", $"lt".cast("long"), $"rt".cast("long"))

        testStream(joined)(
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (1, 5L)), AddData(rightInput, (9, 5L)))),
          CheckNewAnswer(),
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (8, 21L)), AddData(rightInput, (9, 21L)))),
          CheckNewAnswer(),
          AddData(rightInput, (9, 22L)),
          CheckNewAnswer(),
          // The watermark is past 5, but a later rt still matches lt = 5.
          AddData(rightInput, (1, 100L)),
          CheckNewAnswer(Row(1, 5L, 100L)))
      }
    }

    test(s"constant-only conjunct does not cap the state cutoff, V$version") {
      withJoinStateVersion(version) {
        val (leftInput, rightInput, left, right) = inputs()
        val condition = expr(
          "leftKey = rightKey AND lt > rt - INTERVAL 5 SECONDS AND CAST(lt AS LONG) > 10")
        val joined = left.join(right, condition, "left_outer")
          .select($"leftKey", $"lt".cast("long"), $"rt".cast("long"))

        testStream(joined)(
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (1, 20L)), AddData(rightInput, (9, 20L)))),
          CheckNewAnswer(),
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (8, 40L)), AddData(rightInput, (9, 40L)))),
          CheckNewAnswer(),
          // The watermark is 39, so no right row can match lt = 20 any more.
          AddData(rightInput, (9, 41L)),
          CheckNewAnswer(Row(1, 20L, null)))
      }
    }
  }
}
