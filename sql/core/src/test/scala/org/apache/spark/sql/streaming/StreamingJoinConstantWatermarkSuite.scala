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

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.streaming.operators.stateful.join.StreamingSymmetricHashJoinExec
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf

class StreamingJoinConstantWatermarkSuite extends StreamTest {
  import testImplicits._

  for (version <- Seq(2, 3, 4)) {
    test(s"static conjunct cannot stall left outer finalization, V$version") {
      withSQLConf(
          SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> version.toString,
          SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
          SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false") {
        val leftInput = MemoryStream[(Int, Long)]
        val rightInput = MemoryStream[(Int, Long)]
        val left = leftInput.toDF().toDF("leftKey", "time")
          .select($"leftKey", timestamp_seconds($"time").as("lt"))
          .withWatermark("lt", "1 second")
        val right = rightInput.toDF().toDF("rightKey", "time")
          .select($"rightKey", timestamp_seconds($"time").as("rt"))
          .withWatermark("rt", "1 second")
        val condition = expr("leftKey = rightKey AND lt > rt - INTERVAL 5 SECONDS AND " +
          "CAST(lt AS BIGINT) > 10")
        val joined = left.join(right, condition, "left_outer")
          .select($"leftKey", $"lt".cast("long"), $"rt".cast("long"))

        testStream(joined)(
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (1, 20L)), AddData(rightInput, (9, 20L)))),
          CheckNewAnswer(),
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (8, 40L)), AddData(rightInput, (9, 40L)))),
          CheckNewAnswer(),
          AddData(rightInput, (9, 41L)),
          CheckNewAnswer(Row(1, 20L, null)),
          Execute { query =>
            val join = query.lastExecution.executedPlan.collectFirst {
              case j: StreamingSymmetricHashJoinExec => j
            }.get
            assert(join.eventTimeWatermarkForEviction.contains(39000L))
          })
      }
    }

    test(s"static conjunct still caps an inexact bound in a left outer join, V$version") {
      withSQLConf(
          SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> version.toString,
          SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
          SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false") {
        val leftInput = MemoryStream[(Int, Long)]
        val rightInput = MemoryStream[(Int, Long)]
        val left = leftInput.toDF().toDF("leftKey", "ms")
          .select($"leftKey", timestamp_millis($"ms").as("lt"))
          .withWatermark("lt", "0 seconds")
        val right = rightInput.toDF().toDF("rightKey", "ms")
          .select($"rightKey", timestamp_millis($"ms").as("rt"))
          .withWatermark("rt", "0 seconds")
        // The truncating casts make the solver put the left cutoff at the watermark plus 5
        // seconds, above rows that can still match. The constant keeps capping the cutoff.
        val condition = expr("leftKey = rightKey AND " +
          "CAST(lt AS BIGINT) >= CAST(rt AS BIGINT) + 5 AND CAST(lt AS BIGINT) > 10")
        val joined = left.join(right, condition, "left_outer")
          .select($"leftKey", unix_millis($"lt"), unix_millis($"rt"))

        testStream(joined)(
          // The watermark becomes 10.9 seconds.
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (1, 15500L)), AddData(rightInput, (9, 10900L)))),
          CheckNewAnswer(),
          AddData(rightInput, (8, 10910L)),
          CheckNewAnswer(),
          // 10.95 seconds is not late, and CAST(15.5 s) = 15 >= CAST(10.95 s) + 5 = 15.
          AddData(rightInput, (1, 10950L)),
          CheckNewAnswer(Row(1, 15500L, 10950L)))
      }
    }
  }

  // Earlier versions admitted a bound that uses only constants, so disabling the finalization
  // check runs this join. The left state is never evicted, so the matched left row emits no
  // null-padded row later.
  test("left outer join bounded only by a constant runs with the finalization check disabled") {
    val leftInput = MemoryStream[(Int, Long)]
    val rightInput = MemoryStream[(Int, Long)]
    val left = leftInput.toDF().toDF("leftKey", "time")
      .select($"leftKey", timestamp_seconds($"time").as("lt"))
      .withWatermark("lt", "1 second")
    val right = rightInput.toDF().toDF("rightKey", "time")
      .select($"rightKey", timestamp_seconds($"time").as("rt"))
      .withWatermark("rt", "1 second")
    val joined = left.join(right,
      expr("leftKey = rightKey AND CAST(lt AS BIGINT) > 10"), "left_outer")
      .select($"leftKey", $"lt".cast("long"), $"rt".cast("long"))

    val appender = new LogAppender("finalization check disabled")
    withLogAppender(appender, level = Some(Level.WARN)) {
      withSQLConf(SQLConf.STREAMING_JOIN_CHECK_FINALIZATION_ENABLED.key -> "false") {
        testStream(joined)(
          MultiAddData(leftInput, (1, 20L))(rightInput, (1, 20L)),
          CheckNewAnswer(Row(1, 20L, 20L)),
          MultiAddData(leftInput, (2, 100L))(rightInput, (3, 100L)),
          CheckNewAnswer())
      }
    }
    assert(appender.loggingEvents.exists(
      _.getMessage.getFormattedMessage.contains("allow unmatched state to be finalized")))
  }
}
