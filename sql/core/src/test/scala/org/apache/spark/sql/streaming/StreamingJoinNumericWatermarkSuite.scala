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

class StreamingJoinNumericWatermarkSuite extends StreamTest {
  import testImplicits._

  private val timestampRule = "A time range condition must bound a TIMESTAMP column"

  // Analysis rejects the plan before any state format is used.
  test("left outer join cannot use a numeric state field for range eviction") {
    val leftInput = MemoryStream[(Int, Long, Long)]
    val rightInput = MemoryStream[(Int, Long)]
    val left = leftInput.toDF().toDF("leftKey", "n", "time")
      .select($"leftKey", $"n", timestamp_seconds($"time").as("lt"))
      .withWatermark("lt", "1 second")
    val right = rightInput.toDF().toDF("rightKey", "time")
      .select($"rightKey", timestamp_seconds($"time").as("rt"))
      .withWatermark("rt", "1 second")
    val joined = left.join(right,
      expr("leftKey = rightKey AND n > CAST(rt AS BIGINT)"), "left_outer")
      .select($"leftKey", $"n", $"lt")

    val error = intercept[AnalysisException] {
      testStream(joined)(
        AddData(leftInput, (1, 20L, 20L)),
        CheckNewAnswer())
    }
    assert(error.getMessage.contains("a watermark on the right input and a time constraint"))
    assert(error.getMessage.contains(timestampRule))
  }

  // Comparing with the TIMESTAMP event time casts these columns, but eviction compares the
  // cutoff with the column itself.
  for (columnType <- Seq("date", "timestamp_ntz")) {
    test(s"left outer join cannot use a $columnType state field for range eviction") {
      val leftInput = MemoryStream[(Int, Long)]
      val rightInput = MemoryStream[(Int, Long)]
      val left = leftInput.toDF().toDF("leftKey", "time")
        .select($"leftKey", timestamp_seconds($"time").cast(columnType).as("orderDate"))
      val right = rightInput.toDF().toDF("rightKey", "time")
        .select($"rightKey", timestamp_seconds($"time").as("rt"))
        .withWatermark("rt", "1 second")
      val joined = left.join(right,
        expr("leftKey = rightKey AND orderDate >= rt - INTERVAL 7 DAYS"), "left_outer")

      val error = intercept[AnalysisException] {
        testStream(joined)(
          AddData(leftInput, (1, 20L)),
          CheckNewAnswer())
      }
      assert(error.getMessage.contains(timestampRule))
    }
  }

  // window() over a watermarked time column. The window is the input's only event-time column.
  private def windowed(
      input: MemoryStream[(Int, Long)],
      key: String,
      windowName: String,
      duration: String,
      slide: String): DataFrame = {
    input.toDF().toDF(key, "time")
      .select(col(key), timestamp_seconds($"time").as("t"))
      .withWatermark("t", "0 seconds")
      .select(col(key), window($"t", duration, slide).as(windowName))
  }

  test("outer and semi joins cannot use a time window ordering for range eviction") {
    Seq("left_outer", "left_semi", "right_outer").foreach { joinType =>
      withClue(joinType) {
        val leftInput = MemoryStream[(Int, Long)]
        val rightInput = MemoryStream[(Int, Long)]
        val left = windowed(leftInput, "leftKey", "lw", "10 seconds", "10 seconds")
        val right = windowed(rightInput, "rightKey", "rw", "10 seconds", "10 seconds")
        val condition = $"leftKey" === $"rightKey" && $"lw" > $"rw"
        // The right outer join finalizes the same left windows.
        val joined = if (joinType == "right_outer") {
          right.join(left, condition, joinType)
        } else {
          left.join(right, condition, joinType)
        }

        val error = intercept[AnalysisException] {
          testStream(joined)(
            AddData(leftInput, (1, 5L)),
            CheckNewAnswer())
        }
        assert(error.getMessage.contains(timestampRule))
      }
    }
  }

  // Ordering windows compares their starts first. A 20-second right window that ends above the
  // watermark can start before a 10-second left window that ends at or below it, so a bound that
  // compares whole windows must not evict left windows.
  for (version <- Seq(2, 3, 4)) {
    test(s"inner join keeps windows that a longer opposite window can still match, V$version") {
      withSQLConf(
          SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> version.toString,
          SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
          SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false") {
        val leftInput = MemoryStream[(Int, Long)]
        val rightInput = MemoryStream[(Int, Long)]
        val left = windowed(leftInput, "leftKey", "lw", "10 seconds", "10 seconds")
        val right = windowed(rightInput, "rightKey", "rw", "20 seconds", "1 second")
        val joined = left.join(right, $"leftKey" === $"rightKey" && $"lw" > $"rw")
          .select($"leftKey", $"lw.start".cast("long"), $"rw.start".cast("long"))

        testStream(joined)(
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (1, 5L)), AddData(rightInput, (9, 5L)))),
          CheckNewAnswer(),
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (8, 15L)), AddData(rightInput, (9, 15L)))),
          CheckNewAnswer(),
          // The watermark reaches 15 seconds, past the end of left window [0, 10).
          AddData(rightInput, (9, 16L)),
          CheckNewAnswer(),
          // Time 18 is in right window [-1, 19), which is not late and orders below [0, 10).
          AddData(rightInput, (1, 18L)),
          CheckNewAnswer(Row(1, 0L, -1L)))
      }
    }
  }
}
