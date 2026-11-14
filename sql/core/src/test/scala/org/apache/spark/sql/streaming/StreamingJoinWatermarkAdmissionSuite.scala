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
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf

class StreamingJoinWatermarkAdmissionSuite extends StreamTest {
  import testImplicits._

  for {
    (joinType, leftWatermark, rightWatermark, constraint) <- Seq(
      ("left_outer", true, false, "lt = rt"),
      ("right_outer", false, true, "lt = rt"),
      ("full_outer", true, false, "lt = rt"),
      ("full_outer", true, true, "lt > rt - INTERVAL 5 SECONDS"))
  } {
    test(s"reject $joinType without a bound in each required direction: " +
        s"left=$leftWatermark right=$rightWatermark constraint=$constraint") {
      val leftInput = MemoryStream[(Int, Int)]
      val rightInput = MemoryStream[(Int, Int)]
      val left = leftInput.toDF().toDF("lk", "time")
        .select($"lk", timestamp_seconds($"time").as("lt"))
      val right = rightInput.toDF().toDF("rk", "time")
        .select($"rk", timestamp_seconds($"time").as("rt"))
      val watermarkedLeft = if (leftWatermark) left.withWatermark("lt", "10 seconds") else left
      val watermarkedRight = if (rightWatermark) right.withWatermark("rt", "10 seconds") else right
      val joined = watermarkedLeft.join(watermarkedRight,
        expr(s"lk = rk AND $constraint"), joinType)

      val error = intercept[AnalysisException] {
        testStream(joined)(AddData(leftInput, (1, 5)), CheckNewAnswer())
      }
      assert(error.getMessage.contains("allow unmatched state to be finalized"))
      val downstreamNote = joinType match {
        case "left_outer" => Some("uses the right input's event time also needs a watermark " +
          "on the left input")
        case "right_outer" => Some("uses the left input's event time also needs a watermark " +
          "on the right input")
        case _ => None
      }
      downstreamNote.foreach(note => assert(error.getMessage.contains(note)))
    }
  }

  // With the check disabled the join runs, but the left state is never finalized.
  Seq("left_outer", "left_semi").foreach { joinType =>
    test(s"run $joinType without a right watermark when the finalization check is disabled") {
      val leftInput = MemoryStream[(Int, Int)]
      val rightInput = MemoryStream[(Int, Int)]
      val left = leftInput.toDF().toDF("lk", "time")
        .select($"lk", timestamp_seconds($"time").as("lt"))
        .withWatermark("lt", "10 seconds")
      val right = rightInput.toDF().toDF("rk", "time")
        .select($"rk", timestamp_seconds($"time").as("rt"))
      val joined = left.join(right, expr("lk = rk AND lt = rt"), joinType)
      val result = if (joinType == "left_semi") {
        joined.select($"lk", $"lt".cast("long"))
      } else {
        joined.select($"lk", $"lt".cast("long"), $"rt".cast("long"))
      }
      def row(key: Int, time: Long): Row =
        if (joinType == "left_semi") Row(key, time) else Row(key, time, time)

      withSQLConf(SQLConf.STREAMING_JOIN_CHECK_FINALIZATION_ENABLED.key -> "false") {
        testStream(result)(
          MultiAddData(leftInput, (1, 5), (2, 6))(rightInput, (1, 5)),
          CheckNewAnswer(row(1, 5L)),
          // The left watermark passes the unmatched left row, which stays in state.
          AddData(leftInput, (3, 100)),
          CheckNewAnswer(),
          AddData(rightInput, (2, 6)),
          CheckNewAnswer(row(2, 6L)))
      }
    }
  }

  // With allowMultiple=false, late rows are filtered by an input's first watermarked column, so
  // a window key bounds state only when the window comes before its event time.
  test("legacy-mode join on a window after its event time names the column order") {
    withSQLConf(
      SQLConf.STATEFUL_OPERATOR_ALLOW_MULTIPLE.key -> "false",
      SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false") {
      val leftInput = MemoryStream[(Int, Int)]
      val rightInput = MemoryStream[(Int, Int)]
      def windowed(input: MemoryStream[(Int, Int)], side: String): DataFrame =
        input.toDF().toDF(s"${side}k", "time")
          .select(col(s"${side}k"), timestamp_seconds($"time").as(s"${side}t"))
          .withWatermark(s"${side}t", "0 seconds")
          .withColumn(s"${side}w", window(col(s"${side}t"), "10 seconds"))
      val left = windowed(leftInput, "l")
      val right = windowed(rightInput, "r")
      val condition = expr("lk = rk AND lw = rw")

      val error = intercept[AnalysisException] {
        testStream(left.join(right, condition, "left_outer"))(
          AddData(leftInput, (1, 5)), CheckNewAnswer())
      }
      assert(error.getMessage.contains("the right input filters late rows by its first " +
        "watermarked column, rt, so its window key rw is not a time constraint"))

      val joined = left.select($"lk", $"lw", $"lt")
        .join(right.select($"rk", $"rw", $"rt"), condition, "left_outer")
        .select($"lk", $"lt".cast("int"), $"rk")
      testStream(joined)(
        MultiAddData(leftInput, (1, 5), (2, 6))(rightInput, (1, 7)),
        CheckNewAnswer(Row(1, 5, 1)),
        MultiAddData(leftInput, (3, 30))(rightInput, (9, 30)),
        CheckNewAnswer(),
        // At watermark 30 seconds, the window [0, 10) of the unmatched (2, 6) has ended.
        AddData(rightInput, (9, 40)),
        CheckNewAnswer(Row(2, 6, null)))
    }
  }
}
