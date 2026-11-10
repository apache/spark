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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.functions._

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
    }
  }
}
