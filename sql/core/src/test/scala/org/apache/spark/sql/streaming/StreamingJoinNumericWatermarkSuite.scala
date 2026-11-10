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

class StreamingJoinNumericWatermarkSuite extends StreamTest {
  import testImplicits._

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
  }
}
