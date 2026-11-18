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

class StreamingJoinWatermarkDirectionSuite extends StreamTest {
  import testImplicits._

  test("left outer join on event time needs a watermark on the right input") {
    val leftInput = MemoryStream[(Int, Int)]
    val rightInput = MemoryStream[(Int, Int)]
    val left = leftInput.toDF().toDF("lk", "t")
      .select($"lk", timestamp_seconds($"t").as("lt"))
      .withWatermark("lt", "10 seconds")
    val right = rightInput.toDF().toDF("rk", "t")
      .select($"rk", timestamp_seconds($"t").as("rt"))
    // The left watermark would finalize left rows that a late right row can still match.
    val joined = left.join(right, expr("lk = rk AND lt = rt"), "left_outer")

    val e = intercept[AnalysisException] {
      testStream(joined)(
        AddData(leftInput, (1, 5)),
        CheckNewAnswer())
    }
    assert(e.getMessage.contains("without a watermark on the right input that bounds the " +
      "state of the left input"))
  }
}
