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
}
