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
import org.apache.spark.sql.execution.streaming.operators.stateful.join.StreamingSymmetricHashJoinExec
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.functions.{timestamp_seconds, window}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.tags.SlowSQLTest

@SlowSQLTest
class StreamingJoinEqualityOutputWatermarkSuite extends StreamTest {
  import testImplicits._

  for (version <- Seq(2, 3, 4)) {
    test(s"crossed equality keys retain old output event time, V$version") {
      withSQLConf(
          SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> version.toString,
          SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
          SQLConf.STREAMING_MULTIPLE_WATERMARK_POLICY.key -> "min",
          SQLConf.STATEFUL_OPERATOR_ALLOW_MULTIPLE.key -> "true",
          SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false") {
        val leftInput = MemoryStream[(Long, Long)]
        val rightInput = MemoryStream[(Long, Long)]
        val left = leftInput.toDF().toDF("leftA", "leftB")
          .select(timestamp_seconds($"leftA").as("la"),
            timestamp_seconds($"leftB").as("lb"))
          .withWatermark("la", "10 seconds")
        val right = rightInput.toDF().toDF("rightA", "rightB")
          .select(timestamp_seconds($"rightA").as("ra"),
            timestamp_seconds($"rightB").as("rb"))
          .withWatermark("rb", "10 seconds")
        val result = left.join(right, $"la" === $"ra" && $"lb" === $"rb")
          .select($"rb")
          .groupBy(window($"rb", "10 seconds"))
          .count()
          .select($"window.start".cast("long"), $"count")

        testStream(result, OutputMode.Update())(
          MultiAddData(leftInput, (20L, 900L))(rightInput, (100L, 5L)),
          CheckNewAnswer(),
          MultiAddData(leftInput, (60L, 901L))(rightInput, (200L, 50L)),
          CheckNewAnswer(),
          // Advance both watermarks through a batch with no matching equality keys.
          AddData(leftInput, (70L, 902L)),
          CheckNewAnswer(),
          // The right row (ra=100, rb=5) remains joinable at watermark 40 seconds.
          // This fresh left row must update the downstream window [0, 10).
          AddData(leftInput, (100L, 5L)),
          ProcessAllAvailable(),
          Execute { query =>
            val join = query.lastExecution.executedPlan.collectFirst {
              case j: StreamingSymmetricHashJoinExec => j
            }.get
            assert(join.eventTimeWatermarkForLateEvents.contains(40000L))
            assert(join.metrics("numOutputRows").value === 1L)
          },
          CheckNewAnswer(Row(0L, 1L)),
          Execute { query =>
            val join = query.lastExecution.executedPlan.collectFirst {
              case j: StreamingSymmetricHashJoinExec => j
            }.get
            assert(join.produceOutputWatermark(40000L).contains(0L))
          })
      }
    }
  }
}
