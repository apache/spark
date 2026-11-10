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

import org.apache.spark.sql.execution.streaming.operators.stateful.join.StreamingSymmetricHashJoinExec
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.functions.timestamp_seconds
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.tags.SlowSQLTest

@SlowSQLTest
class StreamingJoinOutputWatermarkSuite extends StreamTest {
  import testImplicits._

  for (version <- Seq(2, 3, 4)) {
    test(s"unbounded join state can emit an old event time after watermarks advance, V$version") {
      withSQLConf(
          SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> version.toString,
          SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
          SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false") {
        val leftInput = MemoryStream[(Int, Long)]
        val rightInput = MemoryStream[(Int, Long)]
        val left = leftInput.toDF().toDF("key", "seconds")
          .select($"key", timestamp_seconds($"seconds").as("leftTime"))
          .withWatermark("leftTime", "10 seconds")
        val right = rightInput.toDF().toDF("key", "seconds")
          .select($"key", timestamp_seconds($"seconds").as("rightTime"))
          .withWatermark("rightTime", "10 seconds")
        val result = left.join(right, Seq("key"))
          .select($"key", $"leftTime")
          .dropDuplicatesWithinWatermark("key")
          .select($"key", $"leftTime".cast("long"))

        testStream(result)(
          MultiAddData((leftInput, Seq((1, 5L))), (rightInput, Seq((9, 5L)))),
          CheckNewAnswer(),
          MultiAddData((leftInput, Seq((2, 30L))), (rightInput, Seq((3, 30L)))),
          CheckNewAnswer(),
          // Process another batch so the join's output watermark reaches downstream operators.
          AddData(rightInput, (4, 40L)),
          CheckNewAnswer(),
          // The retained left key 1 can still join after both input watermarks reach 20 seconds.
          // The right event is newer than that watermark, but the joined left time is only 5.
          AddData(rightInput, (1, 100L)),
          CheckNewAnswer((1, 5L)),
          Execute { query =>
            val join = query.lastExecution.executedPlan.collectFirst {
              case j: StreamingSymmetricHashJoinExec => j
            }.get
            assert(join.eventTimeWatermarkForLateEvents.contains(20000L))
            assert(join.produceOutputWatermark(20000L).contains(0L))
          })
      }
    }
  }
}
