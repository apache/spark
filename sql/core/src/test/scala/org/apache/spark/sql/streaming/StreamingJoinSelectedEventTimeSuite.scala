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
import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark
import org.apache.spark.sql.execution.streaming.operators.stateful.join.StreamingSymmetricHashJoinExec
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.functions.timestamp_seconds
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.tags.SlowSQLTest

@SlowSQLTest
class StreamingJoinSelectedEventTimeSuite extends StreamTest {
  import testImplicits._

  for (version <- Seq(2, 3)) {
    test(s"only the first marked input time can bound opposite join state, V$version") {
      withSQLConf(
          SQLConf.STATEFUL_OPERATOR_ALLOW_MULTIPLE.key -> "false",
          SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> version.toString,
          SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
          SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false",
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
        val leftInput = MemoryStream[(Int, Long)]
        val uInput = MemoryStream[(Int, Long)]
        val vInput = MemoryStream[(Int, Long)]

        val left = leftInput.toDF().toDF("lId", "seconds")
          .select($"lId", timestamp_seconds($"seconds").as("l"))
          .withWatermark("l", "10 seconds")
        val u = uInput.toDF().toDF("uId", "seconds")
          .select($"uId", timestamp_seconds($"seconds").as("u"))
          .withWatermark("u", "10 seconds")
        val v = vInput.toDF().toDF("vId", "seconds")
          .select($"vId", timestamp_seconds($"seconds").as("v"))
          .withWatermark("v", "10 seconds")
        val right = u.join(v, $"uId" === $"vId")
          .select($"uId".as("rId"), $"u", $"v")
        val result = left.join(right, $"lId" === $"rId" && $"l" > $"v")
          .select($"lId", $"l".cast("long"), $"u".cast("long"), $"v".cast("long"))

        testStream(result)(
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (1, 10L)),
            AddData(vInput, (1, 5L)))),
          CheckNewAnswer(),
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (8, 60L)),
            AddData(uInput, (9, 60L)),
            AddData(vInput, (10, 60L)))),
          CheckNewAnswer(),
          AddData(uInput, (11, 70L)),
          CheckNewAnswer(),
          Execute { query =>
            val joins = query.lastExecution.executedPlan.collect {
              case join: StreamingSymmetricHashJoinExec => join
            }
            assert(joins.size == 2)
            val downstream = joins.find { join =>
              val names = join.right.output.map(_.name)
              names.contains("u") && names.contains("v")
            }.get
            assert(downstream.right.output.map(_.name) == Seq("rId", "u", "v"))
            val marked = downstream.right.output.filter(
              _.metadata.contains(EventTimeWatermark.delayKey))
            assert(marked.map(_.name) == Seq("u", "v"))
            assert(downstream.eventTimeWatermarkForLateEvents.contains(50000L))
          },
          // The upstream ordinary-key join retained V(1, 5). Its output has fresh u=100,
          // so downstream late filtering admits the row despite v=5 being old.
          AddData(uInput, (1, 100L)),
          CheckNewAnswer(Row(1, 10L, 100L, 5L)))
      }
    }
  }
}
