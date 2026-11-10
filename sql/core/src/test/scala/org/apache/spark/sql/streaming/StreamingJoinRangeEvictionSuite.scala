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
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.tags.SlowSQLTest

@SlowSQLTest
class StreamingJoinRangeEvictionSuite extends StreamTest {
  import testImplicits._

  private def withJoinConf(version: Int)(f: => Unit): Unit = {
    withSQLConf(
      SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION.key -> version.toString,
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
      SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false")(f)
  }

  // V3 shares V2's eviction code. BETWEEN is replaced by the conjunction before planning, so it
  // only adds coverage for the outer-join watermark check during analysis.
  for {
    version <- Seq(2, 4)
    joinType <- Seq("inner", "left_outer", "right_outer")
    shape <- Seq("between", "conjunction", "upper", "interval", "downstream")
    if shape != "between" || joinType != "inner"
  } {
    test(s"range eviction uses bounded timestamp, V$version $joinType $shape") {
      withJoinConf(version) {
        val leftInput = MemoryStream[(Int, Long, Long)]
        val rightInput = MemoryStream[(Int, Long)]
        val left = leftInput.toDF().toDF("leftKey", "start", "end")
          .select($"leftKey", timestamp_seconds($"start").as("leftTime"),
            timestamp_seconds($"end").as("expiry"))
          .withWatermark("leftTime", "60 seconds")
        val right = rightInput.toDF().toDF("rightKey", "time")
          .select($"rightKey", timestamp_seconds($"time").as("rightTime"))
          .withWatermark("rightTime", "60 seconds")
        val range = shape match {
          case "between" | "downstream" => "rightTime BETWEEN leftTime AND expiry"
          case "conjunction" => "rightTime >= leftTime AND rightTime <= expiry"
          case "interval" => "rightTime BETWEEN leftTime AND leftTime + INTERVAL 10 MINUTES"
          case _ => "rightTime <= expiry"
        }
        val condition = expr(s"leftKey = rightKey AND $range")
        val joined = if (joinType == "right_outer") {
          right.join(left, condition, joinType)
        } else {
          left.join(right, condition, joinType)
        }
        val output = joined.select($"leftKey", $"leftTime",
          $"rightTime".cast("long").as("matchTime"))
        val deduplicated = if (shape == "downstream") {
          output.dropDuplicates("leftKey", "matchTime")
        } else {
          output
        }
        val result = deduplicated.select($"leftKey", $"matchTime", $"leftTime".cast("long"))
        val matched = Row(1, 460L, 100L)
        val unmatched = Row(2, null, 100L)
        testStream(result)(
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (1, 100L, 700L), (2, 100L, 700L)), AddData(rightInput, (9, 100L)))),
          CheckNewAnswer(),
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (8, 400L, 1000L)), AddData(rightInput, (9, 400L)))),
          CheckNewAnswer(),
          AddData(rightInput, (9, 400L)),
          // The watermark has passed the start time, but the interval has not expired.
          CheckNewAnswer(),
          Execute { query =>
            val join = query.lastExecution.executedPlan.collectFirst {
              case j: StreamingSymmetricHashJoinExec => j
            }.get
            assert(join.eventTimeWatermarkForEviction.contains(340000L))
            if (shape != "interval") {
              assert(join.produceOutputWatermark(340000L).contains(0L))
            }
          },
          AddData(rightInput, (1, 460L)),
          CheckNewAnswer(matched),
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (8, 1000L, 1600L)), AddData(rightInput, (9, 1000L)))),
          CheckNewAnswer(),
          AddData(rightInput, (9, 1100L)),
          CheckNewAnswer((if (joinType != "inner") Seq(unmatched) else Seq.empty): _*)
        )
      }
    }
  }

  for {
    version <- Seq(2, 3, 4)
    joinType <- Seq("left_outer", "right_outer")
  } {
    test(s"range eviction includes newly admitted expired values, V$version $joinType") {
      withJoinConf(version) {
        val leftInput = MemoryStream[(Int, Long, Long)]
        val rightInput = MemoryStream[(Int, Long)]
        val left = leftInput.toDF().toDF("leftKey", "start", "end")
          .select($"leftKey", timestamp_seconds($"start").as("leftTime"),
            timestamp_seconds($"end").as("expiry"))
          .withWatermark("leftTime", "60 seconds")
        val right = rightInput.toDF().toDF("rightKey", "time")
          .select($"rightKey", timestamp_seconds($"time").as("rightTime"))
          .withWatermark("rightTime", "60 seconds")
        val condition = expr("leftKey = rightKey AND rightTime <= expiry")
        val joined = if (joinType == "right_outer") {
          right.join(left, condition, joinType)
        } else {
          left.join(right, condition, joinType)
        }
        val result = joined.select($"leftKey", $"rightKey", $"leftTime".cast("long"))
        testStream(result)(
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (8, 100L, 700L)), AddData(rightInput, (9, 100L)))),
          CheckNewAnswer(),
          StreamProgressLockedActions(Seq(
            AddData(leftInput, (8, 400L, 1000L)), AddData(rightInput, (9, 400L)))),
          CheckNewAnswer(),
          AddData(rightInput, (9, 400L)), CheckNewAnswer(),
          // The start passes late filtering, but expiry is below the previous eviction cutoff.
          AddData(leftInput, (3, 460L, 200L)), CheckNewAnswer(Row(3, null, 460L)),
          AddData(rightInput, (9, 500L)), CheckNewAnswer())
      }
    }
  }
}
