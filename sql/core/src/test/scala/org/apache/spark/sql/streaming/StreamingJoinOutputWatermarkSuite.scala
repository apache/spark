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

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.streaming.operators.stateful.join.StreamingSymmetricHashJoinExec
import org.apache.spark.sql.execution.streaming.operators.stateful.join.StreamingSymmetricHashJoinHelper.{JoinSide, LeftSide, RightSide}
import org.apache.spark.sql.execution.streaming.runtime.{MemoryStream, StreamExecution}
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.functions.{col, count, expr, timestamp_seconds, window}
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

  // Impressions are bounded by the click watermark; clicks have no lower bound on clickTime.
  private def oneDirectionalJoin(): (MemoryStream[(Int, Long)], MemoryStream[(Int, Long)],
      DataFrame) = {
    val impressionInput = MemoryStream[(Int, Long)]
    val clickInput = MemoryStream[(Int, Long)]
    val impressions = impressionInput.toDF().toDF("impressionAdId", "seconds")
      .select($"impressionAdId", timestamp_seconds($"seconds").as("impressionTime"))
      .withWatermark("impressionTime", "10 seconds")
    val clicks = clickInput.toDF().toDF("clickAdId", "seconds")
      .select($"clickAdId", timestamp_seconds($"seconds").as("clickTime"))
      .withWatermark("clickTime", "10 seconds")
    val joined = impressions.join(clicks,
      expr("clickAdId = impressionAdId AND clickTime <= impressionTime + INTERVAL 10 SECONDS"))
    (impressionInput, clickInput, joined)
  }

  private def windowCounts(df: DataFrame, timeColumn: String): DataFrame = {
    df.groupBy(window(col(timeColumn), "10 seconds")).agg(count("*").as("count"))
      .select($"window.start".cast("long"), $"count")
  }

  private def join(query: StreamExecution): StreamingSymmetricHashJoinExec = {
    query.lastExecution.executedPlan.collectFirst {
      case j: StreamingSymmetricHashJoinExec => j
    }.get
  }

  private def runOneDirectionalJoin(
      impressionInput: MemoryStream[(Int, Long)],
      clickInput: MemoryStream[(Int, Long)],
      result: DataFrame,
      expectedRows: Seq[Row],
      expectedSides: Option[Set[JoinSide]],
      expectedOutputWatermark: Long): Unit = {
    withSQLConf(SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key -> "false") {
      testStream(result, OutputMode.Append())(
        MultiAddData((impressionInput, Seq((1, 5L), (2, 15L), (3, 25L))),
          (clickInput, Seq((1, 8L), (2, 20L)))),
        ProcessAllAvailable(),
        // The click at 30 seconds joins the retained impression at 25 seconds.
        MultiAddData((impressionInput, Seq((4, 60L))), (clickInput, Seq((3, 30L), (9, 70L)))),
        ProcessAllAvailable(),
        MultiAddData((impressionInput, Seq((5, 100L))), (clickInput, Seq((9, 110L)))),
        ProcessAllAvailable(),
        AddData(clickInput, (9, 150L)),
        ProcessAllAvailable(),
        AddData(impressionInput, (6, 200L)),
        ProcessAllAvailable(),
        CheckAnswer(expectedRows: _*),
        Execute { query =>
          val j = join(query)
          assert(j.outputWatermarkSides == expectedSides)
          assert(j.produceOutputWatermark(j.eventTimeWatermarkForEviction.get)
            .contains(expectedOutputWatermark))
          // No joined row arrives behind the watermark of the operator after the join.
          assert(query.recentProgress.flatMap(_.stateOperators)
            .map(_.numRowsDroppedByWatermark).sum == 0)
        })
    }
  }

  test("output watermark follows the bounded input when only its event time is used") {
    val (impressionInput, clickInput, joined) = oneDirectionalJoin()
    // Impressions are evicted below the click watermark (90 seconds) less 10 seconds and 1 ms.
    runOneDirectionalJoin(impressionInput, clickInput,
      windowCounts(joined.select($"impressionTime"), "impressionTime"),
      Seq(Row(0L, 1L), Row(10L, 1L), Row(20L, 1L)), Some(Set(LeftSide)), 79999L)
  }

  Seq("clickTime", "clickTimeAlias").foreach { timeColumn =>
    test(s"output watermark is held back when the unbounded input's event time is used: " +
        timeColumn) {
      val (impressionInput, clickInput, joined) = oneDirectionalJoin()
      val clickTime = if (timeColumn == "clickTime") $"clickTime" else $"clickTime".as(timeColumn)
      runOneDirectionalJoin(impressionInput, clickInput,
        windowCounts(joined.select(clickTime), timeColumn), Nil, Some(Set(RightSide)), 0L)
    }
  }

  test("output watermark is held back for both inputs after a union") {
    val (impressionInput, clickInput, joined) = oneDirectionalJoin()
    val otherInput = MemoryStream[Long]
    val other = otherInput.toDF().select(timestamp_seconds($"value").as("impressionTime"))
      .withWatermark("impressionTime", "10 seconds")
    otherInput.addData(1000L)
    runOneDirectionalJoin(impressionInput, clickInput,
      windowCounts(joined.select($"impressionTime").union(other), "impressionTime"),
      Nil, None, 0L)
  }

  test("output watermark without a downstream stateful operator is unchanged") {
    val (impressionInput, clickInput, joined) = oneDirectionalJoin()
    runOneDirectionalJoin(impressionInput, clickInput,
      joined.select($"impressionAdId", $"impressionTime".cast("long")),
      Seq(Row(1, 5L), Row(2, 15L), Row(3, 25L)), None, 0L)
  }
}
