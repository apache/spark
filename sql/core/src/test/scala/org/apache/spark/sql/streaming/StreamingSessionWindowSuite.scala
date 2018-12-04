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

import java.{util => ju}
import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.scalatest.{Assertions, BeforeAndAfterAll}
import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.state.StateStore
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode._
import org.apache.spark.sql.types.StringType


class StreamingSessionWindowSuite extends StateStoreMetricsTest
    with BeforeAndAfterAll with Assertions {

  import testImplicits._
  override val streamingTimeout = 30.seconds

  override def afterAll(): Unit = {
    super.afterAll()
    StateStore.stop()
  }

  test("session window with single key, complete mode") {
    val inputData = MemoryStream[(String, String, Int)]
    val aggregated =
      inputData.toDS().toDF("time", "key", "value")
        .groupBy(session_window($"time", "10 seconds"), $"key")
        .agg(sum($"value").as("res"))
        .orderBy($"session_window.start".asc)
        .select($"session_window.start".cast(StringType),
             $"session_window.end".cast(StringType), $"key", $"res")

    aggregated.explain(true)

    testStream(aggregated, Complete)(
      // batch 1
      AddData(inputData, ("2018-08-22 19:39:27", "a", 4),
          ("2018-08-22 19:39:34", "a", 1),
          ("2018-08-22 19:39:56", "a", 3),
          ("2018-08-22 19:39:56", "b", 2)),
      CheckAnswer(("2018-08-22 19:39:27", "2018-08-22 19:39:44", "a", 5),
        ("2018-08-22 19:39:56", "2018-08-22 19:40:06", "a", 3),
        ("2018-08-22 19:39:56", "2018-08-22 19:40:06", "b", 2)),
      // batch 2, add the same data as batch 1
      AddData(inputData, ("2018-08-22 19:39:27", "a", 4),
          ("2018-08-22 19:39:34", "a", 1),
          ("2018-08-22 19:39:56", "a", 3),
          ("2018-08-22 19:39:56", "b", 2)),
      CheckAnswer(("2018-08-22 19:39:27", "2018-08-22 19:39:44", "a", 10),
        ("2018-08-22 19:39:56", "2018-08-22 19:40:06", "a", 6),
        ("2018-08-22 19:39:56", "2018-08-22 19:40:06", "b", 4)),
      // batch 3, extends one existed window, and add a new window of key 'a'
      AddData(inputData, ("2018-08-22 19:39:35", "a", 4),
          ("2018-08-22 19:40:10", "a", 1),
          ("2018-08-22 19:40:15", "a", 3)),
      CheckAnswer(("2018-08-22 19:39:27", "2018-08-22 19:39:45", "a", 14),
        ("2018-08-22 19:39:56", "2018-08-22 19:40:06", "a", 6),
        ("2018-08-22 19:40:10", "2018-08-22 19:40:25", "a", 4),
        ("2018-08-22 19:39:56", "2018-08-22 19:40:06", "b", 4)),
      // batch 4, extends one existed window of key 'b'
      AddData(inputData, ("2018-08-22 19:40:06", "b", 4)),
      CheckAnswer(("2018-08-22 19:39:27", "2018-08-22 19:39:45", "a", 14),
        ("2018-08-22 19:39:56", "2018-08-22 19:40:06", "a", 6),
        ("2018-08-22 19:40:10", "2018-08-22 19:40:25", "a", 4),
        ("2018-08-22 19:39:56", "2018-08-22 19:40:16", "b", 8)),
      // batch 5, add a new window of key 'b'
      AddData(inputData, ("2018-08-22 19:40:30", "b", 3)),
      CheckAnswer(("2018-08-22 19:39:27", "2018-08-22 19:39:45", "a", 14),
        ("2018-08-22 19:39:56", "2018-08-22 19:40:06", "a", 6),
        ("2018-08-22 19:40:10", "2018-08-22 19:40:25", "a", 4),
        ("2018-08-22 19:39:56", "2018-08-22 19:40:16", "b", 8),
        ("2018-08-22 19:40:30", "2018-08-22 19:40:40", "b", 3)),
      StopStream
    )
  }

  test("session window with single key and single partitions, complete mode") {
    spark.conf.set("spark.sql.shuffle.partitions", "1")
    val inputData = MemoryStream[(String, Int, Int)]
    val aggregated =
      inputData.toDS().toDF("time", "key", "value")
        .selectExpr("CAST(time as TIMESTAMP)", "key", "value")
        .withWatermark("time", "3 seconds")
        .groupBy(session_window($"time", "3 seconds"), $"key")
        .agg(sum($"value").as("res"))
        .orderBy($"session_window.start".asc)
        .select($"session_window.start".cast(StringType),
             $"session_window.end".cast(StringType), $"key", $"res")

    aggregated.explain(true)

    testStream(aggregated, Complete)(
      AddData(inputData, ("2018-11-29 09:56:58", 0, 0),
        ("2018-11-29 09:56:58", 1, 0),
        ("2018-11-29 09:56:58", 2, 0)),
      CheckAnswer(("2018-11-29 09:56:58", "2018-11-29 09:57:01", 0, 0),
        ("2018-11-29 09:56:58", "2018-11-29 09:57:01", 1, 0),
        ("2018-11-29 09:56:58", "2018-11-29 09:57:01", 2, 0)),
      AddData(inputData, ("2018-11-29 09:58:13", 0, 0),
        ("2018-11-29 09:58:13", 1, 0),
        ("2018-11-29 09:58:13", 2, 0),
        ("2018-11-29 09:58:13", 0, 1),
        ("2018-11-29 09:58:13", 1, 1),
        ("2018-11-29 09:58:13", 2, 1)),
      CheckAnswer(("2018-11-29 09:56:58", "2018-11-29 09:57:01", 0, 0),
        ("2018-11-29 09:56:58", "2018-11-29 09:57:01", 1, 0),
        ("2018-11-29 09:56:58", "2018-11-29 09:57:01", 2, 0),
        ("2018-11-29 09:58:13", "2018-11-29 09:58:16", 0, 1),
        ("2018-11-29 09:58:13", "2018-11-29 09:58:16", 1, 1),
        ("2018-11-29 09:58:13", "2018-11-29 09:58:16", 2, 1)),
      StopStream
    )
  }

  /**
   * Note: the watermark of batch N is valid on batch N + 1 start
   */
  test("session window with watermark and single key in append model") {
    val inputData = MemoryStream[(String, String, Int)]
    val aggregated =
      inputData.toDS().toDF("time", "key", "value")
        .selectExpr("CAST(time as TIMESTAMP)", "key", "value")
        .withWatermark("time", "5 seconds")
        .groupBy(session_window($"time", "10 seconds"), $"key")
        .agg(sum($"value").as("res"))
        .select($"session_window.start".cast(StringType),
             $"session_window.end".cast(StringType), $"key", $"res")

    aggregated.explain(true)
    testStream(aggregated, Append)(
      AddData(inputData,
        ("2018-08-22 19:39:27", "a", 4),
        ("2018-08-22 19:39:34", "a", 1),
        ("2018-08-22 19:39:56", "a", 3),
        ("2018-08-22 19:39:56", "b", 2)), // Advance watermark to '2018-08-22 19:39:56' - 5 seconds
      CheckLastBatch(),
      assertNumStateRows(2),
      assertEventStats { e =>
        assert(e.get("max") === formatTimestamp("2018-08-22 19:39:56"))
        assert(e.get("min") === formatTimestamp("2018-08-22 19:39:27"))
        assert(e.get("watermark") === formatTimestamp(0))
      },
      // will be dropped
      AddData(inputData, ("2018-08-22 19:39:33", "a", 1)),
      CheckLastBatch(
        // Evict windows whose window.end less than previous watermark
        ("2018-08-22 19:39:27", "2018-08-22 19:39:44", "a", 5)),
      assertNumStateRows(2),
      assertEventStats { e =>
        assert(e.get("max") === formatTimestamp("2018-08-22 19:39:33"))
        assert(e.get("min") === formatTimestamp("2018-08-22 19:39:33"))
        assert(e.get("avg") === formatTimestamp("2018-08-22 19:39:33"))
        assert(e.get("watermark") === formatTimestamp("2018-08-22 19:39:51"))
      },
      AddData(inputData,
        ("2018-08-22 19:39:27", "a", 4), // will be dropped
        ("2018-08-22 19:39:34", "a", 1)), // will be dropped
      CheckLastBatch(),
      AddData(inputData,
        // extends window(19:39:56, 19:40:06) of key 'a'
        ("2018-08-22 19:40:05", "a", 2),
        // Advance watermark to '2018-08-22 19:39:56' - 5 seconds
        ("2018-08-22 19:40:21", "a", 2)),
      CheckLastBatch(),
      AddData(inputData,
        // Advance watermark to '2018-08-22 19:40:22' - 5 seconds by key 'b'
        ("2018-08-22 19:40:22", "b", 2)),
      CheckLastBatch(
        ("2018-08-22 19:39:56", "2018-08-22 19:40:15", "a", 5),  // Evict windows whose window.end
        ("2018-08-22 19:39:56", "2018-08-22 19:40:06", "b", 2)), // less than previous watermark
      assertNumStateRows(2), // have state of key 'a', 'b'
      assertEventStats { e =>
        assert(e.get("watermark") === formatTimestamp("2018-08-22 19:40:16"))
      },
      AddData(inputData,
        // Advance watermark to '2018-08-22 19:40:40' - 5 seconds by other key 'c'
        ("2018-08-22 19:40:40", "c", 1)),
      CheckLastBatch(),
      assertNumStateRows(3), // have state of key 'a', 'b', 'c'
      assertEventStats { e =>
        assert(e.get("watermark") === formatTimestamp("2018-08-22 19:40:17"))
      },
      AddData(inputData,
        // Advance watermark to '2018-08-22 19:40:41' by other key 'c'
        ("2018-08-22 19:40:41", "c", 1)),
      CheckLastBatch(
        ("2018-08-22 19:40:21", "2018-08-22 19:40:31", "a", 2), // Evict windows whose window.end
        ("2018-08-22 19:40:22", "2018-08-22 19:40:32", "b", 2)), // less than previous watermark
      assertNumStateRows(1), // have state of key 'c'
      assertEventStats { e =>
        assert(e.get("watermark") === formatTimestamp("2018-08-22 19:40:35"))
      },
      StopStream
    )
  }

  test("session window with watermark and single key in append model with unordered timestamp") {
    val inputData = MemoryStream[(String, String, Int)]
    val aggregated =
      inputData.toDS().toDF("time", "key", "value")
        .selectExpr("CAST(time as TIMESTAMP)", "key", "value")
        .withWatermark("time", "10 seconds")
        .groupBy(session_window($"time", "10 seconds"), $"key")
        .agg(sum($"value").as("res"))
        .select($"session_window.start".cast(StringType),
             $"session_window.end".cast(StringType), $"key", $"res")

    aggregated.explain(true)
    testStream(aggregated, Append)(
      AddData(inputData,
          ("2018-08-22 19:39:27", "a", 4),
          ("2018-08-22 19:39:28", "a", 1),
          ("2018-08-22 19:39:27", "b", 2)),
      CheckLastBatch(),
      assertNumStateRows(2),
      assertEventStats { e =>
        assert(e.get("max") === formatTimestamp("2018-08-22 19:39:28"))
        assert(e.get("min") === formatTimestamp("2018-08-22 19:39:27"))
        assert(e.get("watermark") === formatTimestamp(0))
      },
      AddData(inputData, ("2018-08-22 19:39:23", "a", 1)),
      AddData(inputData, ("2018-08-22 19:39:58", "c", 1)),
      CheckLastBatch(),
      assertNumStateRows(3),
      assertEventStats { e =>
        assert(e.get("max") === formatTimestamp("2018-08-22 19:39:58"))
        assert(e.get("min") === formatTimestamp("2018-08-22 19:39:23"))
        assert(e.get("watermark") === formatTimestamp("2018-08-22 19:39:18"))
      },
      AddData(inputData, ("2018-08-22 19:39:58", "c", 1)),
      CheckLastBatch(
          ("2018-08-22 19:39:23", "2018-08-22 19:39:38", "a", 6),
          ("2018-08-22 19:39:27", "2018-08-22 19:39:37", "b", 2)),
      StopStream
    )
  }

  private def assertNumStateRows(numTotalRows: Long): AssertOnQuery = AssertOnQuery { q =>
    val progressWithData = q.recentProgress.filter(_.numInputRows > 0).lastOption.get
    assert(progressWithData.stateOperators(0).numRowsTotal === numTotalRows)
    true
  }

  private def assertEventStats(body: ju.Map[String, String] => Unit): AssertOnQuery = {
    AssertOnQuery { q =>
      body(q.recentProgress.filter(_.numInputRows > 0).lastOption.get.eventTime)
      true
    }
  }

  private val timestampFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") // ISO8601
  timestampFormat.setTimeZone(ju.TimeZone.getTimeZone("UTC"))

  private def formatTimestamp(str: String): String = {
    val ts = Timestamp.valueOf(str).getTime / 1000 * 1000
    timestampFormat.format(new ju.Date(ts))
  }

  private def formatTimestamp(sec: Long): String = {
    timestampFormat.format(new ju.Date(sec * 1000))
  }
}
