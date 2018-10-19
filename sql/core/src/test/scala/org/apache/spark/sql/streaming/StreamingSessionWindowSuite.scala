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

import org.scalatest.{BeforeAndAfter, Matchers}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.{count, session_window, sum}
import org.apache.spark.sql.internal.SQLConf

class StreamingSessionWindowSuite extends StreamTest
  with BeforeAndAfter with Matchers with Logging {

  import testImplicits._

  after {
    sqlContext.streams.active.foreach(_.stop())
  }

  def testWithAllOptionsMergingSessionInLocalPartition(name: String, confPairs: (String, String)*)
                              (func: => Any): Unit = {
    val key = SQLConf.STREAMING_SESSION_WINDOW_MERGE_SESSIONS_IN_LOCAL_PARTITION.key
    val availableOptions = Seq(true, false)

    for (enabled <- availableOptions) {
      test(s"$name - merging sessions in local partition: $enabled") {
        withSQLConf(confPairs ++ Seq(key -> enabled.toString): _*) {
          func
        }
      }
    }
  }

  testWithAllOptionsMergingSessionInLocalPartition("complete mode - session window") {
    // Implements StructuredSessionization.scala leveraging "session" function
    // as a test, to verify the sessionization works with simple example

    // note that complete mode doesn't honor watermark: even it is specified, watermark will be
    // always Unix timestamp 0

    val inputData = MemoryStream[(String, Long)]

    // Split the lines into words, treat words as sessionId of events
    val events = inputData.toDF()
      .select($"_1".as("value"), $"_2".as("timestamp"))
      .withColumn("eventTime", $"timestamp".cast("timestamp"))
      .selectExpr("explode(split(value, ' ')) AS sessionId", "eventTime")

    val sessionUpdates = events
      .groupBy(session_window($"eventTime", "10 seconds") as 'session, 'sessionId)
      .agg(count("*").as("numEvents"))
      .selectExpr("sessionId", "CAST(session.start AS LONG)", "CAST(session.end AS LONG)",
        "CAST(session.end AS LONG) - CAST(session.start AS LONG) AS durationMs",
        "numEvents")

    testStream(sessionUpdates, OutputMode.Complete())(
      AddData(inputData, ("hello world spark", 10L), ("world hello structured streaming", 11L)),
      CheckNewAnswer(
        ("hello", 10, 21, 11, 2),
        ("world", 10, 21, 11, 2),
        ("spark", 10, 20, 10, 1),
        ("structured", 11, 21, 10, 1),
        ("streaming", 11, 21, 10, 1)
      ),

      AddData(inputData, ("spark streaming", 15L)),
      CheckNewAnswer(
        ("hello", 10, 21, 11, 2),
        ("world", 10, 21, 11, 2),
        ("spark", 10, 25, 15, 2),
        ("structured", 11, 21, 10, 1),
        ("streaming", 11, 25, 14, 2)
      ),

      AddData(inputData, ("hello world", 25L)),
      CheckNewAnswer(
        ("hello", 10, 21, 11, 2),
        ("world", 10, 21, 11, 2),
        ("spark", 10, 25, 15, 2),
        ("structured", 11, 21, 10, 1),
        ("streaming", 11, 25, 14, 2),
        ("hello", 25, 35, 10, 1),
        ("world", 25, 35, 10, 1)
      ),

      AddData(inputData, ("hello world", 3L)),
      CheckNewAnswer(
        ("hello", 3, 21, 18, 3),
        ("world", 3, 21, 18, 3),
        ("spark", 10, 25, 15, 2),
        ("structured", 11, 21, 10, 1),
        ("streaming", 11, 25, 14, 2),
        ("hello", 25, 35, 10, 1),
        ("world", 25, 35, 10, 1)
      ),

      AddData(inputData, ("hello", 31L)),
      CheckNewAnswer(
        ("hello", 3, 21, 18, 3),
        ("world", 3, 21, 18, 3),
        ("spark", 10, 25, 15, 2),
        ("structured", 11, 21, 10, 1),
        ("streaming", 11, 25, 14, 2),
        ("hello", 25, 41, 16, 2),
        ("world", 25, 35, 10, 1)
      ),

      AddData(inputData, ("hello", 35L)),
      CheckNewAnswer(
        ("hello", 3, 21, 18, 3),
        ("world", 3, 21, 18, 3),
        ("spark", 10, 25, 15, 2),
        ("structured", 11, 21, 10, 1),
        ("streaming", 11, 25, 14, 2),
        ("hello", 25, 45, 20, 3),
        ("world", 25, 35, 10, 1)
      ),

      AddData(inputData, ("hello apache spark", 60L)),
      CheckNewAnswer(
        ("hello", 3, 21, 18, 3),
        ("world", 3, 21, 18, 3),
        ("spark", 10, 25, 15, 2),
        ("structured", 11, 21, 10, 1),
        ("streaming", 11, 25, 14, 2),
        ("hello", 25, 45, 20, 3),
        ("world", 25, 35, 10, 1),
        ("hello", 60, 70, 10, 1),
        ("apache", 60, 70, 10, 1),
        ("spark", 60, 70, 10, 1)
      )
    )
  }

  testWithAllOptionsMergingSessionInLocalPartition("complete mode - session window - no key") {
    // complete mode doesn't honor watermark: even it is specified, watermark will be
    // always Unix timestamp 0

    val inputData = MemoryStream[Int]

    val windowedAggregation = inputData.toDF()
      .selectExpr("*")
      .withColumn("eventTime", $"value".cast("timestamp"))
      .groupBy(session_window($"eventTime", "5 seconds") as 'session)
      .agg(count("*") as 'count, sum("value") as 'sum)
      .select($"session".getField("start").cast("long").as[Long],
        $"session".getField("end").cast("long").as[Long], $"count".as[Long], $"sum".as[Long])

    testStream(windowedAggregation, OutputMode.Complete())(
      AddData(inputData, 10, 11),
      CheckNewAnswer((10, 16, 2, 21)),

      AddData(inputData, 17),
      CheckNewAnswer(
        (10, 16, 2, 21),
        (17, 22, 1, 17)
      ),

      AddData(inputData, 35),
      CheckNewAnswer(
        (10, 16, 2, 21),
        (17, 22, 1, 17),
        (35, 40, 1, 35)
      ),

      // should reflect late row
      AddData(inputData, 22),
      CheckNewAnswer(
        (10, 16, 2, 21),
        (17, 27, 2, 39),
        (35, 40, 1, 35)
      ),

      AddData(inputData, 40),
      CheckNewAnswer(
        (10, 16, 2, 21),
        (17, 27, 2, 39),
        (35, 45, 2, 75)
      )
    )
  }

  testWithAllOptionsMergingSessionInLocalPartition("append mode - session window") {
    // Implements StructuredSessionization.scala leveraging "session" function
    // as a test, to verify the sessionization works with simple example

    val inputData = MemoryStream[(String, Long)]

    // Split the lines into words, treat words as sessionId of events
    val events = inputData.toDF()
      .select($"_1".as("value"), $"_2".as("timestamp"))
      .withColumn("eventTime", $"timestamp".cast("timestamp"))
      .selectExpr("explode(split(value, ' ')) AS sessionId", "eventTime")
      .withWatermark("eventTime", "10 seconds")

    val sessionUpdates = events
      .groupBy(session_window($"eventTime", "10 seconds") as 'session, 'sessionId)
      .agg(count("*").as("numEvents"))
      .selectExpr("sessionId", "CAST(session.start AS LONG)", "CAST(session.end AS LONG)",
        "CAST(session.end AS LONG) - CAST(session.start AS LONG) AS durationMs",
        "numEvents")

    testStream(sessionUpdates, OutputMode.Append())(
      AddData(inputData, ("hello world spark", 10L), ("world hello structured streaming", 11L)),
      // Advance watermark to 1 seconds
      // current sessions after batch:
      // ("hello", 10, 21, 11, 2)
      // ("world", 10, 21, 11, 2)
      // ("spark", 10, 20, 10, 1)
      // ("structured", 11, 21, 10, 1)
      // ("streaming", 11, 21, 10, 1)
      CheckNewAnswer(),

      AddData(inputData, ("spark streaming", 15L)),
      // Advance watermark to 5 seconds
      // current sessions after batch:
      // ("hello", 10, 21, 11, 2)
      // ("world", 10, 21, 11, 2)
      // ("structured", 11, 21, 10, 1)
      // ("spark", 10, 25, 15, 2)
      // ("streaming", 11, 25, 14, 2)
      CheckNewAnswer(),

      AddData(inputData, ("hello world", 25L)),
      // Advance watermark to 15 seconds
      // current sessions after batch:
      // ("hello", 10, 21, 11, 2)
      // ("world", 10, 21, 11, 2)
      // ("structured", 11, 21, 10, 2)
      // ("spark", 10, 25, 15, 2)
      // ("streaming", 11, 25, 14, 2)
      // ("hello", 25, 35, 10, 1)
      // ("world", 25, 35, 10, 1)
      CheckNewAnswer(),

      AddData(inputData, ("hello world", 3L)),
      // input can match to not-yet-evicted sessions, but input itself is less than watermark
      // so it should not match existing sessions
      // Watermark kept 15 seconds
      // current sessions after batch:
      // ("hello", 10, 21, 11, 2)
      // ("world", 10, 21, 11, 2)
      // ("structured", 11, 21, 10, 2)
      // ("spark", 10, 25, 15, 2)
      // ("streaming", 11, 25, 14, 2)
      // ("hello", 25, 35, 10, 1)
      // ("world", 25, 35, 10, 1)
      CheckNewAnswer(),

      AddData(inputData, ("hello", 31L)),
      // Advance watermark to 21 seconds
      // current sessions after batch:
      // ("spark", 10, 25, 15, 2)
      // ("streaming", 11, 25, 14, 2)
      // ("hello", 25, 41, 16, 2)
      // ("world", 25, 35, 10, 1)
      CheckNewAnswer(
        ("hello", 10, 21, 11, 2),
        ("world", 10, 21, 11, 2),
        ("structured", 11, 21, 10, 1)
      ),

      AddData(inputData, ("hello", 35L)),
      // Advance watermark to 25 seconds
      // current sessions after batch:
      // ("hello", 25, 45, 20, 3)
      // ("world", 25, 35, 10, 1)
      CheckNewAnswer(
        ("spark", 10, 25, 15, 2),
        ("streaming", 11, 25, 14, 2)
      ),

      AddData(inputData, ("hello apache spark", 60L)),
      // Advance watermark to 50 seconds
      // current sessions after batch:
      // ("hello", 60, 70, 10, 1)
      // ("apache", 60, 70, 10, 1)
      // ("spark", 60, 70, 10, 1)
      CheckNewAnswer(("hello", 25, 45, 20, 3), ("world", 25, 35, 10, 1))
    )
  }

  testWithAllOptionsMergingSessionInLocalPartition("append mode - session window - " +
    "storing multiple sessions in given key") {
    val inputData = MemoryStream[Int]
    val windowedAggregation = inputData.toDF()
      .selectExpr("*", "CAST(MOD(value, 2) AS INT) AS valuegroup")
      .withColumn("eventTime", $"value".cast("timestamp"))
      .withWatermark("eventTime", "10 seconds")
      .groupBy(session_window($"eventTime", "5 seconds") as 'session, 'valuegroup)
      .agg(count("*") as 'count, sum("value") as 'sum)
      .select($"valuegroup", $"session".getField("start").cast("long").as[Long],
        $"session".getField("end").cast("long").as[Long], $"count".as[Long], $"sum".as[Long])

    testStream(windowedAggregation, OutputMode.Append())(
      AddData(inputData, 10, 11, 12, 13),
      // Advance watermark to 3 seconds
      // sessions: key 0 => (10, 17, 2, 22) / key 1 => (11, 18, 2, 24)
      CheckNewAnswer(),
      AddData(inputData, 17),
      // Advance watermark to 7 seconds
      // sessions: key 0 => (10, 17, 2, 22) / key 1 => (11, 22, 3, 41)
      CheckNewAnswer(),
      AddData(inputData, 25),
      // Advance watermark to 15 seconds
      // sessions: key 0 => (10, 17, 2, 22) / key 1 => (11, 22, 3, 41), (25, 30, 1, 25)
      CheckNewAnswer(),
      AddData(inputData, 35),
      // Advance watermark to 25 seconds
      // sessions: key 0 => (10, 17, 2, 22) / key 1 => (11, 22, 3, 41), (25, 30, 1, 25),
      // (35, 40, 1, 35)
      // evicts: key 0 => (10, 17, 2, 22) / key 1 => (11, 22, 3, 41)
      CheckNewAnswer((0, 10, 17, 2, 22), (1, 11, 22, 3, 41)),
      AddData(inputData, 27),
      // don't advance watermark
      // sessions: key 1 => (25, 32, 2, 52), (35, 40, 1, 35)
      CheckNewAnswer(),
      AddData(inputData, 10),   // Should not emit anything as data less than watermark
      CheckNewAnswer(),
      AddData(inputData, 40),
      // Advance watermark to 30 seconds
      // sessions: key 0 => (40, 45, 1, 40) / key 1 => (25, 32, 2, 52), (35, 40, 1, 35)
      CheckNewAnswer()
    )
  }

  testWithAllOptionsMergingSessionInLocalPartition("append mode - session window - no key") {
    val inputData = MemoryStream[Int]

    val windowedAggregation = inputData.toDF()
      .selectExpr("*")
      .withColumn("eventTime", $"value".cast("timestamp"))
      .withWatermark("eventTime", "10 seconds")
      .groupBy(session_window($"eventTime", "5 seconds") as 'session)
      .agg(count("*") as 'count, sum("value") as 'sum)
      .select($"session".getField("start").cast("long").as[Long],
        $"session".getField("end").cast("long").as[Long], $"count".as[Long], $"sum".as[Long])

    testStream(windowedAggregation)(
      AddData(inputData, 10, 11), // sessions: (10,16)
      CheckNewAnswer(),

      AddData(inputData, 17),
      // Advance watermark to 7 seconds
      // sessions: (10,16), (17,23)
      CheckNewAnswer(),

      AddData(inputData, 25),
      // Advance watermark to 15 seconds
      // sessions: (10,16), (17,23), (25,30)
      CheckNewAnswer(),

      AddData(inputData, 35),
      // Advance watermark to 25 seconds
      // sessions: (10,16), (17,22), (25,30), (35,40)
      // evicts: (10,16), (17,22)
      CheckNewAnswer((10, 16, 2, 21), (17, 22, 1, 17)),

      AddData(inputData, 10),   // Should not emit anything as data less than watermark
      CheckNewAnswer(),

      AddData(inputData, 40),
      // Advance watermark to 30 seconds
      // sessions: (25,30) / (35,45)
      // evicts: (25,30)
      CheckNewAnswer((25, 30, 1, 25))
    )
  }

  testWithAllOptionsMergingSessionInLocalPartition("update mode - session window") {
    // Implements StructuredSessionization.scala leveraging "session" function
    // as a test, to verify the sessionization works with simple example

    val inputData = MemoryStream[(String, Long)]

    // Split the lines into words, treat words as sessionId of events
    val events = inputData.toDF()
      .select($"_1".as("value"), $"_2".as("timestamp"))
      .withColumn("eventTime", $"timestamp".cast("timestamp"))
      .selectExpr("explode(split(value, ' ')) AS sessionId", "eventTime")
      .withWatermark("eventTime", "10 seconds")

    val sessionUpdates = events
      .groupBy(session_window($"eventTime", "10 seconds") as 'session, 'sessionId)
      .agg(count("*").as("numEvents"))
      .selectExpr("sessionId", "CAST(session.start AS LONG)", "CAST(session.end AS LONG)",
        "CAST(session.end AS LONG) - CAST(session.start AS LONG) AS durationMs",
        "numEvents")

    testStream(sessionUpdates, OutputMode.Update())(
      AddData(inputData, ("hello world spark", 10L), ("world hello structured streaming", 11L)),
      // Advance watermark to 1 seconds
      // current sessions after batch:
      // ("hello", 10, 21, 11, 2)
      // ("world", 10, 21, 11, 2)
      // ("spark", 10, 20, 10, 1)
      // ("structured", 11, 21, 10, 1)
      // ("streaming", 11, 21, 10, 1)
      CheckNewAnswer(
        ("hello", 10, 21, 11, 2),
        ("world", 10, 21, 11, 2),
        ("spark", 10, 20, 10, 1),
        ("structured", 11, 21, 10, 1),
        ("streaming", 11, 21, 10, 1)
      ),

      AddData(inputData, ("spark streaming", 15L)),
      // Advance watermark to 5 seconds
      // current sessions after batch:
      // ("hello", 10, 21, 11, 2)
      // ("world", 10, 21, 11, 2)
      // ("structured", 11, 21, 10, 1)
      // ("spark", 10, 25, 15, 2)
      // ("streaming", 11, 25, 14, 2)
      CheckNewAnswer(("spark", 10, 25, 15, 2), ("streaming", 11, 25, 14, 2)),

      AddData(inputData, ("hello world", 25L)),
      // Advance watermark to 15 seconds
      // current sessions after batch:
      // ("hello", 10, 21, 11, 2)
      // ("world", 10, 21, 11, 2)
      // ("structured", 11, 21, 10, 1)
      // ("spark", 10, 25, 15, 2)
      // ("streaming", 11, 25, 14, 2)
      // ("hello", 25, 35, 10, 1)
      // ("world", 25, 35, 10, 1)
      CheckNewAnswer(("hello", 25, 35, 10, 1), ("world", 25, 35, 10, 1)),

      AddData(inputData, ("hello world", 3L)),
      // input can match to not-yet-evicted sessions, but input itself is less than watermark
      // so it should not match exiting sessions
      // Watermark kept 15 seconds
      // current sessions after batch:
      // ("hello", 10, 21, 11, 2)
      // ("world", 10, 21, 11, 2)
      // ("structured", 11, 21, 10, 1)
      // ("spark", 10, 25, 15, 2)
      // ("streaming", 11, 25, 14, 2)
      // ("hello", 25, 35, 10, 1)
      // ("world", 25, 35, 10, 1)
      CheckNewAnswer(),

      AddData(inputData, ("hello", 31L)),
      // Advance watermark to 21 seconds
      // current sessions after batch:
      // ("spark", 10, 25, 15, 2)
      // ("streaming", 11, 25, 14, 2)
      // ("hello", 25, 41, 16, 2)
      // ("world", 25, 35, 10, 1)
      CheckNewAnswer(("hello", 25, 41, 16, 2)),

      AddData(inputData, ("hello", 35L)),
      // Advance watermark to 25 seconds
      // current sessions after batch:
      // ("hello", 25, 45, 20, 3)
      // ("world", 25, 35, 10, 1)
      CheckNewAnswer(("hello", 25, 45, 20, 3)),

      AddData(inputData, ("hello apache spark", 60L)),
      // Advance watermark to 50 seconds
      // current sessions after batch:
      // ("hello", 60, 70, 10, 1)
      // ("apache", 60, 70, 10, 1)
      // ("spark", 60, 70, 10, 1)
      CheckNewAnswer(("hello", 60, 70, 10, 1), ("apache", 60, 70, 10, 1), ("spark", 60, 70, 10, 1))
    )
  }

  testWithAllOptionsMergingSessionInLocalPartition("update mode - session window - " +
    "storing multiple sessions in given key") {
    val inputData = MemoryStream[Int]
    val windowedAggregation = inputData.toDF()
      .selectExpr("*", "CAST(MOD(value, 2) AS INT) AS valuegroup")
      .withColumn("eventTime", $"value".cast("timestamp"))
      .withWatermark("eventTime", "10 seconds")
      .groupBy(session_window($"eventTime", "5 seconds") as 'session, 'valuegroup)
      .agg(count("*") as 'count, sum("value") as 'sum)
      .select($"valuegroup", $"session".getField("start").cast("long").as[Long],
        $"session".getField("end").cast("long").as[Long], $"count".as[Long], $"sum".as[Long])

    testStream(windowedAggregation, OutputMode.Update())(
      AddData(inputData, 10, 11, 12, 13),
      // Advance watermark to 3 seconds
      // sessions: key 0 => (10,17) / key 1 => (11, 18)
      CheckNewAnswer((0, 10, 17, 2, 22), (1, 11, 18, 2, 24)),
      AddData(inputData, 17),
      // Advance watermark to 7 seconds
      // sessions: key 0 => (10,17) / key 1 => (11, 22)
      // updated: key 1 => (11,22)
      CheckNewAnswer((1, 11, 22, 3, 41)),
      AddData(inputData, 25),
      // Advance watermark to 15 seconds
      // sessions: key 0 => (10,17) / key 1 => (11,22), (25,30)
      // updated: key 1 => (25,30)
      CheckNewAnswer((1, 25, 30, 1, 25)),
      AddData(inputData, 35),
      // Advance watermark to 25 seconds
      // sessions: key 0 => (10,17) / key 1 => (11,22), (25,30), (35,40)
      // updated: key 1 => (35,40)
      // evicts: key 0 => (10,17) / key 1 => (11,22)
      CheckNewAnswer((1, 35, 40, 1, 35)),
      AddData(inputData, 27),
      // don't advance watermark
      // sessions: key 1 => (25,32), (35,40)
      // updated: key 1 => (25,32)
      CheckNewAnswer((1, 25, 32, 2, 52)),
      AddData(inputData, 10),   // Should not emit anything as data less than watermark
      CheckNewAnswer(),
      AddData(inputData, 40),
      // Advance watermark to 30 seconds
      // sessions: key 0 => (40,45) / key 1 => (25,32), (35,40)
      // updated: key 0 => (40,45)
      CheckNewAnswer((0, 40, 45, 1, 40))
    )
  }

  testWithAllOptionsMergingSessionInLocalPartition("update mode - session window - no key") {
    val inputData = MemoryStream[Int]

    val windowedAggregation = inputData.toDF()
      .selectExpr("*")
      .withColumn("eventTime", $"value".cast("timestamp"))
      .withWatermark("eventTime", "10 seconds")
      .groupBy(session_window($"eventTime", "5 seconds") as 'session)
      .agg(count("*") as 'count, sum("value") as 'sum)
      .select($"session".getField("start").cast("long").as[Long],
        $"session".getField("end").cast("long").as[Long], $"count".as[Long], $"sum".as[Long])

    testStream(windowedAggregation, OutputMode.Update())(

      AddData(inputData, 10, 11),
      // Advance watermark to 1 seconds
      // sessions: (10,16)
      CheckNewAnswer((10, 16, 2, 21)),

      AddData(inputData, 17),
      // Advance watermark to 7 seconds
      // sessions: (10,16), (17,22)
      // updated: (17,22)
      CheckNewAnswer((17, 22, 1, 17)),

      AddData(inputData, 25),
      // Advance watermark to 15 seconds
      // sessions: (10,16), (17,22), (25,30)
      // updated: (25,30)
      CheckNewAnswer((25, 30, 1, 25)),

      AddData(inputData, 35),
      // Advance watermark to 25 seconds
      // sessions: (10,16), (17,22), (25,30), (35,40)
      // updated: (35, 40)
      // evicts: (10,16), (17,22)
      CheckNewAnswer((35, 40, 1, 35)),

      AddData(inputData, 10),   // Should not emit anything as data less than watermark
      CheckNewAnswer(),

      AddData(inputData, 40),
      // Advance watermark to 30 seconds
      // sessions: (25,30), (35,45)
      // updated: (35, 45)
      CheckNewAnswer((35, 45, 2, 75))
    )
  }

}
