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
      AddData(inputData,
        ("hello world spark streaming", 40L),
        ("world hello structured streaming", 41L)
      ),
      CheckNewAnswer(
        ("hello", 40, 51, 11, 2),
        ("world", 40, 51, 11, 2),
        ("streaming", 40, 51, 11, 2),
        ("spark", 40, 50, 10, 1),
        ("structured", 41, 51, 10, 1)
      ),

      // placing new sessions "before" previous sessions
      AddData(inputData, ("spark streaming", 25L)),
      CheckNewAnswer(
        ("spark", 25, 35, 10, 1),
        ("streaming", 25, 35, 10, 1),
        ("hello", 40, 51, 11, 2),
        ("world", 40, 51, 11, 2),
        ("streaming", 40, 51, 11, 2),
        ("spark", 40, 50, 10, 1),
        ("structured", 41, 51, 10, 1)
      ),

      // concatenating multiple previous sessions into one
      AddData(inputData, ("spark streaming", 30L)),
      CheckNewAnswer(
        ("spark", 25, 50, 25, 3),
        ("streaming", 25, 51, 26, 4),
        ("hello", 40, 51, 11, 2),
        ("world", 40, 51, 11, 2),
        ("structured", 41, 51, 10, 1)
      ),

      // placing new sessions after previous sessions
      AddData(inputData, ("hello apache spark", 60L)),
      CheckNewAnswer(
        ("spark", 25, 50, 25, 3),
        ("streaming", 25, 51, 26, 4),
        ("hello", 40, 51, 11, 2),
        ("world", 40, 51, 11, 2),
        ("structured", 41, 51, 10, 1),
        ("hello", 60, 70, 10, 1),
        ("apache", 60, 70, 10, 1),
        ("spark", 60, 70, 10, 1)
      ),

      AddData(inputData, ("structured streaming", 90L)),
      CheckNewAnswer(
        ("spark", 25, 50, 25, 3),
        ("streaming", 25, 51, 26, 4),
        ("hello", 40, 51, 11, 2),
        ("world", 40, 51, 11, 2),
        ("structured", 41, 51, 10, 1),
        ("hello", 60, 70, 10, 1),
        ("apache", 60, 70, 10, 1),
        ("spark", 60, 70, 10, 1),
        ("structured", 90, 100, 10, 1),
        ("streaming", 90, 100, 10, 1)
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
      .withWatermark("eventTime", "30 seconds")

    val sessionUpdates = events
      .groupBy(session_window($"eventTime", "10 seconds") as 'session, 'sessionId)
      .agg(count("*").as("numEvents"))
      .selectExpr("sessionId", "CAST(session.start AS LONG)", "CAST(session.end AS LONG)",
        "CAST(session.end AS LONG) - CAST(session.start AS LONG) AS durationMs",
        "numEvents")

    testStream(sessionUpdates, OutputMode.Append())(
      AddData(inputData,
        ("hello world spark streaming", 40L),
        ("world hello structured streaming", 41L)
      ),

      // watermark: 11
      // current sessions
      // ("hello", 40, 51, 11, 2),
      // ("world", 40, 51, 11, 2),
      // ("streaming", 40, 51, 11, 2),
      // ("spark", 40, 50, 10, 1),
      // ("structured", 41, 51, 10, 1)
      CheckNewAnswer(
      ),

      // placing new sessions "before" previous sessions
      AddData(inputData, ("spark streaming", 25L)),
      // watermark: 11
      // current sessions
      // ("spark", 25, 35, 10, 1),
      // ("streaming", 25, 35, 10, 1),
      // ("hello", 40, 51, 11, 2),
      // ("world", 40, 51, 11, 2),
      // ("streaming", 40, 51, 11, 2),
      // ("spark", 40, 50, 10, 1),
      // ("structured", 41, 51, 10, 1)
      CheckNewAnswer(
      ),

      // late event which session's end 10 would be later than watermark 11: should be dropped
      AddData(inputData, ("spark streaming", 0L)),
      // watermark: 11
      // current sessions
      // ("spark", 25, 35, 10, 1),
      // ("streaming", 25, 35, 10, 1),
      // ("hello", 40, 51, 11, 2),
      // ("world", 40, 51, 11, 2),
      // ("streaming", 40, 51, 11, 2),
      // ("spark", 40, 50, 10, 1),
      // ("structured", 41, 51, 10, 1)
      CheckNewAnswer(
      ),

      // concatenating multiple previous sessions into one
      AddData(inputData, ("spark streaming", 30L)),
      // watermark: 11
      // current sessions
      // ("spark", 25, 50, 25, 3),
      // ("streaming", 25, 51, 26, 4),
      // ("hello", 40, 51, 11, 2),
      // ("world", 40, 51, 11, 2),
      // ("structured", 41, 51, 10, 1)
      CheckNewAnswer(
      ),

      // placing new sessions after previous sessions
      AddData(inputData, ("hello apache spark", 60L)),
      // watermark: 30
      // current sessions
      // ("spark", 25, 50, 25, 3),
      // ("streaming", 25, 51, 26, 4),
      // ("hello", 40, 51, 11, 2),
      // ("world", 40, 51, 11, 2),
      // ("structured", 41, 51, 10, 1),
      // ("hello", 60, 70, 10, 1),
      // ("apache", 60, 70, 10, 1),
      // ("spark", 60, 70, 10, 1)
      CheckNewAnswer(
      ),

      AddData(inputData, ("structured streaming", 90L)),
      // watermark: 60
      // current sessions
      // ("hello", 60, 70, 10, 1),
      // ("apache", 60, 70, 10, 1),
      // ("spark", 60, 70, 10, 1),
      // ("structured", 90, 100, 10, 1),
      // ("streaming", 90, 100, 10, 1)
      CheckNewAnswer(
        ("spark", 25, 50, 25, 3),
        ("streaming", 25, 51, 26, 4),
        ("hello", 40, 51, 11, 2),
        ("world", 40, 51, 11, 2),
        ("structured", 41, 51, 10, 1)
      )
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
      AddData(inputData,
        ("hello world spark streaming", 40L),
        ("world hello structured streaming", 41L)
      ),
      // watermark: 11
      // current sessions
      // ("hello", 40, 51, 11, 2),
      // ("world", 40, 51, 11, 2),
      // ("streaming", 40, 51, 11, 2),
      // ("spark", 40, 50, 10, 1),
      // ("structured", 41, 51, 10, 1)
      CheckNewAnswer(
        ("hello", 40, 51, 11, 2),
        ("world", 40, 51, 11, 2),
        ("streaming", 40, 51, 11, 2),
        ("spark", 40, 50, 10, 1),
        ("structured", 41, 51, 10, 1)
      ),

      // placing new sessions "before" previous sessions
      AddData(inputData, ("spark streaming", 25L)),
      // watermark: 11
      // current sessions
      // ("spark", 25, 35, 10, 1),
      // ("streaming", 25, 35, 10, 1),
      // ("hello", 40, 51, 11, 2),
      // ("world", 40, 51, 11, 2),
      // ("streaming", 40, 51, 11, 2),
      // ("spark", 40, 50, 10, 1),
      // ("structured", 41, 51, 10, 1)
      CheckNewAnswer(
        ("spark", 25, 35, 10, 1),
        ("streaming", 25, 35, 10, 1)
      ),

      // late event which session's end 10 would be later than watermark 11: should be dropped
      AddData(inputData, ("spark streaming", 0L)),
      // watermark: 11
      // current sessions
      // ("spark", 25, 35, 10, 1),
      // ("streaming", 25, 35, 10, 1),
      // ("hello", 40, 51, 11, 2),
      // ("world", 40, 51, 11, 2),
      // ("streaming", 40, 51, 11, 2),
      // ("spark", 40, 50, 10, 1),
      // ("structured", 41, 51, 10, 1)
      CheckNewAnswer(
      ),

      // concatenating multiple previous sessions into one
      AddData(inputData, ("spark streaming", 30L)),
      // watermark: 11
      // current sessions
      // ("spark", 25, 50, 25, 3),
      // ("streaming", 25, 51, 26, 4),
      // ("hello", 40, 51, 11, 2),
      // ("world", 40, 51, 11, 2),
      // ("structured", 41, 51, 10, 1)
      CheckNewAnswer(
        ("spark", 25, 50, 25, 3),
        ("streaming", 25, 51, 26, 4)
      ),

      // placing new sessions after previous sessions
      AddData(inputData, ("hello apache spark", 60L)),
      // watermark: 30
      // current sessions
      // ("spark", 25, 50, 25, 3),
      // ("streaming", 25, 51, 26, 4),
      // ("hello", 40, 51, 11, 2),
      // ("world", 40, 51, 11, 2),
      // ("structured", 41, 51, 10, 1),
      // ("hello", 60, 70, 10, 1),
      // ("apache", 60, 70, 10, 1),
      // ("spark", 60, 70, 10, 1)
      CheckNewAnswer(
        ("hello", 60, 70, 10, 1),
        ("apache", 60, 70, 10, 1),
        ("spark", 60, 70, 10, 1)
      ),

      AddData(inputData, ("structured streaming", 90L)),
      // watermark: 60
      // current sessions
      // ("hello", 60, 70, 10, 1),
      // ("apache", 60, 70, 10, 1),
      // ("spark", 60, 70, 10, 1),
      // ("structured", 90, 100, 10, 1),
      // ("streaming", 90, 100, 10, 1)
      // evicted
      // ("spark", 25, 50, 25, 3),
      // ("streaming", 25, 51, 26, 4),
      // ("hello", 40, 51, 11, 2),
      // ("world", 40, 51, 11, 2),
      // ("structured", 41, 51, 10, 1)
      CheckNewAnswer(
        ("structured", 90, 100, 10, 1),
        ("streaming", 90, 100, 10, 1)
      )
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
