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

import java.util.Locale

import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.must.Matchers

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, Column, DataFrame}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.state.{HDFSBackedStateStoreProvider, RocksDBStateStoreProvider}
import org.apache.spark.sql.functions.{count, session_window, sum}
import org.apache.spark.sql.internal.SQLConf

class StreamingSessionWindowSuite extends StreamTest
  with BeforeAndAfter with Matchers with Logging {

  import testImplicits._

  after {
    sqlContext.streams.active.foreach(_.stop())
  }

  def testWithAllOptions(name: String, confPairs: (String, String)*)
    (func: => Any): Unit = {
    val mergingSessionOptions = Seq(true, false).map { value =>
      (SQLConf.STREAMING_SESSION_WINDOW_MERGE_SESSIONS_IN_LOCAL_PARTITION.key, value)
    }
    val providerOptions = Seq(
      classOf[HDFSBackedStateStoreProvider].getCanonicalName,
      classOf[RocksDBStateStoreProvider].getCanonicalName
    ).map { value =>
      (SQLConf.STATE_STORE_PROVIDER_CLASS.key, value.stripSuffix("$"))
    }

    val availableOptions = for (
      opt1 <- mergingSessionOptions;
      opt2 <- providerOptions
    ) yield (opt1, opt2)

    for (option <- availableOptions) {
      test(s"$name - merging sessions in local partition: ${option._1._2} / " +
        s"provider: ${option._2._2}") {
        withSQLConf(confPairs ++
          Seq(
            option._1._1 -> option._1._2.toString,
            option._2._1 -> option._2._2): _*) {
          func
        }
      }
    }
  }

  testWithAllOptions("complete mode - session window") {
    // Implements StructuredSessionization.scala leveraging "session" function
    // as a test, to verify the sessionization works with simple example

    // note that complete mode doesn't honor watermark: even it is specified, watermark will be
    // always Unix timestamp 0

    val inputData = MemoryStream[(String, Long)]
    val sessionUpdates = sessionWindowQuery(inputData)

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

  testWithAllOptions("complete mode - session window - no key") {
    // complete mode doesn't honor watermark: even it is specified, watermark will be
    // always Unix timestamp 0

    val inputData = MemoryStream[Int]
    val windowedAggregation = sessionWindowQueryOnGlobalKey(inputData)

    val e = intercept[StreamingQueryException] {
      testStream(windowedAggregation, OutputMode.Complete())(
        AddData(inputData, 40),
        CheckAnswer() // this is just to trigger the exception
      )
    }
    Seq("Global aggregation with session window", "not supported").foreach { m =>
      assert(e.getMessage.toLowerCase(Locale.ROOT).contains(m.toLowerCase(Locale.ROOT)))
    }
  }

  testWithAllOptions("append mode - session window") {
    // Implements StructuredSessionization.scala leveraging "session" function
    // as a test, to verify the sessionization works with simple example

    val inputData = MemoryStream[(String, Long)]
    val sessionUpdates = sessionWindowQuery(inputData)

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
      assertNumRowsDroppedByWatermark(2),

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

  testWithAllOptions("SPARK-36465: dynamic gap duration") {
    val inputData = MemoryStream[(String, Long)]

    val udf = spark.udf.register("gapDuration", (s: String) => {
      if (s == "hello") {
        "1 second"
      } else if (s == "structured") {
        // zero gap duration will be filtered out from aggregation
        "0 second"
      } else if (s == "world") {
        // negative gap duration will be filtered out from aggregation
        "-10 seconds"
      } else {
        "10 seconds"
      }
    })

    val sessionUpdates = sessionWindowQuery(inputData,
      session_window($"eventTime", udf($"sessionId")))

    testStream(sessionUpdates, OutputMode.Append())(
      AddData(inputData,
        ("hello world spark streaming", 40L),
        ("world hello structured streaming", 41L)
      ),

      // watermark: 11
      // current sessions
      // ("hello", 40, 42, 2, 2),
      // ("streaming", 40, 51, 11, 2),
      // ("spark", 40, 50, 10, 1),
      CheckNewAnswer(
      ),

      // placing new sessions "before" previous sessions
      AddData(inputData, ("spark streaming", 25L)),
      // watermark: 11
      // current sessions
      // ("spark", 25, 35, 10, 1),
      // ("streaming", 25, 35, 10, 1),
      // ("hello", 40, 42, 2, 2),
      // ("streaming", 40, 51, 11, 2),
      // ("spark", 40, 50, 10, 1),
      CheckNewAnswer(
      ),

      // late event which session's end 10 would be later than watermark 11: should be dropped
      AddData(inputData, ("spark streaming", 0L)),
      // watermark: 11
      // current sessions
      // ("spark", 25, 35, 10, 1),
      // ("streaming", 25, 35, 10, 1),
      // ("hello", 40, 42, 2, 2),
      // ("streaming", 40, 51, 11, 2),
      // ("spark", 40, 50, 10, 1),
      CheckNewAnswer(
      ),
      assertNumRowsDroppedByWatermark(2),

      // concatenating multiple previous sessions into one
      AddData(inputData, ("spark streaming", 30L)),
      // watermark: 11
      // current sessions
      // ("spark", 25, 50, 25, 3),
      // ("streaming", 25, 51, 26, 4),
      // ("hello", 40, 42, 2, 2),
      CheckNewAnswer(
      ),

      // placing new sessions after previous sessions
      AddData(inputData, ("hello apache spark", 60L)),
      // watermark: 30
      // current sessions
      // ("spark", 25, 50, 25, 3),
      // ("streaming", 25, 51, 26, 4),
      // ("hello", 40, 42, 2, 2),
      // ("hello", 60, 61, 1, 1),
      // ("apache", 60, 70, 10, 1),
      // ("spark", 60, 70, 10, 1)
      CheckNewAnswer(
      ),

      AddData(inputData, ("structured streaming", 90L)),
      // watermark: 60
      // current sessions
      // ("hello", 60, 61, 1, 1),
      // ("apache", 60, 70, 10, 1),
      // ("spark", 60, 70, 10, 1),
      // ("streaming", 90, 100, 10, 1)
      CheckNewAnswer(
        ("spark", 25, 50, 25, 3),
        ("streaming", 25, 51, 26, 4),
        ("hello", 40, 42, 2, 2)
      )
    )
  }

  testWithAllOptions("append mode - session window - no key") {
    val inputData = MemoryStream[Int]
    val windowedAggregation = sessionWindowQueryOnGlobalKey(inputData)

    val e = intercept[StreamingQueryException] {
      testStream(windowedAggregation)(
        AddData(inputData, 40),
        CheckAnswer() // this is just to trigger the exception
      )
    }
    Seq("Global aggregation with session window", "not supported").foreach { m =>
      assert(e.getMessage.toLowerCase(Locale.ROOT).contains(m.toLowerCase(Locale.ROOT)))
    }
  }

  testWithAllOptions("update mode - session window") {
    // Implements StructuredSessionization.scala leveraging "session" function
    // as a test, to verify the sessionization works with simple example

    val inputData = MemoryStream[(String, Long)]
    val sessionUpdates = sessionWindowQuery(inputData)

    val e = intercept[AnalysisException] {
      testStream(sessionUpdates, OutputMode.Update())(
        AddData(inputData, ("hello", 40L)),
        CheckAnswer() // this is just to trigger the exception
      )
    }
    Seq("Update output mode", "not supported", "for session window").foreach { m =>
      assert(e.getMessage.toLowerCase(Locale.ROOT).contains(m.toLowerCase(Locale.ROOT)))
    }
  }

  testWithAllOptions("update mode - session window - no key") {
    val inputData = MemoryStream[Int]
    val windowedAggregation = sessionWindowQueryOnGlobalKey(inputData)

    val e = intercept[AnalysisException] {
      testStream(windowedAggregation, OutputMode.Update())(
        AddData(inputData, 40),
        CheckAnswer() // this is just to trigger the exception
      )
    }
    Seq("Update output mode", "not supported", "for session window").foreach { m =>
      assert(e.getMessage.toLowerCase(Locale.ROOT).contains(m.toLowerCase(Locale.ROOT)))
    }
  }

  private def assertNumRowsDroppedByWatermark(
      numRowsDroppedByWatermark: Long): AssertOnQuery = AssertOnQuery { q =>
    q.processAllAvailable()
    val progressWithData = q.recentProgress.filterNot { p =>
      // filter out batches which are falling into one of types:
      // 1) doesn't execute the batch run
      // 2) empty input batch
      p.inputRowsPerSecond == 0
    }.lastOption.get
    assert(progressWithData.stateOperators(0).numRowsDroppedByWatermark
      === numRowsDroppedByWatermark)
    true
  }


  private def sessionWindowQuery(
      input: MemoryStream[(String, Long)],
      sessionWindow: Column = session_window($"eventTime", "10 seconds")): DataFrame = {
    // Split the lines into words, treat words as sessionId of events
    val events = input.toDF()
      .select($"_1".as("value"), $"_2".as("timestamp"))
      .withColumn("eventTime", $"timestamp".cast("timestamp"))
      .withWatermark("eventTime", "30 seconds")
      .selectExpr("explode(split(value, ' ')) AS sessionId", "eventTime")

    events
      .groupBy(sessionWindow as Symbol("session"), $"sessionId")
      .agg(count("*").as("numEvents"))
      .selectExpr("sessionId", "CAST(session.start AS LONG)", "CAST(session.end AS LONG)",
        "CAST(session.end AS LONG) - CAST(session.start AS LONG) AS durationMs",
        "numEvents")
  }

  private def sessionWindowQueryOnGlobalKey(input: MemoryStream[Int]): DataFrame = {
    input.toDF()
      .selectExpr("*")
      .withColumn("eventTime", $"value".cast("timestamp"))
      .withWatermark("eventTime", "10 seconds")
      .groupBy(session_window($"eventTime", "5 seconds") as Symbol("session"))
      .agg(count("*") as Symbol("count"), sum("value") as Symbol("sum"))
      .select($"session".getField("start").cast("long").as[Long],
        $"session".getField("end").cast("long").as[Long], $"count".as[Long], $"sum".as[Long])
  }
}
