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

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration.Duration

import org.scalatest.concurrent.PatienceConfiguration.Timeout

import org.apache.spark.{SparkIllegalArgumentException, SparkIllegalStateException}
import org.apache.spark.sql.execution.streaming.{LowLatencyMemoryStream, RealTimeTrigger}
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.execution.streaming.sources.ContinuousMemorySink
import org.apache.spark.sql.internal.SQLConf

class StreamRealTimeModeSuite extends StreamRealTimeModeSuiteBase {
  import testImplicits._

  test("test trigger") {
    def testTrigger(trigger: Trigger, actual: Long): Unit = {
      val realTimeTrigger = trigger.asInstanceOf[RealTimeTrigger]
      assert(
        realTimeTrigger.batchDurationMs == actual,
        s"Real time trigger duration should be ${actual} ms" +
        s" but got ${realTimeTrigger.batchDurationMs} ms"
      )
    }

    // test default
    testTrigger(Trigger.RealTime(), 300000)

    List(
      ("1 second", 1000),
      ("1 minute", 60000),
      ("1 hour", 3600000),
      ("1 day", 86400000),
      ("1 week", 604800000)
    ).foreach {
      case (str, ms) =>
        testTrigger(Trigger.RealTime(str), ms)
        testTrigger(RealTimeTrigger(str), ms)
        testTrigger(RealTimeTrigger.create(str), ms)

    }

    List(1000, 60000, 3600000, 86400000, 604800000).foreach { ms =>
      testTrigger(Trigger.RealTime(ms), ms)
      testTrigger(RealTimeTrigger(ms), ms)
      testTrigger(new RealTimeTrigger(ms), ms)
    }

    List(
      (Duration.apply(1000, "ms"), 1000),
      (Duration.apply(60, "s"), 60000),
      (Duration.apply(1, "h"), 3600000),
      (Duration.apply(1, "d"), 86400000)
    ).foreach {
      case (duration, ms) =>
        testTrigger(Trigger.RealTime(duration), ms)
        testTrigger(RealTimeTrigger(duration), ms)
        testTrigger(RealTimeTrigger(duration), ms)
    }

    List(
      (1000, TimeUnit.MILLISECONDS, 1000),
      (60, TimeUnit.SECONDS, 60000),
      (1, TimeUnit.HOURS, 3600000),
      (1, TimeUnit.DAYS, 86400000)
    ).foreach {
      case (interval, unit, ms) =>
        testTrigger(Trigger.RealTime(interval, unit), ms)
        testTrigger(RealTimeTrigger(interval, unit), ms)
        testTrigger(RealTimeTrigger.create(interval, unit), ms)
    }
    // test invalid
    List("-1", "0").foreach(
      str =>
        intercept[IllegalArgumentException] {
          testTrigger(Trigger.RealTime(str), -1)
          testTrigger(RealTimeTrigger.create(str), -1)
        }
    )

    List(-1, 0).foreach(
      duration =>
        intercept[IllegalArgumentException] {
          testTrigger(Trigger.RealTime(duration), -1)
          testTrigger(RealTimeTrigger(duration), -1)
        }
    )
  }

  test("processAllAvailable") {
    val inputData = LowLatencyMemoryStream.singlePartition[Int]
    val mapped = inputData.toDS().map(_ + 1)

    testStream(mapped, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
      AddData(inputData, 1, 2, 3),
      StartStream(),
      CheckAnswer(2, 3, 4),
      AddData(inputData, 4, 5, 6),
      CheckAnswer(2, 3, 4, 5, 6, 7),
      AddData(inputData, 7),
      CheckAnswer(2, 3, 4, 5, 6, 7, 8),
      AddData(inputData, 10, 11),
      ProcessAllAvailable(),
      StopStream,
      StartStream(),
      CheckAnswer(2, 3, 4, 5, 6, 7, 8, 11, 12)
    )
  }

  test("error: batch duration is set less than minimum") {
    val inputData = LowLatencyMemoryStream.singlePartition[Int]
    val mapped = inputData.toDS().map(_ + 1)
    val minBatchDuration =
      spark.conf.get(SQLConf.STREAMING_REAL_TIME_MODE_MIN_BATCH_DURATION)
    val ex = intercept[SparkIllegalArgumentException] {
      testStream(mapped, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
        StartStream(RealTimeTrigger(minBatchDuration - 1))
      )
    }
    checkError(
      ex,
      "INVALID_STREAMING_REAL_TIME_MODE_TRIGGER_INTERVAL",
      parameters = Map(
        "interval" -> (minBatchDuration - 1).toString,
        "minBatchDuration" -> minBatchDuration.toString
      )
    )
  }

  test("environment check for real-time mode throws when the valid configurations aren't set") {
    val inputData = LowLatencyMemoryStream.singlePartition[Int]
    val mapped = inputData.toDS().map(_ + 1)

    checkError(
      intercept[SparkIllegalArgumentException] {
        testStream(mapped, OutputMode.Update, Map(
          "asyncProgressTrackingEnabled" -> "true"
        ), new ContinuousMemorySink())(
          StartStream()
        )
      },
      "STREAMING_REAL_TIME_MODE.ASYNC_PROGRESS_TRACKING_NOT_SUPPORTED"
    )
  }

  test("error when unsupported source is used") {
    val inputData = MemoryStream[Int]
    val mapped = inputData.toDS().map(_ + 1)

    testStream(mapped, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
      StartStream(),
      ExpectFailure[SparkIllegalArgumentException] { ex =>
        checkError(
          ex.asInstanceOf[SparkIllegalArgumentException],
          "STREAMING_REAL_TIME_MODE.INPUT_STREAM_NOT_SUPPORTED",
          parameters =
            Map("className" -> "org.apache.spark.sql.execution.streaming.runtime.MemoryStream")
        )
      }
    )
  }

  test("error on self union") {
    val inputData = LowLatencyMemoryStream.singlePartition[Int].toDS()
    val mapped = inputData.map(_ + 1)

    val unioned = mapped
      .union(inputData)
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .map(_.toInt)
      .map(_ + 1)

    testStream(unioned, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
      StartStream(),
      ExpectFailure[SparkIllegalStateException] { ex =>
        checkError(
          ex.asInstanceOf[SparkIllegalStateException],
          "STREAMING_REAL_TIME_MODE.IDENTICAL_SOURCES_IN_UNION_NOT_SUPPORTED",
          parameters = Map("sources" ->
            "MemoryStream\\[value#\\d+\\], MemoryStream\\[value#\\d+\\]"),
          matchPVals = true
        )
      }
    )
  }
}

class StreamRealTimeModeWithManualClockSuite extends StreamRealTimeModeManualClockSuiteBase {
  import testImplicits._

  test("simple map query") {
    val inputData = LowLatencyMemoryStream[Int]
    val mapped = inputData.toDS().map(_ + 1)

    testStream(mapped, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
      AddData(inputData, 1, 2, 3),
      StartStream(),
      CheckAnswerWithTimeout(10000, 2, 3, 4),
      AddData(inputData, 4, 5, 6),
      // make sure we can output data before batch ends
      CheckAnswerWithTimeout(10000, 2, 3, 4, 5, 6, 7),
      advanceRealTimeClock,
      WaitUntilBatchProcessed(0),
      AddData(inputData, 7),
      CheckAnswerWithTimeout(10000, 2, 3, 4, 5, 6, 7, 8),
      StopStream
    )
  }

  test("simple map query with restarts") {
    val inputData = LowLatencyMemoryStream[Int]
    val mapped = inputData.toDS().map(_ + 1)

    testStream(mapped, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
      StartStream(),
      AddData(inputData, 1, 2, 3),
      CheckAnswerWithTimeout(10000, 2, 3, 4),
      advanceRealTimeClock,
      WaitUntilBatchProcessed(0),
      StopStream,
      AddData(inputData, 4, 5, 6),
      StartStream(),
      CheckAnswerWithTimeout(10000, 2, 3, 4, 5, 6, 7),
      StopStream
    )
  }

  test("simple map query switching between RTM and MBM") {
    val inputData = LowLatencyMemoryStream[Int]
    val mapped = inputData.toDS().map(_ + 1)

    testStream(mapped, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
      StartStream(defaultTrigger),
      AddData(inputData, 1, 2, 3),
      CheckAnswerWithTimeout(10000, 2, 3, 4),
      advanceRealTimeClock,
      WaitUntilBatchProcessed(0),
      StopStream,
      AddData(inputData, 4, 5, 6),
      StartStream(Trigger.ProcessingTime(1000)),
      CheckAnswerWithTimeout(10000, 2, 3, 4, 5, 6, 7),
      WaitUntilBatchProcessed(1),
      StopStream,
      AddData(inputData, 7),
      StartStream(defaultTrigger),
      CheckAnswerWithTimeout(10000, 2, 3, 4, 5, 6, 7, 8),
      advanceRealTimeClock,
      WaitUntilBatchProcessed(2),
      StopStream
    )
  }

  test("listener progress") {
    val inputData = LowLatencyMemoryStream.singlePartition[Int]
    val mapped = inputData.toDS().map(_ + 1)

    var expectedStartOffset: String = null
    var expectedEndOffset = "{\"0\":3}"
    var expectedNumInputRows = 3
    val progressCalled = new AtomicInteger(0)
    var exception: Option[Exception] = None

    spark.streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        val progress: StreamingQueryProgress = event.progress
        try {
          assert(progress.sources(0).startOffset == expectedStartOffset, "startOffset not expected")
          assert(progress.sources(0).endOffset == expectedEndOffset, "endOffset not expected")
          assert(
            progress.sources(0).numInputRows == expectedNumInputRows,
            "numInputRows not expected")
        } catch {
          case ex: Exception =>
            exception = Some(ex)
        }
        progressCalled.incrementAndGet()
      }

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {}
    })

    testStream(mapped, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
      AddData(inputData, 1, 2, 3),
      StartStream(),
      CheckAnswerWithTimeout(60000, 2, 3, 4),
      advanceRealTimeClock,
      WaitUntilBatchProcessed(0),
      Execute { q =>
        eventually(Timeout(streamingTimeout)) {
          assert(progressCalled.get() == 1)
        }
        expectedEndOffset = "{\"0\":6}"
        expectedStartOffset = "{\"0\":3}"
        expectedNumInputRows = 3
      },
      AddData(inputData, 4, 5, 6),
      CheckAnswerWithTimeout(10000, 2, 3, 4, 5, 6, 7),
      advanceRealTimeClock,
      WaitUntilBatchProcessed(1),
      Execute { q =>
        eventually(Timeout(streamingTimeout)) {
          assert(progressCalled.get() == 2)
        }
        expectedEndOffset = "{\"0\":7}"
        expectedStartOffset = "{\"0\":6}"
        expectedNumInputRows = 1
      },
      AddData(inputData, 7),
      CheckAnswerWithTimeout(10000, 2, 3, 4, 5, 6, 7, 8),
      advanceRealTimeClock,
      WaitUntilBatchProcessed(2),
      StopStream
    )
    eventually(Timeout(streamingTimeout)) {
      assert(progressCalled.get() == 3)
    }
    assert(!exception.isDefined, s"${exception}")
  }

  test("purge offsetLog when it doesn't match with the commit log") {
    // Simulate when the query fails after commiting the offset log but before the commit log
    // by manually deleting the last entry of the commit log.
    val inputData = LowLatencyMemoryStream[Int](1)
    val mapped = inputData.toDS().map(_ + 1)

    var lastOffset = -1L

    testStream(mapped, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
      AddData(inputData, 1, 2, 3),
      StartStream(defaultTrigger),
      CheckAnswerWithTimeout(60000, 2, 3, 4),
      advanceRealTimeClock,
      WaitUntilBatchProcessed(0),
      AddData(inputData, 4, 5, 6),
      CheckAnswerWithTimeout(60000, 2, 3, 4, 5, 6, 7),
      advanceRealTimeClock,
      WaitUntilBatchProcessed(1),
      AddData(inputData, 7),
      CheckAnswerWithTimeout(60000, 2, 3, 4, 5, 6, 7, 8),
      advanceRealTimeClock,
      WaitUntilBatchProcessed(2),
      StopStream,
      Execute { q =>
        // Delete the last committed batch from the commit log to simulate the query fails
        // between writing the offset log and the commit log.
        val commit = q.commitLog.getLatest().map(_._1).getOrElse(-1L)
        val offset = q.offsetLog.getLatest().map(_._1).getOrElse(-1L)
        assert(commit == offset)
        q.commitLog.purgeAfter(commit - 1)
        val commitAfterDelete = q.commitLog.getLatest().map(_._1).getOrElse(-1L)
        assert(commitAfterDelete == offset - 1)
        lastOffset = commitAfterDelete
      },
      StartStream(defaultTrigger),
      CheckAnswerWithTimeout(60000, 2, 3, 4, 5, 6, 7, 8, 8),
      StopStream,
      Execute { q =>
        val commit = q.commitLog.getLatest().map(_._1).getOrElse(-1L)
        val offset = q.offsetLog.getLatest().map(_._1).getOrElse(-1L)
        assert(commit == offset && commit == lastOffset)
      },
      AddData(inputData, 8),
      StartStream(defaultTrigger),
      CheckAnswerWithTimeout(60000, 2, 3, 4, 5, 6, 7, 8, 8, 8, 9),
      StopStream
    )
  }
}
