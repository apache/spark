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

import java.util.UUID

import scala.collection.mutable

import org.scalactic.TolerantNumerics
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.concurrent.Waiters.Waiter

import org.apache.spark.SparkException
import org.apache.spark.scheduler._
import org.apache.spark.sql.{Encoder, Row, SparkSession}
import org.apache.spark.sql.connector.read.streaming.{Offset => OffsetV2}
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.StreamingQueryListener._
import org.apache.spark.sql.streaming.ui.StreamingQueryStatusListener
import org.apache.spark.sql.streaming.util.StreamManualClock
import org.apache.spark.util.JsonProtocol

class StreamingQueryListenerSuite extends StreamTest with BeforeAndAfter {

  import testImplicits._

  // To make === between double tolerate inexact values
  implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(0.01)

  after {
    spark.streams.active.foreach(_.stop())
    assert(spark.streams.active.isEmpty)
    // Skip check default `StreamingQueryStatusListener` which is for streaming UI.
    assert(spark.streams.listListeners()
      .forall(_.isInstanceOf[StreamingQueryStatusListener]))
    // Make sure we don't leak any events to the next test
    spark.sparkContext.listenerBus.waitUntilEmpty()
  }

  testQuietly("single listener, check trigger events are generated correctly") {
    val clock = new StreamManualClock
    val inputData = new MemoryStream[Int](0, sqlContext)
    val df = inputData.toDS().as[Long].map { 10 / _ }
    val listener = new EventCollector

    case class AssertStreamExecThreadToWaitForClock()
      extends AssertOnQuery(q => {
        eventually(Timeout(streamingTimeout)) {
          if (q.exception.isEmpty) {
            assert(clock.asInstanceOf[StreamManualClock].isStreamWaitingAt(clock.getTimeMillis))
          }
        }
        if (q.exception.isDefined) {
          throw q.exception.get
        }
        true
      }, "")

    try {
      // No events until started
      spark.streams.addListener(listener)
      assert(listener.startEvent === null)
      assert(listener.progressEvents.isEmpty)
      assert(listener.terminationEvent === null)

      testStream(df, OutputMode.Append)(

        // Start event generated when query started
        StartStream(Trigger.ProcessingTime(100), triggerClock = clock),
        AssertOnQuery { query =>
          assert(listener.startEvent !== null)
          assert(listener.startEvent.id === query.id)
          assert(listener.startEvent.runId === query.runId)
          assert(listener.startEvent.name === query.name)
          assert(listener.progressEvents.isEmpty)
          assert(listener.terminationEvent === null)
          true
        },

        // Progress event generated when data processed
        AddData(inputData, 1, 2),
        AdvanceManualClock(100),
        AssertStreamExecThreadToWaitForClock(),
        CheckAnswer(10, 5),
        AssertOnQuery { query =>
          assert(listener.progressEvents.nonEmpty)
          // SPARK-18868: We can't use query.lastProgress, because in progressEvents, we filter
          // out non-zero input rows, but the lastProgress may be a zero input row trigger
          val lastNonZeroProgress = query.recentProgress.filter(_.numInputRows > 0).lastOption
            .getOrElse(fail("No progress updates received in StreamingQuery!"))
          assert(listener.progressEvents.last.json === lastNonZeroProgress.json)
          assert(listener.terminationEvent === null)
          true
        },

        // Termination event generated when stopped cleanly
        StopStream,
        AssertOnQuery { query =>
          eventually(Timeout(streamingTimeout)) {
            assert(listener.terminationEvent !== null)
            assert(listener.terminationEvent.id === query.id)
            assert(listener.terminationEvent.runId === query.runId)
            assert(listener.terminationEvent.exception === None)
          }
          listener.checkAsyncErrors()
          listener.reset()
          true
        },

        // Termination event generated with exception message when stopped with error
        StartStream(Trigger.ProcessingTime(100), triggerClock = clock),
        AssertStreamExecThreadToWaitForClock(),
        AddData(inputData, 0),
        AdvanceManualClock(100), // process bad data
        ExpectFailure[SparkException](),
        AssertOnQuery { query =>
          eventually(Timeout(streamingTimeout)) {
            assert(listener.terminationEvent !== null)
            assert(listener.terminationEvent.id === query.id)
            assert(listener.terminationEvent.exception.nonEmpty)
            // Make sure that the exception message reported through listener
            // contains the actual exception and relevant stack trace
            assert(!listener.terminationEvent.exception.get.contains("StreamingQueryException"))
            assert(
              listener.terminationEvent.exception.get.contains("java.lang.ArithmeticException"))
            assert(listener.terminationEvent.exception.get.contains("StreamingQueryListenerSuite"))
          }
          listener.checkAsyncErrors()
          true
        }
      )
    } finally {
      spark.streams.removeListener(listener)
    }
  }

  test("SPARK-19594: all of listeners should receive QueryTerminatedEvent") {
    val df = MemoryStream[Int].toDS().as[Long]
    val listeners = (1 to 5).map(_ => new EventCollector)
    try {
      listeners.foreach(listener => spark.streams.addListener(listener))
      testStream(df, OutputMode.Append)(
        StartStream(),
        StopStream,
        AssertOnQuery { query =>
          eventually(Timeout(streamingTimeout)) {
            listeners.foreach(listener => assert(listener.terminationEvent !== null))
            listeners.foreach(listener => assert(listener.terminationEvent.id === query.id))
            listeners.foreach(listener => assert(listener.terminationEvent.runId === query.runId))
            listeners.foreach(listener => assert(listener.terminationEvent.exception === None))
          }
          listeners.foreach(listener => listener.checkAsyncErrors())
          listeners.foreach(listener => listener.reset())
          true
        }
      )
    } finally {
      listeners.foreach(spark.streams.removeListener)
    }
  }

  test("continuous processing listeners should receive QueryTerminatedEvent") {
    val df = spark.readStream.format("rate").load()
    val listeners = (1 to 5).map(_ => new EventCollector)
    try {
      listeners.foreach(listener => spark.streams.addListener(listener))
      testStream(df, OutputMode.Append)(
        StartStream(Trigger.Continuous(1000)),
        StopStream,
        AssertOnQuery { query =>
          eventually(Timeout(streamingTimeout)) {
            listeners.foreach(listener => assert(listener.terminationEvent !== null))
            listeners.foreach(listener => assert(listener.terminationEvent.id === query.id))
            listeners.foreach(listener => assert(listener.terminationEvent.runId === query.runId))
            listeners.foreach(listener => assert(listener.terminationEvent.exception === None))
          }
          listeners.foreach(listener => listener.checkAsyncErrors())
          listeners.foreach(listener => listener.reset())
          true
        }
      )
    } finally {
      listeners.foreach(spark.streams.removeListener)
    }
  }

  test("adding and removing listener") {
    def isListenerActive(listener: EventCollector): Boolean = {
      listener.reset()
      testStream(MemoryStream[Int].toDS)(
        StartStream(),
        StopStream
      )
      listener.startEvent != null
    }

    try {
      val listener1 = new EventCollector
      val listener2 = new EventCollector

      spark.streams.addListener(listener1)
      assert(isListenerActive(listener1))
      assert(isListenerActive(listener2) === false)
      spark.streams.addListener(listener2)
      assert(isListenerActive(listener1))
      assert(isListenerActive(listener2))
      spark.streams.removeListener(listener1)
      assert(isListenerActive(listener1) === false)
      assert(isListenerActive(listener2))
    } finally {
      spark.streams.listListeners().foreach(spark.streams.removeListener)
    }
  }

  test("event ordering") {
    val listener = new EventCollector
    withListenerAdded(listener) {
      for (i <- 1 to 50) {
        listener.reset()
        require(listener.startEvent === null)
        testStream(MemoryStream[Int].toDS)(
          StartStream(),
          Assert(listener.startEvent !== null, "onQueryStarted not called before query returned"),
          StopStream,
          Assert { listener.checkAsyncErrors() }
        )
      }
    }
  }

  test("QueryStartedEvent serialization") {
    def testSerialization(event: QueryStartedEvent): Unit = {
      val json = JsonProtocol.sparkEventToJson(event)
      val newEvent = JsonProtocol.sparkEventFromJson(json).asInstanceOf[QueryStartedEvent]
      assert(newEvent.id === event.id)
      assert(newEvent.runId === event.runId)
      assert(newEvent.name === event.name)
    }

    testSerialization(
      new QueryStartedEvent(UUID.randomUUID, UUID.randomUUID, "name", "2016-12-05T20:54:20.827Z"))
    testSerialization(
      new QueryStartedEvent(UUID.randomUUID, UUID.randomUUID, null, "2016-12-05T20:54:20.827Z"))
  }

  test("QueryProgressEvent serialization") {
    def testSerialization(event: QueryProgressEvent): Unit = {
      import scala.collection.JavaConverters._
      val json = JsonProtocol.sparkEventToJson(event)
      val newEvent = JsonProtocol.sparkEventFromJson(json).asInstanceOf[QueryProgressEvent]
      assert(newEvent.progress.json === event.progress.json)  // json as a proxy for equality
      assert(newEvent.progress.durationMs.asScala === event.progress.durationMs.asScala)
      assert(newEvent.progress.eventTime.asScala === event.progress.eventTime.asScala)
    }
    testSerialization(new QueryProgressEvent(StreamingQueryStatusAndProgressSuite.testProgress1))
    testSerialization(new QueryProgressEvent(StreamingQueryStatusAndProgressSuite.testProgress2))
  }

  test("QueryTerminatedEvent serialization") {
    def testSerialization(event: QueryTerminatedEvent): Unit = {
      val json = JsonProtocol.sparkEventToJson(event)
      val newEvent = JsonProtocol.sparkEventFromJson(json).asInstanceOf[QueryTerminatedEvent]
      assert(newEvent.id === event.id)
      assert(newEvent.runId === event.runId)
      assert(newEvent.exception === event.exception)
    }

    val exception = new RuntimeException("exception")
    testSerialization(
      new QueryTerminatedEvent(UUID.randomUUID, UUID.randomUUID, Some(exception.getMessage)))
  }

  test("only one progress event per interval when no data") {
    // This test will start a query but not push any data, and then check if we push too many events
    withSQLConf(SQLConf.STREAMING_NO_DATA_PROGRESS_EVENT_INTERVAL.key -> "100ms") {
      @volatile var numProgressEvent = 0
      val listener = new StreamingQueryListener {
        override def onQueryStarted(event: QueryStartedEvent): Unit = {}
        override def onQueryProgress(event: QueryProgressEvent): Unit = {
          numProgressEvent += 1
        }
        override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {}
      }
      spark.streams.addListener(listener)
      try {
        var numTriggers = 0
        val input = new MemoryStream[Int](0, sqlContext) {
          override def latestOffset(): OffsetV2 = {
            numTriggers += 1
            super.latestOffset()
          }
        }
        val clock = new StreamManualClock()
        val actions = mutable.ArrayBuffer[StreamAction]()
        actions += StartStream(trigger = Trigger.ProcessingTime(10), triggerClock = clock)
        for (_ <- 1 to 100) {
          actions += AdvanceManualClock(10)
        }
        actions += AssertOnQuery { _ =>
          eventually(timeout(streamingTimeout)) {
            assert(numTriggers > 100) // at least 100 triggers have occurred
          }
          true
        }
        // `recentProgress` should not receive too many no data events
        actions += AssertOnQuery { q =>
          q.recentProgress.size > 1 && q.recentProgress.size <= 11
        }
        testStream(input.toDS)(actions.toSeq: _*)
        spark.sparkContext.listenerBus.waitUntilEmpty()
        // 11 is the max value of the possible numbers of events.
        assert(numProgressEvent > 1 && numProgressEvent <= 11)
      } finally {
        spark.streams.removeListener(listener)
      }
    }
  }

  test("listener only posts events from queries started in the related sessions") {
    val session1 = spark.newSession()
    val session2 = spark.newSession()
    val collector1 = new EventCollector
    val collector2 = new EventCollector

    def runQuery(session: SparkSession): Unit = {
      collector1.reset()
      collector2.reset()
      val mem = MemoryStream[Int](implicitly[Encoder[Int]], session.sqlContext)
      testStream(mem.toDS)(
        AddData(mem, 1, 2, 3),
        CheckAnswer(1, 2, 3)
      )
      session.sparkContext.listenerBus.waitUntilEmpty()
    }

    def assertEventsCollected(collector: EventCollector): Unit = {
      assert(collector.startEvent !== null)
      assert(collector.progressEvents.nonEmpty)
      assert(collector.terminationEvent !== null)
    }

    def assertEventsNotCollected(collector: EventCollector): Unit = {
      assert(collector.startEvent === null)
      assert(collector.progressEvents.isEmpty)
      assert(collector.terminationEvent === null)
    }

    assert(session1.ne(session2))
    assert(session1.streams.ne(session2.streams))

    withListenerAdded(collector1, session1) {
      assert(session1.streams.listListeners().nonEmpty)

      withListenerAdded(collector2, session2) {
        assert(session2.streams.listListeners().nonEmpty)

        // query on session1 should send events only to collector1
        runQuery(session1)
        assertEventsCollected(collector1)
        assertEventsNotCollected(collector2)

        // query on session2 should send events only to collector2
        runQuery(session2)
        assertEventsCollected(collector2)
        assertEventsNotCollected(collector1)
      }
    }
  }

  testQuietly("ReplayListenerBus should ignore broken event jsons generated in 2_0_0") {
    // query-event-logs-version-2.0.0.txt has all types of events generated by
    // Structured Streaming in Spark 2.0.0. Because we renamed the classes,
    // SparkListenerApplicationEnd is the only valid event and it's the last event. We use it
    // to verify that we can skip broken jsons generated by Structured Streaming.
    testReplayListenerBusWithBrokenEventJsons("query-event-logs-version-2.0.0.txt", 1)
  }

  testQuietly("ReplayListenerBus should ignore broken event jsons generated in 2_0_1") {
    // query-event-logs-version-2.0.1.txt has all types of events generated by
    // Structured Streaming in Spark 2.0.1. Because we renamed the classes,
    // SparkListenerApplicationEnd is the only valid event and it's the last event. We use it
    // to verify that we can skip broken jsons generated by Structured Streaming.
    testReplayListenerBusWithBrokenEventJsons("query-event-logs-version-2.0.1.txt", 1)
  }

  testQuietly("ReplayListenerBus should ignore broken event jsons generated in 2_0_2") {
    // query-event-logs-version-2.0.2.txt has all types of events generated by
    // Structured Streaming in Spark 2.0.2. SPARK-18516 refactored Structured Streaming query events
    // in 2.1.0. This test is to verify we are able to load events generated by Spark 2.0.2.
    testReplayListenerBusWithBrokenEventJsons("query-event-logs-version-2.0.2.txt", 5)
  }

  test("listener propagates observable metrics") {
    import org.apache.spark.sql.functions._
    val clock = new StreamManualClock
    val inputData = new MemoryStream[Int](0, sqlContext)
    val df = inputData.toDF()
      .observe(
        name = "my_event",
        min($"value").as("min_val"),
        max($"value").as("max_val"),
        sum($"value").as("sum_val"),
        count(when($"value" % 2 === 0, 1)).as("num_even"),
        percentile_approx($"value", lit(0.5), lit(100)).as("percentile_approx_val"))
      .observe(
        name = "other_event",
        avg($"value").cast("int").as("avg_val"))
    val listener = new EventCollector
    def checkMetrics(f: java.util.Map[String, Row] => Unit): StreamAction = {
      AssertOnQuery { _ =>
        eventually(Timeout(streamingTimeout)) {
          assert(listener.allProgressEvents.nonEmpty)
          f(listener.allProgressEvents.last.observedMetrics)
          true
        }
      }
    }

    try {
      val noDataProgressIntervalKey = SQLConf.STREAMING_NO_DATA_PROGRESS_EVENT_INTERVAL.key
      spark.streams.addListener(listener)
      testStream(df, OutputMode.Append)(
        StartStream(
          Trigger.ProcessingTime(100),
          triggerClock = clock,
          Map(noDataProgressIntervalKey -> "100")),
        // Batch 1
        AddData(inputData, 1, 2),
        AdvanceManualClock(100),
        checkMetrics { metrics =>
          assert(metrics.get("my_event") === Row(1, 2, 3L, 1L, 1))
          assert(metrics.get("other_event") === Row(1))
        },

        // Batch 2
        AddData(inputData, 10, 30, -10, 5),
        AdvanceManualClock(100),
        checkMetrics { metrics =>
          assert(metrics.get("my_event") === Row(-10, 30, 35L, 3L, 5))
          assert(metrics.get("other_event") === Row(8))
        },

        // Batch 3 - no data
        AdvanceManualClock(100),
        checkMetrics { metrics =>
          assert(metrics.isEmpty)
        },
        StopStream
      )
    } finally {
      spark.streams.removeListener(listener)
    }
  }

  test("SPARK-31593: remove unnecessary streaming query progress update") {
    withSQLConf(SQLConf.STREAMING_NO_DATA_PROGRESS_EVENT_INTERVAL.key -> "100") {
      @volatile var numProgressEvent = 0
      val listener = new StreamingQueryListener {
        override def onQueryStarted(event: QueryStartedEvent): Unit = {}
        override def onQueryProgress(event: QueryProgressEvent): Unit = {
          numProgressEvent += 1
        }
        override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {}
      }
      spark.streams.addListener(listener)

      def checkProgressEvent(count: Int): StreamAction = {
        AssertOnQuery { _ =>
          eventually(Timeout(streamingTimeout)) {
            assert(numProgressEvent == count)
          }
          true
        }
      }

      try {
        val input = new MemoryStream[Int](0, sqlContext)
        val clock = new StreamManualClock()
        val result = input.toDF().select("value")
        testStream(result)(
          StartStream(trigger = Trigger.ProcessingTime(10), triggerClock = clock),
          AddData(input, 10),
          checkProgressEvent(1),
          AdvanceManualClock(10),
          checkProgressEvent(2),
          AdvanceManualClock(90),
          checkProgressEvent(2),
          AdvanceManualClock(10),
          checkProgressEvent(3)
        )
      } finally {
        spark.streams.removeListener(listener)
      }
    }
  }

  private def testReplayListenerBusWithBrokenEventJsons(
      fileName: String,
      expectedEventSize: Int): Unit = {
    val input = getClass.getResourceAsStream(s"/structured-streaming/$fileName")
    val events = mutable.ArrayBuffer[SparkListenerEvent]()
    try {
      val replayer = new ReplayListenerBus() {
        // Redirect all parsed events to `events`
        override def doPostEvent(
            listener: SparkListenerInterface,
            event: SparkListenerEvent): Unit = {
          events += event
        }
      }
      // Add a dummy listener so that "doPostEvent" will be called.
      replayer.addListener(new SparkListener {})
      replayer.replay(input, fileName)
      // SparkListenerApplicationEnd is the only valid event
      assert(events.size === expectedEventSize)
      assert(events.last.isInstanceOf[SparkListenerApplicationEnd])
    } finally {
      input.close()
    }
  }

  private def withListenerAdded(
      listener: StreamingQueryListener,
      session: SparkSession = spark)(body: => Unit): Unit = {
    try {
      failAfter(streamingTimeout) {
        session.streams.addListener(listener)
        body
      }
    } finally {
      session.streams.removeListener(listener)
    }
  }

  /** Collects events from the StreamingQueryListener for testing */
  class EventCollector extends StreamingQueryListener {
    // to catch errors in the async listener events
    @volatile private var asyncTestWaiter = new Waiter

    @volatile var startEvent: QueryStartedEvent = null
    @volatile var terminationEvent: QueryTerminatedEvent = null

    private val _progressEvents = new mutable.Queue[StreamingQueryProgress]

    def progressEvents: Seq[StreamingQueryProgress] = _progressEvents.synchronized {
      _progressEvents.filter(_.numInputRows > 0).toSeq
    }

    def allProgressEvents: Seq[StreamingQueryProgress] = _progressEvents.synchronized {
      _progressEvents.clone().toSeq
    }

    def reset(): Unit = {
      startEvent = null
      terminationEvent = null
      _progressEvents.clear()
      asyncTestWaiter = new Waiter
    }

    def checkAsyncErrors(): Unit = {
      asyncTestWaiter.await(timeout(streamingTimeout))
    }

    override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
      asyncTestWaiter {
        startEvent = queryStarted
      }
    }

    override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
      asyncTestWaiter {
        assert(startEvent != null, "onQueryProgress called before onQueryStarted")
        _progressEvents.synchronized { _progressEvents += queryProgress.progress }
      }
    }

    override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
      asyncTestWaiter {
        assert(startEvent != null, "onQueryTerminated called before onQueryStarted")
        terminationEvent = queryTerminated
      }
      asyncTestWaiter.dismiss()
    }
  }
}
