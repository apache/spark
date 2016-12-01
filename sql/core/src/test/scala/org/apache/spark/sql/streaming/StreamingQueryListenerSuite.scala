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
import org.scalatest.concurrent.AsyncAssertions.Waiter
import org.scalatest.concurrent.Eventually._
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.BeforeAndAfter
import org.scalatest.PrivateMethodTester._

import org.apache.spark.SparkException
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.streaming.StreamingQueryListener._
import org.apache.spark.util.JsonProtocol

class StreamingQueryListenerSuite extends StreamTest with BeforeAndAfter {

  import testImplicits._

  // To make === between double tolerate inexact values
  implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(0.01)

  after {
    spark.streams.active.foreach(_.stop())
    assert(spark.streams.active.isEmpty)
    assert(addedListeners.isEmpty)
    // Make sure we don't leak any events to the next test
  }

  testQuietly("single listener, check trigger events are generated correctly") {
    val clock = new StreamManualClock
    val inputData = new MemoryStream[Int](0, sqlContext)
    val df = inputData.toDS().as[Long].map { 10 / _ }
    val listener = new EventCollector
    try {
      // No events until started
      spark.streams.addListener(listener)
      assert(listener.startEvent === null)
      assert(listener.progressEvents.isEmpty)
      assert(listener.terminationEvent === null)

      testStream(df, OutputMode.Append)(

        // Start event generated when query started
        StartStream(ProcessingTime(100), triggerClock = clock),
        AssertOnQuery { query =>
          assert(listener.startEvent !== null)
          assert(listener.startEvent.id === query.id)
          assert(listener.startEvent.name === query.name)
          assert(listener.progressEvents.isEmpty)
          assert(listener.terminationEvent === null)
          true
        },

        // Progress event generated when data processed
        AddData(inputData, 1, 2),
        AdvanceManualClock(100),
        CheckAnswer(10, 5),
        AssertOnQuery { query =>
          assert(listener.progressEvents.nonEmpty)
          assert(listener.progressEvents.last.json === query.lastProgress.json)
          assert(listener.terminationEvent === null)
          true
        },

        // Termination event generated when stopped cleanly
        StopStream,
        AssertOnQuery { query =>
          eventually(Timeout(streamingTimeout)) {
            assert(listener.terminationEvent !== null)
            assert(listener.terminationEvent.id === query.id)
            assert(listener.terminationEvent.exception === None)
          }
          listener.checkAsyncErrors()
          listener.reset()
          true
        },

        // Termination event generated with exception message when stopped with error
        StartStream(ProcessingTime(100), triggerClock = clock),
        AddData(inputData, 0),
        AdvanceManualClock(100),
        ExpectFailure[SparkException],
        AssertOnQuery { query =>
          assert(listener.terminationEvent !== null)
          assert(listener.terminationEvent.id === query.id)
          assert(listener.terminationEvent.exception.nonEmpty)
          // Make sure that the exception message reported through listener
          // contains the actual exception and relevant stack trace
          assert(!listener.terminationEvent.exception.get.contains("StreamingQueryException"))
          assert(listener.terminationEvent.exception.get.contains("java.lang.ArithmeticException"))
          assert(listener.terminationEvent.exception.get.contains("StreamingQueryListenerSuite"))
          listener.checkAsyncErrors()
          true
        }
      )
    } finally {
      spark.streams.removeListener(listener)
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
      assert(isListenerActive(listener1) === true)
      assert(isListenerActive(listener2) === false)
      spark.streams.addListener(listener2)
      assert(isListenerActive(listener1) === true)
      assert(isListenerActive(listener2) === true)
      spark.streams.removeListener(listener1)
      assert(isListenerActive(listener1) === false)
      assert(isListenerActive(listener2) === true)
    } finally {
      addedListeners.foreach(spark.streams.removeListener)
    }
  }

  test("event ordering") {
    val listener = new EventCollector
    withListenerAdded(listener) {
      for (i <- 1 to 100) {
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
    val queryStarted = new StreamingQueryListener.QueryStartedEvent(UUID.randomUUID(), "name")
    val json = JsonProtocol.sparkEventToJson(queryStarted)
    val newQueryStarted = JsonProtocol.sparkEventFromJson(json)
      .asInstanceOf[StreamingQueryListener.QueryStartedEvent]
  }

  test("QueryProgressEvent serialization") {
    val event = new StreamingQueryListener.QueryProgressEvent(
      StreamingQueryStatusAndProgressSuite.testProgress)
    val json = JsonProtocol.sparkEventToJson(event)
    val newEvent = JsonProtocol.sparkEventFromJson(json)
      .asInstanceOf[StreamingQueryListener.QueryProgressEvent]
    assert(event.progress.json === newEvent.progress.json)
  }

  test("QueryTerminatedEvent serialization") {
    val exception = new RuntimeException("exception")
    val queryQueryTerminated = new StreamingQueryListener.QueryTerminatedEvent(
      UUID.randomUUID, Some(exception.getMessage))
    val json = JsonProtocol.sparkEventToJson(queryQueryTerminated)
    val newQueryTerminated = JsonProtocol.sparkEventFromJson(json)
      .asInstanceOf[StreamingQueryListener.QueryTerminatedEvent]
    assert(queryQueryTerminated.id === newQueryTerminated.id)
    assert(queryQueryTerminated.exception === newQueryTerminated.exception)
  }

  testQuietly("ReplayListenerBus should ignore broken event jsons generated in 2.0.0") {
    // query-event-logs-version-2.0.0.txt has all types of events generated by
    // Structured Streaming in Spark 2.0.0.
    // SparkListenerApplicationEnd is the only valid event and it's the last event. We use it
    // to verify that we can skip broken jsons generated by Structured Streaming.
    testReplayListenerBusWithBorkenEventJsons("query-event-logs-version-2.0.0.txt")
  }

  testQuietly("ReplayListenerBus should ignore broken event jsons generated in 2.0.1") {
    // query-event-logs-version-2.0.1.txt has all types of events generated by
    // Structured Streaming in Spark 2.0.1.
    // SparkListenerApplicationEnd is the only valid event and it's the last event. We use it
    // to verify that we can skip broken jsons generated by Structured Streaming.
    testReplayListenerBusWithBorkenEventJsons("query-event-logs-version-2.0.1.txt")
  }

  testQuietly("ReplayListenerBus should ignore broken event jsons generated in 2.0.2") {
    // query-event-logs-version-2.0.2.txt has all types of events generated by
    // Structured Streaming in Spark 2.0.2.
    // SparkListenerApplicationEnd is the only valid event and it's the last event. We use it
    // to verify that we can skip broken jsons generated by Structured Streaming.
    testReplayListenerBusWithBorkenEventJsons("query-event-logs-version-2.0.2.txt")
  }

  private def testReplayListenerBusWithBorkenEventJsons(fileName: String): Unit = {
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
      assert(events.size === 1)
      assert(events(0).isInstanceOf[SparkListenerApplicationEnd])
    } finally {
      input.close()
    }
  }

  private def withListenerAdded(listener: StreamingQueryListener)(body: => Unit): Unit = {
    try {
      failAfter(streamingTimeout) {
        spark.streams.addListener(listener)
        body
      }
    } finally {
      spark.streams.removeListener(listener)
    }
  }

  private def addedListeners(): Array[StreamingQueryListener] = {
    val listenerBusMethod =
      PrivateMethod[StreamingQueryListenerBus]('listenerBus)
    val listenerBus = spark.streams invokePrivate listenerBusMethod()
    listenerBus.listeners.toArray.map(_.asInstanceOf[StreamingQueryListener])
  }

  /** Collects events from the StreamingQueryListener for testing */
  class EventCollector extends StreamingQueryListener {
    // to catch errors in the async listener events
    @volatile private var asyncTestWaiter = new Waiter

    @volatile var startEvent: QueryStartedEvent = null
    @volatile var terminationEvent: QueryTerminatedEvent = null

    private val _progressEvents = new mutable.Queue[StreamingQueryProgress]

    def progressEvents: Seq[StreamingQueryProgress] = _progressEvents.synchronized {
      _progressEvents.filter(_.numInputRows > 0)
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
