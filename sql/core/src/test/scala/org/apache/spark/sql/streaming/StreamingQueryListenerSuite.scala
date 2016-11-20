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

import scala.collection.mutable

import org.scalactic.TolerantNumerics
import org.scalatest.BeforeAndAfter
import org.scalatest.PrivateMethodTester._

import org.apache.spark.SparkException
import org.apache.spark.scheduler._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.functions._
import org.apache.spark.util.{JsonProtocol, ManualClock}

class StreamingQueryListenerSuite extends StreamTest with BeforeAndAfter {

  import testImplicits._
  import StreamingQueryListenerSuite._

  // To make === between double tolerate inexact values
  implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(0.01)

  after {
    spark.streams.active.foreach(_.stop())
    assert(spark.streams.active.isEmpty)
    // assert(addedListeners.isEmpty)
    // Make sure we don't leak any events to the next test
  }

  test("single listener, check trigger statuses") {
    import StreamingQueryListenerSuite._
    clock = new StreamManualClock

    /** Custom MemoryStream that waits for manual clock to reach a time */
    val inputData = new MemoryStream[Int](0, sqlContext) {
      // Wait for manual clock to be 100 first time there is data
      override def getOffset: Option[Offset] = {
        val offset = super.getOffset
        if (offset.nonEmpty) {
          clock.waitTillTime(100)
        }
        offset
      }

      // Wait for manual clock to be 300 first time there is data
      override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
        clock.waitTillTime(300)
        super.getBatch(start, end)
      }
    }

    // This is to make sure thatquery waits for manual clock to be 600 first time there is data
    val mapped = inputData.toDS().agg(count("*")).as[Long].coalesce(1).map { x =>
      clock.waitTillTime(600)
      x
    }

    testStream(mapped, OutputMode.Complete)(
      StartStream(triggerClock = clock),
      AssertOnQuery(_.status.isDataAvailable == false),
      AddData(inputData, 1, 2),
      AdvanceManualClock(100),  // unblock getOffset, will block on getBatch
      AdvanceManualClock(200),  // unblock getBatch, will block on computation
      AdvanceManualClock(300),  // unblock computation
      AssertOnQuery { _ => clock.getTimeMillis() === 600 },
      CheckAnswer(2),
      AssertOnQuery { query =>
        val activeProgress = query.recentProgress
              .find(_.numRecords > 0)
              .getOrElse(sys.error("Could not find any records with progress"))

        assert(activeProgress.id === query.id)
        assert(activeProgress.name === query.name)
        assert(activeProgress.batchId == 0)

        assert(activeProgress.durationMs.get("getOffset") === 100)
        assert(activeProgress.durationMs.get("getBatch") === 200)
        assert(activeProgress.durationMs.get("queryPlanning") === 0)
        assert(activeProgress.durationMs.get("walCommit") === 0)
        assert(activeProgress.durationMs.get("triggerExecution") === 600)

        assert(activeProgress.numRecords === 2)
        assert(activeProgress.sources.length === 1)
        assert(activeProgress.sources(0).description contains "MemoryStream")
        assert(activeProgress.sources(0).startOffset == null)
        assert(activeProgress.sources(0).endOffset != null)
        assert(activeProgress.sources(0).processedRecordsPerSecond == 3.3333333333333335)

        assert(activeProgress.stateOperators.length === 1)
        assert(activeProgress.stateOperators(0).numUpdated === 1)
        assert(activeProgress.stateOperators(0).numEntries === 1)

        clock.advance(100)
        true
      },
      AddData(inputData, 1, 2),
      CheckAnswer(4),
      AssertOnQuery { query =>
        val activeProgress = query.recentProgress
            .reverse
            .find(_.numRecords > 0)
            .getOrElse(sys.error("Could not find any records with progress"))
        assert(activeProgress.sources(0).inputRecordsPerSecond == 20)
        true
      }
    )
  }
/*
  test("adding and removing listener") {
    def isListenerActive(listener: QueryStatusCollector): Boolean = {
      listener.reset()
      testStream(MemoryStream[Int].toDS)(
        StartStream(),
        StopStream
      )
      listener.startStatus != null
    }

    try {
      val listener1 = new QueryStatusCollector
      val listener2 = new QueryStatusCollector

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
    val listener = new QueryStatusCollector
    withListenerAdded(listener) {
      for (i <- 1 to 100) {
        listener.reset()
        require(listener.startStatus === null)
        testStream(MemoryStream[Int].toDS)(
          StartStream(),
          Assert(listener.startStatus !== null, "onQueryStarted not called before query returned"),
          StopStream,
          Assert { listener.checkAsyncErrors() }
        )
      }
    }
  }

  testQuietly("exception should be reported in QueryTerminated") {
    val listener = new QueryStatusCollector
    withListenerAdded(listener) {
      val input = MemoryStream[Int]
      testStream(input.toDS.map(_ / 0))(
        StartStream(),
        AddData(input, 1),
        ExpectFailure[SparkException](),
        Assert {
          spark.sparkContext.listenerBus.waitUntilEmpty(10000)
          assert(listener.terminationStatus !== null)
          assert(listener.terminationException.isDefined)
          // Make sure that the exception message reported through listener
          // contains the actual exception and relevant stack trace
          assert(!listener.terminationException.get.contains("StreamingQueryException"))
          assert(listener.terminationException.get.contains("java.lang.ArithmeticException"))
          assert(listener.terminationException.get.contains("StreamingQueryListenerSuite"))
        }
      )
    }
  }

  test("QueryStarted serialization") {
    val queryStarted = new StreamingQueryListener.QueryStartedEvent(StreamingQueryStatus.testStatus)
    val json = JsonProtocol.sparkEventToJson(queryStarted)
    val newQueryStarted = JsonProtocol.sparkEventFromJson(json)
      .asInstanceOf[StreamingQueryListener.QueryStartedEvent]
    assertStreamingQueryInfoEquals(queryStarted.queryStatus, newQueryStarted.queryStatus)
  }

  test("QueryProgress serialization") {
    val queryProcess = new StreamingQueryListener.QueryProgressEvent(
      StreamingQueryStatus.testStatus)
    val json = JsonProtocol.sparkEventToJson(queryProcess)
    val newQueryProcess = JsonProtocol.sparkEventFromJson(json)
      .asInstanceOf[StreamingQueryListener.QueryProgressEvent]
    assertStreamingQueryInfoEquals(queryProcess.queryStatus, newQueryProcess.queryStatus)
  }

  test("QueryTerminated serialization") {
    val exception = new RuntimeException("exception")
    val queryQueryTerminated = new StreamingQueryListener.QueryTerminatedEvent(
      StreamingQueryStatus.testStatus,
      Some(exception.getMessage))
    val json =
      JsonProtocol.sparkEventToJson(queryQueryTerminated)
    val newQueryTerminated = JsonProtocol.sparkEventFromJson(json)
      .asInstanceOf[StreamingQueryListener.QueryTerminatedEvent]
    assertStreamingQueryInfoEquals(queryQueryTerminated.queryStatus, newQueryTerminated.queryStatus)
    assert(queryQueryTerminated.exception === newQueryTerminated.exception)
  }

  test("ReplayListenerBus should ignore broken event jsons generated in 2.0.0") {
    // query-event-logs-version-2.0.0.txt has all types of events generated by
    // Structured Streaming in Spark 2.0.0.
    // SparkListenerApplicationEnd is the only valid event and it's the last event. We use it
    // to verify that we can skip broken jsons generated by Structured Streaming.
    testReplayListenerBusWithBorkenEventJsons("query-event-logs-version-2.0.0.txt")
  }

  test("ReplayListenerBus should ignore broken event jsons generated in 2.0.1") {
    // query-event-logs-version-2.0.1.txt has all types of events generated by
    // Structured Streaming in Spark 2.0.1.
    // SparkListenerApplicationEnd is the only valid event and it's the last event. We use it
    // to verify that we can skip broken jsons generated by Structured Streaming.
    testReplayListenerBusWithBorkenEventJsons("query-event-logs-version-2.0.1.txt")
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

  private def assertStreamingQueryInfoEquals(
      expected: StreamingQueryStatus,
      actual: StreamingQueryStatus): Unit = {
    assert(expected.name === actual.name)
    assert(expected.sourceStatuses.size === actual.sourceStatuses.size)
    expected.sourceStatuses.zip(actual.sourceStatuses).foreach {
      case (expectedSource, actualSource) =>
        assertSourceStatus(expectedSource, actualSource)
    }
    assertSinkStatus(expected.sinkStatus, actual.sinkStatus)
  }

  private def assertSourceStatus(expected: SourceStatus, actual: SourceStatus): Unit = {
    assert(expected.description === actual.description)
    assert(expected.offsetDesc === actual.offsetDesc)
  }

  private def assertSinkStatus(expected: SinkStatus, actual: SinkStatus): Unit = {
    assert(expected.description === actual.description)
    assert(expected.offsetDesc === actual.offsetDesc)
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
  */
}

object StreamingQueryListenerSuite {
  // Singleton reference to clock that does not get serialized in task closures
  @volatile var clock: ManualClock = null
}
