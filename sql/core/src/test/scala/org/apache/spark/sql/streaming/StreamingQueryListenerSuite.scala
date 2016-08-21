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

import java.util.concurrent.ConcurrentLinkedQueue

import org.scalatest.BeforeAndAfter
import org.scalatest.PrivateMethodTester._
import org.scalatest.concurrent.AsyncAssertions.Waiter
import org.scalatest.concurrent.Eventually._
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.SpanSugar._

import org.apache.spark.SparkException
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.util.JsonProtocol


class StreamingQueryListenerSuite extends StreamTest with BeforeAndAfter {

  import testImplicits._
  import StreamingQueryListener._

  after {
    spark.streams.active.foreach(_.stop())
    assert(spark.streams.active.isEmpty)
    assert(addedListeners.isEmpty)
    // Make sure we don't leak any events to the next test
    spark.sparkContext.listenerBus.waitUntilEmpty(10000)
  }

  test("single listener") {
    val listener = new QueryStatusCollector
    val input = MemoryStream[Int]
    withListenerAdded(listener) {
      testStream(input.toDS)(
        StartStream(),
        AssertOnQuery("Incorrect query status in onQueryStarted") { query =>
          val status = listener.startStatus
          assert(status != null)
          assert(status.name === query.name)
          assert(status.id === query.id)
          assert(status.sourceStatuses.size === 1)
          assert(status.sourceStatuses(0).description.contains("Memory"))

          // The source and sink offsets must be None as this must be called before the
          // batches have started
          assert(status.sourceStatuses(0).offsetDesc === None)
          assert(status.sinkStatus.offsetDesc === CompositeOffset(None :: Nil).toString)

          // No progress events or termination events
          assert(listener.progressStatuses.isEmpty)
          assert(listener.terminationStatus === null)
        },
        AddDataMemory(input, Seq(1, 2, 3)),
        CheckAnswer(1, 2, 3),
        AssertOnQuery("Incorrect query status in onQueryProgress") { query =>
          eventually(Timeout(streamingTimeout)) {

            // There should be only on progress event as batch has been processed
            assert(listener.progressStatuses.size === 1)
            val status = listener.progressStatuses.peek()
            assert(status != null)
            assert(status.name === query.name)
            assert(status.id === query.id)
            assert(status.sourceStatuses(0).offsetDesc === Some(LongOffset(0).toString))
            assert(status.sinkStatus.offsetDesc === CompositeOffset.fill(LongOffset(0)).toString)

            // No termination events
            assert(listener.terminationStatus === null)
          }
        },
        StopStream,
        AssertOnQuery("Incorrect query status in onQueryTerminated") { query =>
          eventually(Timeout(streamingTimeout)) {
            val status = listener.terminationStatus
            assert(status != null)
            assert(status.name === query.name)
            assert(status.id === query.id)
            assert(status.sourceStatuses(0).offsetDesc === Some(LongOffset(0).toString))
            assert(status.sinkStatus.offsetDesc === CompositeOffset.fill(LongOffset(0)).toString)
            assert(listener.terminationException === None)
          }
          listener.checkAsyncErrors()
        }
      )
    }
  }

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
    val queryStartedInfo = new StreamingQueryInfo(
      "name",
      1,
      Seq(new SourceStatus("source1", None), new SourceStatus("source2", None)),
      new SinkStatus("sink", CompositeOffset(None :: None :: Nil).toString))
    val queryStarted = new StreamingQueryListener.QueryStarted(queryStartedInfo)
    val json = JsonProtocol.sparkEventToJson(queryStarted)
    val newQueryStarted = JsonProtocol.sparkEventFromJson(json)
      .asInstanceOf[StreamingQueryListener.QueryStarted]
    assertStreamingQueryInfoEquals(queryStarted.queryInfo, newQueryStarted.queryInfo)
  }

  test("QueryProgress serialization") {
    val queryProcessInfo = new StreamingQueryInfo(
      "name",
      1,
      Seq(
        new SourceStatus("source1", Some(LongOffset(0).toString)),
        new SourceStatus("source2", Some(LongOffset(1).toString))),
      new SinkStatus("sink", new CompositeOffset(Array(None, Some(LongOffset(1)))).toString))
    val queryProcess = new StreamingQueryListener.QueryProgress(queryProcessInfo)
    val json = JsonProtocol.sparkEventToJson(queryProcess)
    val newQueryProcess = JsonProtocol.sparkEventFromJson(json)
      .asInstanceOf[StreamingQueryListener.QueryProgress]
    assertStreamingQueryInfoEquals(queryProcess.queryInfo, newQueryProcess.queryInfo)
  }

  test("QueryTerminated serialization") {
    val queryTerminatedInfo = new StreamingQueryInfo(
      "name",
      1,
      Seq(
        new SourceStatus("source1", Some(LongOffset(0).toString)),
        new SourceStatus("source2", Some(LongOffset(1).toString))),
      new SinkStatus("sink", new CompositeOffset(Array(None, Some(LongOffset(1)))).toString))
    val exception = new RuntimeException("exception")
    val queryQueryTerminated = new StreamingQueryListener.QueryTerminated(
      queryTerminatedInfo,
      Some(exception.getMessage))
    val json =
      JsonProtocol.sparkEventToJson(queryQueryTerminated)
    val newQueryTerminated = JsonProtocol.sparkEventFromJson(json)
      .asInstanceOf[StreamingQueryListener.QueryTerminated]
    assertStreamingQueryInfoEquals(queryQueryTerminated.queryInfo, newQueryTerminated.queryInfo)
    assert(queryQueryTerminated.exception === newQueryTerminated.exception)
  }

  private def assertStreamingQueryInfoEquals(
      expected: StreamingQueryInfo,
      actual: StreamingQueryInfo): Unit = {
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
      failAfter(1 minute) {
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

  class QueryStatusCollector extends StreamingQueryListener {
    // to catch errors in the async listener events
    @volatile private var asyncTestWaiter = new Waiter

    @volatile var startStatus: StreamingQueryInfo = null
    @volatile var terminationStatus: StreamingQueryInfo = null
    @volatile var terminationException: Option[String] = null

    val progressStatuses = new ConcurrentLinkedQueue[StreamingQueryInfo]

    def reset(): Unit = {
      startStatus = null
      terminationStatus = null
      progressStatuses.clear()
      asyncTestWaiter = new Waiter
    }

    def checkAsyncErrors(): Unit = {
      asyncTestWaiter.await(timeout(streamingTimeout))
    }


    override def onQueryStarted(queryStarted: QueryStarted): Unit = {
      asyncTestWaiter {
        startStatus = queryStarted.queryInfo
      }
    }

    override def onQueryProgress(queryProgress: QueryProgress): Unit = {
      asyncTestWaiter {
        assert(startStatus != null, "onQueryProgress called before onQueryStarted")
        progressStatuses.add(queryProgress.queryInfo)
      }
    }

    override def onQueryTerminated(queryTerminated: QueryTerminated): Unit = {
      asyncTestWaiter {
        assert(startStatus != null, "onQueryTerminated called before onQueryStarted")
        terminationStatus = queryTerminated.queryInfo
        terminationException = queryTerminated.exception
      }
      asyncTestWaiter.dismiss()
    }
  }
}
