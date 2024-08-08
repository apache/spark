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

package org.apache.spark.sql.connect.service

import java.util.UUID

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

import io.grpc.stub.StreamObserver
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.SparkFunSuite
import org.apache.spark.connect.proto.{Command, ExecutePlanResponse}
import org.apache.spark.sql.connect.SparkConnectTestUtils
import org.apache.spark.sql.connect.execution.ExecuteResponseObserver
import org.apache.spark.sql.connect.planner.SparkConnectStreamingQueryListenerHandler
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryListener}
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.sql.test.SharedSparkSession

class SparkConnectListenerBusListenerSuite
    extends SparkFunSuite
    with SharedSparkSession
    with MockitoSugar {

  override def afterEach(): Unit = {
    try {
      spark.streams.active.foreach(_.stop())
      spark.streams.listListeners().foreach(spark.streams.removeListener)
    } finally {
      super.afterEach()
    }
  }

  // A test listener that caches all events
  private class CacheEventsStreamingQueryListener(
      startEvents: ArrayBuffer[StreamingQueryListener.QueryStartedEvent],
      otherEvents: ArrayBuffer[StreamingQueryListener.Event])
      extends StreamingQueryListener {

    override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
      startEvents += event
    }

    override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
      otherEvents += event
    }

    override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
      otherEvents += event
    }

    override def onQueryIdle(event: StreamingQueryListener.QueryIdleEvent): Unit = {
      otherEvents += event
    }
  }

  private def verifyEventsSent(
      fromCachedEventsListener: ArrayBuffer[StreamingQueryListener.Event],
      fromListenerBusListener: ArrayBuffer[String]): Unit = {
    assert(fromListenerBusListener.toSet === fromCachedEventsListener.map {
      case e: StreamingQueryListener.QueryStartedEvent => e.json
      case e: StreamingQueryListener.QueryProgressEvent => e.json
      case e: StreamingQueryListener.QueryTerminatedEvent => e.json
      case e: StreamingQueryListener.QueryIdleEvent => e.json
    }.toSet)
  }

  private def startQuery(slow: Boolean = false): StreamingQuery = {
    val dsw = spark.readStream.format("rate").load().writeStream.format("noop")
    if (slow) {
      dsw.trigger(ProcessingTime("20 seconds"))
    }
    dsw.start()
  }

  Seq(1, 5, 20).foreach { queryNum =>
    test(
      "Basic functionalities - onQueryStart, onQueryProgress, onQueryTerminated" +
        s" - $queryNum queries") {
      val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
      val responseObserver = mock[StreamObserver[ExecutePlanResponse]]
      val eventJsonBuffer = ArrayBuffer.empty[String]
      val startEventsBuffer = ArrayBuffer.empty[StreamingQueryListener.QueryStartedEvent]
      val otherEventsBuffer = ArrayBuffer.empty[StreamingQueryListener.Event]

      doAnswer((invocation: InvocationOnMock) => {
        val argument = invocation.getArgument[ExecutePlanResponse](0)
        val eventJson = argument.getStreamingQueryListenerEventsResult().getEvents(0).getEventJson
        eventJsonBuffer += eventJson
      }).when(responseObserver).onNext(any[ExecutePlanResponse]())

      val listenerHolder = sessionHolder.streamingServersideListenerHolder
      listenerHolder.init(responseObserver)
      val cachedEventsListener =
        new CacheEventsStreamingQueryListener(startEventsBuffer, otherEventsBuffer)

      spark.streams.addListener(cachedEventsListener)

      for (_ <- 1 to queryNum) startQuery()

      // after all queries made some progresses
      eventually(timeout(60.seconds), interval(2.seconds)) {
        spark.streams.active.foreach { q =>
          assert(q.lastProgress.batchId > 5)
        }
      }

      // stops all queries
      spark.streams.active.foreach(_.stop())

      eventually(timeout(60.seconds), interval(500.milliseconds)) {
        assert(eventJsonBuffer.nonEmpty)
        assert(!listenerHolder.streamingQueryStartedEventCache.isEmpty)
        verifyEventsSent(otherEventsBuffer, eventJsonBuffer)
        assert(
          startEventsBuffer.map(_.json).toSet ===
            listenerHolder.streamingQueryStartedEventCache.asScala.map(_._2.json).toSet)
      }
    }
  }

  test("Basic functionalities - Slow query") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    val responseObserver = mock[StreamObserver[ExecutePlanResponse]]
    val eventJsonBuffer = ArrayBuffer.empty[String]
    val startEventsBuffer = ArrayBuffer.empty[StreamingQueryListener.QueryStartedEvent]
    val otherEventsBuffer = ArrayBuffer.empty[StreamingQueryListener.Event]

    doAnswer((invocation: InvocationOnMock) => {
      val argument = invocation.getArgument[ExecutePlanResponse](0)
      val eventJson = argument.getStreamingQueryListenerEventsResult().getEvents(0).getEventJson
      eventJsonBuffer += eventJson
    }).when(responseObserver).onNext(any[ExecutePlanResponse]())

    val listenerHolder = sessionHolder.streamingServersideListenerHolder
    listenerHolder.init(responseObserver)

    val cachedEventsListener =
      new CacheEventsStreamingQueryListener(startEventsBuffer, otherEventsBuffer)
    spark.streams.addListener(cachedEventsListener)

    // Slow query
    val q = startQuery(true)

    // after the slow query made some progresses
    eventually(timeout(100.seconds), interval(7.seconds)) {
      assert(q.lastProgress.batchId > 2)
    }

    q.stop()

    eventually(timeout(60.seconds), interval(1.second)) {
      assert(eventJsonBuffer.nonEmpty)
      assert(!listenerHolder.streamingQueryStartedEventCache.isEmpty)
      verifyEventsSent(otherEventsBuffer, eventJsonBuffer)
      assert(
        startEventsBuffer.map(_.json).toSet ===
          listenerHolder.streamingQueryStartedEventCache.asScala.map(_._2.json).toSet)
    }
  }

  test("Proper handling on onNext throw - initial response") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)

    val executeHolder = mock[ExecuteHolder]
    when(executeHolder.sessionHolder).thenReturn(sessionHolder)
    when(executeHolder.operationId).thenReturn("operationId")

    val responseObserver = mock[ExecuteResponseObserver[ExecutePlanResponse]]
    doThrow(new RuntimeException("I'm dead"))
      .when(responseObserver)
      .onNext(any[ExecutePlanResponse]())

    val listenerCntBeforeThrow = spark.streams.listListeners().size

    val handler = new SparkConnectStreamingQueryListenerHandler(executeHolder)
    val listenerBusCmdBuilder = Command.newBuilder().getStreamingQueryListenerBusCommandBuilder
    val addListenerCommand = listenerBusCmdBuilder.setAddListenerBusListener(true).build()
    handler.handleListenerCommand(addListenerCommand, responseObserver)

    val listenerHolder = sessionHolder.streamingServersideListenerHolder
    eventually(timeout(5.seconds), interval(500.milliseconds)) {
      assert(
        sessionHolder.streamingServersideListenerHolder.streamingQueryServerSideListener.isEmpty)
      assert(spark.streams.listListeners().size === listenerCntBeforeThrow)
      assert(listenerHolder.streamingQueryStartedEventCache.isEmpty)
    }

  }

  test("Proper handling on onNext throw - query progress") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    val responseObserver = mock[ExecuteResponseObserver[ExecutePlanResponse]]
    doThrow(new RuntimeException("I'm dead"))
      .when(responseObserver)
      .onNext(any[ExecutePlanResponse]())

    val listenerHolder = sessionHolder.streamingServersideListenerHolder
    listenerHolder.init(responseObserver)
    val listenerBusListener = listenerHolder.streamingQueryServerSideListener.get

    // mock a QueryStartedEvent for cleanup test
    val queryStartedEvent = new StreamingQueryListener.QueryStartedEvent(
      UUID.randomUUID,
      UUID.randomUUID,
      "name",
      "timestamp")
    listenerHolder.streamingQueryStartedEventCache.put(
      queryStartedEvent.runId.toString,
      queryStartedEvent)

    startQuery()

    eventually(timeout(5.seconds), interval(500.milliseconds)) {
      assert(!spark.streams.listListeners().contains(listenerBusListener))
      assert(listenerHolder.streamingQueryStartedEventCache.isEmpty)
    }
  }
}
