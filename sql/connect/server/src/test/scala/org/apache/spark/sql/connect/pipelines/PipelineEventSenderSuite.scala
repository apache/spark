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

package org.apache.spark.sql.connect.pipelines

import java.sql.Timestamp
import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.collection.mutable.ArrayBuffer

import io.grpc.stub.StreamObserver
import org.mockito.{ArgumentCaptor, Mockito}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.{ExecutePlanResponse, PipelineEvent => ProtoPipelineEvent}
import org.apache.spark.sql.connect.service.SessionHolder
import org.apache.spark.sql.pipelines.common.FlowStatus
import org.apache.spark.sql.pipelines.logging.{EventDetails, EventLevel, FlowProgress, PipelineEvent, PipelineEventOrigin}

class PipelineEventSenderSuite extends SparkDeclarativePipelinesServerTest with MockitoSugar {

  private def createMockSetup(): (StreamObserver[ExecutePlanResponse], SessionHolder) = {
    val mockObserver = mock[StreamObserver[ExecutePlanResponse]]
    val mockSessionHolder = mock[SessionHolder]
    when(mockSessionHolder.sessionId).thenReturn("test-session-id")
    when(mockSessionHolder.serverSessionId).thenReturn("test-server-session-id")
    (mockObserver, mockSessionHolder)
  }

  private def createTestEvent(
      id: String = "test-event-id",
      message: String = "Test message",
      level: EventLevel = EventLevel.INFO,
      details: EventDetails = FlowProgress(FlowStatus.RUNNING)): PipelineEvent = {
    PipelineEvent(
      id = id,
      timestamp = new Timestamp(System.currentTimeMillis()),
      origin = PipelineEventOrigin(
        flowName = Some("test-flow"),
        datasetName = None,
        sourceCodeLocation = None),
      level = level,
      message = message,
      details = details,
      error = None)
  }

  test("PipelineEventSender sends events") {
    val (mockObserver, mockSessionHolder) = createMockSetup()

    val eventSender = new PipelineEventSender(mockObserver, mockSessionHolder)
    eventSender.start()

    try {
      val testEvent = createTestEvent()
      eventSender.sendEvent(testEvent)

      // Give some time for the background thread to process
      Thread.sleep(100)

      // Verify that onNext was called on the observer
      val responseCaptor = ArgumentCaptor.forClass(classOf[ExecutePlanResponse])
      verify(mockObserver, Mockito.timeout(1000)).onNext(responseCaptor.capture())

      val response = responseCaptor.getValue
      assert(response.getSessionId == "test-session-id")
      assert(response.getServerSideSessionId == "test-server-session-id")
      assert(response.hasPipelineEventResult)

      val pipelineEvent = response.getPipelineEventResult.getEvent
      assert(pipelineEvent.getMessage == "Test message")
    } finally {
      eventSender.shutdown()
    }
  }

  test("PipelineEventSender graceful shutdown waits for previously queued events to process") {
    val (mockObserver, mockSessionHolder) = createMockSetup()

    val eventSender = new PipelineEventSender(mockObserver, mockSessionHolder)
    eventSender.start()

    val events = Seq(1, 2).map { i =>
      createTestEvent(
        id = s"event-$i",
        message = s"Event $i before shutdown",
        level = EventLevel.INFO,
        details = FlowProgress(FlowStatus.RUNNING))
    }

    events.foreach(event => eventSender.sendEvent(event))

    eventSender.shutdown()

    // this event should not be processed because shutdown has been called
    eventSender.sendEvent(
      createTestEvent(
        id = s"event-after-shutdown",
        message = s"Event after shutdown",
        level = EventLevel.INFO,
        details = FlowProgress(FlowStatus.RUNNING)))
    // Allow some time for the processing to complete
    Thread.sleep(50)
    // Event should have been processed before shutdown completed
    val responseCaptor = ArgumentCaptor.forClass(classOf[ExecutePlanResponse])
    verify(mockObserver, times(2)).onNext(responseCaptor.capture())

    val responses = responseCaptor.getAllValues
    assert(responses.size == 2)
    // Only the first two events should be processed
    assert(
      responses.get(0).getPipelineEventResult.getEvent.getMessage == "Event 1 before shutdown")
    assert(
      responses.get(1).getPipelineEventResult.getEvent.getMessage == "Event 2 before shutdown")
  }

  test("pipeline execution is not blocked by slow event delivery") {
    // Create a custom PipelineEventSender that tracks shutdown timing
    class InstrumentedPipelineEventSender(
        responseObserver: StreamObserver[proto.ExecutePlanResponse],
        sessionHolder: SessionHolder)
        extends PipelineEventSender(responseObserver, sessionHolder) {

      @volatile var shutdownCalledTime: Option[Long] = None
      @volatile var shutdownReturnedTime: Option[Long] = None

      override def shutdown(): Unit = {
        shutdownCalledTime = Some(System.currentTimeMillis())
        super.shutdown()
        shutdownReturnedTime = Some(System.currentTimeMillis())
      }
    }

    val mockSessionHolder = mock[SessionHolder]
    when(mockSessionHolder.sessionId).thenReturn("test-session-id")
    when(mockSessionHolder.serverSessionId).thenReturn("test-server-session-id")

    val capturedEvents = new ArrayBuffer[ProtoPipelineEvent]()
    val eventCountDownLatch = new CountDownLatch(5)

    // Simulate a slow client by delaying onNext calls
    val slowObserver = mock[StreamObserver[proto.ExecutePlanResponse]]
    doAnswer(invocation => {
      Thread.sleep(500) // Simulate 500ms delay per event due to slow connection
      val response = invocation.getArgument[proto.ExecutePlanResponse](0)
      if (response.hasPipelineEventResult) {
        capturedEvents.addOne(response.getPipelineEventResult.getEvent)
        eventCountDownLatch.countDown()
      }
      null
    }).when(slowObserver).onNext(any[proto.ExecutePlanResponse]())

    val instrumentedSender = new InstrumentedPipelineEventSender(slowObserver, mockSessionHolder)
    instrumentedSender.start()

    // Simulate pipeline executed quickly and generated many events
    val pipelineStartTime = System.currentTimeMillis()

    (1 to 5).foreach { i =>
      val event = createTestEvent(
        id = s"event-$i",
        message = s"Event $i generated during pipeline execution",
        level = EventLevel.INFO,
        details = FlowProgress(FlowStatus.RUNNING))
      instrumentedSender.sendEvent(event)
      Thread.sleep(10) // Some pipeline execution time
    }

    // Simulates that now the pipeline core logic is done and calls shutdown
    instrumentedSender.shutdown()

    // Events still need to be procesed before the sender shuts down
    assert(
      eventCountDownLatch.await(10, TimeUnit.SECONDS),
      "Not all events delivered within timeout")

    // Key measurements
    val shutdownCalledTime = instrumentedSender.shutdownCalledTime.get
    val coreLogicTime = shutdownCalledTime - pipelineStartTime

    // Core logic should complete quickly (events generated and queued fast)
    assert(
      coreLogicTime < 500,
      s"Core pipeline logic took ${coreLogicTime}ms, but should be fast since events are " +
        s"just queued asynchronously. This suggests events may be processed synchronously.")

    // Verify we got the expected events
    assert(capturedEvents.size == 5, s"Expected 5 events, got ${capturedEvents.size}")
  }
}
