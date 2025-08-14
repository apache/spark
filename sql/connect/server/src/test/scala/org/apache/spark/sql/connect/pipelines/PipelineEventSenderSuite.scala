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

import io.grpc.stub.StreamObserver
import org.mockito.{ArgumentCaptor, Mockito}
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.connect.proto.ExecutePlanResponse
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

    try {
      val testEvent = createTestEvent()
      eventSender.sendEvent(testEvent)

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

    val events = Seq(1, 2).map { i =>
      createTestEvent(
        id = s"event-$i",
        message = s"Event $i before shutdown",
        level = EventLevel.INFO,
        details = FlowProgress(FlowStatus.RUNNING))
    }

    events.foreach(event => eventSender.sendEvent(event))

    eventSender.shutdown() // blocks until all events are processed

    // Event should have been processed before shutdown completed
    val responseCaptor = ArgumentCaptor.forClass(classOf[ExecutePlanResponse])
    verify(mockObserver, times(2)).onNext(responseCaptor.capture())

    val responses = responseCaptor.getAllValues
    assert(responses.size == 2)
    assert(
      responses.get(0).getPipelineEventResult.getEvent.getMessage == "Event 1 before shutdown")
    assert(
      responses.get(1).getPipelineEventResult.getEvent.getMessage == "Event 2 before shutdown")
  }

  test("PipelineEventSender throws exception on send after shutdown") {
    val (mockObserver, mockSessionHolder) = createMockSetup()

    val eventSender = new PipelineEventSender(mockObserver, mockSessionHolder)
    eventSender.shutdown()
    intercept[IllegalStateException] {
      eventSender.sendEvent(createTestEvent())
    }
  }
}
