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

import io.grpc.stub.StreamObserver
import org.apache.logging.log4j.Level
import org.mockito.{ArgumentCaptor, Mockito}
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.connect.proto.ExecutePlanResponse
import org.apache.spark.sql.classic.{RuntimeConfig, SparkSession}
import org.apache.spark.sql.connect.service.{SessionHolder, SessionKey}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.pipelines.common.FlowStatus
import org.apache.spark.sql.pipelines.common.RunState.{COMPLETED, RUNNING}
import org.apache.spark.sql.pipelines.logging.{EventDetails, EventLevel, FlowProgress, PipelineEvent, PipelineEventOrigin, RunProgress}

class PipelineEventSenderSuite extends SparkDeclarativePipelinesServerTest with MockitoSugar {

  private def createMockSetup(
      queueSize: String = "1000"): (StreamObserver[ExecutePlanResponse], SessionHolder) = {
    val mockObserver = mock[StreamObserver[ExecutePlanResponse]]
    val mockSessionHolder = mock[SessionHolder]
    when(mockSessionHolder.sessionId).thenReturn("test-session-id")
    when(mockSessionHolder.userId).thenReturn("test-user-id")
    when(mockSessionHolder.serverSessionId).thenReturn("test-server-session-id")
    when(mockSessionHolder.key).thenReturn(SessionKey("test-user-id", "test-session-id"))
    val mockSession = mock[SparkSession]
    val mockConf = mock[RuntimeConfig]
    when(mockSessionHolder.session).thenReturn(mockSession)
    when(mockSession.conf).thenReturn(mockConf)
    when(mockConf.get(SQLConf.PIPELINES_EVENT_QUEUE_CAPACITY.key))
      .thenReturn(queueSize)
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

      // A healthy sender that never overflows its queue must report zero dropped events.
      assert(eventSender.numDroppedEvents == 0)
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

  test("PipelineEventSender drops events after reaching capacity") {
    // This test simulates a scenario where the event queue is full and verifies that
    // events are dropped when the queue is at capacity, except for terminal FlowProgress events and
    // RunProgress events which should always be queued.

    // Start with queue size of 1 to test capacity handling
    val (mockObserver, mockSessionHolder) = createMockSetup(queueSize = "1")

    val eventSender = new PipelineEventSender(mockObserver, mockSessionHolder) {
      override def sendEventToClient(event: PipelineEvent): Unit = {
        Thread.sleep(2000) // Simulate processing time so that we can test queue capacity
        super.sendEventToClient(event)
      }
    }
    try {
      // Send FlowProgress.RUNNING event - should be sent
      val startedEvent = createTestEvent(
        id = "startedEvent",
        message = "Flow a started",
        details = FlowProgress(FlowStatus.STARTING))
      eventSender.sendEvent(startedEvent)

      // Send FlowProgress.RUNNING event - should be queued
      val firstRunningEvent = createTestEvent(
        id = "firstRunningEvent",
        message = "Flow a running",
        details = FlowProgress(FlowStatus.RUNNING))
      eventSender.sendEvent(firstRunningEvent)

      // Send FlowProgress.RUNNING event - should be discarded due to full queue
      val secondRunningEvent = createTestEvent(
        id = "secondRunningEvent",
        message = "Flow a running",
        details = FlowProgress(FlowStatus.RUNNING))
      eventSender.sendEvent(secondRunningEvent)

      // Send RunProgress.RUNNING event - should be queued and processed
      val runProgressRunningEvent = createTestEvent(
        id = "runProgressRunning",
        message = "Update completed",
        details = RunProgress(RUNNING))
      eventSender.sendEvent(runProgressRunningEvent)

      // Send FlowProgress.RUNNING event - should be discarded due to full queue
      val thirdRunningEvent = createTestEvent(
        id = "thirdRunningEvent",
        message = "Flow a running",
        details = FlowProgress(FlowStatus.RUNNING))
      eventSender.sendEvent(thirdRunningEvent)

      // Send FlowProgress.COMPLETED event - should be queued and processed
      val completedEvent = createTestEvent(
        id = "completed",
        message = "Flow has completed",
        details = FlowProgress(FlowStatus.COMPLETED))
      eventSender.sendEvent(completedEvent)

      // Send RunProgress.COMPLETED event - should be queued and processed
      val runProgressCompletedEvent = createTestEvent(
        id = "runProgressCompletedEvent",
        message = "Update completed",
        details = RunProgress(COMPLETED))
      eventSender.sendEvent(runProgressCompletedEvent)

      // Shutdown to ensure all queued events are processed
      eventSender.shutdown()

      val responseCaptor = ArgumentCaptor.forClass(classOf[ExecutePlanResponse])
      verify(mockObserver, times(5)).onNext(responseCaptor.capture())
      val responses = responseCaptor.getAllValues
      assert(responses.size == 5)

      // FlowProgress.STARTED should be processed immediately
      assert(responses.get(0).getPipelineEventResult.getEvent.getMessage == startedEvent.message)

      // First FlowProgress.RUNNING should be queued and processed
      assert(
        responses.get(1).getPipelineEventResult.getEvent.getMessage
          == firstRunningEvent.message)

      // RunProgress.RUNNING should be queued and processed
      assert(
        responses.get(2).getPipelineEventResult.getEvent.getMessage
          == runProgressRunningEvent.message)

      // FlowProgress.COMPLETED event should also be processed because it is terminal
      assert(
        responses.get(3).getPipelineEventResult.getEvent.getMessage == completedEvent.message)

      // RunProgress.COMPLETED event should also be processed
      assert(
        responses.get(4).getPipelineEventResult.getEvent.getMessage
          == runProgressCompletedEvent.message)
    } finally {
      eventSender.shutdown()
    }
  }

  test("PipelineEventSender logs a warning when it drops events due to a full queue") {
    val (mockObserver, mockSessionHolder) = createMockSetup(queueSize = "1")

    // `workerStarted` fires once the background thread has dequeued and begun processing the
    // first event; `release` keeps that thread parked so the queue fills deterministically
    // (no timing assumptions) while we submit the events that must be dropped.
    val workerStarted = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val eventSender = new PipelineEventSender(mockObserver, mockSessionHolder) {
      override def sendEventToClient(event: PipelineEvent): Unit = {
        workerStarted.countDown()
        release.await()
        super.sendEventToClient(event)
      }
    }

    val logAppender = new LogAppender("dropped pipeline event warning")
    try {
      withLogAppender(logAppender, level = Some(Level.WARN)) {
        // Occupy the single worker thread and wait until it is actually parked.
        eventSender.sendEvent(
          createTestEvent(id = "started", details = FlowProgress(FlowStatus.STARTING)))
        assert(workerStarted.await(10, TimeUnit.SECONDS), "worker thread did not start")

        // Fill the single queue slot.
        eventSender.sendEvent(
          createTestEvent(id = "queued", details = FlowProgress(FlowStatus.RUNNING)))

        // These two non-terminal events have nowhere to go and must be dropped.
        eventSender.sendEvent(
          createTestEvent(id = "dropped-1", details = FlowProgress(FlowStatus.RUNNING)))
        eventSender.sendEvent(
          createTestEvent(id = "dropped-2", details = FlowProgress(FlowStatus.RUNNING)))

        release.countDown()
        eventSender.shutdown() // drains the started + queued events and logs the drop summary
      }

      assert(eventSender.numDroppedEvents == 2)

      val warnings = logAppender.loggingEvents
        .filter(_.getLevel == Level.WARN)
        .map(_.getMessage.getFormattedMessage)
      // The two drops are submitted back-to-back, far inside the default 60s throttle window, so
      // the per-drop warning fires exactly once (first logs, second suppressed) and the running
      // total is logged at shutdown. The assertions match on substrings so they stay robust to
      // the exact WARN wording.
      assert(warnings.count(_.contains("Dropped pipeline event for session")) == 1)
      assert(warnings.exists(_.contains("dropped a total of")))
    } finally {
      release.countDown()
      eventSender.shutdown()
    }
  }

  test("PipelineEventSender re-logs a drop warning once the throttle interval has elapsed") {
    val (mockObserver, mockSessionHolder) = createMockSetup(queueSize = "1")

    val workerStarted = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    // A negative interval makes the throttle treat every drop as past the window, so each drop
    // re-logs. This exercises the re-log branch deterministically, independent of the wall clock
    // (a zero interval would instead depend on currentTimeMillis() being monotonic across drops).
    val eventSender = new PipelineEventSender(mockObserver, mockSessionHolder) {
      override protected def droppedEventLogIntervalMs: Long = Long.MinValue
      override def sendEventToClient(event: PipelineEvent): Unit = {
        workerStarted.countDown()
        release.await()
        super.sendEventToClient(event)
      }
    }

    val logAppender = new LogAppender("throttle reset warning")
    try {
      withLogAppender(logAppender, level = Some(Level.WARN)) {
        eventSender.sendEvent(
          createTestEvent(id = "started", details = FlowProgress(FlowStatus.STARTING)))
        assert(workerStarted.await(10, TimeUnit.SECONDS), "worker thread did not start")

        eventSender.sendEvent(
          createTestEvent(id = "queued", details = FlowProgress(FlowStatus.RUNNING)))
        eventSender.sendEvent(
          createTestEvent(id = "dropped-1", details = FlowProgress(FlowStatus.RUNNING)))
        eventSender.sendEvent(
          createTestEvent(id = "dropped-2", details = FlowProgress(FlowStatus.RUNNING)))

        release.countDown()
        eventSender.shutdown()
      }

      assert(eventSender.numDroppedEvents == 2)

      val warnings = logAppender.loggingEvents
        .filter(_.getLevel == Level.WARN)
        .map(_.getMessage.getFormattedMessage)
      // With the throttle disabled, both drops re-arm the warning, so the per-drop message is
      // logged once per drop rather than being suppressed.
      assert(warnings.count(_.contains("Dropped pipeline event for session")) == 2)
    } finally {
      release.countDown()
      eventSender.shutdown()
    }
  }
}
