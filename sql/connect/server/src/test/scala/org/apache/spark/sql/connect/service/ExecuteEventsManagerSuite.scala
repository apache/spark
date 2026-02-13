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

import scala.util.matching.Regex

import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.{SparkContext, SparkFunSuite}
import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.{ExecutePlanRequest, Plan, UserContext}
import org.apache.spark.scheduler.LiveListenerBus
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.connect.planner.SparkConnectPlanTest
import org.apache.spark.sql.internal.{SessionState, SQLConf}
import org.apache.spark.util.{JsonProtocol, ManualClock}

class ExecuteEventsManagerSuite
    extends SparkFunSuite
    with MockitoSugar
    with SparkConnectPlanTest {

  val DEFAULT_ERROR = "error"
  val DEFAULT_CLOCK = new ManualClock()
  val DEFAULT_NODE_NAME = "nodeName"
  val DEFAULT_TEXT = """limit {
  limit: 10
}
"""
  val DEFAULT_USER_ID = "1"
  val DEFAULT_USER_NAME = "userName"
  val DEFAULT_SESSION_ID = UUID.randomUUID.toString
  val DEFAULT_QUERY_ID = UUID.randomUUID.toString
  val DEFAULT_CLIENT_TYPE = "clientType"

  test("SPARK-43923: post started") {
    val events = setupEvents(ExecuteStatus.Pending)
    events.postStarted()
    val expectedEvent = SparkListenerConnectOperationStarted(
      events.executeHolder.jobTag,
      DEFAULT_QUERY_ID,
      DEFAULT_CLOCK.getTimeMillis(),
      DEFAULT_SESSION_ID,
      DEFAULT_USER_ID,
      DEFAULT_USER_NAME,
      DEFAULT_TEXT,
      Set.empty,
      Map.empty)
    expectedEvent.planRequest = Some(events.executeHolder.request)

    verify(events.executeHolder.sessionHolder.session.sparkContext.listenerBus, times(1))
      .post(expectedEvent)

    assert(
      JsonProtocol
        .sparkEventFromJson(JsonProtocol.sparkEventToJsonString(expectedEvent))
        .isInstanceOf[SparkListenerConnectOperationStarted])
  }

  test("SPARK-43923: post analyzed with plan") {
    val events = setupEvents(ExecuteStatus.Started)

    val mockPlan = mock[LogicalPlan]
    events.postAnalyzed(Some(mockPlan))
    val expectedEvent = SparkListenerConnectOperationAnalyzed(
      events.executeHolder.jobTag,
      DEFAULT_QUERY_ID,
      DEFAULT_CLOCK.getTimeMillis())
    expectedEvent.analyzedPlan = Some(mockPlan)
    verify(events.executeHolder.sessionHolder.session.sparkContext.listenerBus, times(1))
      .post(expectedEvent)

    assert(
      JsonProtocol
        .sparkEventFromJson(JsonProtocol.sparkEventToJsonString(expectedEvent))
        .isInstanceOf[SparkListenerConnectOperationAnalyzed])
  }

  test("SPARK-43923: post analyzed with empty plan") {
    val events = setupEvents(ExecuteStatus.Started)
    events.postAnalyzed()
    verify(events.executeHolder.sessionHolder.session.sparkContext.listenerBus, times(1))
      .post(
        SparkListenerConnectOperationAnalyzed(
          events.executeHolder.jobTag,
          DEFAULT_QUERY_ID,
          DEFAULT_CLOCK.getTimeMillis()))
  }

  test("SPARK-43923: post readyForExecution") {
    val events = setupEvents(ExecuteStatus.Analyzed)
    events.postReadyForExecution()
    val expectedEvent = SparkListenerConnectOperationReadyForExecution(
      events.executeHolder.jobTag,
      DEFAULT_QUERY_ID,
      DEFAULT_CLOCK.getTimeMillis())
    verify(events.executeHolder.sessionHolder.session.sparkContext.listenerBus, times(1))
      .post(expectedEvent)

    assert(
      JsonProtocol
        .sparkEventFromJson(JsonProtocol.sparkEventToJsonString(expectedEvent))
        .isInstanceOf[SparkListenerConnectOperationReadyForExecution])
  }

  test("SPARK-43923: post canceled") {
    val events = setupEvents(ExecuteStatus.Started)
    events.postCanceled()
    val expectedEvent = SparkListenerConnectOperationCanceled(
      events.executeHolder.jobTag,
      DEFAULT_QUERY_ID,
      DEFAULT_CLOCK.getTimeMillis())
    verify(events.executeHolder.sessionHolder.session.sparkContext.listenerBus, times(1))
      .post(expectedEvent)

    assert(
      JsonProtocol
        .sparkEventFromJson(JsonProtocol.sparkEventToJsonString(expectedEvent))
        .isInstanceOf[SparkListenerConnectOperationCanceled])
  }

  test("SPARK-43923: post failed") {
    val events = setupEvents(ExecuteStatus.Started)
    events.postFailed(DEFAULT_ERROR)
    val expectedEvent = SparkListenerConnectOperationFailed(
      events.executeHolder.jobTag,
      DEFAULT_QUERY_ID,
      DEFAULT_CLOCK.getTimeMillis(),
      DEFAULT_ERROR,
      Map.empty[String, String])
    verify(events.executeHolder.sessionHolder.session.sparkContext.listenerBus, times(1))
      .post(expectedEvent)

    assert(
      JsonProtocol
        .sparkEventFromJson(JsonProtocol.sparkEventToJsonString(expectedEvent))
        .isInstanceOf[SparkListenerConnectOperationFailed])
  }

  test("SPARK-43923: post finished") {
    val events = setupEvents(ExecuteStatus.Started)
    events.postFinished()
    val expectedEvent = SparkListenerConnectOperationFinished(
      events.executeHolder.jobTag,
      DEFAULT_QUERY_ID,
      DEFAULT_CLOCK.getTimeMillis())
    verify(events.executeHolder.sessionHolder.session.sparkContext.listenerBus, times(1))
      .post(expectedEvent)

    assert(
      JsonProtocol
        .sparkEventFromJson(JsonProtocol.sparkEventToJsonString(expectedEvent))
        .isInstanceOf[SparkListenerConnectOperationFinished])
  }

  test("SPARK-44776: post finished with row number") {
    val events = setupEvents(ExecuteStatus.Started)
    events.postFinished(Some(100))
    verify(events.executeHolder.sessionHolder.session.sparkContext.listenerBus, times(1))
      .post(
        SparkListenerConnectOperationFinished(
          events.executeHolder.jobTag,
          DEFAULT_QUERY_ID,
          DEFAULT_CLOCK.getTimeMillis(),
          Some(100)))
  }

  test("SPARK-43923: post finished with extra tags") {
    val events = setupEvents(ExecuteStatus.Started)
    events.postFinished(Some(100), Map("someEvent" -> "true"))
    verify(events.executeHolder.sessionHolder.session.sparkContext.listenerBus, times(1))
      .post(
        SparkListenerConnectOperationFinished(
          events.executeHolder.jobTag,
          DEFAULT_QUERY_ID,
          DEFAULT_CLOCK.getTimeMillis(),
          Some(100),
          Map("someEvent" -> "true")))
  }

  test("SPARK-43923: post closed") {
    val events = setupEvents(ExecuteStatus.Finished)
    events.postClosed()
    val expectedEvent = SparkListenerConnectOperationClosed(
      events.executeHolder.jobTag,
      DEFAULT_QUERY_ID,
      DEFAULT_CLOCK.getTimeMillis())
    verify(events.executeHolder.sessionHolder.session.sparkContext.listenerBus, times(1))
      .post(expectedEvent)

    assert(
      JsonProtocol
        .sparkEventFromJson(JsonProtocol.sparkEventToJsonString(expectedEvent))
        .isInstanceOf[SparkListenerConnectOperationClosed])
  }

  test("SPARK-43923: Closed wrong order throws exception") {
    val events = setupEvents(ExecuteStatus.Closed)
    assertThrows[IllegalStateException] {
      events.postStarted()
    }
    assertThrows[IllegalStateException] {
      events.postAnalyzed()
    }
    assertThrows[IllegalStateException] {
      events.postReadyForExecution()
    }
    assertThrows[IllegalStateException] {
      events.postFinished()
    }
    assertThrows[IllegalStateException] {
      events.postCanceled()
    }
    assertThrows[IllegalStateException] {
      events.postClosed()
    }
  }

  test("SPARK-43923: Finished wrong order throws exception") {
    val events = setupEvents(ExecuteStatus.Finished)
    assertThrows[IllegalStateException] {
      events.postStarted()
    }
    assertThrows[IllegalStateException] {
      events.postAnalyzed()
    }
    assertThrows[IllegalStateException] {
      events.postReadyForExecution()
    }
    assertThrows[IllegalStateException] {
      events.postFinished()
    }
  }

  test("SPARK-43923: Failed wrong order throws exception") {
    val events = setupEvents(ExecuteStatus.Finished)
    assertThrows[IllegalStateException] {
      events.postStarted()
    }
    assertThrows[IllegalStateException] {
      events.postAnalyzed()
    }
    assertThrows[IllegalStateException] {
      events.postReadyForExecution()
    }
    assertThrows[IllegalStateException] {
      events.postFinished()
    }
    assertThrows[IllegalStateException] {
      events.postFinished()
    }
  }

  test("SPARK-43923: Canceled wrong order throws exception") {
    val events = setupEvents(ExecuteStatus.Canceled)
    assertThrows[IllegalStateException] {
      events.postStarted()
    }
    assertThrows[IllegalStateException] {
      events.postAnalyzed()
    }
    assertThrows[IllegalStateException] {
      events.postReadyForExecution()
    }
    assertThrows[IllegalStateException] {
      events.postCanceled()
    }
    assertThrows[IllegalStateException] {
      events.postFinished()
    }
    assertThrows[IllegalStateException] {
      events.postFailed(DEFAULT_ERROR)
    }
  }

  test("SPARK-43923: ReadyForExecution wrong order throws exception") {
    val events = setupEvents(ExecuteStatus.ReadyForExecution)
    assertThrows[IllegalStateException] {
      events.postStarted()
    }
    assertThrows[IllegalStateException] {
      events.postAnalyzed()
    }
    assertThrows[IllegalStateException] {
      events.postReadyForExecution()
    }
    assertThrows[IllegalStateException] {
      events.postClosed()
    }
  }

  test("SPARK-43923: Analyzed wrong order throws exception") {
    val events = setupEvents(ExecuteStatus.Analyzed)
    assertThrows[IllegalStateException] {
      events.postStarted()
    }
    assertThrows[IllegalStateException] {
      events.postFinished()
    }
    assertThrows[IllegalStateException] {
      events.postClosed()
    }
  }

  test("SPARK-43923: Started wrong order throws exception") {
    val events = setupEvents(ExecuteStatus.Started)
    assertThrows[IllegalStateException] {
      events.postStarted()
    }
    assertThrows[IllegalStateException] {
      events.postReadyForExecution()
    }
    assertThrows[IllegalStateException] {
      events.postClosed()
    }
  }

  test("SPARK-43923: Started wrong session status") {
    val events = setupEvents(ExecuteStatus.Started, SessionStatus.Pending)
    assertThrows[IllegalStateException] {
      events.postStarted()
    }
  }

  def setupEvents(
      executeStatus: ExecuteStatus,
      sessionStatus: SessionStatus = SessionStatus.Started): ExecuteEventsManager = {
    val mockSession = mock[SparkSession]
    val sessionHolder = SessionHolder(DEFAULT_USER_ID, DEFAULT_SESSION_ID, mockSession)
    sessionHolder.eventManager.status_(sessionStatus)
    val mockContext = mock[SparkContext]
    val mockListenerBus = mock[LiveListenerBus]
    val mockSessionState = mock[SessionState]
    val mockConf = mock[SQLConf]
    when(mockSession.sessionState).thenReturn(mockSessionState)
    when(mockSessionState.conf).thenReturn(mockConf)
    when(mockConf.stringRedactionPattern).thenReturn(Option.empty[Regex])
    when(mockContext.listenerBus).thenReturn(mockListenerBus)
    when(mockSession.sparkContext).thenReturn(mockContext)

    val relation = proto.Relation.newBuilder
      .setLimit(proto.Limit.newBuilder.setLimit(10))
      .build()

    val executePlanRequest = ExecutePlanRequest
      .newBuilder()
      .setPlan(Plan.newBuilder().setRoot(relation))
      .setUserContext(
        UserContext
          .newBuilder()
          .setUserId(DEFAULT_USER_ID)
          .setUserName(DEFAULT_USER_NAME))
      .setSessionId(DEFAULT_SESSION_ID)
      .setOperationId(DEFAULT_QUERY_ID)
      .setClientType(DEFAULT_CLIENT_TYPE)
      .build()

    val executeKey = ExecuteKey(executePlanRequest, sessionHolder)
    val executeHolder = new ExecuteHolder(executeKey, executePlanRequest, sessionHolder)

    val eventsManager = ExecuteEventsManager(executeHolder, DEFAULT_CLOCK)
    eventsManager.status_(executeStatus)
    eventsManager
  }
}
