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

import scala.util.matching.Regex

import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.{SparkContext, SparkFunSuite}
import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.{ExecutePlanRequest, Plan, UserContext}
import org.apache.spark.scheduler.LiveListenerBus
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connect.planner.SparkConnectPlanTest
import org.apache.spark.sql.internal.{SessionState, SQLConf}
import org.apache.spark.util.ManualClock

class RequestEventsSuite extends SparkFunSuite with MockitoSugar with SparkConnectPlanTest {

  val DEFAULT_ERROR = "error"
  val DEFAULT_CLOCK = new ManualClock()
  val DEFAULT_NODE_NAME = "nodeName"
  val DEFAULT_TEXT = """limit {
  limit: 10
}
"""
  val DEFAULT_USER_ID = "1"
  val DEFAULT_USER_NAME = "userName"
  val DEFAULT_SESSION_ID = "2"
  val DEFAULT_QUERY_ID = "3"
  val DEFAULT_CLIENT_TYPE = "clientType"

  test("SPARK-43923: post started") {
    val events = setupEvents()

    events.postStarted()

    verify(events.planHolder.sessionHolder.session.sparkContext.listenerBus, times(1))
      .post(SparkListenerConnectOperationStarted(
        events.planHolder.jobTag,
        DEFAULT_QUERY_ID,
        DEFAULT_CLOCK.getTimeMillis(),
        DEFAULT_SESSION_ID,
        DEFAULT_USER_ID,
        DEFAULT_USER_NAME,
        DEFAULT_TEXT,
        DEFAULT_CLIENT_TYPE,
        Map.empty))
  }

  test("SPARK-43923: post analyzed with plan") {
    val events = setupEvents()
    val mockPlan = mock[LogicalPlan]
    events.postAnalyzed(Some(mockPlan))
    val event = SparkListenerConnectOperationAnalyzed(
      events.planHolder.jobTag,
      DEFAULT_QUERY_ID,
      DEFAULT_CLOCK.getTimeMillis())
    event.analyzedPlan = Some(mockPlan)
    verify(events.planHolder.sessionHolder.session.sparkContext.listenerBus, times(1))
      .post(event)
  }

  test("SPARK-43923: post analyzed with empty plan") {
    val events = setupEvents()
    events.postAnalyzed()
    verify(events.planHolder.sessionHolder.session.sparkContext.listenerBus, times(1))
      .post(
        SparkListenerConnectOperationAnalyzed(
          events.planHolder.jobTag,
          DEFAULT_QUERY_ID,
          DEFAULT_CLOCK.getTimeMillis()))
  }

  test("SPARK-43923: post readyForExecution") {
    val events = setupEvents()
    events.postReadyForExecution()
    val event = SparkListenerConnectOperationReadyForExecution(
      events.planHolder.jobTag,
      DEFAULT_QUERY_ID,
      DEFAULT_CLOCK.getTimeMillis())
    verify(events.planHolder.sessionHolder.session.sparkContext.listenerBus, times(1))
      .post(event)
  }

  test("SPARK-43923: post canceled") {
    val events = setupEvents()
    events.postCanceled()
    verify(events.planHolder.sessionHolder.session.sparkContext.listenerBus, times(1))
      .post(
        SparkListenerConnectOperationCanceled(
          events.planHolder.jobTag,
          DEFAULT_QUERY_ID,
          DEFAULT_CLOCK.getTimeMillis()))
  }

  test("SPARK-43923: post failed") {
    val events = setupEvents()
    events.postFailed(DEFAULT_ERROR)
    verify(events.planHolder.sessionHolder.session.sparkContext.listenerBus, times(1))
      .post(
        SparkListenerConnectOperationFailed(
          events.planHolder.jobTag,
          DEFAULT_QUERY_ID,
          DEFAULT_CLOCK.getTimeMillis(),
          DEFAULT_ERROR,
          Map.empty[String, String]))
  }

  test("SPARK-43923: post finished") {
    val events = setupEvents()
    events.postFinished()
    verify(events.planHolder.sessionHolder.session.sparkContext.listenerBus, times(1))
      .post(
        SparkListenerConnectOperationFinished(
          events.planHolder.jobTag,
          DEFAULT_QUERY_ID,
          DEFAULT_CLOCK.getTimeMillis()))
  }

  test("SPARK-43923: post postAnalyzedAndFinished with empty plan") {
    val events = setupEvents()
    events.postAnalyzedAndFinished()
    verify(events.planHolder.sessionHolder.session.sparkContext.listenerBus, times(1))
      .post(
        SparkListenerConnectOperationAnalyzed(
          events.planHolder.jobTag,
          DEFAULT_QUERY_ID,
          DEFAULT_CLOCK.getTimeMillis()))
    verify(events.planHolder.sessionHolder.session.sparkContext.listenerBus, times(1))
      .post(
        SparkListenerConnectOperationReadyForExecution(
          events.planHolder.jobTag,
          DEFAULT_QUERY_ID,
          DEFAULT_CLOCK.getTimeMillis()))
    verify(events.planHolder.sessionHolder.session.sparkContext.listenerBus, times(1))
      .post(
        SparkListenerConnectOperationFinished(
          events.planHolder.jobTag,
          DEFAULT_QUERY_ID,
          DEFAULT_CLOCK.getTimeMillis()))
  }

  test("SPARK-43923: post postAnalyzedAndReadyForExecution with empty plan") {
    val events = setupEvents()
    events.postAnalyzedAndReadyForExecution()
    verify(events.planHolder.sessionHolder.session.sparkContext.listenerBus, times(1))
      .post(
        SparkListenerConnectOperationAnalyzed(
          events.planHolder.jobTag,
          DEFAULT_QUERY_ID,
          DEFAULT_CLOCK.getTimeMillis()))
    verify(events.planHolder.sessionHolder.session.sparkContext.listenerBus, times(1))
      .post(
        SparkListenerConnectOperationReadyForExecution(
          events.planHolder.jobTag,
          DEFAULT_QUERY_ID,
          DEFAULT_CLOCK.getTimeMillis()))
  }

  test("SPARK-43923: post closed") {
    val events = setupEvents()
    events.postClosed()
    verify(events.planHolder.sessionHolder.session.sparkContext.listenerBus, times(1))
      .post(
        SparkListenerConnectOperationClosed(
          events.planHolder.jobTag,
          DEFAULT_QUERY_ID,
          DEFAULT_CLOCK.getTimeMillis()))
  }

  def setupEvents(): RequestEvents = {
    val mockSession = mock[SparkSession]
    val sessionHolder = SessionHolder(DEFAULT_USER_ID, DEFAULT_SESSION_ID, mockSession)
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
      .setClientType(DEFAULT_CLIENT_TYPE)
      .build()

    val planHolder = ExecutePlanHolder(DEFAULT_QUERY_ID, sessionHolder, executePlanRequest)

    RequestEvents(planHolder, DEFAULT_CLOCK)
  }
}
