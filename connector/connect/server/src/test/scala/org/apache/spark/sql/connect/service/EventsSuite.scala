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

import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.Tag
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.{SparkContext, SparkFunSuite}
import org.apache.spark.SparkContext.SPARK_JOB_GROUP_ID
import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.{ExecutePlanRequest, Plan, UserContext}
import org.apache.spark.scheduler.LiveListenerBus
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connect.planner.SparkConnectPlanTest
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.internal.{SessionState, SQLConf}
import org.apache.spark.util.ManualClock

class EventsSuite extends SparkFunSuite with MockitoSugar with SparkConnectPlanTest {

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
  val DEFAULT_JOB_GROUP_ID =
    s"User_${DEFAULT_USER_ID}_Session_${DEFAULT_SESSION_ID}_Request_${DEFAULT_QUERY_ID}"

  test("post started") {
    val events = setupEvents(Some(DEFAULT_JOB_GROUP_ID))
    val planHolder = mock[ExecutePlanHolder]
    when(planHolder.operationId).thenReturn(DEFAULT_QUERY_ID)
    when(planHolder.jobGroupId).thenReturn(DEFAULT_JOB_GROUP_ID)
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

    events.postStarted(planHolder, executePlanRequest)
    verify(events.sessionHolder.session.sparkContext.listenerBus, times(1))
      .post(SparkListenerConnectOperationStarted(
        DEFAULT_JOB_GROUP_ID,
        DEFAULT_QUERY_ID,
        DEFAULT_CLOCK.getTimeMillis(),
        DEFAULT_SESSION_ID,
        DEFAULT_USER_ID,
        DEFAULT_USER_NAME,
        DEFAULT_TEXT,
        DEFAULT_CLIENT_TYPE,
        Map.empty))
  }

  gridTest("post parsed with jobGroupId and dataframe")(Seq(true, false)) { isEager =>
    val events = setupEvents(Some(DEFAULT_JOB_GROUP_ID))
    val df = setupDf(isEager, false)
    events.postParsed(Some(df))
    val expectedTimes = isEager match {
      case true =>
        0
      case false =>
        1
    }

    verify(df.queryExecution, times(expectedTimes)).executedPlan
    val event = SparkListenerConnectOperationParsed(
      DEFAULT_JOB_GROUP_ID,
      DEFAULT_QUERY_ID,
      DEFAULT_CLOCK.getTimeMillis())
    event.analyzedPlan = Some(df.queryExecution.analyzed)
    verify(events.sessionHolder.session.sparkContext.listenerBus, times(1))
      .post(event)
  }

  test("post parsed with jobGroupId and empty dataframe") {
    val events = setupEvents(Some(DEFAULT_JOB_GROUP_ID))
    events.postParsed(Option.empty[DataFrame])
    verify(events.sessionHolder.session.sparkContext.listenerBus, times(1))
      .post(
        SparkListenerConnectOperationParsed(
          DEFAULT_JOB_GROUP_ID,
          DEFAULT_QUERY_ID,
          DEFAULT_CLOCK.getTimeMillis()))
  }

  test("post canceled with jobGroupId") {
    val events = setupEvents(Some(DEFAULT_JOB_GROUP_ID))
    events.postCanceled(DEFAULT_JOB_GROUP_ID)
    verify(events.sessionHolder.session.sparkContext.listenerBus, times(1))
      .post(
        SparkListenerConnectOperationCanceled(
          DEFAULT_JOB_GROUP_ID,
          DEFAULT_QUERY_ID,
          DEFAULT_CLOCK.getTimeMillis()))
  }

  test("post failed with jobGroupId") {
    val events = setupEvents(Some(DEFAULT_JOB_GROUP_ID))
    events.postFailed(DEFAULT_ERROR)
    verify(events.sessionHolder.session.sparkContext.listenerBus, times(1))
      .post(
        SparkListenerConnectOperationFailed(
          DEFAULT_JOB_GROUP_ID,
          DEFAULT_QUERY_ID,
          DEFAULT_CLOCK.getTimeMillis(),
          DEFAULT_ERROR,
          Map.empty[String, String]))
  }

  test("post finished with jobGroupId") {
    val events = setupEvents(Some(DEFAULT_JOB_GROUP_ID))
    events.postFinished()
    verify(events.sessionHolder.session.sparkContext.listenerBus, times(1))
      .post(
        SparkListenerConnectOperationFinished(
          DEFAULT_JOB_GROUP_ID,
          DEFAULT_QUERY_ID,
          DEFAULT_CLOCK.getTimeMillis()))
  }

  test("post postParsedAndFinished with empty dataframe") {
    val events = setupEvents(Some(DEFAULT_JOB_GROUP_ID))
    events.postParsedAndFinished(Option.empty[DataFrame])
    verify(events.sessionHolder.session.sparkContext.listenerBus, times(1))
      .post(
        SparkListenerConnectOperationParsed(
          DEFAULT_JOB_GROUP_ID,
          DEFAULT_QUERY_ID,
          DEFAULT_CLOCK.getTimeMillis()))
    verify(events.sessionHolder.session.sparkContext.listenerBus, times(1))
      .post(
        SparkListenerConnectOperationFinished(
          DEFAULT_JOB_GROUP_ID,
          DEFAULT_QUERY_ID,
          DEFAULT_CLOCK.getTimeMillis()))
  }

  test("post closed with jobGroupId") {
    val events = setupEvents(Some(DEFAULT_JOB_GROUP_ID))
    events.postClosed()
    verify(events.sessionHolder.session.sparkContext.listenerBus, times(1))
      .post(
        SparkListenerConnectOperationClosed(
          DEFAULT_JOB_GROUP_ID,
          DEFAULT_QUERY_ID,
          DEFAULT_CLOCK.getTimeMillis()))
  }

  test("post session closed") {
    val events = setupEvents()
    events.postSessionClosed()
    verify(events.sessionHolder.session.sparkContext.listenerBus, times(1))
      .post(SparkListenerConnectSessionClosed(DEFAULT_SESSION_ID, DEFAULT_CLOCK.getTimeMillis()))
  }

  test("post without job group id does not post") {
    val events = setupEvents()

    events.postFailed(DEFAULT_ERROR)
    events.postParsed(Option.empty[DataFrame])
    events.postParsedAndFinished(Option.empty[DataFrame])
    events.postFinished()
    events.postClosed()
    verify(events.sessionHolder.session.sparkContext.listenerBus, never()).post(any())
  }

  protected def gridTest[A](testNamePrefix: String, testTags: Tag*)(params: Seq[A])(
      testFun: A => Unit): Unit = {
    for (param <- params) {
      test(testNamePrefix + s" ($param)", testTags: _*)(testFun(param))
    }
  }

  def setupDf(isEager: Boolean, isStreaming: Boolean): DataFrame = {
    val df = mock[DataFrame]
    val plan = mock[LogicalPlan]
    val queryExecution = mock[QueryExecution]
    val findRes = isEager match {
      case true =>
        Some(mock[LogicalPlan])
      case false =>
        Option.empty[LogicalPlan]
    }
    when(plan.find(any())).thenReturn(findRes)
    when(queryExecution.analyzed).thenReturn(plan)
    when(queryExecution.analyzed.isStreaming).thenReturn(isStreaming)
    when(df.queryExecution).thenReturn(queryExecution)
    df
  }

  def setupEvents(jobGroupId: Option[String] = None): Events = {
    val mockSession = mock[SparkSession]
    val sessionHolder = SessionHolder(DEFAULT_USER_ID, DEFAULT_SESSION_ID, mockSession)
    val mockContext = mock[SparkContext]
    val mockListenerBus = mock[LiveListenerBus]
    jobGroupId.foreach {
      when(mockContext.getLocalProperty(SPARK_JOB_GROUP_ID)).thenReturn(_)
    }
    val mockSessionState = mock[SessionState]
    val mockConf = mock[SQLConf]
    when(mockSession.sessionState).thenReturn(mockSessionState)
    when(mockSessionState.conf).thenReturn(mockConf)
    when(mockConf.stringRedactionPattern).thenReturn(Option.empty[Regex])
    when(mockContext.listenerBus).thenReturn(mockListenerBus)
    when(mockSession.sparkContext).thenReturn(mockContext)

    Events(sessionHolder, DEFAULT_CLOCK)
  }
}
