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

import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.{SparkContext, SparkFunSuite}
import org.apache.spark.scheduler.LiveListenerBus
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.connect.planner.SparkConnectPlanTest
import org.apache.spark.util.ManualClock

class SessionEventsManagerSuite
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
  val DEFAULT_SESSION_ID = "2"
  val DEFAULT_QUERY_ID = "3"
  val DEFAULT_CLIENT_TYPE = "clientType"

  test("SPARK-43923: post started") {
    val events = setupEvents(SessionStatus.Pending)
    events.postStarted()

    verify(events.sessionHolder.session.sparkContext.listenerBus, times(1))
      .post(
        SparkListenerConnectSessionStarted(
          DEFAULT_SESSION_ID,
          DEFAULT_USER_ID,
          DEFAULT_CLOCK.getTimeMillis(),
          Map.empty))
  }

  test("SPARK-43923: post closed") {
    val events = setupEvents(SessionStatus.Started)
    events.postClosed()

    verify(events.sessionHolder.session.sparkContext.listenerBus, times(1))
      .post(
        SparkListenerConnectSessionClosed(
          DEFAULT_SESSION_ID,
          DEFAULT_USER_ID,
          DEFAULT_CLOCK.getTimeMillis(),
          Map.empty))
  }

  test("SPARK-43923: Started wrong order throws exception") {
    val events = setupEvents(SessionStatus.Started)
    assertThrows[IllegalStateException] {
      events.postStarted()
    }
  }

  test("SPARK-43923: Closed wrong order throws exception") {
    val events = setupEvents(SessionStatus.Closed)
    assertThrows[IllegalStateException] {
      events.postStarted()
    }
    assertThrows[IllegalStateException] {
      events.postClosed()
    }
  }

  def setupEvents(status: SessionStatus): SessionEventsManager = {
    val mockSession = mock[SparkSession]
    val sessionHolder = SessionHolder(DEFAULT_USER_ID, DEFAULT_SESSION_ID, mockSession)
    val mockContext = mock[SparkContext]
    val mockListenerBus = mock[LiveListenerBus]
    when(mockContext.listenerBus).thenReturn(mockListenerBus)
    when(mockSession.sparkContext).thenReturn(mockContext)

    val eventsManager = SessionEventsManager(sessionHolder, DEFAULT_CLOCK)
    eventsManager.status_(status)
    eventsManager
  }
}
