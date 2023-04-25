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
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration.DurationInt

import org.mockito.Mockito.when
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.Futures.timeout
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.streaming.StreamingQueryManager
import org.apache.spark.util.ManualClock

class SparkConnectStreamingQueryCacheSuite extends SparkFunSuite with MockitoSugar {

  // Creates a manager with short durations for periodic check and expiry.
  private def createSessionManager(keepAliveFn: (String, String) => Unit) = {
    new SparkConnectStreamingQueryCache(
      keepAliveFn,
      clock = new ManualClock(),
      stoppedQueryCachePeriod = 1.minute, // This is on manual clock.
      sessionPollingPeriod = 20.milliseconds // This is real clock. Used for periodic task.
    )
  }

  test("Session cache functionality with a streaming query") {
    // Verifies common happy path for the query cache. Runs a query through its life cycle.

    val numKeepAliveCalls = new AtomicInteger(0)

    val queryId = UUID.randomUUID()
    val mockSession = mock[SparkSession]
    val mockQuery = mock[StreamingQuery]
    val mockStreamingQueryManager = mock[StreamingQueryManager]

    val sessionHolder =
      SessionHolder(userId = "test_user_1", sessionId = "test_session_1", session = mockSession)

    val sessionMgr = createSessionManager(keepAliveFn = { case (userId, sessionId) =>
      assert(userId == sessionHolder.userId)
      assert(sessionId == sessionHolder.sessionId)
      numKeepAliveCalls.incrementAndGet()
    })

    when(mockQuery.id).thenReturn(queryId)
    when(mockQuery.isActive).thenReturn(true) // Query is active.
    when(mockSession.streams).thenReturn(mockStreamingQueryManager)
    when(mockStreamingQueryManager.get(queryId.toString)).thenReturn(mockQuery)

    // Register the query.

    sessionMgr.registerNewStreamingQuery(sessionHolder, mockQuery)

    eventually(timeout(10.seconds)) {
      // Verify keep alive function is called a few times.
      assert(numKeepAliveCalls.get() >= 5)
    }

    sessionMgr.cacheQueryValue(queryId.toString) match {
      case Some(v) =>
        assert(v.sessionId == sessionHolder.sessionId)
        assert(v.expiresAtMs.isEmpty, "No expiry time should be set for active query")

      case None => assert(false, "Query should be found")
    }

    // Verify query is returned only with the correct session.
    assert(sessionMgr.findCachedQuery(queryId.toString, mock[SparkSession]).isEmpty)
    // Query is returned when correct session is used
    assert(sessionMgr.findCachedQuery(queryId.toString, mockSession).contains(mockQuery))

    // Stop the query.
    when(mockQuery.isActive).thenReturn(false)

    val expectedExpiryTimeMs = sessionMgr.clock.getTimeMillis() + 1.minute.toMillis

    // The query should have 'expiresAtMs' set now.
    eventually(timeout(10.seconds)) {
      val expiresAtOpt = sessionMgr.cacheQueryValue(queryId.toString).flatMap(_.expiresAtMs)
      assert(expiresAtOpt.contains(expectedExpiryTimeMs))
    }

    // Advance time by 1 minute so that the query is dropped from the cache.
    sessionMgr.clock.asInstanceOf[ManualClock].advance(1.minute.toMillis)
    eventually(timeout(10.seconds)) {
      assert(sessionMgr.cacheQueryValue(queryId.toString).isEmpty)
    }

    sessionMgr.shutdown()
  }
}
