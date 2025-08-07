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

import scala.concurrent.duration.DurationInt

import org.mockito.Mockito.when
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.Futures.timeout
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.classic.{SparkSession, StreamingQuery, StreamingQueryManager}
import org.apache.spark.util.ManualClock

class SparkConnectStreamingQueryCacheSuite extends SparkFunSuite with MockitoSugar {

  // Creates a manager with short durations for periodic check and expiry.
  private def createSessionManager() = {
    new SparkConnectStreamingQueryCache(
      clock = new ManualClock(),
      stoppedQueryInactivityTimeout = 1.minute, // This is on manual clock.
      sessionPollingPeriod = 20.milliseconds // This is real clock. Used for periodic task.
    )
  }

  test("Session cache functionality with a streaming query") {
    // Verifies common happy path for the query cache. Runs a query through its life cycle.

    val queryId = UUID.randomUUID().toString
    val runId = UUID.randomUUID().toString
    val tag = "test_tag"
    val mockSession = mock[SparkSession]
    val mockQuery = mock[StreamingQuery]
    val mockStreamingQueryManager = mock[StreamingQueryManager]

    val sessionHolder =
      SessionHolder(userId = "test_user_1", sessionId = "test_session_1", session = mockSession)

    val sessionMgr = createSessionManager()

    val clock = sessionMgr.clock.asInstanceOf[ManualClock]

    when(mockQuery.id).thenReturn(UUID.fromString(queryId))
    when(mockQuery.runId).thenReturn(UUID.fromString(runId))
    when(mockQuery.isActive).thenReturn(true) // Query is active.
    when(mockSession.streams).thenReturn(mockStreamingQueryManager)
    when(mockStreamingQueryManager.get(queryId)).thenReturn(mockQuery)

    // Register the query.

    sessionMgr.registerNewStreamingQuery(sessionHolder, mockQuery, Set(tag), "")

    sessionMgr.getCachedValue(queryId, runId) match {
      case Some(v) =>
        assert(v.sessionId == sessionHolder.sessionId)
        assert(v.expiresAtMs.isEmpty, "No expiry time should be set for active query")

        val taggedQueries = sessionMgr.getTaggedQuery(tag, mockSession)
        assert(taggedQueries.contains(v))

      case None => assert(false, "Query should be found")
    }

    // Verify query is returned only with the correct session, not with a different session.
    assert(
      sessionMgr.getCachedQuery(queryId, runId, Set.empty[String], mock[SparkSession]).isEmpty)
    // Query is returned when correct session is used
    assert(
      sessionMgr
        .getCachedQuery(queryId, runId, Set.empty[String], mockSession)
        .map(_.query)
        .contains(mockQuery))

    // Cleanup the query and verify if stop() method has been called.
    when(mockQuery.isActive).thenReturn(false)

    val expectedExpiryTimeMs = sessionMgr.clock.getTimeMillis() + 1.minute.toMillis

    // The query should have 'expiresAtMs' set now.
    eventually(timeout(1.minute)) {
      val expiresAtOpt = sessionMgr.getCachedValue(queryId, runId).flatMap(_.expiresAtMs)
      assert(expiresAtOpt.contains(expectedExpiryTimeMs))
    }

    // Verify that expiry time gets extended when the query is accessed.
    val prevExpiryTimeMs = sessionMgr.getCachedValue(queryId, runId).get.expiresAtMs.get

    clock.advance(30.seconds.toMillis)

    // Access the query. This should advance expiry time by 30 seconds.
    assert(
      sessionMgr
        .getCachedQuery(queryId, runId, Set.empty[String], mockSession)
        .map(_.query)
        .contains(mockQuery))
    val expiresAtMs = sessionMgr.getCachedValue(queryId, runId).get.expiresAtMs.get
    assert(expiresAtMs == prevExpiryTimeMs + 30.seconds.toMillis)

    // During this time ensure that query can be restarted with a new runId.

    val restartedRunId = UUID.randomUUID().toString
    val restartedQuery = mock[StreamingQuery]
    when(restartedQuery.id).thenReturn(UUID.fromString(queryId))
    when(restartedQuery.runId).thenReturn(UUID.fromString(restartedRunId))
    when(restartedQuery.isActive).thenReturn(true)
    when(mockStreamingQueryManager.get(queryId)).thenReturn(restartedQuery)

    sessionMgr.registerNewStreamingQuery(sessionHolder, restartedQuery, Set.empty[String], "")

    // Both queries should existing in the cache.
    assert(sessionMgr.getCachedValue(queryId, runId).map(_.query).contains(mockQuery))
    assert(
      sessionMgr.getCachedValue(queryId, restartedRunId).map(_.query).contains(restartedQuery))
    eventually(timeout(1.minute)) {
      assert(sessionMgr.taggedQueries.containsKey(tag))
    }

    // Advance time by 1 minute and verify the first query is dropped from the cache.
    clock.advance(1.minute.toMillis)
    eventually(timeout(1.minute)) {
      assert(sessionMgr.getCachedValue(queryId, runId).isEmpty)
    }

    // Stop the restarted query and verify gets dropped from the cache too.
    when(restartedQuery.isActive).thenReturn(false)
    eventually(timeout(1.minute)) {
      assert(sessionMgr.getCachedValue(queryId, restartedRunId).flatMap(_.expiresAtMs).nonEmpty)
    }

    // Advance time by one more minute and restarted query should be dropped.
    clock.advance(1.minute.toMillis)
    eventually(timeout(1.minute)) {
      assert(sessionMgr.getCachedValue(queryId, restartedRunId).isEmpty)
      assert(sessionMgr.getTaggedQuery(tag, mockSession).isEmpty)
    }
    eventually(timeout(1.minute)) {
      assert(!sessionMgr.taggedQueries.containsKey(tag))
    }
    sessionMgr.shutdown()
  }
}
