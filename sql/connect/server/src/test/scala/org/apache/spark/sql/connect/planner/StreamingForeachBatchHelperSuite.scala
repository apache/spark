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
package org.apache.spark.sql.connect.planner

import java.util.UUID

import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.sql.connect.SparkConnectTestUtils
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.test.SharedSparkSession

class StreamingForeachBatchHelperSuite extends SharedSparkSession with MockitoSugar {

  private def mockQuery(): StreamingQuery = {
    val query = mock[StreamingQuery]
    val (queryId, runId) = (UUID.randomUUID(), UUID.randomUUID())
    when(query.id).thenReturn(queryId)
    when(query.runId).thenReturn(runId)
    query
  }

  test("CleanerCache functionality: register queries, terminate, full cleanup") {

    val cleaner1 = mock[AutoCloseable]
    val cleaner2 = mock[AutoCloseable]

    val query1 = mockQuery()
    val query2 = mockQuery()

    val cache = new StreamingForeachBatchHelper.CleanerCache(
      SparkConnectTestUtils.createDummySessionHolder(spark))

    cache.registerCleanerForQuery(query1, cleaner1)

    // Verify listener is registered.
    assert(spark.streams.listListeners().contains(cache.listenerForTesting))

    cache.registerCleanerForQuery(query2, cleaner2)

    assert(cache.listEntriesForTesting().size == 2)

    // No calls to close yet.
    verify(cleaner1, times(0)).close()

    // Terminate query1
    val terminatedEvent =
      new StreamingQueryListener.QueryTerminatedEvent(id = query1.id, runId = query1.runId, None)
    cache.listenerForTesting.onQueryTerminated(terminatedEvent)

    // This should close 'cleaner1' and remove it from the cache.
    verify(cleaner1, times(1)).close()
    assert(cache.listEntriesForTesting().size == 1)

    // Clean up remaining entries
    verify(cleaner2, times(0)).close() // cleaner2 is not closed yet.
    cache.cleanUpAll() // It should be closed now.
    verify(cleaner2, times(1)).close()

    // No more entries left in it now.
    assert(cache.listEntriesForTesting().isEmpty)
  }

  test("CleanerCache: a runner registered for a closing session is cleaned up immediately") {
    // Mirrors the SparkConnectStreamingQueryCache shutdown-race guard. A runner registered after
    // SessionHolder.close() has already run cleanUpAll() (for a query started concurrently with
    // close()) would otherwise be missed by both reapers and strand a Python worker.
    val cleaner = mock[AutoCloseable]
    val query = mockQuery()
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    val cache = new StreamingForeachBatchHelper.CleanerCache(sessionHolder)

    // Mark the session as closing before the runner is registered.
    sessionHolder.close()
    assert(sessionHolder.isClosing)

    val listenersBefore = spark.streams.listListeners().length
    cache.registerCleanerForQuery(query, cleaner)

    // The runner must not be stranded: it is closed and never cached.
    verify(cleaner, times(1)).close()
    assert(cache.listEntriesForTesting().isEmpty)
    // The fast path must not register a (leaking) listener on the closing session's streams.
    assert(spark.streams.listListeners().length == listenersBefore)
  }

  test("CleanerCache.cleanUpAll unregisters the streaming listener") {
    // close() does not remove the StreamingRunnerCleanerListener (it is not tracked in the session's
    // listenerCache), so cleanUpAll() must drop it; otherwise the listener keeps the cache / session
    // reachable after the session is closed.
    val cleaner = mock[AutoCloseable]
    val query = mockQuery()
    val cache = new StreamingForeachBatchHelper.CleanerCache(
      SparkConnectTestUtils.createDummySessionHolder(spark))

    cache.registerCleanerForQuery(query, cleaner)
    assert(spark.streams.listListeners().contains(cache.listenerForTesting))

    cache.cleanUpAll()

    verify(cleaner, times(1)).close()
    assert(!spark.streams.listListeners().contains(cache.listenerForTesting))
  }
}
