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

import org.apache.spark.sql.connect.service.SessionHolder
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

    val cache = new StreamingForeachBatchHelper.CleanerCache(SessionHolder.forTesting(spark))

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
}
