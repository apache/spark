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

package org.apache.spark.sql.streaming.ui

import java.util.UUID

import org.mockito.Mockito.{mock, when, RETURNS_SMART_NULLS}

import org.apache.spark.sql.streaming.{StreamingQueryListener, StreamingQueryProgress, StreamTest}
import org.apache.spark.sql.streaming

class StreamingQueryStatusListenerSuite extends StreamTest {

  test("onQueryStarted, onQueryProgress, onQueryTerminated") {
    val listener = new StreamingQueryStatusListener(spark.sparkContext.conf)

    // hanlde query started event
    val id = UUID.randomUUID()
    val runId = UUID.randomUUID()
    val startEvent = new StreamingQueryListener.QueryStartedEvent(
      id, runId, "test", "2016-12-05T20:54:20.827Z")
    listener.onQueryStarted(startEvent)

    // result checking
    assert(listener.activeQueryStatus.size() == 1)
    assert(listener.activeQueryStatus.get(runId).name == "test")

    // handle query progress event
    val progress = mock(classOf[StreamingQueryProgress], RETURNS_SMART_NULLS)
    when(progress.id).thenReturn(id)
    when(progress.runId).thenReturn(runId)
    when(progress.timestamp).thenReturn("2001-10-01T01:00:00.100Z")
    when(progress.inputRowsPerSecond).thenReturn(10.0)
    when(progress.processedRowsPerSecond).thenReturn(12.0)
    when(progress.batchId).thenReturn(2)
    when(progress.prettyJson).thenReturn("""{"a":1}""")
    val processEvent = new streaming.StreamingQueryListener.QueryProgressEvent(progress)
    listener.onQueryProgress(processEvent)

    // result checking
    val activeQuery = listener.activeQueryStatus.get(runId)
    assert(activeQuery.isActive)
    assert(activeQuery.recentProgress.length == 1)
    assert(activeQuery.lastProgress.id == id)
    assert(activeQuery.lastProgress.runId == runId)
    assert(activeQuery.lastProgress.timestamp == "2001-10-01T01:00:00.100Z")
    assert(activeQuery.lastProgress.inputRowsPerSecond == 10.0)
    assert(activeQuery.lastProgress.processedRowsPerSecond == 12.0)
    assert(activeQuery.lastProgress.batchId == 2)
    assert(activeQuery.lastProgress.prettyJson == """{"a":1}""")

    // handle terminate event
    val terminateEvent = new StreamingQueryListener.QueryTerminatedEvent(id, runId, None)
    listener.onQueryTerminated(terminateEvent)

    assert(!listener.inactiveQueryStatus.head.isActive)
    assert(listener.inactiveQueryStatus.head.runId == runId)
    assert(listener.inactiveQueryStatus.head.id == id)
  }

  test("same query start multiple times") {
    val listener = new StreamingQueryStatusListener(spark.sparkContext.conf)

    // handle first time start
    val id = UUID.randomUUID()
    val runId0 = UUID.randomUUID()
    val startEvent0 = new StreamingQueryListener.QueryStartedEvent(
      id, runId0, "test", "2016-12-05T20:54:20.827Z")
    listener.onQueryStarted(startEvent0)

    // handle terminate event
    val terminateEvent0 = new StreamingQueryListener.QueryTerminatedEvent(id, runId0, None)
    listener.onQueryTerminated(terminateEvent0)

    // handle second time start
    val runId1 = UUID.randomUUID()
    val startEvent1 = new StreamingQueryListener.QueryStartedEvent(
      id, runId1, "test", "2016-12-05T20:54:20.827Z")
    listener.onQueryStarted(startEvent1)

    // result checking
    assert(listener.activeQueryStatus.size() == 1)
    assert(listener.inactiveQueryStatus.length == 1)
    assert(listener.activeQueryStatus.containsKey(runId1))
    assert(listener.activeQueryStatus.get(runId1).id == id)
    assert(listener.inactiveQueryStatus.head.runId == runId0)
    assert(listener.inactiveQueryStatus.head.id == id)
  }
}
