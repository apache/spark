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

import java.text.SimpleDateFormat
import java.util.{Date, UUID}

import org.mockito.Mockito.{mock, when, RETURNS_SMART_NULLS}
import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.catalyst.util.DateTimeUtils.getTimeZone
import org.apache.spark.sql.execution.ui.StreamingQueryStatusStore
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.streaming.{StreamingQueryListener, StreamingQueryProgress, StreamTest}
import org.apache.spark.sql.streaming
import org.apache.spark.status.{ElementTrackingStore, KVUtils}
import org.apache.spark.util.Utils
import org.apache.spark.util.kvstore.{InMemoryStore, KVStore, RocksDB}

class StreamingQueryStatusListenerSuite extends StreamTest {

  test("onQueryStarted, onQueryProgress, onQueryTerminated") {
    val kvStore = new ElementTrackingStore(new InMemoryStore(), sparkConf)
    val listener = new StreamingQueryStatusListener(spark.sparkContext.conf, kvStore)
    val queryStore = new StreamingQueryStatusStore(kvStore)

    // handle query started event
    val id = UUID.randomUUID()
    val runId = UUID.randomUUID()
    val startEvent = new StreamingQueryListener.QueryStartedEvent(
      id, runId, "test", "2016-12-05T20:54:20.827Z")
    listener.onQueryStarted(startEvent)

    // result checking
    assert(queryStore.allQueryUIData.count(_.summary.isActive) == 1)
    assert(queryStore.allQueryUIData.filter(_.summary.isActive).exists(uiData =>
      uiData.summary.runId == runId.toString && uiData.summary.name.equals("test")))

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
    val activeQuery =
      queryStore.allQueryUIData.filter(_.summary.isActive).find(_.summary.runId == runId.toString)
    assert(activeQuery.isDefined)
    assert(activeQuery.get.summary.isActive)
    assert(activeQuery.get.recentProgress.length == 1)
    assert(activeQuery.get.lastProgress.id == id)
    assert(activeQuery.get.lastProgress.runId == runId)
    assert(activeQuery.get.lastProgress.timestamp == "2001-10-01T01:00:00.100Z")
    assert(activeQuery.get.lastProgress.inputRowsPerSecond == 10.0)
    assert(activeQuery.get.lastProgress.processedRowsPerSecond == 12.0)
    assert(activeQuery.get.lastProgress.batchId == 2)
    assert(activeQuery.get.lastProgress.prettyJson == """{"a":1}""")

    // handle terminate event
    val terminateEvent = new StreamingQueryListener.QueryTerminatedEvent(id, runId, None)
    listener.onQueryTerminated(terminateEvent)

    assert(!queryStore.allQueryUIData.filterNot(_.summary.isActive).head.summary.isActive)
    assert(
      queryStore.allQueryUIData.filterNot(_.summary.isActive).head.summary.runId == runId.toString)
    assert(queryStore.allQueryUIData.filterNot(_.summary.isActive).head.summary.id == id)
  }

  test("same query start multiple times") {
    val kvStore = new ElementTrackingStore(new InMemoryStore(), sparkConf)
    val listener = new StreamingQueryStatusListener(spark.sparkContext.conf, kvStore)
    val queryStore = new StreamingQueryStatusStore(kvStore)

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
    assert(queryStore.allQueryUIData.count(_.summary.isActive) == 1)
    assert(queryStore.allQueryUIData.filterNot(_.summary.isActive).length == 1)
    assert(queryStore.allQueryUIData.filter(_.summary.isActive).exists(
      _.summary.runId == runId1.toString))
    assert(queryStore.allQueryUIData.filter(_.summary.isActive).exists(uiData =>
      uiData.summary.runId == runId1.toString && uiData.summary.id == id))
    assert(
      queryStore.allQueryUIData.filterNot(_.summary.isActive).head.summary.runId == runId0.toString)
    assert(queryStore.allQueryUIData.filterNot(_.summary.isActive).head.summary.id == id)
  }

  test("test small retained queries") {
    val kvStore = new ElementTrackingStore(new InMemoryStore(), sparkConf)
    val conf = spark.sparkContext.conf
    conf.set(StaticSQLConf.STREAMING_UI_RETAINED_QUERIES.key, "2")
    val listener = new StreamingQueryStatusListener(conf, kvStore)
    val queryStore = new StreamingQueryStatusStore(kvStore)

    def addNewQuery(): (UUID, UUID) = {
      val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") // ISO8601
      format.setTimeZone(getTimeZone("UTC"))
      val id = UUID.randomUUID()
      val runId = UUID.randomUUID()
      val startEvent = new StreamingQueryListener.QueryStartedEvent(
        id, runId, "test1", format.format(new Date(System.currentTimeMillis())))
      listener.onQueryStarted(startEvent)
      (id, runId)
    }

    def checkInactiveQueryStatus(numInactives: Int, targetInactives: Seq[UUID]): Unit = {
      eventually(timeout(10.seconds)) {
        val inactiveQueries = queryStore.allQueryUIData.filter(!_.summary.isActive)
        assert(inactiveQueries.size == numInactives)
        assert(inactiveQueries.map(_.summary.id).toSet == targetInactives.toSet)
      }
    }

    val (id1, runId1) = addNewQuery()
    val (id2, runId2) = addNewQuery()
    val (id3, runId3) = addNewQuery()
    assert(queryStore.allQueryUIData.count(!_.summary.isActive) == 0)

    val terminateEvent1 = new StreamingQueryListener.QueryTerminatedEvent(id1, runId1, None)
    listener.onQueryTerminated(terminateEvent1)
    checkInactiveQueryStatus(1, Seq(id1))
    val terminateEvent2 = new StreamingQueryListener.QueryTerminatedEvent(id2, runId2, None)
    listener.onQueryTerminated(terminateEvent2)
    checkInactiveQueryStatus(2, Seq(id1, id2))
    val terminateEvent3 = new StreamingQueryListener.QueryTerminatedEvent(id3, runId3, None)
    listener.onQueryTerminated(terminateEvent3)
    checkInactiveQueryStatus(2, Seq(id2, id3))
  }

  test("test small retained progress") {
    val kvStore = new ElementTrackingStore(new InMemoryStore(), sparkConf)
    val conf = spark.sparkContext.conf
    conf.set(StaticSQLConf.STREAMING_UI_RETAINED_PROGRESS_UPDATES.key, "5")
    val listener = new StreamingQueryStatusListener(conf, kvStore)
    val queryStore = new StreamingQueryStatusStore(kvStore)

    val id = UUID.randomUUID()
    val runId = UUID.randomUUID()
    val startEvent = new StreamingQueryListener.QueryStartedEvent(
      id, runId, "test", "2016-12-05T20:54:20.827Z")
    listener.onQueryStarted(startEvent)

    var batchId: Int = 0

    def addQueryProgress(): Unit = {
      val progress = mockProgressData(id, runId)
      val processEvent = new streaming.StreamingQueryListener.QueryProgressEvent(progress)
      listener.onQueryProgress(processEvent)
    }

    def mockProgressData(id: UUID, runId: UUID): StreamingQueryProgress = {
      val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") // ISO8601
      format.setTimeZone(getTimeZone("UTC"))

      val progress = mock(classOf[StreamingQueryProgress], RETURNS_SMART_NULLS)
      when(progress.id).thenReturn(id)
      when(progress.runId).thenReturn(runId)
      when(progress.timestamp).thenReturn(format.format(new Date(System.currentTimeMillis())))
      when(progress.inputRowsPerSecond).thenReturn(10.0)
      when(progress.processedRowsPerSecond).thenReturn(12.0)
      when(progress.batchId).thenReturn(batchId)
      when(progress.prettyJson).thenReturn("""{"a":1}""")

      batchId += 1
      progress
    }

    def checkQueryProcessData(targetNum: Int): Unit = {
      eventually(timeout(10.seconds)) {
        assert(queryStore.getQueryProgressData(runId).size == targetNum)
      }
    }

    Array.tabulate(4) { _ => addQueryProgress() }
    checkQueryProcessData(4)
    addQueryProgress()
    checkQueryProcessData(5)
    addQueryProgress()
    checkQueryProcessData(5)
  }

  test("SPARK-38056: test writing StreamingQueryData to an in-memory store") {
    testStreamingQueryData(new InMemoryStore())
  }

  test("SPARK-38056: test writing StreamingQueryData to a LevelDB store") {
    assume(!Utils.isMacOnAppleSilicon)
    val testDir = Utils.createTempDir()
    val kvStore = KVUtils.open(testDir, getClass.getName)
    try {
      testStreamingQueryData(kvStore)
    } finally {
      kvStore.close()
      Utils.deleteRecursively(testDir)
    }
  }

  test("SPARK-38056: test writing StreamingQueryData to a RocksDB store") {
    assume(!Utils.isMacOnAppleSilicon)
    val testDir = Utils.createTempDir()
    val kvStore = new RocksDB(testDir)
    try {
      testStreamingQueryData(kvStore)
    } finally {
      kvStore.close()
      Utils.deleteRecursively(testDir)
    }
  }

  private def testStreamingQueryData(kvStore: KVStore): Unit = {
    val id = UUID.randomUUID()
    val testData = new StreamingQueryData(
      "some-query",
      id,
      id.toString,
      isActive = false,
      None,
      1L,
      None
    )
    val store = new ElementTrackingStore(kvStore, sparkConf)
    store.write(testData)
    store.close(closeParent = false)
  }
}
