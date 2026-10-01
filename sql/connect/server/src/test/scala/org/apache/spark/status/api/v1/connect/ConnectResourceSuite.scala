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

package org.apache.spark.status.api.v1.connect

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.scalatest.PrivateMethodTester

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config.Status.{ASYNC_TRACKING_ENABLED, LIVE_ENTITY_UPDATE_PERIOD}
import org.apache.spark.sql.connect.service.ExecuteJobTag
import org.apache.spark.sql.connect.ui.{ExecutionInfo, ExecutionState, SessionInfo, SparkConnectServerAppStatusStore}
import org.apache.spark.status.ElementTrackingStore
import org.apache.spark.util.kvstore.InMemoryStore

/**
 * Unit tests for the Spark Connect REST API resource's data mapping. Mirrors SqlResourceSuite:
 * the private prepare* methods are exercised directly so no servlet context / live SparkUI is
 * required.
 */
class ConnectResourceSuite extends SparkFunSuite with PrivateMethodTester {

  private val resource = new ConnectResource()
  private val prepareSessionData = PrivateMethod[SessionData](Symbol("prepareSessionData"))
  private val prepareExecutionData = PrivateMethod[ExecutionData](Symbol("prepareExecutionData"))

  test("prepareSessionData maps SessionInfo fields") {
    val info = new SessionInfo(
      sessionId = "session-1",
      startTimestamp = 1000L,
      userId = "user-1",
      finishTimestamp = 3000L,
      totalExecution = 5L)

    val data = resource invokePrivate prepareSessionData(info)

    assert(data.sessionId === "session-1")
    assert(data.userId === "user-1")
    assert(data.startTimestamp === 1000L)
    assert(data.finishTimestamp === 3000L)
    assert(data.totalExecution === 5L)
    assert(data.totalTime === 2000L)
  }

  test("prepareExecutionData maps ExecutionInfo fields and derived durations") {
    val info = new ExecutionInfo(
      jobTag = "job-tag-1",
      statement = "SELECT 1",
      sessionId = "session-1",
      startTimestamp = 1000L,
      userId = "user-1",
      operationId = "op-1",
      sparkSessionTags = Set("tagB", "tagA"),
      finishTimestamp = 2000L,
      closeTimestamp = 3000L,
      detail = "",
      state = ExecutionState.CLOSED,
      jobId = ArrayBuffer("10", "2"),
      // A SQL execution id can exceed Int.MaxValue (it comes from a process-wide AtomicLong).
      sqlExecId = mutable.Set("4294967296", "10"))

    val data = resource invokePrivate prepareExecutionData(info)

    assert(data.jobTag === "job-tag-1")
    assert(data.operationId === "op-1")
    assert(data.sessionId === "session-1")
    assert(data.userId === "user-1")
    assert(data.statement === "SELECT 1")
    assert(data.state === "CLOSED")
    assert(data.startTimestamp === 1000L)
    assert(data.finishTimestamp === 2000L)
    assert(data.closeTimestamp === 3000L)
    assert(data.duration === 2000L) // closeTimestamp - startTimestamp
    assert(data.executionTime === 1000L) // finishTimestamp - startTimestamp
    assert(data.sparkSessionTags === Seq("tagA", "tagB"))
    // Numeric ids stored as strings must sort numerically, not lexicographically ("2" before "10").
    // SQL execution ids are Long-valued, so they must not be parsed as Int.
    assert(data.jobIds === Seq("2", "10"))
    assert(data.sqlExecIds === Seq("10", "4294967296"))
    assert(data.detail === "")
  }

  test("SPARK-57941: getExecution looks up by the unique job tag") {
    // Backs the operation-detail endpoint, and covers the store the History Server populates when
    // it replays events into a fresh KVStore. The job tag is the unique natural key; two sessions
    // may legally share an operationId UUID, so the lookup must key on the full identity.
    val conf = new SparkConf()
      .set(ASYNC_TRACKING_ENABLED, false)
      .set(LIVE_ENTITY_UPDATE_PERIOD, 0L)
    val kvstore = new ElementTrackingStore(new InMemoryStore, conf)
    try {
      val store = new SparkConnectServerAppStatusStore(kvstore)
      val userId = "tenant/alice"
      val operationId = "00112233-4455-6677-8899-aabbccddeeff"
      // Same operationId, different sessions -> distinct executions with distinct job tags.
      val jobTag1 = ExecuteJobTag(userId, "session-1", operationId)
      val jobTag2 = ExecuteJobTag(userId, "session-2", operationId)
      kvstore.write(newExecutionInfo(jobTag1, "session-1", userId, operationId))
      kvstore.write(newExecutionInfo(jobTag2, "session-2", userId, operationId))

      val found1 = store.getExecution(jobTag1)
      assert(found1.isDefined, "execution should be retrievable by its job tag")
      assert(found1.get.jobTag === jobTag1)
      assert(found1.get.sessionId === "session-1")
      assert(found1.get.jobTag.contains(userId), "job tag embeds the raw user id")
      // The shared operationId must not collapse the two rows onto each other.
      assert(store.getExecution(jobTag2).map(_.sessionId).contains("session-2"))
      assert(store.getExecution("no-such-tag").isEmpty)
    } finally {
      kvstore.close()
    }
  }

  private def newExecutionInfo(
      jobTag: String,
      sessionId: String,
      userId: String,
      operationId: String): ExecutionInfo = new ExecutionInfo(
    jobTag = jobTag,
    statement = "SELECT 1",
    sessionId = sessionId,
    startTimestamp = 1000L,
    userId = userId,
    operationId = operationId,
    sparkSessionTags = Set.empty,
    finishTimestamp = 2000L,
    closeTimestamp = 3000L,
    detail = "",
    state = ExecutionState.CLOSED,
    jobId = ArrayBuffer.empty,
    sqlExecId = mutable.Set.empty)
}
