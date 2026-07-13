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

package org.apache.spark.sql.connect.ui

import java.util.Properties

import org.scalatest.BeforeAndAfter

import org.apache.spark.{SharedSparkContext, SparkConf, SparkContext, SparkEnv, SparkFunSuite}
import org.apache.spark.internal.config.Status.{ASYNC_TRACKING_ENABLED, LIVE_ENTITY_UPDATE_PERIOD}
import org.apache.spark.scheduler.SparkListenerJobStart
import org.apache.spark.sql.connect.config.Connect.{CONNECT_UI_SESSION_LIMIT, CONNECT_UI_STATEMENT_LIMIT}
import org.apache.spark.sql.connect.service._
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart
import org.apache.spark.status.ElementTrackingStore
import org.apache.spark.util.kvstore.InMemoryStore

class SparkConnectServerListenerSuite
    extends SparkFunSuite
    with BeforeAndAfter
    with SharedSparkContext {

  private var kvstore: ElementTrackingStore = _

  private val jobTag = ExecuteJobTag("userId", "sessionId", "operationId")

  after {
    if (kvstore != null) {
      kvstore.close()
      kvstore = null
    }
  }

  Seq(true, false).foreach { live =>
    test(s"listener events should store successfully (live = $live)") {
      val (statusStore: SparkConnectServerAppStatusStore, listener: SparkConnectServerListener) =
        createAppStatusStore(live)
      listener.onOtherEvent(
        SparkListenerConnectSessionStarted("sessionId", "userId", System.currentTimeMillis()))
      listener.onOtherEvent(
        SparkListenerConnectOperationStarted(
          jobTag,
          "operationId",
          System.currentTimeMillis(),
          "sessionId",
          "userId",
          "userName",
          "dummy query",
          Set()))
      listener.onOtherEvent(
        SparkListenerConnectOperationAnalyzed(jobTag, "operationId", System.currentTimeMillis()))
      listener.onOtherEvent(
        SparkListenerSQLExecutionStart(
          0,
          None,
          null,
          null,
          null,
          null,
          System.currentTimeMillis(),
          null,
          Set(jobTag)))
      listener.onJobStart(
        SparkListenerJobStart(0, System.currentTimeMillis(), Nil, createProperties))
      listener.onOtherEvent(
        SparkListenerConnectOperationFinished(jobTag, "sessionId", System.currentTimeMillis()))
      listener.onOtherEvent(
        SparkListenerConnectOperationClosed(jobTag, "sessionId", System.currentTimeMillis()))

      if (live) {
        assert(statusStore.getOnlineSessionNum === 1)
      }

      listener.onOtherEvent(
        SparkListenerConnectSessionClosed("sessionId", "userId", System.currentTimeMillis()))

      if (!live) {
        // To update history store
        kvstore.close(false)
      }
      assert(statusStore.getOnlineSessionNum === 0)
      assert(statusStore.getExecutionList.size === 1)

      val storeExecData = statusStore.getExecutionList.head

      assert(storeExecData.jobTag === jobTag)
      assert(storeExecData.sessionId === "sessionId")
      assert(storeExecData.statement === "dummy query")
      assert(storeExecData.jobId === Seq("0"))
      assert(storeExecData.sqlExecId === Set("0"))
      assert(listener.noLiveData())
    }
  }

  Seq(true, false).foreach { live =>
    test(s"cleanup session if exceeds the threshold (live = $live)") {
      val (statusStore: SparkConnectServerAppStatusStore, listener: SparkConnectServerListener) =
        createAppStatusStore(live)
      var time = 0
      listener.onOtherEvent(
        SparkListenerConnectSessionStarted("sessionId1", "userId", System.currentTimeMillis()))
      time += 1
      listener.onOtherEvent(
        SparkListenerConnectSessionStarted("sessionId2", "userId", System.currentTimeMillis()))
      time += 1
      listener.onOtherEvent(SparkListenerConnectSessionClosed("sessionId1", "userId", time))
      time += 1
      listener.onOtherEvent(SparkListenerConnectSessionClosed("sessionId2", "userId", time))
      listener.onOtherEvent(
        SparkListenerConnectSessionStarted("sessionId3", "userId", System.currentTimeMillis()))
      time += 1
      listener.onOtherEvent(SparkListenerConnectSessionClosed("sessionId3", "userId", time))

      if (!live) {
        kvstore.close(false)
      }
      assert(statusStore.getOnlineSessionNum === 0)
      assert(statusStore.getSessionCount === 1)
      assert(statusStore.getSession("userId", "sessionId1") === None)
      assert(listener.noLiveData())
    }
  }

  Seq(true, false).foreach { live =>
    test(s"SPARK-58097: sessions sharing a UUID across users stay distinct (live = $live)") {
      val (statusStore: SparkConnectServerAppStatusStore, listener: SparkConnectServerListener) =
        createAppStatusStore(live, sessionLimit = 10)
      val sessionId = "shared-session-uuid"
      // Two different users open a session with the same UUID; Spark Connect keeps them distinct.
      listener.onOtherEvent(
        SparkListenerConnectSessionStarted(sessionId, "userA", System.currentTimeMillis()))
      listener.onOtherEvent(
        SparkListenerConnectSessionStarted(sessionId, "userB", System.currentTimeMillis()))
      // Register one operation against userA's session only.
      listener.onOtherEvent(
        SparkListenerConnectOperationStarted(
          ExecuteJobTag("userA", sessionId, "operationId"),
          "operationId",
          System.currentTimeMillis(),
          sessionId,
          "userA",
          "userName",
          "dummy query",
          Set()))
      // Close userA's session; userB's must remain open (only observable in the live store).
      listener.onOtherEvent(SparkListenerConnectSessionClosed(sessionId, "userA", 1000L))
      if (live) {
        assert(statusStore.getOnlineSessionNum === 1)
        assert(statusStore.getSession("userB", sessionId).exists(_.finishTimestamp === 0))
      }
      // Close userB's session at a different time so the history store also has both records.
      listener.onOtherEvent(SparkListenerConnectSessionClosed(sessionId, "userB", 2000L))

      if (!live) {
        kvstore.close(false)
      }

      // The two sessions are stored as separate records and did not collapse.
      assert(statusStore.getSessionCount === 2)
      val sessionA = statusStore.getSession("userA", sessionId)
      val sessionB = statusStore.getSession("userB", sessionId)
      assert(sessionA.isDefined && sessionB.isDefined)
      // The operation count did not leak from userA's session into userB's.
      assert(sessionA.get.totalExecution === 1)
      assert(sessionB.get.totalExecution === 0)
      // Each session kept its own close time, i.e. closing one did not finish the other.
      assert(sessionA.get.finishTimestamp === 1000L)
      assert(sessionB.get.finishTimestamp === 2000L)
    }
  }

  test(
    "update execution info when event reordering causes job and sql" +
      " start to come after operation closed") {
    val (statusStore: SparkConnectServerAppStatusStore, listener: SparkConnectServerListener) =
      createAppStatusStore(true)
    listener.onOtherEvent(
      SparkListenerConnectSessionStarted("sessionId", "userId", System.currentTimeMillis()))
    listener.onOtherEvent(
      SparkListenerConnectOperationStarted(
        jobTag,
        "operationId",
        System.currentTimeMillis(),
        "sessionId",
        "userId",
        "userName",
        "dummy query",
        Set()))
    listener.onOtherEvent(
      SparkListenerConnectOperationAnalyzed(jobTag, "operationId", System.currentTimeMillis()))
    listener.onOtherEvent(
      SparkListenerConnectOperationFinished(jobTag, "operationId", System.currentTimeMillis()))
    listener.onOtherEvent(
      SparkListenerConnectOperationClosed(jobTag, "operationId", System.currentTimeMillis()))
    listener.onOtherEvent(
      SparkListenerSQLExecutionStart(
        0,
        None,
        null,
        null,
        null,
        null,
        System.currentTimeMillis(),
        null,
        Set(jobTag)))
    listener.onJobStart(
      SparkListenerJobStart(0, System.currentTimeMillis(), Nil, createProperties))
    listener.onOtherEvent(
      SparkListenerConnectSessionClosed("sessionId", "userId", System.currentTimeMillis()))
    val exec = statusStore.getExecution(ExecuteJobTag("userId", "sessionId", "operationId"))
    assert(exec.isDefined)
    assert(exec.get.jobId === Seq("0"))
    assert(exec.get.sqlExecId === Set("0"))
    assert(listener.noLiveData())
  }

  test("SPARK-31387 - listener update methods should not throw exception with unknown input") {
    val (statusStore: SparkConnectServerAppStatusStore, listener: SparkConnectServerListener) =
      createAppStatusStore(true)

    val unknownSession = "unknown_session"
    val unknownJob = "unknown_job_tag"
    listener.onOtherEvent(SparkListenerConnectSessionClosed(unknownSession, "userId", 0))
    listener.onOtherEvent(
      SparkListenerConnectOperationStarted(
        ExecuteJobTag("userId", "sessionId", "operationId"),
        "operationId",
        System.currentTimeMillis(),
        unknownSession,
        "userId",
        "userName",
        "dummy query",
        Set()))
    listener.onOtherEvent(
      SparkListenerConnectOperationAnalyzed(
        unknownJob,
        "operationId",
        System.currentTimeMillis()))
    listener.onOtherEvent(SparkListenerConnectOperationCanceled(unknownJob, "userId", 0))
    listener.onOtherEvent(
      SparkListenerConnectOperationFailed(unknownJob, "operationId", 0, "msg"))
    listener.onOtherEvent(SparkListenerConnectOperationFinished(unknownJob, "operationId", 0))
    listener.onOtherEvent(SparkListenerConnectOperationClosed(unknownJob, "operationId", 0))
  }

  test("SPARK-57601: listener can be created without an active SparkEnv (history server)") {
    // The History Server replays event logs without a SparkContext/SparkEnv, so
    // SparkEnv.get returns null there. The listener must read its config from the
    // SparkConf passed to its constructor rather than from SparkEnv.get.conf.
    val previousEnv = SparkEnv.get
    try {
      SparkEnv.set(null)
      val sparkConf = new SparkConf()
        .set(ASYNC_TRACKING_ENABLED, false)
        .set(LIVE_ENTITY_UPDATE_PERIOD, 0L)
        .set(CONNECT_UI_SESSION_LIMIT, 1)
        .set(CONNECT_UI_STATEMENT_LIMIT, 10)
      val store = new ElementTrackingStore(new InMemoryStore, sparkConf)
      try {
        val listener = new SparkConnectServerListener(store, sparkConf, live = false)
        assert(listener.noLiveData())
      } finally {
        store.close(false)
      }
    } finally {
      SparkEnv.set(previousEnv)
    }
  }

  private def createProperties: Properties = {
    val properties = new Properties()
    properties.setProperty(SparkContext.SPARK_JOB_TAGS, jobTag)
    properties
  }

  private def createAppStatusStore(live: Boolean, sessionLimit: Int = 1) = {
    val sparkConf = new SparkConf()
    sparkConf
      .set(ASYNC_TRACKING_ENABLED, false)
      .set(LIVE_ENTITY_UPDATE_PERIOD, 0L)
      .set(CONNECT_UI_SESSION_LIMIT, sessionLimit)
      .set(CONNECT_UI_STATEMENT_LIMIT, 10)
    kvstore = new ElementTrackingStore(new InMemoryStore, sparkConf)
    if (live) {
      val listener = new SparkConnectServerListener(kvstore, sparkConf)
      (new SparkConnectServerAppStatusStore(kvstore), listener)
    } else {
      (
        new SparkConnectServerAppStatusStore(kvstore),
        new SparkConnectServerListener(kvstore, sparkConf, false))
    }
  }
}
