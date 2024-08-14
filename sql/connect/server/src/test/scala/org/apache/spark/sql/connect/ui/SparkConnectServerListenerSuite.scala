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
        SparkListenerConnectSessionStarted("sessionId", "user", System.currentTimeMillis()))
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
        SparkListenerConnectSessionStarted("sessionId1", "user", System.currentTimeMillis()))
      time += 1
      listener.onOtherEvent(
        SparkListenerConnectSessionStarted("sessionId2", "user", System.currentTimeMillis()))
      time += 1
      listener.onOtherEvent(SparkListenerConnectSessionClosed("sessionId1", "userId", time))
      time += 1
      listener.onOtherEvent(SparkListenerConnectSessionClosed("sessionId2", "userId", time))
      listener.onOtherEvent(
        SparkListenerConnectSessionStarted("sessionId3", "user", System.currentTimeMillis()))
      time += 1
      listener.onOtherEvent(SparkListenerConnectSessionClosed("sessionId3", "userId", time))

      if (!live) {
        kvstore.close(false)
      }
      assert(statusStore.getOnlineSessionNum === 0)
      assert(statusStore.getSessionCount === 1)
      assert(statusStore.getSession("sessionId1") === None)
      assert(listener.noLiveData())
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

  private def createProperties: Properties = {
    val properties = new Properties()
    properties.setProperty(SparkContext.SPARK_JOB_TAGS, jobTag)
    properties
  }

  private def createAppStatusStore(live: Boolean) = {
    val sparkConf = new SparkConf()
    sparkConf
      .set(ASYNC_TRACKING_ENABLED, false)
      .set(LIVE_ENTITY_UPDATE_PERIOD, 0L)
    SparkEnv.get.conf
      .set(CONNECT_UI_SESSION_LIMIT, 1)
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
