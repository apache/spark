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

package org.apache.spark.sql.hive.thriftserver.ui

import java.io.File
import java.util.Properties

import org.mockito.Mockito.{mock, RETURNS_SMART_NULLS}
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.internal.config.Status.{ASYNC_TRACKING_ENABLED, LIVE_ENTITY_UPDATE_PERIOD}
import org.apache.spark.scheduler.SparkListenerJobStart
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.status.ElementTrackingStore
import org.apache.spark.util.kvstore.InMemoryStore

class HiveThriftServer2ListenerSuite extends SparkFunSuite with BeforeAndAfter {

  private var kvstore: ElementTrackingStore = _

  protected override def beforeAll(): Unit = {
    val tmpDirName = System.getProperty("java.io.tmpdir")
    val tmpDir = new File(tmpDirName)
    if (!tmpDir.exists()) {
      tmpDir.mkdirs()
    }
    super.beforeAll()
  }

  after {
    if (kvstore != null) {
      kvstore.close()
      kvstore = null
    }
  }

  Seq(true, false).foreach { live =>
    test(s"listener events should store successfully (live = $live)") {
      val (statusStore: HiveThriftServer2AppStatusStore,
      listener: HiveThriftServer2Listener) = createAppStatusStore(live)

      listener.onOtherEvent(SparkListenerThriftServerSessionCreated("localhost", "sessionId",
        "user", System.currentTimeMillis()))
      listener.onOtherEvent(SparkListenerThriftServerOperationStart("id", "sessionId",
        "dummy query", "groupId", System.currentTimeMillis(), "user"))
      listener.onOtherEvent(SparkListenerThriftServerOperationParsed("id", "dummy plan"))
      listener.onJobStart(SparkListenerJobStart(
        0,
        System.currentTimeMillis(),
        Nil,
        createProperties))
      listener.onOtherEvent(SparkListenerThriftServerOperationFinish("id",
        System.currentTimeMillis()))
      listener.onOtherEvent(SparkListenerThriftServerOperationClosed("id",
        System.currentTimeMillis()))

      if (live) {
        assert(statusStore.getOnlineSessionNum === 1)
      }

      listener.onOtherEvent(SparkListenerThriftServerSessionClosed("sessionId",
        System.currentTimeMillis()))

      if (!live) {
        // To update history store
        kvstore.close(false)
      }
      assert(statusStore.getOnlineSessionNum === 0)
      assert(statusStore.getExecutionList.size === 1)

      val storeExecData = statusStore.getExecutionList.head

      assert(storeExecData.execId === "id")
      assert(storeExecData.sessionId === "sessionId")
      assert(storeExecData.executePlan === "dummy plan")
      assert(storeExecData.jobId === Seq("0"))
      assert(listener.noLiveData())
    }
  }

  Seq(true, false).foreach { live =>
    test(s"cleanup session if exceeds the threshold (live = $live)") {
      val (statusStore: HiveThriftServer2AppStatusStore,
      listener: HiveThriftServer2Listener) = createAppStatusStore(true)
      var time = 0
      listener.onOtherEvent(SparkListenerThriftServerSessionCreated("localhost", "sessionId1",
        "user", time))
      time += 1
      listener.onOtherEvent(SparkListenerThriftServerSessionCreated("localhost", "sessionId2",
        "user", time))
      time += 1
      listener.onOtherEvent(SparkListenerThriftServerSessionClosed("sessionId1", time))
      time += 1
      listener.onOtherEvent(SparkListenerThriftServerSessionClosed("sessionId2", time))
      listener.onOtherEvent(SparkListenerThriftServerSessionCreated("localhost", "sessionId3",
        "user", time))
      time += 1
      listener.onOtherEvent(SparkListenerThriftServerSessionClosed("sessionId3", time))

      if (!live) {
        kvstore.close(false)
      }
      assert(statusStore.getOnlineSessionNum === 0)
      assert(statusStore.getSessionCount === 1)
      assert(statusStore.getSession("sessionId1") === None)
      assert(listener.noLiveData())
    }
  }

  test("update execution info when jobstart event come after execution end event") {
    val (statusStore: HiveThriftServer2AppStatusStore,
    listener: HiveThriftServer2Listener) = createAppStatusStore(true)

    listener.onOtherEvent(SparkListenerThriftServerSessionCreated("localhost", "sessionId", "user",
      System.currentTimeMillis()))
    listener.onOtherEvent(SparkListenerThriftServerOperationStart("id", "sessionId", "dummy query",
      "groupId", System.currentTimeMillis(), "user"))
    listener.onOtherEvent(SparkListenerThriftServerOperationParsed("id", "dummy plan"))
    listener.onOtherEvent(SparkListenerThriftServerOperationFinish("id",
      System.currentTimeMillis()))
    listener.onOtherEvent(SparkListenerThriftServerOperationClosed("id",
      System.currentTimeMillis()))
    listener.onJobStart(SparkListenerJobStart(
      0,
      System.currentTimeMillis(),
      Nil,
      createProperties))
    listener.onOtherEvent(SparkListenerThriftServerSessionClosed("sessionId",
      System.currentTimeMillis()))
    val exec = statusStore.getExecution("id")
    assert(exec.isDefined)
    assert(exec.get.jobId === Seq("0"))
    assert(listener.noLiveData())
  }

  test("SPARK-31387 - listener update methods should not throw exception with unknown input") {
    val (statusStore: HiveThriftServer2AppStatusStore, listener: HiveThriftServer2Listener) =
      createAppStatusStore(true)

    val unknownSession = "unknown_session"
    val unknownOperation = "unknown_operation"
    listener.onOtherEvent(SparkListenerThriftServerSessionClosed(unknownSession, 0))
    listener.onOtherEvent(SparkListenerThriftServerOperationStart("id", unknownSession,
      "stmt", "groupId", 0))
    listener.onOtherEvent(SparkListenerThriftServerOperationParsed(unknownOperation, "query"))
    listener.onOtherEvent(SparkListenerThriftServerOperationCanceled(unknownOperation, 0))
    listener.onOtherEvent(SparkListenerThriftServerOperationTimeout(unknownOperation, 0))
    listener.onOtherEvent(SparkListenerThriftServerOperationError(unknownOperation,
      "msg", "trace", 0))
    listener.onOtherEvent(SparkListenerThriftServerOperationFinish(unknownOperation, 0))
    listener.onOtherEvent(SparkListenerThriftServerOperationClosed(unknownOperation, 0))
  }

  private def createProperties: Properties = {
    val properties = new Properties()
    properties.setProperty(SparkContext.SPARK_JOB_GROUP_ID, "groupId")
    properties
  }

  private def createAppStatusStore(live: Boolean) = {
    val sparkConf = new SparkConf()
    sparkConf.set(ASYNC_TRACKING_ENABLED, false)
      .set(SQLConf.THRIFTSERVER_UI_SESSION_LIMIT, 1)
      .set(LIVE_ENTITY_UPDATE_PERIOD, 0L)
    kvstore = new ElementTrackingStore(new InMemoryStore, sparkConf)
    if (live) {
      val server = mock(classOf[HiveThriftServer2], RETURNS_SMART_NULLS)
      val listener = new HiveThriftServer2Listener(kvstore, sparkConf, Some(server))
      (new HiveThriftServer2AppStatusStore(kvstore), listener)
    } else {
      (new HiveThriftServer2AppStatusStore(kvstore),
        new HiveThriftServer2Listener(kvstore, sparkConf, None, false))
    }
  }
}
