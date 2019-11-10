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

package org.apache.spark.sql.hive.thriftserver

import java.util.Properties

import org.mockito.Mockito.{mock, RETURNS_SMART_NULLS}
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.internal.config.Status.ASYNC_TRACKING_ENABLED
import org.apache.spark.scheduler.SparkListenerJobStart
import org.apache.spark.sql.hive.thriftserver.ui.{HiveThriftServer2AppStatusStore, SessionInfo}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.status.ElementTrackingStore
import org.apache.spark.util.kvstore.InMemoryStore

class HiveThriftServer2ListenerSuite extends SparkFunSuite with BeforeAndAfter {

  private var kvstore: ElementTrackingStore = _

  after {
    if (kvstore != null) {
      kvstore.close()
      kvstore = null
    }
  }

  private def createProperties: Properties = {
    val properties = new Properties()
    properties.setProperty(SparkContext.SPARK_JOB_GROUP_ID, "groupId")
    properties
  }

  private def createStatusStore: HiveThriftServer2AppStatusStore = {
    val sparkConf = new SparkConf()
    sparkConf.set(ASYNC_TRACKING_ENABLED, false)
    kvstore = new ElementTrackingStore(new InMemoryStore, sparkConf)
    val server = mock(classOf[HiveThriftServer2], RETURNS_SMART_NULLS)
    val sc = mock(classOf[SparkContext])
    val sqlConf = new SQLConf
    sqlConf.setConfString("spark.sql.thriftserver.ui.retainedSessions", "1")
    val listener = new HiveThriftServer2Listener(kvstore, Some(server), Some(sqlConf), Some(sc))

    new HiveThriftServer2AppStatusStore(kvstore, Some(listener))
  }

  test("listener events should store successfully") {
    val statusStore = createStatusStore
    val listener = statusStore.listener.get

    listener.onOtherEvent(SparkListenerSessionCreated("localhost", "sessionId", "user",
      System.currentTimeMillis()))
    listener.onOtherEvent(SparkListenerStatementStart("id", "sessionId", "dummy query",
      "groupId", System.currentTimeMillis(), "user"))
    listener.onOtherEvent(SparkListenerStatementParsed("id", "dummy plan"))
    listener.onJobStart(SparkListenerJobStart(
      0,
      System.currentTimeMillis(),
      Nil,
      createProperties))
    listener.onOtherEvent(SparkListenerStatementFinish("id", System.currentTimeMillis()))
    listener.onOtherEvent(SparkListenerOperationClosed("id", System.currentTimeMillis()))

    assert(statusStore.getOnlineSessionNum == 1)

    listener.onOtherEvent(SparkListenerSessionClosed("sessionId", System.currentTimeMillis()))


    assert(statusStore.getOnlineSessionNum == 0)
    assert(statusStore.getExecutionList.size == 1)

    val storeExecData = statusStore.getExecutionList.head

    assert(storeExecData.execId == "id")
    assert(storeExecData.sessionId == "sessionId")
    assert(storeExecData.executePlan == "dummy plan")
    assert(storeExecData.jobId == Seq("0"))
    assert(listener.noLiveData())
  }

  test("cleanup session if exceeds the threshold") {
    val statusStore = createStatusStore
    val listener = statusStore.listener.get
    var time = 0
    listener.onOtherEvent(SparkListenerSessionCreated("localhost", "sessionId1", "user", time))
    time += 1
    listener.onOtherEvent(SparkListenerSessionCreated("localhost", "sessionId2", "user", time))

    assert(statusStore.getOnlineSessionNum === 2)
    assert(statusStore.getSessionCount() === 2)

    time += 1
    listener.onOtherEvent(SparkListenerSessionClosed("sessionId1", time))

    time += 1
    listener.onOtherEvent(SparkListenerSessionCreated("localhost", "sessionId3", "user", time))

    listener.onOtherEvent(SparkListenerSessionClosed("sessionId2", time))

    assert(statusStore.getOnlineSessionNum === 1)
    assert(statusStore.getSessionCount() === 1)
    assert(statusStore.getSession("sessionId1") === None)
    listener.onOtherEvent(SparkListenerSessionClosed("sessionId3", 4))
    assert(listener.noLiveData())
  }

  test("update execution info when jobstart event come after execution end event") {
    val statusStore = createStatusStore
    val listener = statusStore.listener.get
    listener.onOtherEvent(SparkListenerSessionCreated("localhost", "sessionId", "user",
      System.currentTimeMillis()))
    listener.onOtherEvent(SparkListenerStatementStart("id", "sessionId", "dummy query",
      "groupId", System.currentTimeMillis(), "user"))
    listener.onOtherEvent(SparkListenerStatementParsed("id", "dummy plan"))
    listener.onOtherEvent(SparkListenerStatementFinish("id", System.currentTimeMillis()))
    listener.onOtherEvent(SparkListenerOperationClosed("id", System.currentTimeMillis()))
    listener.onJobStart(SparkListenerJobStart(
      0,
      System.currentTimeMillis(),
      Nil,
      createProperties))
    listener.onOtherEvent(SparkListenerSessionClosed("sessionId", System.currentTimeMillis()))
    val exec = statusStore.getExecution("id")
    assert(exec.isDefined)
    assert(exec.get.jobId.toSeq === Seq("0"))
    assert(listener.noLiveData())
  }
}
