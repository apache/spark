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

import org.scalatest.BeforeAndAfterEach
import org.scalatest.time.SpanSugar._

import org.apache.spark.SparkSQLException
import org.apache.spark.sql.test.SharedSparkSession

class SparkConnectSessionManagerSuite extends SharedSparkSession with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    super.beforeEach()
    SparkConnectService.sessionManager.invalidateAllSessions()
  }

  test("sessionId needs to be an UUID") {
    val key = SessionKey("user", "not an uuid")
    val exGetOrCreate = intercept[SparkSQLException] {
      SparkConnectService.sessionManager.getOrCreateIsolatedSession(key, None)
    }
    assert(exGetOrCreate.getCondition == "INVALID_HANDLE.FORMAT")
  }

  test(
    "getOrCreateIsolatedSession/getIsolatedSession/getIsolatedSessionIfPresent " +
      "gets the existing session") {
    val key = SessionKey("user", UUID.randomUUID().toString)
    val sessionHolder = SparkConnectService.sessionManager.getOrCreateIsolatedSession(key, None)

    val sessionGetOrCreate =
      SparkConnectService.sessionManager.getOrCreateIsolatedSession(key, None)
    assert(sessionGetOrCreate === sessionHolder)

    val sessionGet = SparkConnectService.sessionManager.getIsolatedSession(key, None)
    assert(sessionGet === sessionHolder)

    val sessionGetIfPresent = SparkConnectService.sessionManager.getIsolatedSessionIfPresent(key)
    assert(sessionGetIfPresent.get === sessionHolder)
  }

  test("client-observed session id validation works") {
    val key = SessionKey("user", UUID.randomUUID().toString)
    val sessionHolder = SparkConnectService.sessionManager.getOrCreateIsolatedSession(key, None)
    // Works if the client doesn't set the observed session id.
    SparkConnectService.sessionManager.getOrCreateIsolatedSession(key, None)
    // Works with the correct existing session id.
    SparkConnectService.sessionManager.getOrCreateIsolatedSession(
      key,
      Some(sessionHolder.session.sessionUUID))
    // Fails with the different session id.
    val exGet = intercept[SparkSQLException] {
      SparkConnectService.sessionManager.getOrCreateIsolatedSession(
        key,
        Some(sessionHolder.session.sessionUUID + "invalid"))
    }
    assert(exGet.getCondition == "INVALID_HANDLE.SESSION_CHANGED")
  }

  test(
    "getOrCreateIsolatedSession/getIsolatedSession/getIsolatedSessionIfPresent " +
      "doesn't recreate closed session") {
    val key = SessionKey("user", UUID.randomUUID().toString)
    val sessionHolder = SparkConnectService.sessionManager.getOrCreateIsolatedSession(key, None)
    SparkConnectService.sessionManager.closeSession(key)

    val exGetOrCreate = intercept[SparkSQLException] {
      SparkConnectService.sessionManager.getOrCreateIsolatedSession(key, None)
    }
    assert(exGetOrCreate.getCondition == "INVALID_HANDLE.SESSION_CLOSED")

    val exGet = intercept[SparkSQLException] {
      SparkConnectService.sessionManager.getIsolatedSession(key, None)
    }
    assert(exGet.getCondition == "INVALID_HANDLE.SESSION_CLOSED")

    val sessionGetIfPresent = SparkConnectService.sessionManager.getIsolatedSessionIfPresent(key)
    assert(sessionGetIfPresent.isEmpty)
  }

  test("getIsolatedSession/getIsolatedSessionIfPresent when session doesn't exist") {
    val key = SessionKey("user", UUID.randomUUID().toString)

    val exGet = intercept[SparkSQLException] {
      SparkConnectService.sessionManager.getIsolatedSession(key, None)
    }
    assert(exGet.getCondition == "INVALID_HANDLE.SESSION_NOT_FOUND")

    val sessionGetIfPresent = SparkConnectService.sessionManager.getIsolatedSessionIfPresent(key)
    assert(sessionGetIfPresent.isEmpty)
  }

  test("SessionHolder with custom expiration time is not cleaned up due to inactivity") {
    val key = SessionKey("user", UUID.randomUUID().toString)
    val sessionHolder = SparkConnectService.sessionManager.getOrCreateIsolatedSession(key, None)

    assert(
      SparkConnectService.sessionManager.listActiveSessions.exists(
        _.sessionId == sessionHolder.sessionId))
    sessionHolder.setCustomInactiveTimeoutMs(Some(5.days.toMillis))

    // clean up with inactivity timeout of 0.
    SparkConnectService.sessionManager.periodicMaintenance(defaultInactiveTimeoutMs = 0L)
    // session should still be there.
    assert(
      SparkConnectService.sessionManager.listActiveSessions.exists(
        _.sessionId == sessionHolder.sessionId))

    sessionHolder.setCustomInactiveTimeoutMs(None)
    // it will be cleaned up now.
    SparkConnectService.sessionManager.periodicMaintenance(defaultInactiveTimeoutMs = 0L)
    assert(SparkConnectService.sessionManager.listActiveSessions.isEmpty)
    assert(
      SparkConnectService.sessionManager.listClosedSessions.exists(
        _.sessionId == sessionHolder.sessionId))
  }

  test("SessionHolder is recorded with status closed after close") {
    val key = SessionKey("user", UUID.randomUUID().toString)
    val sessionHolder = SparkConnectService.sessionManager.getOrCreateIsolatedSession(key, None)

    val activeSessionInfo = SparkConnectService.sessionManager.listActiveSessions.find(
      _.sessionId == sessionHolder.sessionId)
    assert(activeSessionInfo.isDefined)
    assert(activeSessionInfo.get.status == SessionStatus.Started)
    assert(activeSessionInfo.get.closedTimeMs.isEmpty)

    SparkConnectService.sessionManager.closeSession(sessionHolder.key)

    assert(SparkConnectService.sessionManager.listActiveSessions.isEmpty)
    val closedSessionInfo = SparkConnectService.sessionManager.listClosedSessions.find(
      _.sessionId == sessionHolder.sessionId)
    assert(closedSessionInfo.isDefined)
    assert(closedSessionInfo.get.status == SessionStatus.Closed)
    assert(closedSessionInfo.get.closedTimeMs.isDefined)
  }
}
