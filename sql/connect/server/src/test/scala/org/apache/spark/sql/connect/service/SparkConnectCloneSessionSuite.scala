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

import org.apache.spark.SparkSQLException
import org.apache.spark.sql.test.SharedSparkSession

class SparkConnectCloneSessionSuite extends SharedSparkSession with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    super.beforeEach()
    SparkConnectService.sessionManager.invalidateAllSessions()
  }

  test("clone session with invalid target session ID format") {
    val sourceKey = SessionKey("testUser", UUID.randomUUID.toString)
    val invalidSessionId = "not-a-valid-uuid"

    // Create the source session first
    SparkConnectService.sessionManager.getOrCreateIsolatedSession(sourceKey, None)

    val ex = intercept[SparkSQLException] {
      SparkConnectService.sessionManager.cloneSession(sourceKey, invalidSessionId, None)
    }

    assert(ex.getCondition == "INVALID_CLONE_SESSION_REQUEST.TARGET_SESSION_ID_FORMAT")
    assert(ex.getMessage.contains("Target session ID not-a-valid-uuid"))
    assert(ex.getMessage.contains("must be an UUID string"))
  }

  test("clone session with target session ID already closed") {
    val sourceKey = SessionKey("testUser", UUID.randomUUID.toString)
    val targetSessionId = UUID.randomUUID.toString
    val targetKey = SessionKey("testUser", targetSessionId)

    // Create and then close a session to put it in the closed cache
    SparkConnectService.sessionManager.getOrCreateIsolatedSession(sourceKey, None)
    SparkConnectService.sessionManager.getOrCreateIsolatedSession(targetKey, None)
    SparkConnectService.sessionManager.closeSession(targetKey)

    val ex = intercept[SparkSQLException] {
      SparkConnectService.sessionManager.cloneSession(sourceKey, targetSessionId, None)
    }

    assert(ex.getCondition ==
      "INVALID_CLONE_SESSION_REQUEST.TARGET_SESSION_ID_ALREADY_CLOSED")
    assert(ex.getMessage.contains(s"target session ID $targetSessionId"))
    assert(ex.getMessage.contains("was previously closed"))
  }

  test("clone session with target session ID already exists") {
    val sourceKey = SessionKey("testUser", UUID.randomUUID.toString)
    val targetSessionId = UUID.randomUUID.toString
    val targetKey = SessionKey("testUser", targetSessionId)

    // Create both source and target sessions
    SparkConnectService.sessionManager.getOrCreateIsolatedSession(sourceKey, None)
    SparkConnectService.sessionManager.getOrCreateIsolatedSession(targetKey, None)

    val ex = intercept[SparkSQLException] {
      SparkConnectService.sessionManager.cloneSession(sourceKey, targetSessionId, None)
    }

    assert(ex.getCondition ==
      "INVALID_CLONE_SESSION_REQUEST.TARGET_SESSION_ID_ALREADY_EXISTS")
    assert(ex.getMessage.contains(s"target session ID $targetSessionId"))
    assert(ex.getMessage.contains("already exists"))
  }

  test("clone session with source session not found") {
    val sourceKey = SessionKey("testUser", UUID.randomUUID.toString)
    val targetSessionId = UUID.randomUUID.toString

    // Don't create the source session, so it doesn't exist
    val ex = intercept[SparkSQLException] {
      SparkConnectService.sessionManager.cloneSession(sourceKey, targetSessionId, None)
    }

    // Source session errors should remain as standard INVALID_HANDLE errors
    assert(ex.getCondition == "INVALID_HANDLE.SESSION_NOT_FOUND")
    assert(ex.getMessage.contains("Session not found"))
  }

  test("successful clone session creates new session") {
    val sourceKey = SessionKey("testUser", UUID.randomUUID.toString)
    val targetSessionId = UUID.randomUUID.toString

    // Create source session
    val sourceSession = SparkConnectService.sessionManager
      .getOrCreateIsolatedSession(sourceKey, None)

    // Clone the session
    val clonedSession = SparkConnectService.sessionManager
      .cloneSession(sourceKey, targetSessionId, None)

    // Verify the cloned session has the expected session ID
    assert(clonedSession.sessionId == targetSessionId)
    assert(clonedSession.sessionId != sourceSession.sessionId)

    // Both sessions should be different objects
    assert(clonedSession != sourceSession)
  }
}
