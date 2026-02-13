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

package org.apache.spark.sql.connect

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.connect.service.{ExecuteStatus, SessionStatus}

class IllegalStateErrorsSuite extends SparkFunSuite {

  test("streamLifecycleAlreadyCompleted should construct error correctly") {
    val operation = "testOperation"
    val error = IllegalStateErrors.streamLifecycleAlreadyCompleted(operation)

    assert(error.getCondition.contains("STREAM_LIFECYCLE_ALREADY_COMPLETED"))
    assert(error.getMessage.contains(operation))
  }

  test("cursorOutOfBounds should construct error correctly") {
    val cursor = 100L
    val batchSize = 50L
    val error = IllegalStateErrors.cursorOutOfBounds(cursor, batchSize)

    assert(error.getCondition.contains("CURSOR_OUT_OF_BOUNDS"))
    assert(error.getMessage.contains(cursor.toString))
    assert(error.getMessage.contains(batchSize.toString))
  }

  test("executionStateTransitionInvalidOperationStatus should construct error correctly") {
    val operationId = "op-123"
    val currentStatus = ExecuteStatus.Started
    val validStatuses = List(ExecuteStatus.Pending, ExecuteStatus.Finished)
    val eventStatus = ExecuteStatus.Closed

    val error = IllegalStateErrors.executionStateTransitionInvalidOperationStatus(
      operationId,
      currentStatus,
      validStatuses,
      eventStatus)

    assert(error.getCondition.contains("INVALID_OPERATION_STATUS_MISMATCH"))
    assert(error.getMessage.contains(operationId))
    assert(error.getMessage.contains(currentStatus.toString))
    assert(error.getMessage.contains(eventStatus.toString))
  }

  test("executionStateTransitionInvalidSessionNotStarted should construct error correctly") {
    val sessionId = "session-456"
    val sessionStatus = SessionStatus.Pending
    val eventStatus = ExecuteStatus.Started

    val error = IllegalStateErrors.executionStateTransitionInvalidSessionNotStarted(
      sessionId,
      sessionStatus,
      eventStatus)

    assert(error.getCondition.contains("INVALID_SESSION_NOT_STARTED"))
    assert(error.getMessage.contains(sessionId))
    assert(error.getMessage.contains(sessionStatus.toString))
    assert(error.getMessage.contains(eventStatus.toString))
  }

  test("executeHolderAlreadyExists should construct error correctly") {
    val operationId = "op-789"
    val error = IllegalStateErrors.executeHolderAlreadyExists(operationId)

    assert(error.getCondition.contains("EXECUTE_HOLDER_ALREADY_EXISTS"))
    assert(error.getMessage.contains(operationId))
  }

  test("executeHolderAlreadyExistsGraphId should construct error correctly") {
    val graphId = "graph-123"
    val error = IllegalStateErrors.executeHolderAlreadyExistsGraphId(graphId)

    assert(error.getCondition.contains("EXECUTE_HOLDER_ALREADY_EXISTS_GRAPH"))
    assert(error.getMessage.contains(graphId))
  }

  test("sessionAlreadyClosed should construct error correctly") {
    val sessionKey = "SessionKey(userId=user1,sessionId=session1)"
    val error = IllegalStateErrors.sessionAlreadyClosed(sessionKey)

    assert(error.getCondition.contains("SESSION_ALREADY_CLOSED"))
    assert(error.getMessage.contains(sessionKey))
  }

  test("operationOrphaned should construct error correctly") {
    val executeKey = "ExecuteKey(userId=user1,sessionId=session1,operationId=op1)"
    val error = IllegalStateErrors.operationOrphaned(executeKey)

    assert(error.getCondition.contains("OPERATION_ORPHANED"))
    assert(error.getMessage.contains(executeKey))
  }

  test("sessionStateTransitionInvalid should construct error correctly") {
    val sessionId = "session-999"
    val fromState = SessionStatus.Started
    val toState = SessionStatus.Closed
    val validStates = List(SessionStatus.Pending, SessionStatus.Started)

    val error = IllegalStateErrors.sessionStateTransitionInvalid(
      sessionId,
      fromState,
      toState,
      validStates)

    assert(error.getCondition.contains("SESSION_STATE_TRANSITION_INVALID"))
    assert(error.getMessage.contains(sessionId))
    assert(error.getMessage.contains(fromState.toString))
    assert(error.getMessage.contains(toState.toString))
  }

  test("serviceNotStarted should construct error correctly") {
    val error = IllegalStateErrors.serviceNotStarted()

    assert(error.getCondition.contains("SERVICE_NOT_STARTED"))
  }

  test("streamingQueryUnexpectedReturnValue should construct error correctly") {
    val key = "SessionKey(userId=user1,sessionId=session1)"
    val value = 42
    val context = "foreachBatch function"

    val error = IllegalStateErrors.streamingQueryUnexpectedReturnValue(key, value, context)

    assert(error.getCondition.contains("UNEXPECTED_RETURN_VALUE"))
    assert(error.getMessage.contains(key))
    assert(error.getMessage.contains(value.toString))
    assert(error.getMessage.contains(context))
  }

  test("cleanerAlreadySet should construct error correctly") {
    val key = "SessionKey(userId=user1,sessionId=session1)"
    val queryKey = "CacheKey(queryId=q1,runId=r1)"

    val error = IllegalStateErrors.cleanerAlreadySet(key, queryKey)

    assert(error.getCondition.contains("CLEANER_ALREADY_SET"))
    assert(error.getMessage.contains(key))
    assert(error.getMessage.contains(queryKey.toString))
  }

  test("eventSendAfterShutdown should construct error correctly") {
    val key = "SessionKey(userId=user1,sessionId=session1)"
    val error = IllegalStateErrors.eventSendAfterShutdown(key)

    assert(error.getCondition.contains("EVENT_SEND_AFTER_SHUTDOWN"))
    assert(error.getMessage.contains(key))
  }

  test("noBatchesAvailable should construct error correctly") {
    val response = "ExecutePlanResponse{}"
    val error = IllegalStateErrors.noBatchesAvailable(response)

    assert(error.getCondition.contains("NO_BATCHES_AVAILABLE"))
    assert(error.getMessage.contains(response))
  }

  // Test error message parameter substitution
  test("error parameters should be correctly substituted in message") {
    val operationId = "test-op-id"
    val error = IllegalStateErrors.executeHolderAlreadyExists(operationId)

    val message = error.getMessage
    assert(message.contains(operationId))
    assert(error.getCondition.contains("EXECUTE_HOLDER_ALREADY_EXISTS"))
  }

  // Test with special characters in string parameters
  test("error should handle special characters in parameters") {
    val operationId = "op-with-special-chars-!@#$%"
    val error = IllegalStateErrors.executeHolderAlreadyExists(operationId)

    assert(error.getMessage.contains(operationId))
  }

  // Test with empty string parameters
  test("error should handle empty string parameters") {
    val emptyOperationId = ""
    val error = IllegalStateErrors.executeHolderAlreadyExists(emptyOperationId)

    assert(error.getCondition.contains("EXECUTE_HOLDER_ALREADY_EXISTS"))
  }

  // Test that all errors extend SparkIllegalStateException
  test("all errors should be SparkIllegalStateException instances") {
    val errors = Seq(
      IllegalStateErrors.streamLifecycleAlreadyCompleted("op"),
      IllegalStateErrors.cursorOutOfBounds(1, 2),
      IllegalStateErrors.executionStateTransitionInvalidOperationStatus(
        "op",
        ExecuteStatus.Started,
        List(ExecuteStatus.Pending),
        ExecuteStatus.Closed),
      IllegalStateErrors.executionStateTransitionInvalidSessionNotStarted(
        "session",
        SessionStatus.Pending,
        ExecuteStatus.Started),
      IllegalStateErrors.executeHolderAlreadyExists("op"),
      IllegalStateErrors.executeHolderAlreadyExistsGraphId("graph"),
      IllegalStateErrors.sessionAlreadyClosed("key"),
      IllegalStateErrors.operationOrphaned("key"),
      IllegalStateErrors.sessionStateTransitionInvalid(
        "session",
        SessionStatus.Started,
        SessionStatus.Closed,
        List(SessionStatus.Pending)),
      IllegalStateErrors.serviceNotStarted(),
      IllegalStateErrors.streamingQueryUnexpectedReturnValue("key", "value", "context"),
      IllegalStateErrors.cleanerAlreadySet("key", "queryKey"),
      IllegalStateErrors.eventSendAfterShutdown("key"),
      IllegalStateErrors.noBatchesAvailable("response"))

    errors.foreach { error =>
      assert(error.isInstanceOf[org.apache.spark.SparkIllegalStateException])
      assert(error.getCondition.contains("SPARK_CONNECT_ILLEGAL_STATE"))
    }
  }
}
