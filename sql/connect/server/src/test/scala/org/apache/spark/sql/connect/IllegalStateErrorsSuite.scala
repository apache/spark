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
    val operation = "next"
    val error = IllegalStateErrors.streamLifecycleAlreadyCompleted(operation)
    assert(error.isInstanceOf[org.apache.spark.SparkIllegalStateException])
    assert(error.getCondition.contains("STREAM_LIFECYCLE_ALREADY_COMPLETED"))
    assert(error.getMessage.contains(operation))
  }

  test("cursorOutOfBounds should construct error correctly") {
    val cursor = 15L
    val batchSize = 10L
    val error = IllegalStateErrors.cursorOutOfBounds(cursor, batchSize)
    assert(error.getCondition.contains("DATA_INTEGRITY_CURSOR_OUT_OF_BOUNDS"))
    assert(error.getMessage.contains(cursor.toString))
    assert(error.getMessage.contains(batchSize.toString))
  }

  test("executionStateTransitionInvalidOperationStatus should construct error correctly") {
    val operationId = "op-123"
    val currentStatus = ExecuteStatus.Pending
    val validStatuses = List(ExecuteStatus.Started, ExecuteStatus.Finished)
    val eventStatus = ExecuteStatus.Analyzed
    val error = IllegalStateErrors.executionStateTransitionInvalidOperationStatus(
      operationId,
      currentStatus,
      validStatuses,
      eventStatus)
    val expectedCondition = "SPARK_CONNECT_ILLEGAL_STATE." +
      "STATE_CONSISTENCY_EXECUTION_STATE_TRANSITION_INVALID_OPERATION_STATUS_MISMATCH"
    assert(error.getCondition.contains(expectedCondition))
    assert(error.getMessage.contains(operationId))
    assert(error.getMessage.contains(currentStatus.toString))
  }

  test("executionStateTransitionInvalidSessionNotStarted should construct error correctly") {
    val sessionId = "session-456"
    val sessionStatus = SessionStatus.Pending
    val eventStatus = ExecuteStatus.Started
    val error = IllegalStateErrors.executionStateTransitionInvalidSessionNotStarted(
      sessionId,
      sessionStatus,
      eventStatus)
    val expectedCondition = "SPARK_CONNECT_ILLEGAL_STATE." +
      "STATE_CONSISTENCY_EXECUTION_STATE_TRANSITION_INVALID_SESSION_NOT_STARTED"
    assert(error.getCondition.contains(expectedCondition))
    assert(error.getMessage.contains(sessionId))
    assert(error.getMessage.contains(sessionStatus.toString))
  }

  test("executeHolderAlreadyExists should construct error correctly") {
    val operationId = "op-789"
    val error = IllegalStateErrors.executeHolderAlreadyExists(operationId)
    assert(error.getCondition.contains("EXECUTION_STATE_EXECUTE_HOLDER_ALREADY_EXISTS"))
    assert(error.getMessage.contains(operationId))
  }

  test("executeHolderAlreadyExistsGraphId should construct error correctly") {
    val graphId = "graph-123"
    val error = IllegalStateErrors.executeHolderAlreadyExistsGraphId(graphId)
    val expectedCondition =
      "EXECUTION_STATE_EXECUTE_HOLDER_ALREADY_EXISTS_GRAPH"
    assert(error.getCondition.contains(expectedCondition))
    assert(error.getMessage.contains(graphId))
  }

  test("sessionAlreadyClosed should construct error correctly") {
    val sessionKey = "session-key-456"
    val error = IllegalStateErrors.sessionAlreadyClosed(sessionKey)
    assert(error.getCondition.contains("SESSION_MANAGEMENT_SESSION_ALREADY_CLOSED"))
    assert(error.getMessage.contains(sessionKey))
  }

  test("operationOrphaned should construct error correctly") {
    val executeKey = "execute-key-789"
    val error = IllegalStateErrors.operationOrphaned(executeKey)
    assert(error.getCondition.contains("EXECUTION_STATE_OPERATION_ORPHANED"))
    assert(error.getMessage.contains(executeKey))
  }

  test("sessionStateTransitionInvalid should construct error correctly") {
    val sessionId = "session-111"
    val fromState = SessionStatus.Started
    val toState = SessionStatus.Closed
    val validStates = List(SessionStatus.Pending, SessionStatus.Started)
    val error =
      IllegalStateErrors.sessionStateTransitionInvalid(sessionId, fromState, toState, validStates)
    val expectedCondition =
      "STATE_CONSISTENCY_SESSION_STATE_TRANSITION_INVALID"
    assert(error.getCondition.contains(expectedCondition))
    assert(error.getMessage.contains(sessionId))
    assert(error.getMessage.contains(fromState.toString))
    assert(error.getMessage.contains(toState.toString))
  }

  test("serviceNotStarted should construct error correctly") {
    val error = IllegalStateErrors.serviceNotStarted()
    assert(error.getCondition.contains("SESSION_MANAGEMENT_SERVICE_NOT_STARTED"))
  }

  test("streamingQueryUnexpectedReturnValue should construct error correctly") {
    val key = "query-key-123"
    val value = 42
    val context = "test-context"
    val error = IllegalStateErrors.streamingQueryUnexpectedReturnValue(key, value, context)
    assert(error.getCondition.contains("STREAMING_QUERY_UNEXPECTED_RETURN_VALUE"))
    assert(error.getMessage.contains(key))
    assert(error.getMessage.contains(value.toString))
    assert(error.getMessage.contains(context))
  }

  test("cleanerAlreadySet should construct error correctly") {
    val key = "session-key-222"
    val queryKey = "query-key-333"
    val error = IllegalStateErrors.cleanerAlreadySet(key, queryKey)
    assert(error.getCondition.contains("STATE_CONSISTENCY_CLEANER_ALREADY_SET"))
    assert(error.getMessage.contains(key))
    assert(error.getMessage.contains(queryKey))
  }

  test("eventSendAfterShutdown should construct error correctly") {
    val key = "session-key-444"
    val error = IllegalStateErrors.eventSendAfterShutdown(key)
    assert(error.getCondition.contains("STREAM_LIFECYCLE_EVENT_SEND_AFTER_SHUTDOWN"))
    assert(error.getMessage.contains(key))
  }

  test("noBatchesAvailable should construct error correctly") {
    val response = "empty-response"
    val error = IllegalStateErrors.noBatchesAvailable(response)
    assert(error.getCondition.contains("STATE_CONSISTENCY_NO_BATCHES_AVAILABLE"))
    assert(error.getMessage.contains(response))
  }

  test("error messages should handle special characters correctly") {
    val operation = "operation with spaces and special chars: <>&\""
    val error = IllegalStateErrors.streamLifecycleAlreadyCompleted(operation)
    assert(error.getMessage.contains(operation))
  }

  test("error messages should handle empty strings") {
    val emptyString = ""
    val error = IllegalStateErrors.streamLifecycleAlreadyCompleted(emptyString)
    assert(error.isInstanceOf[org.apache.spark.SparkIllegalStateException])
  }

  test("all errors should be SparkIllegalStateException instances") {
    val errors = Seq(
      IllegalStateErrors.streamLifecycleAlreadyCompleted("op"),
      IllegalStateErrors.cursorOutOfBounds(1L, 2L),
      IllegalStateErrors.executionStateTransitionInvalidOperationStatus(
        "op",
        ExecuteStatus.Pending,
        List(ExecuteStatus.Started),
        ExecuteStatus.Analyzed),
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
      IllegalStateErrors.streamingQueryUnexpectedReturnValue("key", 123, "context"),
      IllegalStateErrors.cleanerAlreadySet("key", "queryKey"),
      IllegalStateErrors.eventSendAfterShutdown("key"),
      IllegalStateErrors.noBatchesAvailable("response"))

    errors.foreach { error =>
      assert(error.isInstanceOf[org.apache.spark.SparkIllegalStateException])
      assert(error.getCondition.contains("SPARK_CONNECT_ILLEGAL_STATE"))
    }
  }
}
