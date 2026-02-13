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

import org.apache.spark.SparkIllegalStateException
import org.apache.spark.sql.connect.service.{ExecuteStatus, SessionStatus}

object IllegalStateErrors {

  def streamLifecycleAlreadyCompleted(operation: String): SparkIllegalStateException =
    new SparkIllegalStateException(
      errorClass = "SPARK_CONNECT_ILLEGAL_STATE.STREAM_LIFECYCLE_ALREADY_COMPLETED",
      messageParameters = Map("operation" -> operation))

  def cursorOutOfBounds(cursor: Long, batchSize: Long): SparkIllegalStateException =
    new SparkIllegalStateException(
      errorClass = "SPARK_CONNECT_ILLEGAL_STATE.DATA_INTEGRITY_CURSOR_OUT_OF_BOUNDS",
      messageParameters = Map(
        "cursor" -> cursor.toString,
        "batchSize" -> batchSize.toString))

  def executionStateTransitionInvalidOperationStatus(
      operationId: String,
      currentStatus: ExecuteStatus,
      validStatuses: List[ExecuteStatus],
      eventStatus: ExecuteStatus): SparkIllegalStateException =
    new SparkIllegalStateException(
      errorClass = "SPARK_CONNECT_ILLEGAL_STATE." +
        "STATE_CONSISTENCY_EXECUTION_STATE_TRANSITION_INVALID_OPERATION_STATUS_MISMATCH",
      messageParameters = Map(
        "operationId" -> operationId,
        "currentStatus" -> currentStatus.toString,
        "validStatuses" -> validStatuses.map(_.toString).mkString(", "),
        "eventStatus" -> eventStatus.toString))

  def executionStateTransitionInvalidSessionNotStarted(
      sessionId: String,
      sessionStatus: SessionStatus,
      eventStatus: ExecuteStatus): SparkIllegalStateException =
    new SparkIllegalStateException(
      errorClass = "SPARK_CONNECT_ILLEGAL_STATE." +
        "STATE_CONSISTENCY_EXECUTION_STATE_TRANSITION_INVALID_SESSION_NOT_STARTED",
      messageParameters = Map(
        "sessionId" -> sessionId,
        "sessionStatus" -> sessionStatus.toString,
        "eventStatus" -> eventStatus.toString))

  def executeHolderAlreadyExists(operationId: String): SparkIllegalStateException =
    new SparkIllegalStateException(
      errorClass = "SPARK_CONNECT_ILLEGAL_STATE.EXECUTION_STATE_EXECUTE_HOLDER_ALREADY_EXISTS",
      messageParameters = Map("operationId" -> operationId))

  def executeHolderAlreadyExistsGraphId(graphId: String): SparkIllegalStateException =
    new SparkIllegalStateException(
      errorClass = "SPARK_CONNECT_ILLEGAL_STATE." +
        "EXECUTION_STATE_EXECUTE_HOLDER_ALREADY_EXISTS_GRAPH",
      messageParameters = Map("graphId" -> graphId))

  def sessionAlreadyClosed(sessionKey: String): SparkIllegalStateException =
    new SparkIllegalStateException(
      errorClass = "SPARK_CONNECT_ILLEGAL_STATE.SESSION_MANAGEMENT_SESSION_ALREADY_CLOSED",
      messageParameters = Map("key" -> sessionKey))

  def operationOrphaned(executeKey: String): SparkIllegalStateException =
    new SparkIllegalStateException(
      errorClass = "SPARK_CONNECT_ILLEGAL_STATE.EXECUTION_STATE_OPERATION_ORPHANED",
      messageParameters = Map("key" -> executeKey))

  def sessionStateTransitionInvalid(
      sessionId: String,
      fromState: SessionStatus,
      toState: SessionStatus,
      validStates: List[SessionStatus]): SparkIllegalStateException =
    new SparkIllegalStateException(
      errorClass =
        "SPARK_CONNECT_ILLEGAL_STATE.STATE_CONSISTENCY_SESSION_STATE_TRANSITION_INVALID",
      messageParameters = Map(
        "sessionId" -> sessionId,
        "fromState" -> fromState.toString,
        "toState" -> toState.toString,
        "validStates" -> validStates.map(_.toString).mkString(", ")))

  def serviceNotStarted(): SparkIllegalStateException =
    new SparkIllegalStateException(
      errorClass = "SPARK_CONNECT_ILLEGAL_STATE.SESSION_MANAGEMENT_SERVICE_NOT_STARTED",
      messageParameters = Map.empty)

  def streamingQueryUnexpectedReturnValue(
      key: String,
      value: Int,
      context: String): SparkIllegalStateException =
    new SparkIllegalStateException(
      errorClass = "SPARK_CONNECT_ILLEGAL_STATE.STREAMING_QUERY_UNEXPECTED_RETURN_VALUE",
      messageParameters = Map(
        "key" -> key,
        "value" -> value.toString,
        "context" -> context))

  def cleanerAlreadySet(
      key: String,
      queryKey: String): SparkIllegalStateException =
    new SparkIllegalStateException(
      errorClass = "SPARK_CONNECT_ILLEGAL_STATE.STATE_CONSISTENCY_CLEANER_ALREADY_SET",
      messageParameters = Map(
        "key" -> key,
        "queryKey" -> queryKey.toString))

  def eventSendAfterShutdown(key: String): SparkIllegalStateException =
    new SparkIllegalStateException(
      errorClass = "SPARK_CONNECT_ILLEGAL_STATE.STREAM_LIFECYCLE_EVENT_SEND_AFTER_SHUTDOWN",
      messageParameters = Map("key" -> key))

  def noBatchesAvailable(response: String): SparkIllegalStateException =
    new SparkIllegalStateException(
      errorClass = "SPARK_CONNECT_ILLEGAL_STATE.STATE_CONSISTENCY_NO_BATCHES_AVAILABLE",
      messageParameters = Map("response" -> response))
}
