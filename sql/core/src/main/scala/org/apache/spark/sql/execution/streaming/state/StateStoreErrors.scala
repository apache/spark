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

package org.apache.spark.sql.execution.streaming.state

import org.apache.spark.{SparkException, SparkUnsupportedOperationException}

/**
 * Object for grouping error messages from (most) exceptions thrown from State API V2
 *
 * ERROR_CLASS has a prefix of "STATE_STORE_" to indicate where the error is from
 */
object StateStoreErrors {
  def implicitKeyNotFound(stateName: String): SparkException = {
    SparkException.internalError(
      msg = s"Implicit key not found in state store for stateName=$stateName",
      category = "TWS"
    )
  }

  def missingTimeoutValues(timeoutMode: String): SparkException = {
    SparkException.internalError(
      msg = s"Failed to find timeout values for timeoutMode=$timeoutMode",
      category = "TWS"
    )
  }

  def unsupportedOperationOnMissingColumnFamily(operationName: String, colFamilyName: String):
    StateStoreUnsupportedOperationOnMissingColumnFamily = {
    new StateStoreUnsupportedOperationOnMissingColumnFamily(operationName, colFamilyName)
  }

  def multipleColumnFamiliesNotSupported(stateStoreProvider: String):
    StateStoreMultipleColumnFamiliesNotSupportedException = {
    new StateStoreMultipleColumnFamiliesNotSupportedException(stateStoreProvider)
  }

  def removingColumnFamiliesNotSupported(stateStoreProvider: String):
    StateStoreRemovingColumnFamiliesNotSupportedException = {
    new StateStoreRemovingColumnFamiliesNotSupportedException(stateStoreProvider)
  }

  def cannotUseColumnFamilyWithInvalidName(operationName: String, colFamilyName: String):
    StateStoreCannotUseColumnFamilyWithInvalidName = {
      new StateStoreCannotUseColumnFamilyWithInvalidName(operationName, colFamilyName)
  }

  def unsupportedOperationException(operationName: String, entity: String):
    StateStoreUnsupportedOperationException = {
    new StateStoreUnsupportedOperationException(operationName, entity)
  }

  def requireNonNullStateValue(value: Any, stateName: String): Unit = {
    SparkException.require(value != null,
      errorClass = "ILLEGAL_STATE_STORE_VALUE.NULL_VALUE",
      messageParameters = Map("stateName" -> stateName))
  }

  def requireNonEmptyListStateValue[S](value: Array[S], stateName: String): Unit = {
    SparkException.require(value.nonEmpty,
      errorClass = "ILLEGAL_STATE_STORE_VALUE.EMPTY_LIST_VALUE",
      messageParameters = Map("stateName" -> stateName))
  }

  def cannotCreateColumnFamilyWithReservedChars(colFamilyName: String):
    StateStoreCannotCreateColumnFamilyWithReservedChars = {
      new StateStoreCannotCreateColumnFamilyWithReservedChars(colFamilyName)
  }

  def cannotPerformOperationWithInvalidTimeoutMode(
      operationType: String,
      timeoutMode: String): StatefulProcessorCannotPerformOperationWithInvalidTimeoutMode = {
    new StatefulProcessorCannotPerformOperationWithInvalidTimeoutMode(operationType, timeoutMode)
  }

  def cannotPerformOperationWithInvalidHandleState(
      operationType: String,
      handleState: String): StatefulProcessorCannotPerformOperationWithInvalidHandleState = {
    new StatefulProcessorCannotPerformOperationWithInvalidHandleState(operationType, handleState)
  }
}

class StateStoreMultipleColumnFamiliesNotSupportedException(stateStoreProvider: String)
  extends SparkUnsupportedOperationException(
    errorClass = "UNSUPPORTED_FEATURE.STATE_STORE_MULTIPLE_COLUMN_FAMILIES",
    messageParameters = Map("stateStoreProvider" -> stateStoreProvider))

class StateStoreRemovingColumnFamiliesNotSupportedException(stateStoreProvider: String)
  extends SparkUnsupportedOperationException(
    errorClass = "UNSUPPORTED_FEATURE.STATE_STORE_REMOVING_COLUMN_FAMILIES",
    messageParameters = Map("stateStoreProvider" -> stateStoreProvider))

class StateStoreCannotUseColumnFamilyWithInvalidName(operationName: String, colFamilyName: String)
  extends SparkUnsupportedOperationException(
    errorClass = "STATE_STORE_CANNOT_USE_COLUMN_FAMILY_WITH_INVALID_NAME",
    messageParameters = Map("operationName" -> operationName, "colFamilyName" -> colFamilyName))

class StateStoreCannotCreateColumnFamilyWithReservedChars(colFamilyName: String)
  extends SparkUnsupportedOperationException(
    errorClass = "STATE_STORE_CANNOT_CREATE_COLUMN_FAMILY_WITH_RESERVED_CHARS",
    messageParameters = Map("colFamilyName" -> colFamilyName)
  )

class StateStoreUnsupportedOperationException(operationType: String, entity: String)
  extends SparkUnsupportedOperationException(
    errorClass = "STATE_STORE_UNSUPPORTED_OPERATION",
    messageParameters = Map("operationType" -> operationType, "entity" -> entity)
  )

class StatefulProcessorCannotPerformOperationWithInvalidTimeoutMode(
    operationType: String,
    timeoutMode: String)
  extends SparkUnsupportedOperationException(
    errorClass = "STATEFUL_PROCESSOR_CANNOT_PERFORM_OPERATION_WITH_INVALID_TIMEOUT_MODE",
    messageParameters = Map("operationType" -> operationType, "timeoutMode" -> timeoutMode)
  )

class StatefulProcessorCannotPerformOperationWithInvalidHandleState(
    operationType: String,
    handleState: String)
  extends SparkUnsupportedOperationException(
    errorClass = "STATEFUL_PROCESSOR_CANNOT_PERFORM_OPERATION_WITH_INVALID_HANDLE_STATE",
    messageParameters = Map("operationType" -> operationType, "handleState" -> handleState)
  )

class StateStoreUnsupportedOperationOnMissingColumnFamily(
    operationType: String,
    colFamilyName: String) extends SparkUnsupportedOperationException(
  errorClass = "STATE_STORE_UNSUPPORTED_OPERATION_ON_MISSING_COLUMN_FAMILY",
  messageParameters = Map("operationType" -> operationType, "colFamilyName" -> colFamilyName))
