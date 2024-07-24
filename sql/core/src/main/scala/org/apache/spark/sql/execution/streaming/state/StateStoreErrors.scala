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

import org.apache.spark.{SparkException, SparkRuntimeException, SparkUnsupportedOperationException}

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

  def missingTimeValues(timeMode: String): SparkException = {
    SparkException.internalError(
      msg = s"Failed to find time values for timeMode=$timeMode",
      category = "TWS"
    )
  }

  def keyRowFormatValidationFailure(errorMsg: String):
    StateStoreKeyRowFormatValidationFailure = {
    new StateStoreKeyRowFormatValidationFailure(errorMsg)
  }

  def valueRowFormatValidationFailure(errorMsg: String):
    StateStoreValueRowFormatValidationFailure = {
    new StateStoreValueRowFormatValidationFailure(errorMsg)
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

  def incorrectNumOrderingColsForPrefixScan(numPrefixCols: String):
    StateStoreIncorrectNumOrderingColsForPrefixScan = {
    new StateStoreIncorrectNumOrderingColsForPrefixScan(numPrefixCols)
  }

  def incorrectNumOrderingColsForRangeScan(numOrderingCols: String):
    StateStoreIncorrectNumOrderingColsForRangeScan = {
    new StateStoreIncorrectNumOrderingColsForRangeScan(numOrderingCols)
  }

  def nullTypeOrderingColsNotSupported(fieldName: String, index: String):
    StateStoreNullTypeOrderingColsNotSupported = {
    new StateStoreNullTypeOrderingColsNotSupported(fieldName, index)
  }

  def variableSizeOrderingColsNotSupported(fieldName: String, index: String):
    StateStoreVariableSizeOrderingColsNotSupported = {
    new StateStoreVariableSizeOrderingColsNotSupported(fieldName, index)
  }

  def cannotCreateColumnFamilyWithReservedChars(colFamilyName: String):
    StateStoreCannotCreateColumnFamilyWithReservedChars = {
    new StateStoreCannotCreateColumnFamilyWithReservedChars(colFamilyName)
  }

  def cannotPerformOperationWithInvalidTimeMode(
      operationType: String,
      timeMode: String): StatefulProcessorCannotPerformOperationWithInvalidTimeMode = {
    new StatefulProcessorCannotPerformOperationWithInvalidTimeMode(operationType, timeMode)
  }

  def cannotPerformOperationWithInvalidHandleState(
      operationType: String,
      handleState: String): StatefulProcessorCannotPerformOperationWithInvalidHandleState = {
    new StatefulProcessorCannotPerformOperationWithInvalidHandleState(operationType, handleState)
  }

  def cannotReInitializeStateOnKey(groupingKey: String):
    StatefulProcessorCannotReInitializeState = {
    new StatefulProcessorCannotReInitializeState(groupingKey)
  }

  def cannotProvideTTLConfigForTimeMode(stateName: String, timeMode: String):
    StatefulProcessorCannotAssignTTLInTimeMode = {
    new StatefulProcessorCannotAssignTTLInTimeMode(stateName, timeMode)
  }

  def ttlMustBePositive(operationType: String,
      stateName: String): StatefulProcessorTTLMustBePositive = {
    new StatefulProcessorTTLMustBePositive(operationType, stateName)
  }

  def stateStoreKeySchemaNotCompatible(
      storedKeySchema: String,
      newKeySchema: String): StateStoreKeySchemaNotCompatible = {
    new StateStoreKeySchemaNotCompatible(storedKeySchema, newKeySchema)
  }

  def stateStoreValueSchemaNotCompatible(
      storedValueSchema: String,
      newValueSchema: String): StateStoreValueSchemaNotCompatible = {
    new StateStoreValueSchemaNotCompatible(storedValueSchema, newValueSchema)
  }

  def stateStoreColumnFamilyMismatch(
      columnFamilyName: String,
      oldColumnFamilySchema: String,
      newColumnFamilySchema: String): StateStoreColumnFamilyMismatch = {
    new StateStoreColumnFamilyMismatch(
      columnFamilyName, oldColumnFamilySchema, newColumnFamilySchema)
  }

  def stateStoreSnapshotFileNotFound(fileToRead: String, clazz: String):
    StateStoreSnapshotFileNotFound = {
    new StateStoreSnapshotFileNotFound(fileToRead, clazz)
  }

  def stateStoreSnapshotPartitionNotFound(
      snapshotPartitionId: Long, operatorId: Int, checkpointLocation: String):
    StateStoreSnapshotPartitionNotFound = {
    new StateStoreSnapshotPartitionNotFound(snapshotPartitionId, operatorId, checkpointLocation)
  }

  def stateStoreProviderDoesNotSupportFineGrainedReplay(inputClass: String):
    StateStoreProviderDoesNotSupportFineGrainedReplay = {
    new StateStoreProviderDoesNotSupportFineGrainedReplay(inputClass)
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

class StateStoreColumnFamilyMismatch(
    columnFamilyName: String,
    oldColumnFamilySchema: String,
    newColumnFamilySchema: String)
  extends SparkUnsupportedOperationException(
    errorClass = "STATE_STORE_COLUMN_FAMILY_SCHEMA_INCOMPATIBLE",
    messageParameters = Map(
      "columnFamilyName" -> columnFamilyName,
      "oldColumnFamilySchema" -> oldColumnFamilySchema,
      "newColumnFamilySchema" -> newColumnFamilySchema))

class StatefulProcessorCannotPerformOperationWithInvalidTimeMode(
    operationType: String,
    timeMode: String)
  extends SparkUnsupportedOperationException(
    errorClass = "STATEFUL_PROCESSOR_CANNOT_PERFORM_OPERATION_WITH_INVALID_TIME_MODE",
    messageParameters = Map("operationType" -> operationType, "timeMode" -> timeMode)
  )

class StatefulProcessorCannotPerformOperationWithInvalidHandleState(
    operationType: String,
    handleState: String)
  extends SparkUnsupportedOperationException(
    errorClass = "STATEFUL_PROCESSOR_CANNOT_PERFORM_OPERATION_WITH_INVALID_HANDLE_STATE",
    messageParameters = Map("operationType" -> operationType, "handleState" -> handleState)
  )

class StatefulProcessorCannotReInitializeState(groupingKey: String)
  extends SparkUnsupportedOperationException(
  errorClass = "STATEFUL_PROCESSOR_CANNOT_REINITIALIZE_STATE_ON_KEY",
  messageParameters = Map("groupingKey" -> groupingKey))

class StateStoreUnsupportedOperationOnMissingColumnFamily(
    operationType: String,
    colFamilyName: String) extends SparkUnsupportedOperationException(
  errorClass = "STATE_STORE_UNSUPPORTED_OPERATION_ON_MISSING_COLUMN_FAMILY",
  messageParameters = Map("operationType" -> operationType, "colFamilyName" -> colFamilyName))

class StateStoreIncorrectNumOrderingColsForPrefixScan(numPrefixCols: String)
  extends SparkUnsupportedOperationException(
    errorClass = "STATE_STORE_INCORRECT_NUM_PREFIX_COLS_FOR_PREFIX_SCAN",
    messageParameters = Map("numPrefixCols" -> numPrefixCols))

class StateStoreIncorrectNumOrderingColsForRangeScan(numOrderingCols: String)
  extends SparkUnsupportedOperationException(
    errorClass = "STATE_STORE_INCORRECT_NUM_ORDERING_COLS_FOR_RANGE_SCAN",
    messageParameters = Map("numOrderingCols" -> numOrderingCols))

class StateStoreVariableSizeOrderingColsNotSupported(fieldName: String, index: String)
  extends SparkUnsupportedOperationException(
    errorClass = "STATE_STORE_VARIABLE_SIZE_ORDERING_COLS_NOT_SUPPORTED",
    messageParameters = Map("fieldName" -> fieldName, "index" -> index))

class StateStoreNullTypeOrderingColsNotSupported(fieldName: String, index: String)
  extends SparkUnsupportedOperationException(
    errorClass = "STATE_STORE_NULL_TYPE_ORDERING_COLS_NOT_SUPPORTED",
    messageParameters = Map("fieldName" -> fieldName, "index" -> index))

class StatefulProcessorCannotAssignTTLInTimeMode(stateName: String, timeMode: String)
  extends SparkUnsupportedOperationException(
    errorClass = "STATEFUL_PROCESSOR_INCORRECT_TIME_MODE_TO_ASSIGN_TTL",
    messageParameters = Map("stateName" -> stateName, "timeMode" -> timeMode))

class StatefulProcessorTTLMustBePositive(
    operationType: String,
    stateName: String)
  extends SparkUnsupportedOperationException(
    errorClass = "STATEFUL_PROCESSOR_TTL_DURATION_MUST_BE_POSITIVE",
    messageParameters = Map("operationType" -> operationType, "stateName" -> stateName))

class StateStoreKeySchemaNotCompatible(
    storedKeySchema: String,
    newKeySchema: String)
  extends SparkUnsupportedOperationException(
    errorClass = "STATE_STORE_KEY_SCHEMA_NOT_COMPATIBLE",
    messageParameters = Map(
      "storedKeySchema" -> storedKeySchema,
      "newKeySchema" -> newKeySchema))

class StateStoreValueSchemaNotCompatible(
    storedValueSchema: String,
    newValueSchema: String)
  extends SparkUnsupportedOperationException(
    errorClass = "STATE_STORE_VALUE_SCHEMA_NOT_COMPATIBLE",
    messageParameters = Map(
      "storedValueSchema" -> storedValueSchema,
      "newValueSchema" -> newValueSchema))

class StateStoreSnapshotFileNotFound(fileToRead: String, clazz: String)
  extends SparkRuntimeException(
    errorClass = "CANNOT_LOAD_STATE_STORE.CANNOT_READ_MISSING_SNAPSHOT_FILE",
    messageParameters = Map(
      "fileToRead" -> fileToRead,
      "clazz" -> clazz))

class StateStoreSnapshotPartitionNotFound(
  snapshotPartitionId: Long, operatorId: Int, checkpointLocation: String)
  extends SparkRuntimeException(
    errorClass = "CANNOT_LOAD_STATE_STORE.SNAPSHOT_PARTITION_ID_NOT_FOUND",
    messageParameters = Map(
      "snapshotPartitionId" -> snapshotPartitionId.toString,
      "operatorId" -> operatorId.toString,
      "checkpointLocation" -> checkpointLocation))

class StateStoreKeyRowFormatValidationFailure(errorMsg: String)
  extends SparkRuntimeException(
    errorClass = "STATE_STORE_KEY_ROW_FORMAT_VALIDATION_FAILURE",
    messageParameters = Map("errorMsg" -> errorMsg))

class StateStoreValueRowFormatValidationFailure(errorMsg: String)
  extends SparkRuntimeException(
    errorClass = "STATE_STORE_VALUE_ROW_FORMAT_VALIDATION_FAILURE",
    messageParameters = Map("errorMsg" -> errorMsg))

class StateStoreProviderDoesNotSupportFineGrainedReplay(inputClass: String)
  extends SparkUnsupportedOperationException(
    errorClass = "STATE_STORE_PROVIDER_DOES_NOT_SUPPORT_FINE_GRAINED_STATE_REPLAY",
    messageParameters = Map("inputClass" -> inputClass))
