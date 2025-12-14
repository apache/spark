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

import org.apache.spark.{SparkIllegalArgumentException, SparkIllegalStateException}

/**
 * Errors thrown by Offline state repartitioning.
 */
object OfflineStateRepartitionErrors {
  def parameterIsEmptyError(parameter: String): StateRepartitionInvalidParameterError = {
    new StateRepartitionParameterIsEmptyError(parameter)
  }

  def parameterIsNotGreaterThanZeroError(
      parameter: String): StateRepartitionInvalidParameterError = {
    new StateRepartitionParameterIsNotGreaterThanZeroError(parameter)
  }

  def parameterIsNullError(parameter: String): StateRepartitionInvalidParameterError = {
    new StateRepartitionParameterIsNullError(parameter)
  }

  def lastBatchAbandonedRepartitionError(
      checkpointLocation: String,
      lastBatchId: Long,
      lastBatchShufflePartitions: Int,
      numPartitions: Int): StateRepartitionInvalidCheckpointError = {
    new StateRepartitionLastBatchAbandonedRepartitionError(
      checkpointLocation, lastBatchId, lastBatchShufflePartitions, numPartitions)
  }

  def lastBatchFailedError(
      checkpointLocation: String,
      lastBatchId: Long): StateRepartitionInvalidCheckpointError = {
    new StateRepartitionLastBatchFailedError(checkpointLocation, lastBatchId)
  }

  def missingOffsetSeqMetadataError(
      checkpointLocation: String,
      version: Int,
      batchId: Long): StateRepartitionInvalidCheckpointError = {
    new StateRepartitionMissingOffsetSeqMetadataError(checkpointLocation, version, batchId)
  }

  def noBatchFoundError(checkpointLocation: String): StateRepartitionInvalidCheckpointError = {
    new StateRepartitionNoBatchFoundError(checkpointLocation)
  }

  def noCommittedBatchError(checkpointLocation: String): StateRepartitionInvalidCheckpointError = {
    new StateRepartitionNoCommittedBatchError(checkpointLocation)
  }

  def offsetSeqNotFoundError(
      checkpointLocation: String,
      batchId: Long): StateRepartitionInvalidCheckpointError = {
    new StateRepartitionOffsetSeqNotFoundError(checkpointLocation, batchId)
  }

  def shufflePartitionsAlreadyMatchError(
      checkpointLocation: String,
      batchId: Long,
      numPartitions: Int): StateRepartitionInvalidCheckpointError = {
    new StateRepartitionShufflePartitionsAlreadyMatchError(
      checkpointLocation, batchId, numPartitions)
  }

  def unsupportedOffsetSeqVersionError(
      checkpointLocation: String,
      version: Int): StateRepartitionInvalidCheckpointError = {
    new StateRepartitionUnsupportedOffsetSeqVersionError(checkpointLocation, version)
  }

  def unsupportedStateStoreProviderError(
      checkpointLocation: String,
      providerClass: String): StateRepartitionInvalidCheckpointError = {
    new StateRepartitionUnsupportedProviderError(checkpointLocation, providerClass)
  }

  def unsupportedStatefulOperatorError(
      checkpointLocation: String,
      operatorName: String): StateRepartitionInvalidCheckpointError = {
    new StateRepartitionUnsupportedStatefulOperatorError(checkpointLocation, operatorName)
  }

  def unsupportedTransformWithStateVarTypeError(
      checkpointLocation: String,
      variableType: String,
      ttlEnabled: Boolean,
      colFamilyName: String): StateRepartitionInvalidCheckpointError = {
    new StateRepartitionUnsupportedTransformWithStateVarTypeError(
      checkpointLocation, variableType, ttlEnabled, colFamilyName)
  }
}

/**
 * Base class for exceptions thrown when an invalid parameter is passed
 * into the repartition operation.
 */
abstract class StateRepartitionInvalidParameterError(
    parameter: String,
    subClass: String,
    messageParameters: Map[String, String] = Map.empty,
    cause: Throwable = null)
  extends SparkIllegalArgumentException(
    errorClass = s"STATE_REPARTITION_INVALID_PARAMETER.$subClass",
    messageParameters = Map("parameter" -> parameter) ++ messageParameters,
    cause = cause)

class StateRepartitionParameterIsEmptyError(parameter: String)
  extends StateRepartitionInvalidParameterError(
    parameter,
    subClass = "IS_EMPTY")

class StateRepartitionParameterIsNotGreaterThanZeroError(parameter: String)
  extends StateRepartitionInvalidParameterError(
    parameter,
    subClass = "IS_NOT_GREATER_THAN_ZERO")

class StateRepartitionParameterIsNullError(parameter: String)
  extends StateRepartitionInvalidParameterError(
    parameter,
    subClass = "IS_NULL")

/**
 * Base class for exceptions thrown when the checkpoint location is in an invalid state
 * for repartitioning.
 */
abstract class StateRepartitionInvalidCheckpointError(
    checkpointLocation: String,
    subClass: String,
    messageParameters: Map[String, String],
    cause: Throwable = null)
  extends SparkIllegalStateException(
    errorClass = s"STATE_REPARTITION_INVALID_CHECKPOINT.$subClass",
    messageParameters = Map("checkpointLocation" -> checkpointLocation) ++ messageParameters,
    cause = cause)

class StateRepartitionLastBatchAbandonedRepartitionError(
    checkpointLocation: String,
    lastBatchId: Long,
    lastBatchShufflePartitions: Int,
    numPartitions: Int)
  extends StateRepartitionInvalidCheckpointError(
    checkpointLocation,
    subClass = "LAST_BATCH_ABANDONED_REPARTITION",
    messageParameters = Map(
      "lastBatchId" -> lastBatchId.toString,
      "lastBatchShufflePartitions" -> lastBatchShufflePartitions.toString,
      "numPartitions" -> numPartitions.toString
    ))

class StateRepartitionLastBatchFailedError(
    checkpointLocation: String,
    lastBatchId: Long)
  extends StateRepartitionInvalidCheckpointError(
    checkpointLocation,
    subClass = "LAST_BATCH_FAILED",
    messageParameters = Map("lastBatchId" -> lastBatchId.toString))

class StateRepartitionMissingOffsetSeqMetadataError(
    checkpointLocation: String,
    version: Int,
    batchId: Long)
  extends StateRepartitionInvalidCheckpointError(
    checkpointLocation,
    subClass = "MISSING_OFFSET_SEQ_METADATA",
    messageParameters = Map("version" -> version.toString, "batchId" -> batchId.toString))

class StateRepartitionNoBatchFoundError(
    checkpointLocation: String)
  extends StateRepartitionInvalidCheckpointError(
    checkpointLocation,
    subClass = "NO_BATCH_FOUND",
    messageParameters = Map.empty)

class StateRepartitionNoCommittedBatchError(
    checkpointLocation: String)
  extends StateRepartitionInvalidCheckpointError(
    checkpointLocation,
    subClass = "NO_COMMITTED_BATCH",
    messageParameters = Map.empty)

class StateRepartitionOffsetSeqNotFoundError(
    checkpointLocation: String,
    batchId: Long)
  extends StateRepartitionInvalidCheckpointError(
    checkpointLocation,
    subClass = "OFFSET_SEQ_NOT_FOUND",
    messageParameters = Map("batchId" -> batchId.toString))

class StateRepartitionShufflePartitionsAlreadyMatchError(
    checkpointLocation: String,
    batchId: Long,
    numPartitions: Int)
  extends StateRepartitionInvalidCheckpointError(
    checkpointLocation,
    subClass = "SHUFFLE_PARTITIONS_ALREADY_MATCH",
    messageParameters = Map(
      "batchId" -> batchId.toString,
      "numPartitions" -> numPartitions.toString))

class StateRepartitionUnsupportedOffsetSeqVersionError(
    checkpointLocation: String,
    version: Int)
  extends StateRepartitionInvalidCheckpointError(
    checkpointLocation,
    subClass = "UNSUPPORTED_OFFSET_SEQ_VERSION",
    messageParameters = Map("version" -> version.toString))

class StateRepartitionUnsupportedProviderError(
    checkpointLocation: String,
    provider: String)
  extends StateRepartitionInvalidCheckpointError(
    checkpointLocation,
    subClass = "UNSUPPORTED_PROVIDER",
    messageParameters = Map("provider" -> provider))

class StateRepartitionUnsupportedStatefulOperatorError(
    checkpointLocation: String,
    operatorName: String)
  extends StateRepartitionInvalidCheckpointError(
    checkpointLocation,
    subClass = "UNSUPPORTED_STATEFUL_OPERATOR",
    messageParameters = Map("operatorName" -> operatorName))

class StateRepartitionUnsupportedTransformWithStateVarTypeError(
    checkpointLocation: String,
    variableType: String,
    ttlEnabled: Boolean,
    colFamilyName: String)
  extends StateRepartitionInvalidCheckpointError(
    checkpointLocation,
    subClass = "UNSUPPORTED_TRANSFORM_WITH_STATE_VARIABLE_TYPE",
    messageParameters = Map(
      "variableType" -> variableType,
      "ttlEnabled" -> ttlEnabled.toString,
      "colFamilyName" -> colFamilyName))
