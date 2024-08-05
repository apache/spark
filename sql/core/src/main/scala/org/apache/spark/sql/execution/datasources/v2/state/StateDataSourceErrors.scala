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
package org.apache.spark.sql.execution.datasources.v2.state

import org.apache.spark.SparkRuntimeException

/**
 * Object for grouping error messages from (most) exceptions thrown from State Data Source.
 * State Metadata Data Source may (re/co)use this object.
 *
 * ERROR_CLASS has a prefix of "STDS_" representing STateDataSource.
 */
object StateDataSourceErrors {
  def internalError(message: String): StateDataSourceException = {
    new StateDataSourceInternalError(message)
  }

  def invalidOptionValue(optionName: String, message: String): StateDataSourceException = {
    new StateDataSourceInvalidOptionValue(optionName, message)
  }

  def invalidOptionValueIsNegative(optionName: String): StateDataSourceException = {
    new StateDataSourceInvalidOptionValueIsNegative(optionName)
  }

  def invalidOptionValueIsEmpty(optionName: String): StateDataSourceException = {
    new StateDataSourceInvalidOptionValueIsEmpty(optionName)
  }

  def requiredOptionUnspecified(missingOptionName: String): StateDataSourceException = {
    new StateDataSourceUnspecifiedRequiredOption(missingOptionName)
  }

  def offsetLogUnavailable(
      batchId: Long,
      checkpointLocation: String): StateDataSourceException = {
    new StateDataSourceOffsetLogUnavailable(batchId, checkpointLocation)
  }

  def offsetMetadataLogUnavailable(
      batchId: Long,
      checkpointLocation: String): StateDataSourceException = {
    new StateDataSourceOffsetMetadataLogUnavailable(batchId, checkpointLocation)
  }

  def failedToReadStateSchema(
      sourceOptions: StateSourceOptions,
      cause: Throwable): StateDataSourceException = {
    new StateDataSourceReadStateSchemaFailure(sourceOptions, cause)
  }

  def failedToReadOperatorMetadata(
      checkpointLocation: String,
      batchId: Long): StateDataSourceException = {
    new StateDataSourceReadOperatorMetadataFailure(checkpointLocation, batchId)
  }

  def conflictOptions(options: Seq[String]): StateDataSourceException = {
    new StateDataSourceConflictOptions(options)
  }

  def committedBatchUnavailable(checkpointLocation: String): StateDataSourceException = {
    new StataDataSourceCommittedBatchUnavailable(checkpointLocation)
  }

  def noPartitionDiscoveredInStateStore(
      sourceOptions: StateSourceOptions): StateDataSourceException = {
    new StateDataSourceNoPartitionDiscoveredInStateStore(sourceOptions)
  }
}

abstract class StateDataSourceException(
    errorClass: String,
    messageParameters: Map[String, String],
    cause: Throwable)
  extends SparkRuntimeException(
    errorClass,
    messageParameters,
    cause)

class StateDataSourceInternalError(message: String, cause: Throwable = null)
  extends StateDataSourceException(
    "STDS_INTERNAL_ERROR",
    Map("message" -> message),
    cause)

class StateDataSourceInvalidOptionValue(optionName: String, message: String)
  extends StateDataSourceException(
    "STDS_INVALID_OPTION_VALUE.WITH_MESSAGE",
    Map("optionName" -> optionName, "message" -> message),
    cause = null)

class StateDataSourceInvalidOptionValueIsNegative(optionName: String)
  extends StateDataSourceException(
    "STDS_INVALID_OPTION_VALUE.IS_NEGATIVE",
    Map("optionName" -> optionName),
    cause = null)

class StateDataSourceInvalidOptionValueIsEmpty(optionName: String)
  extends StateDataSourceException(
    "STDS_INVALID_OPTION_VALUE.IS_EMPTY",
    Map("optionName" -> optionName),
    cause = null)

class StateDataSourceUnspecifiedRequiredOption(
    missingOptionName: String)
  extends StateDataSourceException(
    "STDS_REQUIRED_OPTION_UNSPECIFIED",
    Map("optionName" -> missingOptionName),
    cause = null)

class StateDataSourceOffsetLogUnavailable(
    batchId: Long,
    checkpointLocation: String)
  extends StateDataSourceException(
    "STDS_OFFSET_LOG_UNAVAILABLE",
    Map("batchId" -> batchId.toString, "checkpointLocation" -> checkpointLocation),
    cause = null)

class StateDataSourceOffsetMetadataLogUnavailable(
    batchId: Long,
    checkpointLocation: String)
  extends StateDataSourceException(
    "STDS_OFFSET_METADATA_LOG_UNAVAILABLE",
    Map("batchId" -> batchId.toString, "checkpointLocation" -> checkpointLocation),
    cause = null)

class StateDataSourceReadStateSchemaFailure(
   sourceOptions: StateSourceOptions,
   cause: Throwable)
  extends StateDataSourceException(
    "STDS_FAILED_TO_READ_STATE_SCHEMA",
    Map("sourceOptions" -> sourceOptions.toString),
    cause)

class StateDataSourceConflictOptions(options: Seq[String])
  extends StateDataSourceException(
    "STDS_CONFLICT_OPTIONS",
    Map("options" -> options.map(x => s"'$x'").mkString("[", ", ", "]")),
    cause = null)

class StataDataSourceCommittedBatchUnavailable(checkpointLocation: String)
  extends StateDataSourceException(
    "STDS_COMMITTED_BATCH_UNAVAILABLE",
    Map("checkpointLocation" -> checkpointLocation),
    cause = null)

class StateDataSourceNoPartitionDiscoveredInStateStore(sourceOptions: StateSourceOptions)
  extends StateDataSourceException(
    "STDS_NO_PARTITION_DISCOVERED_IN_STATE_STORE",
    Map("sourceOptions" -> sourceOptions.toString),
    cause = null)

class StateDataSourceReadOperatorMetadataFailure(
    checkpointLocation: String,
    batchId: Long)
  extends StateDataSourceException(
    "STDS_FAILED_TO_READ_OPERATOR_METADATA",
    Map("checkpointLocation" -> checkpointLocation, "batchId" -> batchId.toString),
    cause = null)
