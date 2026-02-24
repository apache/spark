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

import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkIllegalStateException, SparkThrowable, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.v2.state.StateSourceOptions
import org.apache.spark.sql.execution.datasources.v2.state.metadata.StateMetadataPartitionReader
import org.apache.spark.sql.execution.streaming.checkpointing.OffsetSeqMetadata
import org.apache.spark.sql.execution.streaming.operators.stateful.{StatefulOperatorStateInfo, StatefulOperatorsUtils}
import org.apache.spark.sql.execution.streaming.operators.stateful.transformwithstate.{StateVariableType, TransformWithStateOperatorProperties, TransformWithStateVariableInfo}
import org.apache.spark.sql.execution.streaming.runtime.{StreamingCheckpointConstants, StreamingQueryCheckpointMetadata}
import org.apache.spark.sql.execution.streaming.state.{StatePartitionAllColumnFamiliesWriter, StateSchemaCompatibilityChecker}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.{SerializableConfiguration, Utils}

/**
 * State Rewriter is used to rewrite the state stores for a stateful streaming query.
 * It reads state from a checkpoint location, optionally applies transformation to the state,
 * and then writes the state back to a (possibly different) checkpoint location for a new batch ID.
 *
 * Example use case is for offline state repartitioning.
 * Can also be used to support other use cases.
 *
 * @param sparkSession The active Spark session.
 * @param readBatchId The batch ID for reading state.
 * @param writeBatchId The batch ID to which the (transformed) state will be written.
 * @param resolvedCheckpointLocation The resolved checkpoint path where state will be written.
 * @param hadoopConf Hadoop configuration for file system operations.
 * @param readResolvedCheckpointLocation Optional separate checkpoint location to read state from.
 *                                       If None, reads from resolvedCheckpointLocation.
 * @param transformFunc Optional transformation function applied to each operator's state
 *                      DataFrame. If None, state is written as-is.
 * @param writeCheckpointMetadata Optional checkpoint metadata for the resolvedCheckpointLocation.
 *                                If None, will create a new one for resolvedCheckpointLocation.
 *                                Helps us to reuse already cached checkpoint log entries,
 *                                instead of starting from scratch.
 */
class StateRewriter(
    sparkSession: SparkSession,
    readBatchId: Long,
    writeBatchId: Long,
    resolvedCheckpointLocation: String,
    hadoopConf: Configuration,
    readResolvedCheckpointLocation: Option[String] = None,
    transformFunc: Option[DataFrame => DataFrame] = None,
    writeCheckpointMetadata: Option[StreamingQueryCheckpointMetadata] = None
) extends Logging {
  require(readResolvedCheckpointLocation.isDefined || readBatchId < writeBatchId,
    s"Read batch id $readBatchId must be less than write batch id $writeBatchId " +
      "when reading and writing to the same checkpoint location")

  // If a different location was specified for reading state, use it.
  // Else, use same location for reading and writing state.
  private val checkpointLocationForRead =
    readResolvedCheckpointLocation.getOrElse(resolvedCheckpointLocation)
  private val stateRootLocation = new Path(
    resolvedCheckpointLocation, StreamingCheckpointConstants.DIR_NAME_STATE).toString
  private lazy val writeCheckpoint = writeCheckpointMetadata.getOrElse(
    new StreamingQueryCheckpointMetadata(sparkSession, resolvedCheckpointLocation))
  private lazy val readCheckpoint = if (readResolvedCheckpointLocation.isDefined) {
    new StreamingQueryCheckpointMetadata(sparkSession, readResolvedCheckpointLocation.get)
  } else {
    // Same checkpoint for read & write
    writeCheckpoint
  }

  // If checkpoint id is enabled, return
  // Map[operatorId, Array[partition -> Array[stateStore -> StateStoreCheckpointId]]].
  // Otherwise, return None
  def run(): Option[Map[Long, Array[Array[String]]]] = {
    logInfo(log"Starting state rewrite for " +
      log"checkpointLocation=${MDC(CHECKPOINT_LOCATION, resolvedCheckpointLocation)}, " +
      log"readCheckpointLocation=" +
      log"${MDC(CHECKPOINT_LOCATION, readResolvedCheckpointLocation.getOrElse(""))}, " +
      log"readBatchId=${MDC(BATCH_ID, readBatchId)}, " +
      log"writeBatchId=${MDC(BATCH_ID, writeBatchId)}")

    val (checkpointIds, timeTakenMs) = Utils.timeTakenMs {
      runInternal()
    }

    logInfo(log"State rewrite completed in ${MDC(DURATION, timeTakenMs)} ms for " +
      log"checkpointLocation=${MDC(CHECKPOINT_LOCATION, resolvedCheckpointLocation)}")
    checkpointIds
  }

  private def extractCheckpointIdsPerPartition(
      checkpointInfos: Map[Long, Array[Array[StateStoreCheckpointInfo]]]
    ): Option[Map[Long, Array[Array[String]]]] = {
    val enableCheckpointId = StatefulOperatorStateInfo.
      enableStateStoreCheckpointIds(sparkSession.sessionState.conf)
    if (!enableCheckpointId) {
      return None
    }
    // convert Map[operatorId, Array[stateStore -> Array[partition -> StateStoreCheckpointInfo]]]
    // to Map[operatorId, Array[partition -> Array[stateStore -> StateStoreCheckpointId]]].
    Option(checkpointInfos.map {
      case(operator, storesSeq) =>
        val numPartitions = storesSeq.head.length
        operator -> (0 until numPartitions).map { partitionIdx =>
          storesSeq.map { storePartitions =>
            val checkpointInfoPerPartition = storePartitions(partitionIdx)
            assert(checkpointInfoPerPartition.partitionId == partitionIdx)
            assert(checkpointInfoPerPartition.batchVersion == writeBatchId + 1)
            // expect baseStateStoreCkptId empty because we didn't load
            // any previous stores when doing the rewrite
            assert(checkpointInfoPerPartition.baseStateStoreCkptId.isEmpty)
            checkpointInfoPerPartition.stateStoreCkptId.get
          }
        }.toArray
    })
  }

  private def runInternal(): Option[Map[Long, Array[Array[String]]]] = {
    try {
      verifyCheckpointFormatVersion()
      val stateMetadataReader = new StateMetadataPartitionReader(
        resolvedCheckpointLocation,
        new SerializableConfiguration(hadoopConf),
        readBatchId)

      val allOperatorsMetadata = stateMetadataReader.allOperatorStateMetadata
      if (allOperatorsMetadata.isEmpty) {
        // Its possible that the query is stateless
        // or ran on older spark version without op metadata
        throw StateRewriterErrors.missingOperatorMetadataError(
          resolvedCheckpointLocation, readBatchId)
      }

      // Use the same conf in the offset log to create the store conf,
      // to make sure the state is written with the right conf.
      val (storeConf, sqlConf) = createConfsFromOffsetLog()

      // A Hadoop Configuration can be about 10 KB, which is pretty big, so broadcast it
      val hadoopConfBroadcast =
        SerializableConfiguration.broadcast(sparkSession.sparkContext, hadoopConf)

      // Do rewrite for each operator
      // We can potentially parallelize this, but for now, do sequentially
      val checkpointInfos = allOperatorsMetadata.map { opMetadata =>
        val stateStoresMetadata = opMetadata.stateStoresMetadata
        assert(!stateStoresMetadata.isEmpty,
          s"Operator ${opMetadata.operatorInfo.operatorName} has no state stores")

        val storeToSchemaFilesMap = getStoreToSchemaFilesMap(opMetadata)
        val stateVarsIfTws = getStateVariablesIfTWS(opMetadata)

        // Rewrite each state store of the operator
        val checkpointInfo = stateStoresMetadata.map { stateStoreMetadata =>
          rewriteStore(
            opMetadata,
            stateStoreMetadata,
            storeConf,
            hadoopConfBroadcast,
            storeToSchemaFilesMap(stateStoreMetadata.storeName),
            stateVarsIfTws
          )
        }.toArray
        opMetadata.operatorInfo.operatorId -> checkpointInfo
      }.toMap
      extractCheckpointIdsPerPartition(checkpointInfos)
    } catch {
      case e: Throwable =>
        logError(log"State rewrite failed for " +
          log"checkpointLocation=${MDC(CHECKPOINT_LOCATION, resolvedCheckpointLocation)}, " +
          log"readBatchId=${MDC(BATCH_ID, readBatchId)}, " +
          log"writeBatchId=${MDC(BATCH_ID, writeBatchId)}", e)
        throw e
    }
  }

  private def rewriteStore(
      opMetadata: OperatorStateMetadata,
      stateStoreMetadata: StateStoreMetadata,
      storeConf: StateStoreConf,
      hadoopConfBroadcast: Broadcast[SerializableConfiguration],
      storeSchemaFiles: List[Path],
      stateVarsIfTws: Map[String, TransformWithStateVariableInfo]
  ): Array[StateStoreCheckpointInfo] = {
    // Read state
    val stateDf = sparkSession.read
      .format("statestore")
      .option(StateSourceOptions.PATH, checkpointLocationForRead)
      .option(StateSourceOptions.BATCH_ID, readBatchId)
      .option(StateSourceOptions.OPERATOR_ID, opMetadata.operatorInfo.operatorId)
      .option(StateSourceOptions.STORE_NAME, stateStoreMetadata.storeName)
      .option(StateSourceOptions.INTERNAL_ONLY_READ_ALL_COLUMN_FAMILIES, "true")
      .load()

    // Run the caller state transformation func if provided
    // Otherwise, use the state as is
    val updatedStateDf = transformFunc.map(func => func(stateDf)).getOrElse(stateDf)
    require(updatedStateDf.schema == stateDf.schema,
      s"State transformation function must return a DataFrame with the same schema " +
        s"as the original state DataFrame. Original schema: ${stateDf.schema}, " +
        s"Updated schema: ${updatedStateDf.schema}")

    val schemaProvider = createStoreSchemaProviderIfTWS(
      opMetadata.operatorInfo.operatorName,
      storeSchemaFiles
    )
    val writerColFamilyInfoMap = getWriterColFamilyInfoMap(
      opMetadata.operatorInfo.operatorId,
      stateStoreMetadata,
      storeSchemaFiles,
      stateVarsIfTws
    )

    logInfo(log"Writing new state for " +
      log"operator=${MDC(OP_TYPE, opMetadata.operatorInfo.operatorName)}, " +
      log"stateStore=${MDC(STATE_NAME, stateStoreMetadata.storeName)}, " +
      log"numColumnFamilies=${MDC(COUNT, writerColFamilyInfoMap.size)}, " +
      log"numSchemaFiles=${MDC(NUM_FILES, storeSchemaFiles.size)}, " +
      log"for new batch=${MDC(BATCH_ID, writeBatchId)}, " +
      log"for checkpoint=${MDC(CHECKPOINT_LOCATION, resolvedCheckpointLocation)}")

    // Write state for each partition on the executor.
    // Setting this as local val,
    // to avoid serializing the entire Rewriter object per partition.
    val targetCheckpointLocation = resolvedCheckpointLocation
    val currentBatchId = writeBatchId
    updatedStateDf.queryExecution.toRdd.mapPartitions { partitionIter: Iterator[InternalRow] =>
      val partitionWriter = new StatePartitionAllColumnFamiliesWriter(
        storeConf,
        hadoopConfBroadcast.value.value,
        TaskContext.get().partitionId(),
        targetCheckpointLocation,
        opMetadata.operatorInfo.operatorId,
        stateStoreMetadata.storeName,
        currentBatchId,
        writerColFamilyInfoMap,
        schemaProvider
      )
      Iterator(partitionWriter.write(partitionIter))
    }.collect()
  }

  /** Create the store and sql confs from the conf written in the offset log */
  private def createConfsFromOffsetLog(): (StateStoreConf, SQLConf) = {
    val offsetLog = writeCheckpointMetadata.getOrElse(
      new StreamingQueryCheckpointMetadata(sparkSession, resolvedCheckpointLocation)).offsetLog

    // We want to use the same confs written in the offset log for the new batch
    val offsetSeq = offsetLog.get(writeBatchId)
    require(offsetSeq.isDefined, s"Offset seq must be present for the new batch $writeBatchId")
    val metadata = offsetSeq.get.metadataOpt
    require(metadata.isDefined, s"Metadata must be present for the new batch $writeBatchId")

    val clonedSqlConf = sparkSession.sessionState.conf.clone()
    OffsetSeqMetadata.setSessionConf(metadata.get, clonedSqlConf)
    (StateStoreConf(clonedSqlConf), clonedSqlConf)
  }

  /** Get the map of state store name to schema files, for an operator */
  private def getStoreToSchemaFilesMap(
      opMetadata: OperatorStateMetadata): Map[String, List[Path]] = {
    opMetadata.stateStoresMetadata.map { storeMetadata =>
      val schemaFiles = storeMetadata match {
        // No schema files for v1. It has a fixed/known schema file path
        case _: StateStoreMetadataV1 => List.empty[Path]
        case v2: StateStoreMetadataV2 => v2.stateSchemaFilePaths.map(new Path(_))
        case _ =>
          throw StateRewriterErrors.unsupportedStateStoreMetadataVersionError(
            resolvedCheckpointLocation)
      }
      storeMetadata.storeName -> schemaFiles
    }.toMap
  }

  private def getWriterColFamilyInfoMap(
      operatorId: Long,
      storeMetadata: StateStoreMetadata,
      schemaFiles: List[Path],
      twsStateVariables: Map[String, TransformWithStateVariableInfo] = Map.empty
  ): Map[String, StatePartitionWriterColumnFamilyInfo] = {
    getLatestColFamilyToSchemaMap(operatorId, storeMetadata, schemaFiles)
      .map { case (colFamilyName, schema) =>
        colFamilyName -> StatePartitionWriterColumnFamilyInfo(schema,
          useMultipleValuesPerKey = twsStateVariables.get(colFamilyName)
            .map(_.stateVariableType == StateVariableType.ListState).getOrElse(false))
      }
  }

  private def getLatestColFamilyToSchemaMap(
      operatorId: Long,
      storeMetadata: StateStoreMetadata,
      schemaFiles: List[Path]): Map[String, StateStoreColFamilySchema] = {
    val storeId = new StateStoreId(
      stateRootLocation,
      operatorId,
      StateStore.PARTITION_ID_TO_CHECK_SCHEMA,
      storeMetadata.storeName)
    // using a placeholder runId since we are not running a streaming query
    val providerId = new StateStoreProviderId(storeId, queryRunId = UUID.randomUUID())
    val manager = new StateSchemaCompatibilityChecker(providerId, hadoopConf,
      oldSchemaFilePaths = schemaFiles)
    // Read the latest state schema from the provided path for v2 or from the dedicated path
    // for v1
    manager
      .readSchemaFile()
      .map { schema =>
        schema.colFamilyName -> createKeyEncoderSpecIfAbsent(schema, storeMetadata) }.toMap
  }

  private def createKeyEncoderSpecIfAbsent(
      colFamilySchema: StateStoreColFamilySchema,
      storeMetadata: StateStoreMetadata): StateStoreColFamilySchema = {
    colFamilySchema.keyStateEncoderSpec match {
      case Some(encoderSpec) => colFamilySchema
      case None if storeMetadata.isInstanceOf[StateStoreMetadataV1] =>
        // Create the spec if missing for v1 metadata
        if (storeMetadata.numColsPrefixKey > 0) {
          colFamilySchema.copy(keyStateEncoderSpec =
            Some(PrefixKeyScanStateEncoderSpec(colFamilySchema.keySchema,
              storeMetadata.numColsPrefixKey)))
        } else {
          colFamilySchema.copy(keyStateEncoderSpec =
            Some(NoPrefixKeyStateEncoderSpec(colFamilySchema.keySchema)))
        }
      case _ =>
        // Key encoder spec is expected in v2 metadata
        throw StateRewriterErrors.missingKeyEncoderSpecError(
          resolvedCheckpointLocation, colFamilySchema.colFamilyName)
    }
  }

  private def getStateVariablesIfTWS(
      opMetadata: OperatorStateMetadata): Map[String, TransformWithStateVariableInfo] = {
    if (StatefulOperatorsUtils.TRANSFORM_WITH_STATE_OP_NAMES
      .contains(opMetadata.operatorInfo.operatorName)) {
      val operatorProperties = TransformWithStateOperatorProperties.fromJson(
        opMetadata.asInstanceOf[OperatorStateMetadataV2].operatorPropertiesJson)
      operatorProperties.stateVariables.map(s => s.stateName -> s).toMap
    } else {
      Map.empty
    }
  }

  // Needed only for schema evolution for TWS
  private def createStoreSchemaProviderIfTWS(
      opName: String,
      schemaFiles: List[Path]): Option[StateSchemaProvider] = {
    if (StatefulOperatorsUtils.TRANSFORM_WITH_STATE_OP_NAMES.contains(opName)) {
      val schemaMetadata = StateSchemaMetadata.createStateSchemaMetadata(
        stateRootLocation, hadoopConf, schemaFiles.map(_.toString))
      Some(new InMemoryStateSchemaProvider(schemaMetadata))
    } else {
      None
    }
  }

  private def verifyCheckpointFormatVersion(): Unit = {
    // Verify checkpoint version in sqlConf based on commitLog for readCheckpoint
    // in case user forgot to set STATE_STORE_CHECKPOINT_FORMAT_VERSION.
    // Using read batch commit since the latest commit could be a skipped batch.
    // If SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION is wrong, readCheckpoint.commitLog
    // will throw an exception, and we will propagate this exception upstream.
    // This prevents the StateRewriter from failing to write the correct state files
    try {
      readCheckpoint.commitLog.get(readBatchId)
    } catch {
        case e: IllegalStateException if e.getCause != null &&
            e.getCause.isInstanceOf[SparkThrowable] =>
          val sparkThrowable = e.getCause.asInstanceOf[SparkThrowable]
          if (sparkThrowable.getCondition == "INVALID_LOG_VERSION.EXACT_MATCH_VERSION") {
            val params = sparkThrowable.getMessageParameters
            val expectedVersion = params.get("version")
            val actualVersion = params.get("matchVersion")
            throw StateRewriterErrors.stateCheckpointFormatVersionMismatchError(
              checkpointLocationForRead, expectedVersion, actualVersion)
          }
          throw e
      }
  }
}

/**
 * Errors thrown by StateRewriter.
 */
private[state] object StateRewriterErrors {
  def missingKeyEncoderSpecError(
      checkpointLocation: String,
      colFamilyName: String): StateRewriterInvalidCheckpointError = {
    new StateRewriterMissingKeyEncoderSpecError(checkpointLocation, colFamilyName)
  }

  def missingOperatorMetadataError(
      checkpointLocation: String,
      batchId: Long): StateRewriterInvalidCheckpointError = {
    new StateRewriterMissingOperatorMetadataError(checkpointLocation, batchId)
  }

  def unsupportedStateStoreMetadataVersionError(
      checkpointLocation: String): StateRewriterInvalidCheckpointError = {
    new StateRewriterUnsupportedStoreMetadataVersionError(checkpointLocation)
  }

  def stateCheckpointFormatVersionMismatchError(
      checkpointLocation: String,
      expectedVersion: String,
      actualVersion: String): StateRewriterInvalidCheckpointError = {
    new StateRewriterStateCheckpointFormatVersionMismatchError(
      checkpointLocation, expectedVersion, actualVersion)
  }
}

/**
 * Base class for exceptions thrown when the checkpoint location is in an invalid state
 * for state rewriting.
 */
private[state] abstract class StateRewriterInvalidCheckpointError(
    checkpointLocation: String,
    subClass: String,
    messageParameters: Map[String, String],
    cause: Throwable = null)
  extends SparkIllegalStateException(
    errorClass = s"STATE_REWRITER_INVALID_CHECKPOINT.$subClass",
    messageParameters = Map("checkpointLocation" -> checkpointLocation) ++ messageParameters,
    cause = cause)

private[state] class StateRewriterMissingKeyEncoderSpecError(
    checkpointLocation: String,
    colFamilyName: String)
  extends StateRewriterInvalidCheckpointError(
    checkpointLocation,
    subClass = "MISSING_KEY_ENCODER_SPEC",
    messageParameters = Map("colFamilyName" -> colFamilyName))

private[state] class StateRewriterMissingOperatorMetadataError(
    checkpointLocation: String,
    batchId: Long)
  extends StateRewriterInvalidCheckpointError(
    checkpointLocation,
    subClass = "MISSING_OPERATOR_METADATA",
    messageParameters = Map("batchId" -> batchId.toString))

private[state] class StateRewriterUnsupportedStoreMetadataVersionError(
    checkpointLocation: String)
  extends StateRewriterInvalidCheckpointError(
    checkpointLocation,
    subClass = "UNSUPPORTED_STATE_STORE_METADATA_VERSION",
    messageParameters = Map.empty)

private[state] class StateRewriterStateCheckpointFormatVersionMismatchError(
    checkpointLocation: String,
    expectedVersion: String,
    actualVersion: String)
  extends StateRewriterInvalidCheckpointError(
    checkpointLocation,
    subClass = "STATE_CHECKPOINT_FORMAT_VERSION_MISMATCH",
    messageParameters = Map(
      "expectedVersion" -> expectedVersion,
      "actualVersion" -> actualVersion,
      "sqlConfKey" -> SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key))
