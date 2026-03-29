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

import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.checkpointing.CheckpointFileManager
import org.apache.spark.sql.execution.streaming.operators.stateful.join.StreamingSymmetricHashJoinHelper.{JoinSide, LeftSide}
import org.apache.spark.sql.execution.streaming.operators.stateful.join.SymmetricHashJoinStateManager
import org.apache.spark.sql.execution.streaming.state.{StateSchemaCompatibilityChecker, StateStore, StateStoreId, StateStoreProviderId}
import org.apache.spark.sql.types.{BooleanType, StructType}

/**
 * A helper object to read the state schema for stream-stream join.
 *
 * The parameter `excludeAuxColumns` in methods represents whether the result schema should
 * include the columns the operator added in addition to the input schema.
 */
object StreamStreamJoinStateHelper {
  def readSchema(
      session: SparkSession,
      stateCheckpointLocation: String,
      operatorId: Int,
      side: JoinSide,
      oldSchemaFilePaths: List[Path],
      excludeAuxColumns: Boolean = true,
      joinStateFormatVersion: Option[Int] = None): StructType = {
    val (keySchema, valueSchema) = readKeyValueSchema(session, stateCheckpointLocation,
      operatorId, side, oldSchemaFilePaths, excludeAuxColumns, joinStateFormatVersion)

    new StructType()
      .add("key", keySchema)
      .add("value", valueSchema)
  }

  // Returns whether the checkpoint uses VCF for the join (stateFormatVersion >= 3).
  def usesVirtualColumnFamilies(
    hadoopConf: Configuration,
    stateCheckpointLocation: String,
    operatorId: Int): Boolean = {
    // If the schema exists for operatorId/partitionId/left-keyToNumValues, it is not
    // stateFormatVersion >= 3 (which uses VCF).
    val partitionId = StateStore.PARTITION_ID_TO_CHECK_SCHEMA
    val storeId = new StateStoreId(stateCheckpointLocation, operatorId,
      partitionId, SymmetricHashJoinStateManager.allStateStoreNames(LeftSide).toList.head)
    val schemaFilePath = StateSchemaCompatibilityChecker.schemaFile(
      storeId.storeCheckpointLocation())
    val fm = CheckpointFileManager.create(schemaFilePath, hadoopConf)
    !fm.exists(schemaFilePath)
  }

  def readKeyValueSchema(
      session: SparkSession,
      stateCheckpointLocation: String,
      operatorId: Int,
      side: JoinSide,
      oldSchemaFilePaths: List[Path],
      excludeAuxColumns: Boolean = true,
      joinStateFormatVersion: Option[Int] = None): (StructType, StructType) = {

    val newHadoopConf = session.sessionState.newHadoopConf()
    val partitionId = StateStore.PARTITION_ID_TO_CHECK_SCHEMA

    val (keySchema, valueSchema) = joinStateFormatVersion match {
      case Some(1) | Some(2) | None =>
        // v1/v2: separate state stores per store type.
        // None handles old checkpoints without operator metadata (always v1/v2).
        val storeNames = SymmetricHashJoinStateManager.allStateStoreNames(side).toList
        val storeIdForKeyToNumValues = new StateStoreId(stateCheckpointLocation, operatorId,
          partitionId, storeNames(0))
        val providerIdForKeyToNumValues = new StateStoreProviderId(storeIdForKeyToNumValues,
          UUID.randomUUID())

        val storeIdForKeyWithIndexToValue = new StateStoreId(stateCheckpointLocation,
          operatorId, partitionId, storeNames(1))
        val providerIdForKeyWithIndexToValue = new StateStoreProviderId(
          storeIdForKeyWithIndexToValue, UUID.randomUUID())

        val manager = new StateSchemaCompatibilityChecker(
          providerIdForKeyToNumValues, newHadoopConf, oldSchemaFilePaths,
          createSchemaDir = false)
        val kSchema = manager.readSchemaFile().head.keySchema

        val manager2 = new StateSchemaCompatibilityChecker(providerIdForKeyWithIndexToValue,
          newHadoopConf, oldSchemaFilePaths, createSchemaDir = false)
        val vSchema = manager2.readSchemaFile().head.valueSchema

        (kSchema, vSchema)

      case Some(3) =>
        // v3: single state store with virtual column families
        val storeNames = SymmetricHashJoinStateManager.allStateStoreNames(side).toList
        val storeId = new StateStoreId(stateCheckpointLocation, operatorId,
          partitionId, StateStoreId.DEFAULT_STORE_NAME)
        val providerId = new StateStoreProviderId(storeId, UUID.randomUUID())

        val manager = new StateSchemaCompatibilityChecker(
          providerId, newHadoopConf, oldSchemaFilePaths, createSchemaDir = false)
        val schemas = manager.readSchemaFile()

        val kSchema = schemas.find(_.colFamilyName == storeNames(0)).map(_.keySchema).get
        val vSchema = schemas.find(_.colFamilyName == storeNames(1)).map(_.valueSchema).get

        (kSchema, vSchema)

      case Some(4) =>
        // v4: single state store with virtual column families, timestamp-based keys
        val v4Names = SymmetricHashJoinStateManager.allStateStoreNamesV4(side).toList
        val storeId = new StateStoreId(stateCheckpointLocation, operatorId,
          partitionId, StateStoreId.DEFAULT_STORE_NAME)
        val providerId = new StateStoreProviderId(storeId, UUID.randomUUID())

        val manager = new StateSchemaCompatibilityChecker(
          providerId, newHadoopConf, oldSchemaFilePaths, createSchemaDir = false)
        val schemas = manager.readSchemaFile()

        // In v4, the primary CF (keyWithTsToValues) stores both the key and value schemas.
        // This differs from v3 where keyToNumValues has the key schema and
        // keyWithIndexToValue has the value schema.
        val primaryCF = v4Names(0)
        val kSchema = schemas.find(_.colFamilyName == primaryCF).map(_.keySchema).get
        val vSchema = schemas.find(_.colFamilyName == primaryCF).map(_.valueSchema).get

        (kSchema, vSchema)

      case Some(v) =>
        throw new IllegalArgumentException(
          s"Unsupported join state format version: $v")
    }

    val maybeMatchedColumn = valueSchema.last

    // remove internal column `matched` for format version >= 2
    if (excludeAuxColumns
      && maybeMatchedColumn.name == "matched"
      && maybeMatchedColumn.dataType == BooleanType) {
      (keySchema, StructType(valueSchema.dropRight(1)))
    } else {
      (keySchema, valueSchema)
    }
  }
}
