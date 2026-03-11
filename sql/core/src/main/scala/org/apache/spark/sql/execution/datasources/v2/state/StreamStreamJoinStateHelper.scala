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
      excludeAuxColumns: Boolean = true): StructType = {
    val (keySchema, valueSchema) = readKeyValueSchema(session, stateCheckpointLocation,
      operatorId, side, oldSchemaFilePaths, excludeAuxColumns)

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
      excludeAuxColumns: Boolean = true): (StructType, StructType) = {

    val newHadoopConf = session.sessionState.newHadoopConf()
    val partitionId = StateStore.PARTITION_ID_TO_CHECK_SCHEMA
    val storeNames = SymmetricHashJoinStateManager.allStateStoreNames(side).toList

    val (keySchema, valueSchema) =
      if (!usesVirtualColumnFamilies(
        newHadoopConf, stateCheckpointLocation, operatorId)) {
        // v1/v2: separate state stores per store type
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
      } else {
        // v3/v4: single state store with virtual column families
        val storeId = new StateStoreId(stateCheckpointLocation, operatorId,
          partitionId, StateStoreId.DEFAULT_STORE_NAME)
        val providerId = new StateStoreProviderId(storeId, UUID.randomUUID())

        val manager = new StateSchemaCompatibilityChecker(
          providerId, newHadoopConf, oldSchemaFilePaths, createSchemaDir = false)
        val schemas = manager.readSchemaFile()

        // Try v3 CF names first; if not found, use v4 CF names
        val v3Names = storeNames
        val v4Names = SymmetricHashJoinStateManager.allStateStoreNamesV4(side).toList

        val primaryCfName = schemas.find(_.colFamilyName == v3Names(1)) match {
          case Some(_) => v3Names(1) // v3: keyWithIndexToValue
          case None => v4Names(0)    // v4: keyWithTsToValues
        }
        val keyCfName = schemas.find(_.colFamilyName == v3Names(0)) match {
          case Some(_) => v3Names(0) // v3: keyToNumValues
          case None => v4Names(0)    // v4: keyWithTsToValues (key schema is the join key)
        }

        val kSchema = schemas.find(_.colFamilyName == keyCfName).map(_.keySchema).get
        val vSchema = schemas.find(_.colFamilyName == primaryCfName).map(_.valueSchema).get

        (kSchema, vSchema)
      }

    val maybeMatchedColumn = valueSchema.last

    if (excludeAuxColumns
      && maybeMatchedColumn.name == "matched"
      && maybeMatchedColumn.dataType == BooleanType) {
      (keySchema, StructType(valueSchema.dropRight(1)))
    } else {
      (keySchema, valueSchema)
    }
  }
}
