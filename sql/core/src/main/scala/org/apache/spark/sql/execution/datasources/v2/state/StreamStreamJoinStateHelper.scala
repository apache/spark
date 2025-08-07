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
import org.apache.spark.sql.execution.streaming.CheckpointFileManager
import org.apache.spark.sql.execution.streaming.StreamingSymmetricHashJoinHelper.{JoinSide, LeftSide}
import org.apache.spark.sql.execution.streaming.state.{StateSchemaCompatibilityChecker, StateStore, StateStoreId, StateStoreProviderId, SymmetricHashJoinStateManager}
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

  // Returns whether the checkpoint uses stateFormatVersion 3 which uses VCF for the join.
  def usesVirtualColumnFamilies(
    hadoopConf: Configuration,
    stateCheckpointLocation: String,
    operatorId: Int): Boolean = {
    // If the schema exists for operatorId/partitionId/left-keyToNumValues, it is not
    // stateFormatVersion 3.
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
    // KeyToNumValuesType, KeyWithIndexToValueType
    val storeNames = SymmetricHashJoinStateManager.allStateStoreNames(side).toList

    val (keySchema, valueSchema) =
      if (!usesVirtualColumnFamilies(
        newHadoopConf, stateCheckpointLocation, operatorId)) {
        val storeIdForKeyToNumValues = new StateStoreId(stateCheckpointLocation, operatorId,
          partitionId, storeNames(0))
        val providerIdForKeyToNumValues = new StateStoreProviderId(storeIdForKeyToNumValues,
          UUID.randomUUID())

        val storeIdForKeyWithIndexToValue = new StateStoreId(stateCheckpointLocation,
          operatorId, partitionId, storeNames(1))
        val providerIdForKeyWithIndexToValue = new StateStoreProviderId(
          storeIdForKeyWithIndexToValue, UUID.randomUUID())

        // read the key schema from the keyToNumValues store for the join keys
        val manager = new StateSchemaCompatibilityChecker(
          providerIdForKeyToNumValues, newHadoopConf, oldSchemaFilePaths)
        val kSchema = manager.readSchemaFile().head.keySchema

        // read the value schema from the keyWithIndexToValue store for the values
        val manager2 = new StateSchemaCompatibilityChecker(providerIdForKeyWithIndexToValue,
          newHadoopConf, oldSchemaFilePaths)
        val vSchema = manager2.readSchemaFile().head.valueSchema

        (kSchema, vSchema)
      } else {
        val storeId = new StateStoreId(stateCheckpointLocation, operatorId,
          partitionId, StateStoreId.DEFAULT_STORE_NAME)
        val providerId = new StateStoreProviderId(storeId, UUID.randomUUID())

        val manager = new StateSchemaCompatibilityChecker(
          providerId, newHadoopConf, oldSchemaFilePaths)
        val kSchema = manager.readSchemaFile().find { schema =>
          schema.colFamilyName == storeNames(0)
        }.map(_.keySchema).get

        val vSchema = manager.readSchemaFile().find { schema =>
          schema.colFamilyName == storeNames(1)
        }.map(_.valueSchema).get

        (kSchema, vSchema)
      }

    val maybeMatchedColumn = valueSchema.last

    if (excludeAuxColumns
      && maybeMatchedColumn.name == "matched"
      && maybeMatchedColumn.dataType == BooleanType) {
      // remove internal column `matched` for format version 2
      (keySchema, StructType(valueSchema.dropRight(1)))
    } else {
      (keySchema, valueSchema)
    }
  }
}
