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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.StreamingSymmetricHashJoinHelper.JoinSide
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
      excludeAuxColumns: Boolean = true): StructType = {
    val (keySchema, valueSchema) = readKeyValueSchema(session, stateCheckpointLocation,
      operatorId, side, excludeAuxColumns)

    new StructType()
      .add("key", keySchema)
      .add("value", valueSchema)
  }

  def readKeyValueSchema(
      session: SparkSession,
      stateCheckpointLocation: String,
      operatorId: Int,
      side: JoinSide,
      excludeAuxColumns: Boolean = true): (StructType, StructType) = {

    // KeyToNumValuesType, KeyWithIndexToValueType
    val storeNames = SymmetricHashJoinStateManager.allStateStoreNames(side).toList

    val partitionId = StateStore.PARTITION_ID_TO_CHECK_SCHEMA
    val storeIdForKeyToNumValues = new StateStoreId(stateCheckpointLocation, operatorId,
      partitionId, storeNames(0))
    val providerIdForKeyToNumValues = new StateStoreProviderId(storeIdForKeyToNumValues,
      UUID.randomUUID())

    val storeIdForKeyWithIndexToValue = new StateStoreId(stateCheckpointLocation,
      operatorId, partitionId, storeNames(1))
    val providerIdForKeyWithIndexToValue = new StateStoreProviderId(storeIdForKeyWithIndexToValue,
      UUID.randomUUID())

    val newHadoopConf = session.sessionState.newHadoopConf()

    // read the key schema from the keyToNumValues store for the join keys
    val manager = new StateSchemaCompatibilityChecker(providerIdForKeyToNumValues, newHadoopConf)
    val keySchema = manager.readSchemaFile().head.keySchema

    // read the value schema from the keyWithIndexToValue store for the values
    val manager2 = new StateSchemaCompatibilityChecker(providerIdForKeyWithIndexToValue,
      newHadoopConf)
    val valueSchema = manager2.readSchemaFile().head.valueSchema

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
