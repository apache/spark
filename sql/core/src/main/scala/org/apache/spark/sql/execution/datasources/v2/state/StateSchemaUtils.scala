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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeRow}
import org.apache.spark.sql.execution.streaming.{StateVariableType, TransformWithStateVariableInfo}
import org.apache.spark.sql.execution.streaming.state.StateStoreColFamilySchema
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}

/**
 * Utility functions to generate and process schema for state store data source.
 */
object StateSchemaUtils {

  private def generateSchemaForStateVar(
      stateVarInfo: TransformWithStateVariableInfo,
      stateStoreColFamilySchema: StateStoreColFamilySchema): StructType = {
    val stateVarType = stateVarInfo.stateVariableType
    val hasTTLEnabled = stateVarInfo.ttlEnabled

    stateVarType match {
      case StateVariableType.ValueState =>
        if (hasTTLEnabled) {
          new StructType()
            .add("key", stateStoreColFamilySchema.keySchema)
            .add("value", stateStoreColFamilySchema.valueSchema)
            .add("expiration_timestamp", LongType)
            .add("partition_id", IntegerType)
        } else {
          new StructType()
            .add("key", stateStoreColFamilySchema.keySchema)
            .add("value", stateStoreColFamilySchema.valueSchema)
            .add("partition_id", IntegerType)
        }

      case _ =>
        throw StateDataSourceErrors.internalError(s"Unsupported state variable type $stateVarType")
    }
  }

  def getSourceSchema(
      sourceOptions: StateSourceOptions,
      keySchema: StructType,
      valueSchema: StructType,
      transformWithStateVariableInfoOpt: Option[TransformWithStateVariableInfo],
      stateStoreColFamilySchemaOpt: Option[StateStoreColFamilySchema]): StructType = {
    if (sourceOptions.readChangeFeed) {
      new StructType()
        .add("batch_id", LongType)
        .add("change_type", StringType)
        .add("key", keySchema)
        .add("value", valueSchema)
        .add("partition_id", IntegerType)
    } else if (transformWithStateVariableInfoOpt.isDefined) {
      require(stateStoreColFamilySchemaOpt.isDefined)
      generateSchemaForStateVar(transformWithStateVariableInfoOpt.get,
        stateStoreColFamilySchemaOpt.get)
    } else {
      new StructType()
        .add("key", keySchema)
        .add("value", valueSchema)
        .add("partition_id", IntegerType)
    }
  }

  def unifyStateRowPair(pair: (UnsafeRow, UnsafeRow), partition: Int): InternalRow = {
    val row = new GenericInternalRow(3)
    row.update(0, pair._1)
    row.update(1, pair._2)
    row.update(2, partition)
    row
  }

  def unifyStateRowPairWithTTL(
      pair: (UnsafeRow, UnsafeRow),
      valueSchema: StructType,
      partition: Int): InternalRow = {
    val row = new GenericInternalRow(4)
    row.update(0, pair._1)
    row.update(1, pair._2.get(0, valueSchema))
    row.update(2, pair._2.get(1, LongType))
    row.update(3, partition)
    row
  }
}
