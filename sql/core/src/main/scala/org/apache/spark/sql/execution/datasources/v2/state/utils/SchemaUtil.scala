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
package org.apache.spark.sql.execution.datasources.v2.state.utils

import scala.collection.mutable

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeRow}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData}
import org.apache.spark.sql.execution.datasources.v2.state.{StateDataSourceErrors, StateSourceOptions}
import org.apache.spark.sql.execution.streaming.StateVariableType._
import org.apache.spark.sql.execution.streaming.TransformWithStateVariableInfo
import org.apache.spark.sql.execution.streaming.state.{StateStoreColFamilySchema, UnsafeRowPair}
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, MapType, StringType, StructType}
import org.apache.spark.util.ArrayImplicits._

object SchemaUtil {
  def getSchemaAsDataType(schema: StructType, fieldName: String): DataType = {
    schema.getFieldIndex(fieldName) match {
      case Some(idx) => schema(idx).dataType
      case _ => throw new AnalysisException(
        errorClass = "_LEGACY_ERROR_TEMP_3074",
        messageParameters = Map(
          "fieldName" -> fieldName,
          "schema" -> schema.toString()))
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

  def isValidSchema(
      sourceOptions: StateSourceOptions,
      schema: StructType,
      transformWithStateVariableInfoOpt: Option[TransformWithStateVariableInfo]): Boolean = {
  val expectedTypes = Map(
      "batch_id" -> classOf[LongType],
      "change_type" -> classOf[StringType],
      "key" -> classOf[StructType],
      "value" -> classOf[StructType],
      "map_value" -> classOf[MapType],
      "partition_id" -> classOf[IntegerType],
      "expiration_timestamp" -> classOf[LongType])

    val expectedFieldNames = if (sourceOptions.readChangeFeed) {
      Seq("batch_id", "change_type", "key", "value", "partition_id")
    } else if (transformWithStateVariableInfoOpt.isDefined) {
      val stateVarInfo = transformWithStateVariableInfoOpt.get
      val hasTTLEnabled = stateVarInfo.ttlEnabled
      val stateVarType = stateVarInfo.stateVariableType

      stateVarType match {
        case ValueState =>
          if (hasTTLEnabled) {
            Seq("key", "value", "expiration_timestamp", "partition_id")
          } else {
            Seq("key", "value", "partition_id")
          }

        case MapState =>
          Seq("key", "map_value", "partition_id")

        case _ =>
          throw StateDataSourceErrors
            .internalError(s"Unsupported state variable type $stateVarType")
      }
    } else {
      Seq("key", "value", "partition_id")
    }

    if (schema.fieldNames.toImmutableArraySeq != expectedFieldNames) {
      false
    } else {
      schema.fieldNames.forall { fieldName =>
        expectedTypes(fieldName).isAssignableFrom(
          SchemaUtil.getSchemaAsDataType(schema, fieldName).getClass)
      }
    }
  }

  private def generateSchemaForStateVar(
      stateVarInfo: TransformWithStateVariableInfo,
      stateStoreColFamilySchema: StateStoreColFamilySchema): StructType = {
    val stateVarType = stateVarInfo.stateVariableType
    val hasTTLEnabled = stateVarInfo.ttlEnabled

    stateVarType match {
      case ValueState =>
        if (hasTTLEnabled) {
          val ttlValueSchema = SchemaUtil.getSchemaAsDataType(
            stateStoreColFamilySchema.valueSchema, "value").asInstanceOf[StructType]
          new StructType()
            .add("key", stateStoreColFamilySchema.keySchema)
            .add("value", ttlValueSchema)
            .add("expiration_timestamp", LongType)
            .add("partition_id", IntegerType)
        } else {
          new StructType()
            .add("key", stateStoreColFamilySchema.keySchema)
            .add("value", stateStoreColFamilySchema.valueSchema)
            .add("partition_id", IntegerType)
        }

      case MapState =>
        val groupingKeySchema = SchemaUtil.getSchemaAsDataType(
          stateStoreColFamilySchema.keySchema, "key")
        val userKeySchema = stateStoreColFamilySchema.userKeyEncoderSchema.get
        val valueMapSchema = new MapType(
          keyType = userKeySchema,
          valueType = stateStoreColFamilySchema.valueSchema,
          valueContainsNull = false
        )

        new StructType()
          .add("key", groupingKeySchema)
          .add("map_value", valueMapSchema)
          .add("partition_id", IntegerType)

      case _ =>
        throw StateDataSourceErrors.internalError(s"Unsupported state variable type $stateVarType")
    }
  }

  private def getUserKeySchema(schema: StructType): Option[StructType] = {
    try {
      Option(
        SchemaUtil.getSchemaAsDataType(schema, "map_value").asInstanceOf[MapType]
          .keyType.asInstanceOf[StructType])
    } catch {
      case _: Exception =>
        None
    }
  }

  def getCompositeKeySchema(schema: StructType): StructType = {
    val groupingKeySchema = SchemaUtil.getSchemaAsDataType(
      schema, "key").asInstanceOf[StructType]
    val userKeySchema = getUserKeySchema(schema)
    new StructType()
      .add("key", groupingKeySchema)
      .add("userKey", userKeySchema.get)
  }

  def getValueSchema(schema: StructType): StructType = {
    SchemaUtil.getSchemaAsDataType(schema, "map_value").asInstanceOf[MapType]
      .valueType.asInstanceOf[StructType]
  }

  def unifyMapStateRowPair(
      stateRows: Iterator[UnsafeRowPair],
      compositeKeySchema: StructType,
      partitionId: Int): Iterator[InternalRow] = {
    val groupingKeySchema = SchemaUtil.getSchemaAsDataType(
      compositeKeySchema, "key"
    ).asInstanceOf[StructType]
    val userKeySchema = SchemaUtil.getSchemaAsDataType(
      compositeKeySchema, "userKey"
    ).asInstanceOf[StructType]

    def appendKVPairToMap(
        curMap: mutable.Map[Any, Any],
        stateRowPair: UnsafeRowPair): Unit = {
      curMap += (
        stateRowPair.key.get(1, userKeySchema)
          .asInstanceOf[UnsafeRow].copy() ->
          stateRowPair.value.copy()
        )
    }

    def updateDataRow(
        groupingKey: Any,
        curMap: mutable.Map[Any, Any]): GenericInternalRow = {
      val row = new GenericInternalRow(3)
      val mapData = new ArrayBasedMapData(
        ArrayData.toArrayData(curMap.keys.toArray),
        ArrayData.toArrayData(curMap.values.toArray)
      )
      row.update(0, groupingKey)
      row.update(1, mapData)
      row.update(2, partitionId)
      row
    }

    // state rows are sorted in rocksDB. So all of the rows with same
    // grouping key were grouped together consecutively
    new Iterator[InternalRow] {
      var curGroupingKey: UnsafeRow = _
      var curStateRowPair: UnsafeRowPair = _
      val curMap = mutable.Map.empty[Any, Any]

      override def hasNext: Boolean =
        stateRows.hasNext || !curMap.isEmpty

      override def next(): InternalRow = {
        var keepGoing = true
        while (stateRows.hasNext && keepGoing) {
          curStateRowPair = stateRows.next()
          if (curGroupingKey == null) {
            // First time in the iterator
            // Need to make a copy because we need to keep the
            // value across function calls
            curGroupingKey = curStateRowPair.key
              .get(0, groupingKeySchema).asInstanceOf[UnsafeRow].copy()
            appendKVPairToMap(curMap, curStateRowPair)
          } else {
            val curPairGroupingKey =
              curStateRowPair.key.get(0, groupingKeySchema)
            if (curPairGroupingKey == curGroupingKey) {
              appendKVPairToMap(curMap, curStateRowPair)
            } else {
              // find a different grouping key, exit loop and return a row
              keepGoing = false
            }
          }
        }
        if (!keepGoing) {
          // found a different grouping key
          val row = updateDataRow(curGroupingKey, curMap)
          // update vars
          curGroupingKey =
            curStateRowPair.key.get(0, groupingKeySchema)
              .asInstanceOf[UnsafeRow].copy()
          // empty the map, append current row
          curMap.clear()
          appendKVPairToMap(curMap, curStateRowPair)
          // return map value of previous grouping key
          row
        } else {
          // reach the end of the state rows
          if (curMap.isEmpty) null.asInstanceOf[InternalRow]
          else {
            val row = updateDataRow(curGroupingKey, curMap)
            // clear the map to end the iterator
            curMap.clear()
            row
          }
        }
      }
    }
  }

  def isMapStateVariable(
      stateVariableInfoOpt: Option[TransformWithStateVariableInfo]): Boolean = {
    stateVariableInfoOpt.isDefined &&
      stateVariableInfoOpt.get.stateVariableType == MapState
  }
}
