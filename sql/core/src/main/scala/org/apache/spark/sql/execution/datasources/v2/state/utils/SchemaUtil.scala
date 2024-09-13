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
import scala.util.control.NonFatal

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeRow}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.execution.datasources.v2.state.{StateDataSourceErrors, StateSourceOptions}
import org.apache.spark.sql.execution.streaming.StateVariableType._
import org.apache.spark.sql.execution.streaming.TransformWithStateVariableInfo
import org.apache.spark.sql.execution.streaming.state.{StateStoreColFamilySchema, UnsafeRowPair}
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, LongType, MapType, StringType, StructType}
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
        stateStoreColFamilySchemaOpt.get, sourceOptions)
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

  def unifyStateRowPairWithMultipleValues(
      pair: (UnsafeRow, GenericArrayData),
      partition: Int): InternalRow = {
    val row = new GenericInternalRow(3)
    row.update(0, pair._1)
    row.update(1, pair._2)
    row.update(2, partition)
    row
  }

  /**
   * For map state variables, state rows are stored as composite key.
   * To return grouping key -> Map{user key -> value} as one state reader row to
   * the users, we need to perform grouping on state rows by their grouping key,
   * and construct a map for that grouping key.
   *
   * We traverse the iterator returned from state store,
   * and will only return a row for `next()` only if the grouping key in the next row
   * from state store is different (or there are no more rows)
   *
   * Note that all state rows with the same grouping key are co-located so they will
   * appear consecutively during the iterator traversal.
   */
  def unifyMapStateRowPair(
      stateRows: Iterator[UnsafeRowPair],
      compositeKeySchema: StructType,
      partitionId: Int,
      stateSourceOptions: StateSourceOptions): Iterator[InternalRow] = {
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

    def createDataRow(
        groupingKey: Any,
        curMap: mutable.Map[Any, Any]): GenericInternalRow = {
      val row = new GenericInternalRow(3)
      val mapData = ArrayBasedMapData(curMap)
      row.update(0, groupingKey)
      row.update(1, mapData)
      row.update(2, partitionId)
      row
    }

    def createFlattenedRow(
        groupingKey: UnsafeRow,
        userMapKey: UnsafeRow,
        userMapValue: UnsafeRow,
        partitionId: Int): GenericInternalRow = {
      val row = new GenericInternalRow(4)
      row.update(0, groupingKey)
      row.update(1, userMapKey)
      row.update(2, userMapValue)
      row.update(3, partitionId)
      row
    }

    if (stateSourceOptions.flattenCollectionTypes) {
      stateRows
        .map { pair =>
          val groupingKey = pair.key.get(0, groupingKeySchema).asInstanceOf[UnsafeRow]
          val userMapKey = pair.key.get(1, userKeySchema).asInstanceOf[UnsafeRow]
          val userMapValue = pair.value
          createFlattenedRow(groupingKey, userMapKey, userMapValue, partitionId)
        }
    } else {
      // All of the rows with the same grouping key were co-located and were
      // grouped together consecutively.
      new Iterator[InternalRow] {
        var curGroupingKey: UnsafeRow = _
        var curStateRowPair: UnsafeRowPair = _
        val curMap = mutable.Map.empty[Any, Any]

        override def hasNext: Boolean =
          stateRows.hasNext || !curMap.isEmpty

        override def next(): InternalRow = {
          var foundNewGroupingKey = false
          while (stateRows.hasNext && !foundNewGroupingKey) {
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
                foundNewGroupingKey = true
              }
            }
          }
          if (foundNewGroupingKey) {
            // found a different grouping key
            val row = createDataRow(curGroupingKey, curMap)
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
            if (curMap.isEmpty) {
              throw new NoSuchElementException("Please check if the iterator hasNext(); Likely " +
                "user is trying to get element from an exhausted iterator.")
            }
            else {
              // reach the end of the state rows
              val row = createDataRow(curGroupingKey, curMap)
              // clear the map to end the iterator
              curMap.clear()
              row
            }
          }
        }
      }
    }
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
      "single_value" -> classOf[StructType],
      "list_value" -> classOf[ArrayType],
      "map_value" -> classOf[MapType],
      "user_map_key" -> classOf[StructType],
      "user_map_value" -> classOf[StructType],
      "partition_id" -> classOf[IntegerType])

    val expectedFieldNames = if (sourceOptions.readChangeFeed) {
      Seq("batch_id", "change_type", "key", "value", "partition_id")
    } else if (transformWithStateVariableInfoOpt.isDefined) {
      val stateVarInfo = transformWithStateVariableInfoOpt.get
      val stateVarType = stateVarInfo.stateVariableType

      stateVarType match {
        case ValueState =>
          Seq("key", "value", "partition_id")

        case ListState =>
          if (sourceOptions.flattenCollectionTypes) {
            Seq("key", "value", "partition_id")
          } else {
            Seq("key", "list_value", "partition_id")
          }

        case MapState =>
          if (sourceOptions.flattenCollectionTypes) {
            Seq("key", "user_map_key", "user_map_value", "partition_id")
          } else {
            Seq("key", "map_value", "partition_id")
          }

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
      stateStoreColFamilySchema: StateStoreColFamilySchema,
      stateSourceOptions: StateSourceOptions): StructType = {
    val stateVarType = stateVarInfo.stateVariableType

    stateVarType match {
      case ValueState =>
        new StructType()
          .add("key", stateStoreColFamilySchema.keySchema)
          .add("value", stateStoreColFamilySchema.valueSchema)
          .add("partition_id", IntegerType)

      case ListState =>
        if (stateSourceOptions.flattenCollectionTypes) {
          new StructType()
            .add("key", stateStoreColFamilySchema.keySchema)
            .add("value", stateStoreColFamilySchema.valueSchema)
            .add("partition_id", IntegerType)
        } else {
          new StructType()
            .add("key", stateStoreColFamilySchema.keySchema)
            .add("list_value", ArrayType(stateStoreColFamilySchema.valueSchema))
            .add("partition_id", IntegerType)
        }

      case MapState =>
        val groupingKeySchema = SchemaUtil.getSchemaAsDataType(
          stateStoreColFamilySchema.keySchema, "key")
        val userKeySchema = stateStoreColFamilySchema.userKeyEncoderSchema.get
        val valueMapSchema = MapType.apply(
          keyType = userKeySchema,
          valueType = stateStoreColFamilySchema.valueSchema
        )

        if (stateSourceOptions.flattenCollectionTypes) {
          new StructType()
            .add("key", groupingKeySchema)
            .add("user_map_key", userKeySchema)
            .add("user_map_value", stateStoreColFamilySchema.valueSchema)
            .add("partition_id", IntegerType)
        } else {
          new StructType()
            .add("key", groupingKeySchema)
            .add("map_value", valueMapSchema)
            .add("partition_id", IntegerType)
        }

      case _ =>
        throw StateDataSourceErrors.internalError(s"Unsupported state variable type $stateVarType")
    }
  }

  def checkVariableType(
      stateVariableInfoOpt: Option[TransformWithStateVariableInfo],
      varType: StateVariableType): Boolean = {
    stateVariableInfoOpt.isDefined &&
      stateVariableInfoOpt.get.stateVariableType == varType
  }

  /**
   * Given key-value schema generated from `generateSchemaForStateVar()`,
   * returns the compositeKey schema that key is stored in the state store
   */
  def getCompositeKeySchema(schema: StructType): StructType = {
    val groupingKeySchema = SchemaUtil.getSchemaAsDataType(
      schema, "key").asInstanceOf[StructType]
    val userKeySchema = try {
      Option(
        SchemaUtil.getSchemaAsDataType(schema, "map_value").asInstanceOf[MapType]
          .keyType.asInstanceOf[StructType])
    } catch {
      case NonFatal(e) =>
        throw StateDataSourceErrors.internalError(s"No such field named as 'map_value' " +
          s"during state source reader schema initialization. Internal exception message: $e")
    }
    new StructType()
      .add("key", groupingKeySchema)
      .add("userKey", userKeySchema.get)
  }
}
