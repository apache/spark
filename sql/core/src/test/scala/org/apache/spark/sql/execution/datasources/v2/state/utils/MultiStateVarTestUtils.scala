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

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.execution.streaming.state.{KeyStateEncoderSpec, NoPrefixKeyStateEncoderSpec, PrefixKeyScanStateEncoderSpec, StateStore}
import org.apache.spark.sql.streaming.{ListState, MapState, OutputMode, SimpleMapValue, StatefulProcessor, TimeMode, TimerValues, TTLConfig, ValueState}
import org.apache.spark.sql.types.{BinaryType, IntegerType, LongType, StringType, StructField, StructType}

/**
 * Stateful processor with multiple state variables (value + list + map)
 * for testing transformWithState operator.
 */
class MultiStateVarProcessor extends StatefulProcessor[String, String, (String, String)] {
  @transient private var _countState: ValueState[Long] = _
  @transient private var _itemsList: ListState[String] = _
  @transient private var _itemsMap: MapState[String, SimpleMapValue] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    _countState = getHandle.getValueState[Long]("countState", Encoders.scalaLong, TTLConfig.NONE)
    _itemsList = getHandle.getListState[String]("itemsList", Encoders.STRING, TTLConfig.NONE)
    _itemsMap = getHandle.getMapState[String, SimpleMapValue](
      "itemsMap", Encoders.STRING, Encoders.product[SimpleMapValue], TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[String],
      timerValues: TimerValues): Iterator[(String, String)] = {
    val currentCount = Option(_countState.get()).getOrElse(0L)
    var newCount = currentCount
    inputRows.foreach { item =>
      newCount += 1
      _itemsList.appendValue(item)
      _itemsMap.updateValue(item, SimpleMapValue(newCount.toInt))
    }
    _countState.update(newCount)
    Iterator((key, newCount.toString))
  }
}

/**
 * Test utility object providing schema definitions and constants for MultiStateVarProcessor
 */
object MultiStateVarProcessorTestUtils {
  case class ColumnFamilyMetadata(
      keySchema: StructType,
      valueSchema: StructType,
      encoderSpec: KeyStateEncoderSpec,
      useMultipleValuePerKey: Boolean = false)

  // Column family names
  val COUNT_STATE = "countState"
  val ITEMS_LIST = "itemsList"
  val ITEMS_MAP = "itemsMap"
  val ROW_COUNTER = "$rowCounter_itemsList"

  val ALL_COLUMN_FAMILIES: Set[String] = Set(
    COUNT_STATE, ITEMS_LIST, ITEMS_MAP, ROW_COUNTER
  )

  /**
   * @return A map from column family name to (keySchema, valueSchema, encoderSpec) tuple
   */
  def getSchemas(): Map[String, (StructType, StructType, KeyStateEncoderSpec)] = {
    getSchemasWithMetadata().view.mapValues { metadata =>
      (metadata.keySchema, metadata.valueSchema, metadata.encoderSpec)
    }.toMap
  }

  /**
   * @return A map from column family name to ColumnFamilyMetadata
   */
  def getSchemasWithMetadata(): Map[String, ColumnFamilyMetadata] = {
    val groupByKeySchema = StructType(Array(
      StructField("value", StringType, nullable = true)
    ))
    val defaultValueSchema = StructType(Array(
      StructField("value", BinaryType, nullable = true)
    ))
    val countStateValueSchema = StructType(Array(
      StructField("value", LongType, nullable = false)
    ))
    val itemsListValueSchema = StructType(Array(
      StructField("value", StringType, nullable = true)
    ))
    val rowCounterValueSchema = StructType(Array(
      StructField("count", LongType, nullable = true)
    ))
    val itemsMapKeySchema = StructType(Array(
      StructField("key", groupByKeySchema),
      StructField("user_map_key", groupByKeySchema, nullable = true)
    ))
    val itemsMapValueSchema = StructType(Array(
      StructField("user_map_value", IntegerType, nullable = true)
    ))
    val countStateEncoderSpec = NoPrefixKeyStateEncoderSpec(groupByKeySchema)
    val itemsMapEncoderSpec = PrefixKeyScanStateEncoderSpec(itemsMapKeySchema, 1)

    Map(
      StateStore.DEFAULT_COL_FAMILY_NAME -> ColumnFamilyMetadata(
        groupByKeySchema, defaultValueSchema, countStateEncoderSpec),
      COUNT_STATE -> ColumnFamilyMetadata(
        groupByKeySchema, countStateValueSchema, countStateEncoderSpec),
      ITEMS_LIST -> ColumnFamilyMetadata(
        groupByKeySchema, itemsListValueSchema, countStateEncoderSpec,
        useMultipleValuePerKey = true),
      ROW_COUNTER -> ColumnFamilyMetadata(
        groupByKeySchema, rowCounterValueSchema, countStateEncoderSpec),
      ITEMS_MAP -> ColumnFamilyMetadata(
        itemsMapKeySchema, itemsMapValueSchema, itemsMapEncoderSpec)
    )
  }

  /**
   * Returns select expressions for reading multi-state variable data.
   *
   * @param columnFamilyName The column family name
   * @return Select expressions as a Seq of strings
   */
  def getSelectExpressions(columnFamilyName: String): Seq[String] = {
    columnFamilyName match {
      case ITEMS_LIST =>
        Seq("partition_id", "key", "list_element")
      case ITEMS_MAP =>
        Seq("partition_id", "STRUCT(key, user_map_key) AS KEY",
            "user_map_value AS value")
      case _ =>
        Seq("partition_id", "key", "value")
    }
  }

  def getColumnFamilyToSelectExprs(): Map[String, Seq[String]] = {
    Map(
      ITEMS_LIST -> getSelectExpressions(ITEMS_LIST),
      ITEMS_MAP -> getSelectExpressions(ITEMS_MAP)
    )
  }
}
