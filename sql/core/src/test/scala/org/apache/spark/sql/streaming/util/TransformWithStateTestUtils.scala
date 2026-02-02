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
package org.apache.spark.sql.streaming.util

import java.sql.Timestamp

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.execution.streaming.operators.stateful.transformwithstate.timers.TimerStateUtils
import org.apache.spark.sql.execution.streaming.state.{KeyStateEncoderSpec, NoPrefixKeyStateEncoderSpec, PrefixKeyScanStateEncoderSpec, RangeKeyScanStateEncoderSpec, StateStore}
import org.apache.spark.sql.streaming.{ListState, MapState, OutputMode, SimpleMapValue, StatefulProcessor, TimeMode, TimerValues, TTLConfig, ValueState}
import org.apache.spark.sql.types.{BinaryType, IntegerType, LongType, NullType, StringType, StructField, StructType}

case class ColumnFamilyMetadata(
    keySchema: StructType,
    valueSchema: StructType,
    encoderSpec: KeyStateEncoderSpec,
    useMultipleValuePerKey: Boolean = false)
/**
 * Test utility object providing schema definitions and constants for TTL state tests.
 * The schema is based on ListStateTTLProcessor, MapStateTTLProcessor, ValueStateTTLProcessor
 * defined in the test suite
 */
// TODO: Move the corresponding TTLProcessors to this file
object TTLProcessorUtils {
  // List state column families
  val LIST_STATE = "listState"
  val LIST_STATE_TTL_INDEX = "$ttl_listState"
  val LIST_STATE_MIN = "$min_listState"
  val LIST_STATE_COUNT = "$count_listState"
  val LIST_STATE_ALL: Set[String] = Set(
    LIST_STATE, LIST_STATE_TTL_INDEX, LIST_STATE_MIN, LIST_STATE_COUNT
  )

  // Map state column families
  val MAP_STATE = "mapState"
  val MAP_STATE_TTL_INDEX = "$ttl_mapState"
  val MAP_STATE_ALL: Set[String] = Set(MAP_STATE, MAP_STATE_TTL_INDEX)

  // Value state column families
  val VALUE_STATE = "valueState"
  val VALUE_STATE_TTL_INDEX = "$ttl_valueState"
  val VALUE_STATE_ALL: Set[String] = Set(VALUE_STATE, VALUE_STATE_TTL_INDEX)

  def getListStateTTLSchemas(): Map[String, (StructType, StructType)] = {
    getListStateTTLSchemasWithMetadata().view.mapValues { metadata =>
      (metadata.keySchema, metadata.valueSchema)
    }.toMap
  }

  def getListStateTTLSchemasWithMetadata(): Map[String, ColumnFamilyMetadata] = {
    val groupByKeySchema = StructType(Array(
      StructField("value", StringType, nullable = true)
    ))
    val listStateValueSchema = StructType(Array(
      StructField("value", StructType(Array(
        StructField("value", IntegerType, nullable = true)
      )), nullable = false),
      StructField("ttlExpirationMs", LongType, nullable = false)
    ))
    val ttlIndexKeySchema = StructType(Array(
      StructField("expirationMs", LongType, nullable = false),
      StructField("elementKey", groupByKeySchema)
    ))
    val minExpiryValueSchema = StructType(Array(
      StructField("minExpiry", LongType)
    ))
    val countValueSchema = StructType(Array(
      StructField("count", LongType)
    ))
    val dummyValueSchema = StructType(Array(StructField("__dummy__", NullType)))
    val defaultValueSchema = StructType(Array(StructField("value", BinaryType, nullable = true)))

    val encoderSpec = NoPrefixKeyStateEncoderSpec(groupByKeySchema)
    val ttlIndexEncoderSpec = RangeKeyScanStateEncoderSpec(ttlIndexKeySchema, Seq(0))

    Map(
      StateStore.DEFAULT_COL_FAMILY_NAME -> ColumnFamilyMetadata(
        groupByKeySchema, defaultValueSchema, encoderSpec),
      LIST_STATE -> ColumnFamilyMetadata(
        groupByKeySchema, listStateValueSchema, encoderSpec, useMultipleValuePerKey = true),
      LIST_STATE_TTL_INDEX -> ColumnFamilyMetadata(
        ttlIndexKeySchema, dummyValueSchema, ttlIndexEncoderSpec),
      LIST_STATE_MIN -> ColumnFamilyMetadata(
        groupByKeySchema, minExpiryValueSchema, encoderSpec),
      LIST_STATE_COUNT -> ColumnFamilyMetadata(
        groupByKeySchema, countValueSchema, encoderSpec)
    )
  }

  def getMapStateTTLSchemas(): Map[String, (StructType, StructType)] = {
    getMapStateTTLSchemasWithMetadata().view.mapValues { metadata =>
      (metadata.keySchema, metadata.valueSchema)
    }.toMap
  }

  def getMapStateTTLSchemasWithMetadata(): Map[String, ColumnFamilyMetadata] = {
    val groupByKeySchema = StructType(Array(
      StructField("value", StringType, nullable = true)
    ))
    val userKeySchema = StructType(Array(
      StructField("value", StringType, nullable = true)
    ))
    val compositeKeySchema = StructType(Array(
      StructField("key", groupByKeySchema),
      StructField("userKey", userKeySchema)
    ))
    val mapStateValueSchema = StructType(Array(
      StructField("value", StructType(Array(
        StructField("value", IntegerType, nullable = true)
      )), nullable = false),
      StructField("ttlExpirationMs", LongType, nullable = false)
    ))
    val ttlIndexKeySchema = StructType(Array(
      StructField("expirationMs", LongType, nullable = false),
      StructField("elementKey", compositeKeySchema)
    ))
    val dummyValueSchema = StructType(Array(StructField("__empty__", NullType)))
    val defaultValueSchema = StructType(Array(StructField("value", BinaryType, nullable = true)))

    val encoderSpec = NoPrefixKeyStateEncoderSpec(groupByKeySchema)
    val mapStateEncoderSpec = PrefixKeyScanStateEncoderSpec(compositeKeySchema, 1)
    val ttlIndexEncoderSpec = RangeKeyScanStateEncoderSpec(ttlIndexKeySchema, Seq(0))

    Map(
      StateStore.DEFAULT_COL_FAMILY_NAME -> ColumnFamilyMetadata(
        groupByKeySchema, defaultValueSchema, encoderSpec),
      MAP_STATE -> ColumnFamilyMetadata(
        compositeKeySchema, mapStateValueSchema, mapStateEncoderSpec),
      MAP_STATE_TTL_INDEX -> ColumnFamilyMetadata(
        ttlIndexKeySchema, dummyValueSchema, ttlIndexEncoderSpec)
    )
  }

  def getValueStateTTLSchemas(): Map[String, (StructType, StructType)] = {
    getValueStateTTLSchemasWithMetadata().view.mapValues { metadata =>
      (metadata.keySchema, metadata.valueSchema)
    }.toMap
  }

  def getValueStateTTLSchemasWithMetadata(): Map[String, ColumnFamilyMetadata] = {
    val groupByKeySchema = StructType(Array(
      StructField("value", StringType, nullable = true)
    ))
    val valueStateValueSchema = StructType(Array(
      StructField("value", StructType(Array(
        StructField("value", IntegerType, nullable = true)
      )), nullable = false),
      StructField("ttlExpirationMs", LongType, nullable = false)
    ))
    val ttlIndexKeySchema = StructType(Array(
      StructField("expirationMs", LongType, nullable = false),
      StructField("elementKey", groupByKeySchema)
    ))
    val dummyValueSchema = StructType(Array(StructField("__empty__", NullType)))
    val defaultValueSchema = StructType(Array(StructField("value", BinaryType, nullable = true)))

    val encoderSpec = NoPrefixKeyStateEncoderSpec(groupByKeySchema)
    val ttlIndexEncoderSpec = RangeKeyScanStateEncoderSpec(ttlIndexKeySchema, Seq(0))

    Map(
      StateStore.DEFAULT_COL_FAMILY_NAME -> ColumnFamilyMetadata(
        groupByKeySchema, defaultValueSchema, encoderSpec),
      VALUE_STATE -> ColumnFamilyMetadata(
        groupByKeySchema, valueStateValueSchema, encoderSpec),
      VALUE_STATE_TTL_INDEX -> ColumnFamilyMetadata(
        ttlIndexKeySchema, dummyValueSchema, ttlIndexEncoderSpec)
    )
  }

  def getTTLSelectExpressions(columnFamilyName: String): Seq[String] = {
    columnFamilyName match {
      case MAP_STATE =>
        Seq("partition_id", "STRUCT(key, user_map_key) AS KEY",
          "user_map_value AS value")
      case LIST_STATE =>
        Seq("partition_id", "key", "list_element")
      case _ =>
        Seq("partition_id", "key", "value")
    }
  }
}

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
  // Column family names
  val COUNT_STATE = "countState"
  val ITEMS_LIST = "itemsList"
  val ITEMS_MAP = "itemsMap"
  val ROW_COUNTER = "$rowCounter_itemsList"

  val ALL_COLUMN_FAMILIES: Set[String] = Set(
    COUNT_STATE, ITEMS_LIST, ITEMS_MAP, ROW_COUNTER
  )

  def getSchemas(): Map[String, (StructType, StructType, KeyStateEncoderSpec)] = {
    getSchemasWithMetadata().view.mapValues { metadata =>
      (metadata.keySchema, metadata.valueSchema, metadata.encoderSpec)
    }.toMap
  }

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

class EventTimeTimerProcessor
  extends StatefulProcessor[String, (String, Timestamp), (String, String)] {
  @transient var _valueState: ValueState[Long] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    _valueState = getHandle.getValueState("countState", Encoders.scalaLong, TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      rows: Iterator[(String, Timestamp)],
      timerValues: TimerValues): Iterator[(String, String)] = {
    var maxTimestamp = 0L
    var rowCount = 0
    rows.foreach { case (_, timestamp) =>
      maxTimestamp = Math.max(maxTimestamp, timestamp.getTime)
      rowCount += 1
    }

    val count = Option(_valueState.get()).getOrElse(0L) + rowCount
    _valueState.update(count)

    // Register an event time timer
    if (maxTimestamp > 0) {
      getHandle.registerTimer(maxTimestamp + 5000)
    }

    Iterator((key, count.toString))
  }
}

/**
 * Test utility providing schema definitions and constants for EventTimeTimerProcessor
 * and RunningCountStatefulProcessorWithProcTimeTimer in TransformWithStateSuite
 */
object TimerTestUtils {
  def getCountStateSchemas(): (StructType, StructType) = {
    val groupByKeySchema = StructType(Array(
      StructField("key", StringType, nullable = true)
    ))
    val stateValueSchema = StructType(Array(
      StructField("value", LongType, nullable = true)
    ))
    (groupByKeySchema, stateValueSchema)
  }

  def getTimerKeySchemas(groupingKeySchema: StructType): (StructType, StructType) = {
    val keyToTimestampKeySchema = StructType(Array(
      StructField("key", groupingKeySchema),
      StructField("expiryTimestampMs", LongType, nullable = false)
    ))
    val timestampToKeyKeySchema = StructType(Array(
      StructField("expiryTimestampMs", LongType, nullable = false),
      StructField("key", groupingKeySchema)
    ))

    (keyToTimestampKeySchema, timestampToKeyKeySchema)
  }

  def getTimerConfigsForCountState(timeMode: TimeMode): Map[String, ColumnFamilyMetadata] = {
    val (groupByKeySchema, stateValueSchema) = getCountStateSchemas()
    val (keyToTimestampKeySchema, timestampToKeyKeySchema) = getTimerKeySchemas(groupByKeySchema)
    val defaultValueSchema = StructType(Array(StructField("value", BinaryType, nullable = true)))

    val encoderSpec = NoPrefixKeyStateEncoderSpec(groupByKeySchema)
    val keyToTimestampEncoderSpec = PrefixKeyScanStateEncoderSpec(keyToTimestampKeySchema, 1)
    val timestampToKeyEncoderSpec = RangeKeyScanStateEncoderSpec(timestampToKeyKeySchema, Seq(0))

    val (keyToTimestampCF, timestampToKeyCF) =
      TimerStateUtils.getTimerStateVarNames(timeMode.toString)

    val dummyValueSchema = StructType(Array(StructField("__dummy__", NullType)))

    Map(
      StateStore.DEFAULT_COL_FAMILY_NAME -> ColumnFamilyMetadata(
        groupByKeySchema, defaultValueSchema, encoderSpec),
      "countState" -> ColumnFamilyMetadata(
        groupByKeySchema, stateValueSchema, encoderSpec),
      keyToTimestampCF -> ColumnFamilyMetadata(
        keyToTimestampKeySchema, dummyValueSchema, keyToTimestampEncoderSpec),
      timestampToKeyCF -> ColumnFamilyMetadata(
        timestampToKeyKeySchema, dummyValueSchema, timestampToKeyEncoderSpec)
    )
  }

  def getTimerSelectExpressions(columnFamilyName: String): Seq[String] = {
    if (columnFamilyName.endsWith(TimerStateUtils.KEY_TO_TIMESTAMP_CF)) {
      Seq("partition_id",
          "STRUCT(key AS groupingKey, expiration_timestamp_ms AS key)",
          "NULL AS value")
    } else if (columnFamilyName.endsWith(TimerStateUtils.TIMESTAMP_TO_KEY_CF)) {
      Seq("partition_id",
          "STRUCT(expiration_timestamp_ms AS key, key AS groupingKey)",
          "NULL AS value")
    } else {
      // For countState or other regular column families
      Seq("partition_id", "key", "value")
    }
  }

  def getTimerColumnFamilyToSelectExprs(timeMode: TimeMode): Map[String, Seq[String]] = {
    val (keyToTimeCF, timeToKeyCF) = TimerStateUtils.getTimerStateVarNames(timeMode.toString)
    Map(
      keyToTimeCF -> getTimerSelectExpressions(keyToTimeCF),
      timeToKeyCF -> getTimerSelectExpressions(timeToKeyCF)
    )
  }
}

