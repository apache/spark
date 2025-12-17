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

import java.sql.Timestamp

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.execution.streaming.operators.stateful.transformwithstate.timers.TimerStateUtils
import org.apache.spark.sql.execution.streaming.state.{KeyStateEncoderSpec, NoPrefixKeyStateEncoderSpec, PrefixKeyScanStateEncoderSpec, RangeKeyScanStateEncoderSpec, StateStore}
import org.apache.spark.sql.streaming.{OutputMode, StatefulProcessor, TimeMode, TimerValues, TTLConfig, ValueState}
import org.apache.spark.sql.types.{BinaryType, LongType, NullType, StringType, StructField, StructType}

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
  case class ColumnFamilyMetadata(
      keySchema: StructType,
      valueSchema: StructType,
      encoderSpec: KeyStateEncoderSpec,
      useMultipleValuePerKey: Boolean = false)

  /**
   * Returns the grouping key schema and state value schema for a simple count state.
   * This is commonly used with timer tests where state tracks counts.
   *
   * @return A tuple of (keySchema, valueSchema)
   */
  def getCountStateSchemas(): (StructType, StructType) = {
    val groupByKeySchema = StructType(Array(
      StructField("key", StringType, nullable = true)
    ))
    val stateValueSchema = StructType(Array(
      StructField("value", LongType, nullable = true)
    ))
    (groupByKeySchema, stateValueSchema)
  }

  /**
   * Returns schemas for timer-related column families
   *
   * @param groupingKeySchema The schema for the grouping key
   * @return A tuple of (keyToTimestampKeySchema, timestampToKeyKeySchema)
   */
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

  /**
   * Returns complete metadata for timer-related column families including encoder specs.
   * Used for tests with timers and a count state.
   * @param timeMode The time mode (EventTime or ProcessingTime)
   * @return A map from column family name to ColumnFamilyMetadata
   */
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

  /**
   * Returns select expressions for timer column families with standardized column order.
   *
   * @param columnFamilyName The timer column family name
   * @return Select expressions as a Seq of strings with order (partition_id, key, value)
   */
  def getTimerSelectExpressions(columnFamilyName: String): Seq[String] = {
    if (columnFamilyName.endsWith("_keyToTimestamp")) {
      Seq("partition_id",
          "STRUCT(key AS groupingKey, expiration_timestamp_ms AS key)",
          "NULL AS value")
    } else if (columnFamilyName.endsWith("_timestampToKey")) {
      Seq("partition_id",
          "STRUCT(expiration_timestamp_ms AS key, key AS groupingKey)",
          "NULL AS value")
    } else {
      // For countState or other regular column families
      Seq("partition_id", "key", "value")
    }
  }

  /**
   * Returns a map of timer column family names to their select expressions.
   *
   * @param timeMode The time mode (EventTime or ProcessingTime)
   * @return Map of column family names to select expressions
   */
  def getTimerColumnFamilyToSelectExprs(timeMode: TimeMode): Map[String, Seq[String]] = {
    val (keyToTimeCF, timeToKeyCF) = TimerStateUtils.getTimerStateVarNames(timeMode.toString)
    Map(
      keyToTimeCF -> getTimerSelectExpressions(keyToTimeCF),
      timeToKeyCF -> getTimerSelectExpressions(timeToKeyCF)
    )
  }
}
