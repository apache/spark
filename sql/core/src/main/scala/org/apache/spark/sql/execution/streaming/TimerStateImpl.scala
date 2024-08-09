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
package org.apache.spark.sql.execution.streaming

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{EXPIRY_TIMESTAMP, KEY}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.streaming.TimeMode
import org.apache.spark.sql.types._
import org.apache.spark.util.NextIterator

/**
 * Singleton utils class used primarily while interacting with TimerState
 */
object TimerStateUtils {
  val PROC_TIMERS_STATE_NAME = "_procTimers"
  val EVENT_TIMERS_STATE_NAME = "_eventTimers"
  val KEY_TO_TIMESTAMP_CF = "_keyToTimestamp"
  val TIMESTAMP_TO_KEY_CF = "_timestampToKey"
  val TIMER_VALUE_ROW_SCHEMA = StructType(Array(StructField("__dummy__", NullType)))
}

/**
 * Class that provides the implementation for storing timers
 * used within the `transformWithState` operator.
 * @param store - state store to be used for storing timer data
 * @param timeMode - mode of timeout (event time or processing time)
 * @param keyExprEnc - encoder for key expression
 */
class TimerStateImpl(
    store: StateStore,
    timeMode: TimeMode,
    keyExprEnc: ExpressionEncoder[Any],
    colFamilyIds: Map[String, Short]) extends Logging {

  private val EMPTY_ROW =
    UnsafeProjection.create(Array[DataType](NullType)).apply(InternalRow.apply(null))

  private val schemaForValueRow: StructType =
    StructType(Array(StructField("__dummy__", NullType)))

  private val timerCFName = if (timeMode == TimeMode.ProcessingTime) {
    TimerStateUtils.PROC_TIMERS_STATE_NAME
  } else {
    TimerStateUtils.EVENT_TIMERS_STATE_NAME
  }

  private val rowEncoder = new TimerKeyEncoder(keyExprEnc)

  private val schemaForKeyRow = rowEncoder.schemaForKeyRow
  private val keyToTsCFName = timerCFName + TimerStateUtils.KEY_TO_TIMESTAMP_CF
  store.createColFamilyIfAbsent(keyToTsCFName, colFamilyIds(keyToTsCFName), schemaForKeyRow,
    schemaForValueRow, PrefixKeyScanStateEncoderSpec(schemaForKeyRow, 1),
    useMultipleValuesPerKey = false, isInternal = true)

  // We maintain a secondary index that inverts the ordering of the timestamp
  // and grouping key
  private val keySchemaForSecIndex = rowEncoder.keySchemaForSecIndex
  private val tsToKeyCFName = timerCFName + TimerStateUtils.TIMESTAMP_TO_KEY_CF
  store.createColFamilyIfAbsent(tsToKeyCFName, colFamilyIds(tsToKeyCFName), keySchemaForSecIndex,
    schemaForValueRow, RangeKeyScanStateEncoderSpec(keySchemaForSecIndex, Seq(0)),
    useMultipleValuesPerKey = false, isInternal = true)

  private def getGroupingKey(cfName: String): Any = {
    val keyOption = ImplicitGroupingKeyTracker.getImplicitKeyOption
    if (keyOption.isEmpty) {
      throw StateStoreErrors.implicitKeyNotFound(cfName)
    }
    keyOption.get
  }

  /**
   * Function to check if the timer for the given key and timestamp is already registered
   * @param expiryTimestampMs - expiry timestamp of the timer
   * @return - true if the timer is already registered, false otherwise
   */
  private def exists(groupingKey: Any, expiryTimestampMs: Long): Boolean = {
    getImpl(groupingKey, expiryTimestampMs) != null
  }

  private def getImpl(groupingKey: Any, expiryTimestampMs: Long): UnsafeRow = {
    store.get(rowEncoder.encodedKey(groupingKey, expiryTimestampMs), keyToTsCFName)
  }

  /**
   * Function to add a new timer for the given key and timestamp
   * @param expiryTimestampMs - expiry timestamp of the timer
   */
  def registerTimer(expiryTimestampMs: Long): Unit = {
    val groupingKey = getGroupingKey(keyToTsCFName)
    if (exists(groupingKey, expiryTimestampMs)) {
      logWarning(log"Failed to register timer for key=${MDC(KEY, groupingKey)} and " +
        log"timestamp=${MDC(EXPIRY_TIMESTAMP, expiryTimestampMs)} ms since it already exists")
    } else {
      store.put(rowEncoder.encodedKey(groupingKey, expiryTimestampMs), EMPTY_ROW, keyToTsCFName)
      store.put(rowEncoder.encodeSecIndexKey(groupingKey, expiryTimestampMs),
        EMPTY_ROW, tsToKeyCFName)
      logDebug(s"Registered timer for key=$groupingKey and timestamp=$expiryTimestampMs")
    }
  }

  /**
   * Function to remove the timer for the given key and timestamp
   * @param expiryTimestampMs - expiry timestamp of the timer
   */
  def deleteTimer(expiryTimestampMs: Long): Unit = {
    val groupingKey = getGroupingKey(keyToTsCFName)

    if (!exists(groupingKey, expiryTimestampMs)) {
      logWarning(log"Failed to delete timer for key=${MDC(KEY, groupingKey)} and " +
        log"timestamp=${MDC(EXPIRY_TIMESTAMP, expiryTimestampMs)} ms since it does not exist")
    } else {
      store.remove(rowEncoder.encodedKey(groupingKey, expiryTimestampMs), keyToTsCFName)
      store.remove(rowEncoder.encodeSecIndexKey(groupingKey, expiryTimestampMs), tsToKeyCFName)
      logDebug(s"Deleted timer for key=$groupingKey and timestamp=$expiryTimestampMs")
    }
  }

  def listTimers(): Iterator[Long] = {
    val groupingKey = getGroupingKey(keyToTsCFName)
    val encodedGroupingKey: UnsafeRow = rowEncoder.encodePrefixKey(groupingKey)

    val iter = store.prefixScan(encodedGroupingKey, keyToTsCFName)
    iter.map { kv =>
      val keyRow = kv.key
      keyRow.getLong(1)
    }
  }

  private def getTimerRowFromSecIndex(keyRow: UnsafeRow): (Any, Long) = {
    // Decode the key object from the UnsafeRow
    val retUnsafeRow = keyRow.getStruct(1, keyExprEnc.schema.length)
    val keyObj = rowEncoder.decodePrefixKey(retUnsafeRow)

    val expiryTimestampMs = keyRow.getLong(0)
    (keyObj, expiryTimestampMs)
  }

  /**
   * Function to get all the expired registered timers for all grouping keys.
   * Perform a range scan on timestamp and will stop iterating once the key row timestamp equals or
   * exceeds the limit (as timestamp key is increasingly sorted).
   * @param expiryTimestampMs Threshold for expired timestamp in milliseconds, this function
   *                          will return all timers that have timestamp less than passed threshold.
   * @return - iterator of all the registered timers for all grouping keys
   */
  def getExpiredTimers(expiryTimestampMs: Long): Iterator[(Any, Long)] = {
    // this iter is increasingly sorted on timestamp
    val iter = store.iterator(tsToKeyCFName)

    new NextIterator[(Any, Long)] {
      override protected def getNext(): (Any, Long) = {
        if (iter.hasNext) {
          val rowPair = iter.next()
          val keyRow = rowPair.key
          val result = getTimerRowFromSecIndex(keyRow)
          if (result._2 < expiryTimestampMs) {
            result
          } else {
            finished = true
            null.asInstanceOf[(Any, Long)]
          }
        } else {
          finished = true
          null.asInstanceOf[(Any, Long)]
        }
      }

      override protected def close(): Unit = { }
    }
  }
}
