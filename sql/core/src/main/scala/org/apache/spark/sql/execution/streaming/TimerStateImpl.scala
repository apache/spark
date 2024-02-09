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

import java.io.Serializable

import org.apache.commons.lang3.SerializationUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.streaming.TimeoutMode
import org.apache.spark.sql.types._

/**
 * Singleton utils class used primarily while interacting with TimerState
 */
object TimerStateUtils {
  case class TimestampWithKey(
      key: Any,
      expiryTimestampMs: Long) extends Serializable

  val PROC_TIMERS_STATE_NAME = "_procTimers"
  val EVENT_TIMERS_STATE_NAME = "_eventTimers"
  val KEY_TO_TIMESTAMP_CF = "_keyToTimestamp"
  val TIMESTAMP_TO_KEY_CF = "_timestampToKey"
}

/**
 * Class that provides the implementation for storing timers
 * used within the `transformWithState` operator.
 * @param store - state store to be used for storing timer data
 * @param timeoutMode - mode of timeout (event time or processing time)
 * @param keyExprEnc - encoder for key expression
 * @tparam S - type of timer value
 */
class TimerStateImpl[S](
    store: StateStore,
    timeoutMode: TimeoutMode,
    keyExprEnc: ExpressionEncoder[Any]) extends Logging {

  private val EMPTY_ROW =
    UnsafeProjection.create(Array[DataType](NullType)).apply(InternalRow.apply(null))

  private val schemaForKeyRow: StructType = new StructType()
    .add("key", BinaryType)
    .add("expiryTimestampMs", LongType, nullable = false)

  private val schemaForValueRow: StructType =
    StructType(Array(StructField("__dummy__", NullType)))

  val timerCfName = if (timeoutMode == TimeoutMode.ProcessingTime) {
    TimerStateUtils.PROC_TIMERS_STATE_NAME
  } else {
    TimerStateUtils.EVENT_TIMERS_STATE_NAME
  }

  val keyToTsCFName = timerCfName + TimerStateUtils.KEY_TO_TIMESTAMP_CF
  store.createColFamilyIfAbsent(keyToTsCFName,
    schemaForKeyRow, numColsPrefixKey = 0, schemaForValueRow, true)

  private def encodeKey(expiryTimestampMs: Long): UnsafeRow = {
    val keyOption = ImplicitGroupingKeyTracker.getImplicitKeyOption
    if (!keyOption.isDefined) {
      throw new UnsupportedOperationException("Implicit key not found for operation on" +
        s"stateName=$keyToTsCFName")
    }

    val toRow = keyExprEnc.createSerializer()
    val keyByteArr = toRow
      .apply(keyOption.get).asInstanceOf[UnsafeRow].getBytes()

    val keyEncoder = UnsafeProjection.create(schemaForKeyRow)
    val keyRow = keyEncoder(InternalRow(keyByteArr, expiryTimestampMs))
    keyRow
  }

  private def encodeValue(value: S): UnsafeRow = {
    val schemaForValueRow: StructType = new StructType().add("value", BinaryType)
    val valueByteArr = SerializationUtils.serialize(value.asInstanceOf[Serializable])
    val valueEncoder = UnsafeProjection.create(schemaForValueRow)
    val valueRow = valueEncoder(InternalRow(valueByteArr))
    valueRow
  }

  /**
   * Function to check if the timer for the given key and timestamp is already registered
   * @param expiryTimestampMs - expiry timestamp of the timer
   * @return - true if the timer is already registered, false otherwise
   */
  def exists(expiryTimestampMs: Long): Boolean = {
    getImpl(expiryTimestampMs) != null
  }

  private def getImpl(expiryTimestampMs: Long): UnsafeRow = {
    store.get(encodeKey(expiryTimestampMs), keyToTsCFName)
  }

  /**
   * Function to add a new timer for the given key and timestamp
   * @param expiryTimestampMs - expiry timestamp of the timer
   * @param newState = boolean value to be stored for the state value
   */
  def add(expiryTimestampMs: Long, newState: S): Unit = {
    store.put(encodeKey(expiryTimestampMs), EMPTY_ROW, keyToTsCFName)
  }

  /**
   * Function to remove the timer for the given key and timestamp
   * @param expiryTimestampMs - expiry timestamp of the timer
   */
  def remove(expiryTimestampMs: Long): Unit = {
    store.remove(encodeKey(expiryTimestampMs), keyToTsCFName)
  }

  def getExpiredTimers(): Iterator[UnsafeRowPair] = {
    store.iterator(keyToTsCFName)
  }

  def getTimerRow(keyRow: UnsafeRow): (Any, Long) = {
    val bytes = keyRow.getBinary(0)
    val retUnsafeRow = new UnsafeRow(1)
    retUnsafeRow.pointTo(bytes, bytes.length)
    val key = keyExprEnc.resolveAndBind().createDeserializer().apply(retUnsafeRow).asInstanceOf[Any]
    val expiryTimestampMs = keyRow.getLong(1)
    (key, expiryTimestampMs)
  }
}
