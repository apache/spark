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
import java.nio.{ByteBuffer, ByteOrder}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.streaming.TimeoutMode
import org.apache.spark.sql.types._
import org.apache.spark.util.NextIterator

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

  private val keySchemaForSecIndex: StructType = new StructType()
    .add("expiryTimestampMs", BinaryType, nullable = false)
    .add("key", BinaryType)

  private val schemaForValueRow: StructType =
    StructType(Array(StructField("__dummy__", NullType)))

  val timerCfName = if (timeoutMode == TimeoutMode.ProcessingTime) {
    TimerStateUtils.PROC_TIMERS_STATE_NAME
  } else {
    TimerStateUtils.EVENT_TIMERS_STATE_NAME
  }

  val keyToTsCFName = timerCfName + TimerStateUtils.KEY_TO_TIMESTAMP_CF
  store.createColFamilyIfAbsent(keyToTsCFName,
    schemaForKeyRow, numColsPrefixKey = 0,
    schemaForValueRow, true,
    isInternal = true)

  val tsToKeyCFName = timerCfName + TimerStateUtils.TIMESTAMP_TO_KEY_CF
  store.createColFamilyIfAbsent(tsToKeyCFName,
    keySchemaForSecIndex, numColsPrefixKey = 0,
    schemaForValueRow, true,
    isInternal = true)

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

  private def encodeSecIndexKey(expiryTimestampMs: Long): UnsafeRow = {
    val keyOption = ImplicitGroupingKeyTracker.getImplicitKeyOption
    if (!keyOption.isDefined) {
      throw new UnsupportedOperationException("Implicit key not found for operation on" +
        s"stateName=$keyToTsCFName")
    }

    val toRow = keyExprEnc.createSerializer()
    val keyByteArr = toRow
      .apply(keyOption.get).asInstanceOf[UnsafeRow].getBytes()

    val keyEncoder = UnsafeProjection.create(keySchemaForSecIndex)
    val bbuf = ByteBuffer.allocate(8)
    bbuf.order(ByteOrder.BIG_ENDIAN)
    bbuf.putLong(expiryTimestampMs)
    val keyRow = keyEncoder(InternalRow(bbuf.array(), keyByteArr))
    keyRow
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
    store.put(encodeSecIndexKey(expiryTimestampMs), EMPTY_ROW, tsToKeyCFName)
  }

  /**
   * Function to remove the timer for the given key and timestamp
   * @param expiryTimestampMs - expiry timestamp of the timer
   */
  def remove(expiryTimestampMs: Long): Unit = {
    store.remove(encodeKey(expiryTimestampMs), keyToTsCFName)
    store.remove(encodeSecIndexKey(expiryTimestampMs), tsToKeyCFName)
  }

  private def getTimerRow(keyRow: UnsafeRow): (Any, Long) = {
    // Decode the key object from the UnsafeRow
    val keyBytes = keyRow.getBinary(1)
    val retUnsafeRow = new UnsafeRow(1)
    retUnsafeRow.pointTo(keyBytes, keyBytes.length)
    val keyObj = keyExprEnc.resolveAndBind().
    createDeserializer().apply(retUnsafeRow).asInstanceOf[Any]

    // Decode the expiry timestamp from the UnsafeRow encoded as BIG_ENDIAN
    val bytes = keyRow.getBinary(0)
    val byteBuffer = ByteBuffer.wrap(bytes)
    byteBuffer.order(ByteOrder.BIG_ENDIAN)
    val expiryTimestampMs = byteBuffer.getLong
    (keyObj, expiryTimestampMs)
  }

  /**
   * Function to get all the timers that have expired till the given expiryTimestampThreshold
   * @param expiryTimestampThreshold - threshold timestamp for expiry
   * @return - iterator of all the timers that have expired till the given threshold
   */
  def getExpiredTimers(expiryTimestampThreshold: Long): Iterator[(Any, Long)] = {
    val iter = store.iterator(tsToKeyCFName)

    new NextIterator[(Any, Long)] {
      override protected def getNext(): (Any, Long) = {
        if (iter.hasNext) {
          val rowPair = iter.next()
          val keyRow = rowPair.key
          val result = getTimerRow(keyRow)
          if (result._2 <= expiryTimestampThreshold) {
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
