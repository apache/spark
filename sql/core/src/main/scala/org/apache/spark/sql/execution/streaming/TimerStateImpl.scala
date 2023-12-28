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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.streaming.state.StateStore
import org.apache.spark.sql.types._

object TimerStateUtils {
  case class TimestampWithKey(
      key: Any,
      expiryTimestampMs: Long) extends Serializable

  val PROC_TIMERS_STATE_NAME = "_procTimers"
}

/**
 * Class that provides a concrete implementation for TimerState
 * @param store - state store to be used for storing timer data
 * @param stateName - name of the timer state variable
 * @tparam S - type of timer value
 */
class TimerStateImpl[S](
    store: StateStore,
    stateName: String) extends Logging {

  // TODO: validate places that are trying to encode the key and check if we can eliminate/
  // add caching for some of these calls.
  private def encodeKey(expiryTimestampMs: Long): UnsafeRow = {
    val keyOption = ImplicitKeyTracker.getImplicitKeyOption
    if (!keyOption.isDefined) {
      throw new UnsupportedOperationException("Implicit key not found for operation on" +
        s"stateName=$stateName")
    }

    val tsWithKey = TimerStateUtils.TimestampWithKey(keyOption.get, expiryTimestampMs)

    val schemaForKeyRow: StructType = new StructType().add("key", BinaryType)
    val keyByteArr = SerializationUtils.serialize(tsWithKey.asInstanceOf[Serializable])
    val keyEncoder = UnsafeProjection.create(schemaForKeyRow)
    val keyRow = keyEncoder(InternalRow(keyByteArr))
    keyRow
  }

  private def encodeValue(value: S): UnsafeRow = {
    val schemaForValueRow: StructType = new StructType().add("value", BinaryType)
    val valueByteArr = SerializationUtils.serialize(value.asInstanceOf[Serializable])
    val valueEncoder = UnsafeProjection.create(schemaForValueRow)
    val valueRow = valueEncoder(InternalRow(valueByteArr))
    valueRow
  }

  /** Function to check if state exists. Returns true if present and false otherwise */
  def exists(expiryTimestampMs: Long): Boolean = {
    getImpl(expiryTimestampMs) != null
  }

  private def getImpl(expiryTimestampMs: Long): UnsafeRow = {
    store.get(encodeKey(expiryTimestampMs), stateName)
  }

  /** Function to update and overwrite state associated with given key */
  def add(expiryTimestampMs: Long, newState: S): Unit = {
    store.put(encodeKey(expiryTimestampMs), encodeValue(newState), stateName)
  }

  /** Function to remove state for given key */
  def remove(expiryTimestampMs: Long): Unit = {
    store.remove(encodeKey(expiryTimestampMs), stateName)
  }
}
