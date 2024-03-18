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

import java.time.Duration

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.execution.streaming.state.{NoPrefixKeyStateEncoderSpec, StateStore}
import org.apache.spark.sql.streaming.TTLMode
import org.apache.spark.sql.types.{BinaryType, DataType, LongType, NullType, StructField, StructType}

object StateTTLSchema {
  val KEY_ROW_SCHEMA: StructType = new StructType()
    .add("expirationMs", LongType)
    .add("groupingKey", BinaryType)
  val VALUE_ROW_SCHEMA: StructType =
    StructType(Array(StructField("__dummy__", NullType)))
}

trait StateVariableTTLSupport {
  def clearIfExpired(groupingKey: Array[Byte]): Unit
}

trait TTLState {
  def clearExpiredState(): Unit
}

class SingleKeyTTLState(
    ttlMode: TTLMode,
    stateName: String,
    store: StateStore,
    batchTimestampMs: Option[Long],
    eventTimeWatermarkMs: Option[Long],
    state: StateVariableTTLSupport)
  extends TTLState {

  import org.apache.spark.sql.execution.streaming.StateTTLSchema._

  private val ttlColumnFamilyName = s"_ttl_$stateName"
  private val ttlKeyEncoder = UnsafeProjection.create(KEY_ROW_SCHEMA)

  // empty row used for values
  private val EMPTY_ROW =
    UnsafeProjection.create(Array[DataType](NullType)).apply(InternalRow.apply(null))

  store.createColFamilyIfAbsent(ttlColumnFamilyName, KEY_ROW_SCHEMA, VALUE_ROW_SCHEMA,
    NoPrefixKeyStateEncoderSpec(KEY_ROW_SCHEMA), isInternal = true)

  def upsertTTLForStateKey(
      expirationMs: Long,
      groupingKey: Array[Byte]): Unit = {
    val encodedTtlKey = ttlKeyEncoder(InternalRow(expirationMs, groupingKey))
    store.put(encodedTtlKey, EMPTY_ROW, ttlColumnFamilyName)
  }

  override def clearExpiredState(): Unit = {
    store.iterator(ttlColumnFamilyName).foreach { kv =>
      val expirationMs = kv.key.getLong(0)
      val isExpired = StateTTL.isExpired(ttlMode, expirationMs,
        batchTimestampMs, eventTimeWatermarkMs)

      if (isExpired) {
        val groupingKey = kv.key.getBinary(1)
        state.clearIfExpired(groupingKey)

        // TODO(sahnib) ; validate its safe to update inside iterator
        store.remove(kv.key, ttlColumnFamilyName)
      }
    }
  }
}

object StateTTL {
  def calculateExpirationTimeForDuration(
      ttlMode: TTLMode,
      ttlDuration: Duration,
      batchTimestampMs: Option[Long],
      eventTimeWatermarkMs: Option[Long]): Long = {
    if (ttlMode == TTLMode.ProcessingTimeTTL()) {
      batchTimestampMs.get + ttlDuration.toMillis
    } else if (ttlMode == TTLMode.EventTimeTTL()) {
      eventTimeWatermarkMs.get + ttlDuration.toMillis
    } else {
      -1L
    }
  }

  def isExpired(
      ttlMode: TTLMode,
      expirationMs: Long,
      batchTimestampMs: Option[Long],
      eventTimeWatermarkMs: Option[Long]): Boolean = {
    if (ttlMode == TTLMode.ProcessingTimeTTL()) {
      batchTimestampMs.get > expirationMs
    } else if (ttlMode == TTLMode.EventTimeTTL()) {
      eventTimeWatermarkMs.get > expirationMs
    } else {
      false
    }
  }
}
