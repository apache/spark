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

import org.apache.spark.internal.Logging
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

/**
 * Encapsulates the ttl row information stored in [[SingleKeyTTLState]].
 * @param groupingKey grouping key for which ttl is set
 * @param expirationMs expiration time for the grouping key
 */
case class SingleKeyTTLRow(
    groupingKey: Array[Byte],
    expirationMs: Long)

/**
 * Represents a State variable which supports TTL.
 */
trait StateVariableWithTTLSupport {

  /**
   * Clears the user state associated with this grouping key
   * if it has expired. This function is called by Spark to perform
   * cleanup at the end of transformWithState processing.
   *
   * Spark uses a secondary index to determine if the user state for
   * this grouping key has expired. However, its possible that the user
   * has updated the TTL and secondary index is out of date. Implementations
   * must validate that the user State has actually expired before cleanup based
   * on their own State data.
   *
   * @param groupingKey grouping key for which cleanup should be performed.
   */
  def clearIfExpired(groupingKey: Array[Byte]): Unit
}

/**
 * Represents the underlying state for secondary TTL Index for a user defined
 * state variable.
 *
 * This state allows Spark to query ttl values based on expiration time
 * allowing efficient ttl cleanup.
 */
trait TTLState {

  /**
   * Perform the user state clean yp based on ttl values stored in
   * this state. NOTE that its not safe to call this operation concurrently
   * when the user can also modify the underlying State. Cleanup should be initiated
   * after arbitrary state operations are completed by the user.
   */
  def clearExpiredState(): Unit
}

/**
 * Manages the ttl information for user state keyed with a single key (grouping key).
 */
class SingleKeyTTLState(
    ttlMode: TTLMode,
    stateName: String,
    store: StateStore,
    batchTimestampMs: Option[Long],
    eventTimeWatermarkMs: Option[Long])
  extends TTLState
  with Logging {

  import org.apache.spark.sql.execution.streaming.StateTTLSchema._

  private val ttlColumnFamilyName = s"_ttl_$stateName"
  private val ttlKeyEncoder = UnsafeProjection.create(KEY_ROW_SCHEMA)
  private var state: StateVariableWithTTLSupport = _

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

        store.remove(kv.key, ttlColumnFamilyName)
      }
    }
  }

  private[sql] def setStateVariable(
      state: StateVariableWithTTLSupport): Unit = {
    this.state = state
  }

  private[sql] def iterator(): Iterator[SingleKeyTTLRow] = {
    val ttlIterator = store.iterator(ttlColumnFamilyName)

    new Iterator[SingleKeyTTLRow] {
      override def hasNext: Boolean = ttlIterator.hasNext

      override def next(): SingleKeyTTLRow = {
        val kv = ttlIterator.next()
        SingleKeyTTLRow(
          expirationMs = kv.key.getLong(0),
          groupingKey = kv.key.getBinary(1)
        )
      }
    }
  }
}

/**
 * Helper methods for user State TTL.
 */
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
      throw new IllegalStateException(s"cannot calculate expiration time for" +
        s" unknown ttl Mode $ttlMode")
    }
  }

  def isExpired(
      ttlMode: TTLMode,
      expirationMs: Long,
      batchTimestampMs: Option[Long],
      eventTimeWatermarkMs: Option[Long]): Boolean = {
    if (ttlMode == TTLMode.ProcessingTimeTTL()) {
      batchTimestampMs.get >= expirationMs
    } else if (ttlMode == TTLMode.EventTimeTTL()) {
      eventTimeWatermarkMs.get >= expirationMs
    } else {
      throw new IllegalStateException(s"cannot evaluate expiry condition for" +
        s" unknown ttl Mode $ttlMode")
    }
  }
}
