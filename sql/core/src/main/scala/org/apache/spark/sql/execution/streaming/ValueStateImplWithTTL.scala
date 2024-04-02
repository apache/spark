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

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.streaming.TransformWithStateKeyValueRowSchema.{KEY_ROW_SCHEMA, VALUE_ROW_SCHEMA_WITH_TTL}
import org.apache.spark.sql.execution.streaming.state.{NoPrefixKeyStateEncoderSpec, StateStore, StateStoreErrors}
import org.apache.spark.sql.streaming.{TTLMode, ValueState}

/**
 * Class that provides a concrete implementation for a single value state associated with state
 * variables (with ttl expiration support) used in the streaming transformWithState operator.
 *
 * @param store - reference to the StateStore instance to be used for storing state
 * @param stateName - name of logical state partition
 * @param keyExprEnc - Spark SQL encoder for key
 * @param valEncoder - Spark SQL encoder for value
 * @param ttlMode    - TTL Mode for values  stored in this state
 * @param batchTtlExpirationMs - ttl expiration for the current batch.
 * @tparam S - data type of object that will be stored
 */
class ValueStateImplWithTTL[S](
    store: StateStore,
    stateName: String,
    keyExprEnc: ExpressionEncoder[Any],
    valEncoder: Encoder[S],
    ttlMode: TTLMode,
    batchTtlExpirationMs: Long)
  extends SingleKeyTTLStateImpl(stateName, store, batchTtlExpirationMs) with ValueState[S] {

  private val keySerializer = keyExprEnc.createSerializer()
  private val stateTypesEncoder = StateTypesEncoder(keySerializer, valEncoder,
    stateName, hasTtl = true)

  initialize()

  private def initialize(): Unit = {
    assert(ttlMode != TTLMode.NoTTL())

    store.createColFamilyIfAbsent(stateName, KEY_ROW_SCHEMA, VALUE_ROW_SCHEMA_WITH_TTL,
      NoPrefixKeyStateEncoderSpec(KEY_ROW_SCHEMA))
  }

  /** Function to check if state exists. Returns true if present and false otherwise */
  override def exists(): Boolean = {
    get() != null
  }

  /** Function to return Option of value if exists and None otherwise */
  override def getOption(): Option[S] = {
    Option(get())
  }

  /** Function to return associated value with key if exists and null otherwise */
  override def get(): S = {
    val encodedGroupingKey = stateTypesEncoder.encodeGroupingKey()
    val retRow = store.get(encodedGroupingKey, stateName)

    if (retRow != null) {
      val resState = stateTypesEncoder.decodeValue(retRow)

      if (!isExpired(retRow)) {
        resState
      } else {
        null.asInstanceOf[S]
      }
    } else {
      null.asInstanceOf[S]
    }
  }

  /** Function to update and overwrite state associated with given key */
  override def update(
      newState: S,
      ttlDuration: Duration = Duration.ZERO): Unit = {

    if (ttlMode == TTLMode.EventTimeTTL() && ttlDuration != Duration.ZERO) {
      throw StateStoreErrors.cannotProvideTTLDurationForEventTimeTTLMode("update", stateName)
    }

    if (ttlDuration != null && ttlDuration.isNegative) {
      throw StateStoreErrors.ttlCannotBeNegative("update", stateName)
    }

    val expirationTimeInMs =
      if (ttlDuration != null && ttlDuration != Duration.ZERO) {
        StateTTL.calculateExpirationTimeForDuration(ttlDuration, batchTtlExpirationMs)
      } else {
        -1
      }

    doUpdate(newState, expirationTimeInMs)
  }

  override def update(
      newState: S,
      expirationTimeInMs: Long): Unit = {

    if (expirationTimeInMs < 0) {
      throw StateStoreErrors.ttlCannotBeNegative(
        "update", stateName)
    }

    doUpdate(newState, expirationTimeInMs)
  }

  private def doUpdate(newState: S,
      expirationTimeInMs: Long): Unit = {
    val encodedValue = stateTypesEncoder.encodeValue(newState, expirationTimeInMs)

    val serializedGroupingKey = stateTypesEncoder.serializeGroupingKey()
    store.put(stateTypesEncoder.encodeSerializedGroupingKey(serializedGroupingKey),
      encodedValue, stateName)

    if (expirationTimeInMs != -1) {
      upsertTTLForStateKey(expirationTimeInMs, serializedGroupingKey)
    }
  }

  /** Function to remove state for given key */
  override def clear(): Unit = {
    store.remove(stateTypesEncoder.encodeGroupingKey(), stateName)
  }

  def clearIfExpired(groupingKey: Array[Byte]): Unit = {
    val encodedGroupingKey = stateTypesEncoder.encodeSerializedGroupingKey(groupingKey)
    val retRow = store.get(encodedGroupingKey, stateName)

    if (retRow != null) {
      if (isExpired(retRow)) {
        store.remove(encodedGroupingKey, stateName)
      }
    }
  }

  private def isExpired(valueRow: UnsafeRow): Boolean = {
    val expirationMs = stateTypesEncoder.decodeTtlExpirationMs(valueRow)
    val isExpired = expirationMs.map(
      StateTTL.isExpired(_, batchTtlExpirationMs))

    isExpired.isDefined && isExpired.get
  }

  /*
   * Internal methods to probe state for testing. The below methods exist for unit tests
   * to read the state ttl values, and ensure that values are persisted correctly in
   * the underlying  state store.
   */

  /**
   * Retrieves the value from State even if its expired. This method is used
   * in tests to read the state store value, and ensure if its cleaned up at the
   * end of the micro-batch.
   */
  private[sql] def getWithoutEnforcingTTL(): Option[S] = {
    val encodedGroupingKey = stateTypesEncoder.encodeGroupingKey()
    val retRow = store.get(encodedGroupingKey, stateName)

    if (retRow != null) {
      val resState = stateTypesEncoder.decodeValue(retRow)
      Some(resState)
    } else {
      None
    }
  }

  /**
   * Read the ttl value associated with the grouping key.
   */
  private[sql] def getTTLValue(): Option[Long] = {
    val encodedGroupingKey = stateTypesEncoder.encodeGroupingKey()
    val retRow = store.get(encodedGroupingKey, stateName)

    if (retRow != null) {
      stateTypesEncoder.decodeTtlExpirationMs(retRow)
    } else {
      None
    }
  }

  /**
   * Get all ttl values stored in ttl state for current implicit
   * grouping key.
   */
  private[sql] def getValuesInTTLState(): Iterator[Long] = {
    val ttlIterator = ttlIndexIterator()
    val implicitGroupingKey = stateTypesEncoder.serializeGroupingKey()
    var nextValue: Option[Long] = None

    new Iterator[Long] {
      override def hasNext: Boolean = {
        while (nextValue.isEmpty && ttlIterator.hasNext) {
          val nextTtlValue = ttlIterator.next()
          val groupingKey = nextTtlValue.groupingKey
          if (groupingKey sameElements implicitGroupingKey) {
            nextValue = Some(nextTtlValue.expirationMs)
          }
        }
        nextValue.isDefined
      }

      override def next(): Long = {
        val result = nextValue.get
        nextValue = None
        result
      }
    }
  }
}

