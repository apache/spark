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

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.streaming.TransformWithStateKeyValueRowSchemaUtils._
import org.apache.spark.sql.execution.streaming.state.{NoPrefixKeyStateEncoderSpec, StateStore, StateStoreErrors}
import org.apache.spark.sql.streaming.{ListState, TTLConfig}
import org.apache.spark.util.NextIterator

/**
 * Class that provides a concrete implementation for a list state state associated with state
 * variables (with ttl expiration support) used in the streaming transformWithState operator.
 *
 * @param store - reference to the StateStore instance to be used for storing state
 * @param stateName - name of logical state partition
 * @param keyExprEnc - Spark SQL encoder for key
 * @param valEncoder - Spark SQL encoder for value
 * @param ttlConfig  - TTL configuration for values  stored in this state
 * @param batchTimestampMs - current batch processing timestamp.
 * @param metrics - metrics to be updated as part of stateful processing
 * @tparam S - data type of object that will be stored
 */
class ListStateImplWithTTL[S](
    store: StateStore,
    stateName: String,
    keyExprEnc: ExpressionEncoder[Any],
    valEncoder: Encoder[S],
    ttlConfig: TTLConfig,
    batchTimestampMs: Long,
    metrics: Map[String, SQLMetric] = Map.empty)
  extends SingleKeyTTLStateImpl(stateName, store, keyExprEnc, batchTimestampMs)
  with ListStateMetricsImpl
  with ListState[S] {

  private lazy val stateTypesEncoder = StateTypesEncoder(keyExprEnc, valEncoder,
    stateName, hasTtl = true)

  private lazy val ttlExpirationMs =
    StateTTL.calculateExpirationTimeForDuration(ttlConfig.ttlDuration, batchTimestampMs)

  initialize()

  private def initialize(): Unit = {
    store.createColFamilyIfAbsent(stateName, keyExprEnc.schema,
      getValueSchemaWithTTL(valEncoder.schema, true),
      NoPrefixKeyStateEncoderSpec(keyExprEnc.schema), useMultipleValuesPerKey = true)

    initMetrics(store, keyExprEnc, stateName)
  }

  /** Whether state exists or not. */
  override def exists(): Boolean = {
    get().nonEmpty
  }

  /**
   * Get the state value if it exists. If the state does not exist in state store, an
   * empty iterator is returned.
   */
  override def get(): Iterator[S] = {
    val encodedKey = stateTypesEncoder.encodeGroupingKey()
    val unsafeRowValuesIterator = store.valuesIterator(encodedKey, stateName)

    new NextIterator[S] {

      override protected def getNext(): S = {
        val iter = unsafeRowValuesIterator.dropWhile { row =>
          stateTypesEncoder.isExpired(row, batchTimestampMs)
        }

        if (iter.hasNext) {
          val currentRow = iter.next()
          stateTypesEncoder.decodeValue(currentRow)
        } else {
          finished = true
          null.asInstanceOf[S]
        }
      }

      override protected def close(): Unit = {}
    }
  }

  /** Update the value of the list. */
  override def put(newState: Array[S]): Unit = {
    validateNewState(newState)

    val encodedKey = stateTypesEncoder.encodeGroupingKey()
    var isFirst = true
    var entryCount = getEntryCount(store, encodedKey, stateName)

    newState.foreach { v =>
      val encodedValue = stateTypesEncoder.encodeValue(v, ttlExpirationMs)
      if (isFirst) {
        store.put(encodedKey, encodedValue, stateName)
        isFirst = false
      } else {
        store.merge(encodedKey, encodedValue, stateName)
      }
      entryCount += 1
      TWSMetricsUtils.incrementMetric(metrics, "numUpdatedStateRows")
    }
    upsertTTLForStateKey(encodedKey)
    updateEntryCount(store, encodedKey, stateName, entryCount)
  }

  /** Append an entry to the list. */
  override def appendValue(newState: S): Unit = {
    StateStoreErrors.requireNonNullStateValue(newState, stateName)
    val encodedKey = stateTypesEncoder.encodeGroupingKey()
    val entryCount = getEntryCount(store, encodedKey, stateName)
    store.merge(encodedKey,
      stateTypesEncoder.encodeValue(newState, ttlExpirationMs), stateName)
    TWSMetricsUtils.incrementMetric(metrics, "numUpdatedStateRows")
    upsertTTLForStateKey(encodedKey)
    updateEntryCount(store, encodedKey, stateName, entryCount + 1)
  }

  /** Append an entire list to the existing value. */
  override def appendList(newState: Array[S]): Unit = {
    validateNewState(newState)

    val encodedKey = stateTypesEncoder.encodeGroupingKey()
    var entryCount = getEntryCount(store, encodedKey, stateName)
    newState.foreach { v =>
      val encodedValue = stateTypesEncoder.encodeValue(v, ttlExpirationMs)
      store.merge(encodedKey, encodedValue, stateName)
      entryCount += 1
      TWSMetricsUtils.incrementMetric(metrics, "numUpdatedStateRows")
    }
    upsertTTLForStateKey(encodedKey)
    updateEntryCount(store, encodedKey, stateName, entryCount)
  }

  /** Remove this state. */
  override def clear(): Unit = {
    val encodedKey = stateTypesEncoder.encodeGroupingKey()
    store.remove(encodedKey, stateName)
    val entryCount = getEntryCount(store, encodedKey, stateName)
    TWSMetricsUtils.incrementMetric(metrics, "numRemovedStateRows", entryCount)
    removeEntryCount(store, encodedKey, stateName)
    clearTTLState()
  }

  private def validateNewState(newState: Array[S]): Unit = {
    StateStoreErrors.requireNonNullStateValue(newState, stateName)
    StateStoreErrors.requireNonEmptyListStateValue(newState, stateName)

    newState.foreach { v =>
      StateStoreErrors.requireNonNullStateValue(v, stateName)
    }
  }

  /**
   * Loops through all the values associated with the grouping key, and removes
   * the expired elements from the list.
   * @param groupingKey grouping key for which cleanup should be performed.
   */
  override def clearIfExpired(groupingKey: UnsafeRow): Long = {
    var numValuesExpired = 0L
    val unsafeRowValuesIterator = store.valuesIterator(groupingKey, stateName)
    // We clear the list, and use the iterator to put back all of the non-expired values
    store.remove(groupingKey, stateName)
    var isFirst = true
    unsafeRowValuesIterator.foreach { encodedValue =>
      if (!stateTypesEncoder.isExpired(encodedValue, batchTimestampMs)) {
        if (isFirst) {
          store.put(groupingKey, encodedValue, stateName)
          isFirst = false
        } else {
          store.merge(groupingKey, encodedValue, stateName)
        }
      } else {
        numValuesExpired += 1
      }
    }
    numValuesExpired
  }

  private def upsertTTLForStateKey(encodedGroupingKey: UnsafeRow): Unit = {
    upsertTTLForStateKey(ttlExpirationMs, encodedGroupingKey)
  }

  /*
    * Internal methods to probe state for testing. The below methods exist for unit tests
    * to read the state ttl values, and ensure that values are persisted correctly in
    * the underlying state store.
    */

  /**
   * Retrieves the value from State even if its expired. This method is used
   * in tests to read the state store value, and ensure if its cleaned up at the
   * end of the micro-batch.
   */
  private[sql] def getWithoutEnforcingTTL(): Iterator[S] = {
    val encodedGroupingKey = stateTypesEncoder.encodeGroupingKey()
    val unsafeRowValuesIterator = store.valuesIterator(encodedGroupingKey, stateName)
    unsafeRowValuesIterator.map { valueUnsafeRow =>
      stateTypesEncoder.decodeValue(valueUnsafeRow)
    }
  }

  /**
   * Read the ttl value associated with the grouping key.
   */
  private[sql] def getTTLValues(): Iterator[(S, Long)] = {
    val encodedGroupingKey = stateTypesEncoder.encodeGroupingKey()
    val unsafeRowValuesIterator = store.valuesIterator(encodedGroupingKey, stateName)
    unsafeRowValuesIterator.map { valueUnsafeRow =>
      (stateTypesEncoder.decodeValue(valueUnsafeRow),
        stateTypesEncoder.decodeTtlExpirationMs(valueUnsafeRow).get)
    }
  }

  /**
   * Get all ttl values stored in ttl state for current implicit
   * grouping key.
   */
  private[sql] def getValuesInTTLState(): Iterator[Long] = {
    getValuesInTTLState(stateTypesEncoder.encodeGroupingKey())
  }
}
