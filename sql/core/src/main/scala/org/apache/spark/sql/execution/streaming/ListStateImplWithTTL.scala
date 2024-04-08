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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.streaming.TransformWithStateKeyValueRowSchema.{KEY_ROW_SCHEMA, VALUE_ROW_SCHEMA_WITH_TTL}
import org.apache.spark.sql.execution.streaming.state.{NoPrefixKeyStateEncoderSpec, StateStore, StateStoreErrors}
import org.apache.spark.sql.streaming.{ListState, TTLConfig}
import org.apache.spark.util.NextIterator

/**
 * Provides concrete implementation for list of values associated with a state variable
 * used in the streaming transformWithState operator.
 *
 * @param store - reference to the StateStore instance to be used for storing state
 * @param stateName - name of logical state partition
 * @param keyEnc - Spark SQL encoder for key
 * @param valEncoder - Spark SQL encoder for value
 * @tparam S - data type of object that will be stored in the list
 */
class ListStateImplWithTTL[S](
  store: StateStore,
  stateName: String,
  keyExprEnc: ExpressionEncoder[Any],
  valEncoder: Encoder[S],
  ttlConfig: TTLConfig,
  batchTimestampMs: Long)
  extends SingleKeyTTLStateImpl(stateName, store, batchTimestampMs) with ListState[S] with Logging {

  private val keySerializer = keyExprEnc.createSerializer()

  private val stateTypesEncoder = StateTypesEncoder(
    keySerializer, valEncoder, stateName, hasTtl = true)

  private val ttlExpirationMs =
    StateTTL.calculateExpirationTimeForDuration(ttlConfig.ttlDuration, batchTimestampMs)

  initialize()

  private def initialize(): Unit = {
    store.createColFamilyIfAbsent(stateName, KEY_ROW_SCHEMA, VALUE_ROW_SCHEMA_WITH_TTL,
      NoPrefixKeyStateEncoderSpec(KEY_ROW_SCHEMA), useMultipleValuesPerKey = true)
  }
  /** Whether state exists or not. */
  override def exists(): Boolean = {
    val encodedGroupingKey = stateTypesEncoder.encodeGroupingKey()
    val stateValue = store.get(encodedGroupingKey, stateName)
    stateValue != null
  }

  /**
   * Get the state value if it exists. If the state does not exist in state store, an
   * empty iterator is returned.
   */
  override def get(): Iterator[S] = {
    val encodedKey = stateTypesEncoder.encodeGroupingKey()

    val unsafeRowValuesIterator = store.valuesIterator(encodedKey, stateName)

    new NextIterator[S] {
      private var currentRow: UnsafeRow = _

      override protected def getNext(): S = {
        if (unsafeRowValuesIterator.hasNext) {
          currentRow = unsafeRowValuesIterator.next()
          while (unsafeRowValuesIterator.hasNext && isExpired(currentRow)) {
            currentRow = unsafeRowValuesIterator.next()
          }
          // in this case, we have iterated to the end, and there are no
          // non-expired values
          if (currentRow != null && isExpired(currentRow)) {
            currentRow = null
          }
        } else {
          finished = true
        }
        if (currentRow != null) stateTypesEncoder.decodeValue(currentRow) else null.asInstanceOf[S]
      }

      override protected def close(): Unit = {}
    }
  }

  /** Update the value of the list. */
  override def put(newState: Array[S]): Unit = {
    validateNewState(newState)

    val encodedKey = stateTypesEncoder.encodeGroupingKey()
    var isFirst = true

    newState.foreach { v =>
      val encodedValue = stateTypesEncoder.encodeValue(v, ttlExpirationMs)
      if (isFirst) {
        store.put(encodedKey, encodedValue, stateName)
        isFirst = false
      } else {
        store.merge(encodedKey, encodedValue, stateName)
      }
    }
    val serializedGroupingKey = stateTypesEncoder.serializeGroupingKey()
    upsertTTLForStateKey(ttlExpirationMs, serializedGroupingKey)
  }

  /** Append an entry to the list. */
  override def appendValue(newState: S): Unit = {
    StateStoreErrors.requireNonNullStateValue(newState, stateName)
    store.merge(stateTypesEncoder.encodeGroupingKey(),
      stateTypesEncoder.encodeValue(newState, ttlExpirationMs), stateName)
    val serializedGroupingKey = stateTypesEncoder.serializeGroupingKey()
    upsertTTLForStateKey(ttlExpirationMs, serializedGroupingKey)
  }

  /** Append an entire list to the existing value. */
  override def appendList(newState: Array[S]): Unit = {
    validateNewState(newState)

    val encodedKey = stateTypesEncoder.encodeGroupingKey()
    newState.foreach { v =>
      val encodedValue = stateTypesEncoder.encodeValue(v, ttlExpirationMs)
      store.merge(encodedKey, encodedValue, stateName)
    }
    val serializedGroupingKey = stateTypesEncoder.serializeGroupingKey()
    upsertTTLForStateKey(ttlExpirationMs, serializedGroupingKey)
  }

  /** Remove this state. */
  override def clear(): Unit = {
    store.remove(stateTypesEncoder.encodeGroupingKey(), stateName)
  }

  private def validateNewState(newState: Array[S]): Unit = {
    StateStoreErrors.requireNonNullStateValue(newState, stateName)
    StateStoreErrors.requireNonEmptyListStateValue(newState, stateName)

    newState.foreach { v =>
      StateStoreErrors.requireNonNullStateValue(v, stateName)
    }
  }

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
  override def clearIfExpired(groupingKey: Array[Byte]): Unit = {
    val encodedGroupingKey = stateTypesEncoder.encodeSerializedGroupingKey(groupingKey)
    val unsafeRowValuesIterator = store.valuesIterator(encodedGroupingKey, stateName)
    // We clear the list, and use the iterator to put back all of the non-expired values
    store.remove(encodedGroupingKey, stateName)
    var isFirst = true
    unsafeRowValuesIterator.foreach { encodedValue =>
      if (!isExpired(encodedValue)) {
        if (isFirst) {
          store.put(encodedGroupingKey, encodedValue, stateName)
          isFirst = false
        } else {
          store.merge(encodedGroupingKey, encodedValue, stateName)
        }
      }
    }
  }

  private def isExpired(valueRow: UnsafeRow): Boolean = {
    val expirationMs = stateTypesEncoder.decodeTtlExpirationMs(valueRow)
    expirationMs.exists(StateTTL.isExpired(_, batchTimestampMs))
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
    new Iterator[S] {
      override def hasNext: Boolean = {
        unsafeRowValuesIterator.hasNext
      }

      override def next(): S = {
        val valueUnsafeRow = unsafeRowValuesIterator.next()
        if (valueUnsafeRow != null) {
          stateTypesEncoder.decodeValue(valueUnsafeRow)
        } else {
          null.asInstanceOf[S]
        }
      }
    }
  }

  /**
   * Read the ttl value associated with the grouping key.
   */
  private[sql] def getTTLValues(): Iterator[Option[Long]] = {
    val encodedGroupingKey = stateTypesEncoder.encodeGroupingKey()
    val unsafeRowValuesIterator = store.valuesIterator(encodedGroupingKey, stateName)
    new Iterator[Option[Long]] {
      override def hasNext: Boolean = {
        unsafeRowValuesIterator.hasNext
      }

      override def next(): Option[Long] = {
        val valueUnsafeRow = unsafeRowValuesIterator.next()
        stateTypesEncoder.decodeTtlExpirationMs(valueUnsafeRow)
      }
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
