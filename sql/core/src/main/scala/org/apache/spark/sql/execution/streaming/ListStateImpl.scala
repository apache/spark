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
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.streaming.state.{AvroSerde, NoPrefixKeyStateEncoderSpec, StateStore, StateStoreErrors}
import org.apache.spark.sql.streaming.ListState
import org.apache.spark.sql.types.StructType

/**
 * Provides concrete implementation for list of values associated with a state variable
 * used in the streaming transformWithState operator.
 *
 * @param store - reference to the StateStore instance to be used for storing state
 * @param stateName - name of logical state partition
 * @param keyExprEnc - Spark SQL encoder for key
 * @param valEncoder - Spark SQL encoder for value
 * @param metrics - metrics to be updated as part of stateful processing
 * @tparam S - data type of object that will be stored in the list
 */
class ListStateImpl[S](
    store: StateStore,
    stateName: String,
    keyExprEnc: ExpressionEncoder[Any],
    valEncoder: Encoder[S],
    avroSerde: Option[AvroSerde],
    metrics: Map[String, SQLMetric] = Map.empty)
  extends ListStateMetricsImpl
    with ListState[S]
    with Logging {

  override def stateStore: StateStore = store
  override def baseStateName: String = stateName
  override def exprEncSchema: StructType = keyExprEnc.schema

  // If we are using Avro, the avroSerde parameter must be populated
  // else, we will default to using UnsafeRow.
  private val usingAvro: Boolean = avroSerde.isDefined
  private val avroTypesEncoder = new AvroTypesEncoder[S](
    keyExprEnc, valEncoder, stateName, hasTtl = false, avroSerde)
  private val unsafeRowTypesEncoder = new UnsafeRowTypesEncoder[S](
    keyExprEnc, valEncoder, stateName, hasTtl = false)

  store.createColFamilyIfAbsent(stateName, keyExprEnc.schema, valEncoder.schema,
    NoPrefixKeyStateEncoderSpec(keyExprEnc.schema), useMultipleValuesPerKey = true)

  /** Whether state exists or not. */
  override def exists(): Boolean = {
    if (usingAvro) {
      val encodedKey: Array[Byte] = avroTypesEncoder.encodeGroupingKey()
      store.get(encodedKey, stateName) != null
    } else {
      val encodedKey = unsafeRowTypesEncoder.encodeGroupingKey()
      store.get(encodedKey, stateName) != null
    }
  }

  /**
   * Get the state value if it exists. If the state does not exist in state store, an
   * empty iterator is returned.
   */
  override def get(): Iterator[S] = {
    if (usingAvro) {
      getAvro()
    } else {
      getUnsafeRow()
    }
  }

  private def getAvro(): Iterator[S] = {
    val encodedKey: Array[Byte] = avroTypesEncoder.encodeGroupingKey()
    val avroValuesIterator = store.valuesIterator(encodedKey, stateName)
    new Iterator[S] {
      override def hasNext: Boolean = {
        avroValuesIterator.hasNext
      }

      override def next(): S = {
        val valueRow = avroValuesIterator.next()
        avroTypesEncoder.decodeValue(valueRow)
      }
    }
  }

  private def getUnsafeRow(): Iterator[S] = {
    val encodedKey = unsafeRowTypesEncoder.encodeGroupingKey()
    val unsafeRowValuesIterator = store.valuesIterator(encodedKey, stateName)
    new Iterator[S] {
      override def hasNext: Boolean = {
        unsafeRowValuesIterator.hasNext
      }

      override def next(): S = {
        val valueUnsafeRow = unsafeRowValuesIterator.next()
        unsafeRowTypesEncoder.decodeValue(valueUnsafeRow)
      }
    }
  }

  /** Update the value of the list. */
  override def put(newState: Array[S]): Unit = {
    validateNewState(newState)

    if (usingAvro) {
      putAvro(newState)
    } else {
      putUnsafeRow(newState)
    }
  }

  private def putAvro(newState: Array[S]): Unit = {
    val encodedKey: Array[Byte] = avroTypesEncoder.encodeGroupingKey()
    var isFirst = true
    var entryCount = 0L
    TWSMetricsUtils.resetMetric(metrics, "numUpdatedStateRows")

    newState.foreach { v =>
      val encodedValue = avroTypesEncoder.encodeValue(v)
      if (isFirst) {
        store.put(encodedKey, encodedValue, stateName)
        isFirst = false
      } else {
        store.merge(encodedKey, encodedValue, stateName)
      }
      entryCount += 1
      TWSMetricsUtils.incrementMetric(metrics, "numUpdatedStateRows")
    }
  }

  private def putUnsafeRow(newState: Array[S]): Unit = {
    val encodedKey = unsafeRowTypesEncoder.encodeGroupingKey()
    var isFirst = true
    var entryCount = 0L
    TWSMetricsUtils.resetMetric(metrics, "numUpdatedStateRows")

    newState.foreach { v =>
      val encodedValue = unsafeRowTypesEncoder.encodeValue(v)
      if (isFirst) {
        store.put(encodedKey, encodedValue, stateName)
        isFirst = false
      } else {
        store.merge(encodedKey, encodedValue, stateName)
      }
      entryCount += 1
      TWSMetricsUtils.incrementMetric(metrics, "numUpdatedStateRows")
    }
    updateEntryCount(encodedKey, entryCount)
  }

  /** Append an entry to the list. */
  override def appendValue(newState: S): Unit = {
    StateStoreErrors.requireNonNullStateValue(newState, stateName)

    if (usingAvro) {
      val encodedKey: Array[Byte] = avroTypesEncoder.encodeGroupingKey()
      val encodedValue = avroTypesEncoder.encodeValue(newState)
      store.merge(encodedKey, encodedValue, stateName)
      TWSMetricsUtils.incrementMetric(metrics, "numUpdatedStateRows")
    } else {
      val encodedKey = unsafeRowTypesEncoder.encodeGroupingKey()
      val entryCount = getEntryCount(encodedKey)
      val encodedValue = unsafeRowTypesEncoder.encodeValue(newState)
      store.merge(encodedKey, encodedValue, stateName)
      TWSMetricsUtils.incrementMetric(metrics, "numUpdatedStateRows")
      updateEntryCount(encodedKey, entryCount + 1)
    }
  }

  /** Append an entire list to the existing value. */
  override def appendList(newState: Array[S]): Unit = {
    validateNewState(newState)

    if (usingAvro) {
      val encodedKey: Array[Byte] = avroTypesEncoder.encodeGroupingKey()
      newState.foreach { v =>
        val encodedValue = avroTypesEncoder.encodeValue(v)
        store.merge(encodedKey, encodedValue, stateName)
        TWSMetricsUtils.incrementMetric(metrics, "numUpdatedStateRows")
      }
    } else {
      val encodedKey = unsafeRowTypesEncoder.encodeGroupingKey()
      var entryCount = getEntryCount(encodedKey)
      newState.foreach { v =>
        val encodedValue = unsafeRowTypesEncoder.encodeValue(v)
        store.merge(encodedKey, encodedValue, stateName)
        entryCount += 1
        TWSMetricsUtils.incrementMetric(metrics, "numUpdatedStateRows")
      }
      updateEntryCount(encodedKey, entryCount)
    }
  }

  /** Remove this state. */
  override def clear(): Unit = {
    if (usingAvro) {
      val encodedKey: Array[Byte] = avroTypesEncoder.encodeGroupingKey()
      store.remove(encodedKey, stateName)
      // val entryCount = getEntryCount(encodedKey)
      // TWSMetricsUtils.incrementMetric(metrics, "numRemovedStateRows", entryCount)
      // removeEntryCount(encodedKey)
    } else {
      val encodedKey = unsafeRowTypesEncoder.encodeGroupingKey()
      store.remove(encodedKey, stateName)
      val entryCount = getEntryCount(encodedKey)
      TWSMetricsUtils.incrementMetric(metrics, "numRemovedStateRows", entryCount)
      removeEntryCount(encodedKey)
    }
  }

  private def validateNewState(newState: Array[S]): Unit = {
    StateStoreErrors.requireNonNullStateValue(newState, stateName)
    StateStoreErrors.requireNonEmptyListStateValue(newState, stateName)

    newState.foreach { v =>
      StateStoreErrors.requireNonNullStateValue(v, stateName)
    }
  }
}
