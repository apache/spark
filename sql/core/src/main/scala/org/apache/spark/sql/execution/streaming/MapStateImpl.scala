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
import org.apache.spark.sql.execution.streaming.TransformWithStateKeyValueRowSchemaUtils._
import org.apache.spark.sql.execution.streaming.state.{AvroEncoderSpec, ByteArrayPair, PrefixKeyScanStateEncoderSpec, StateStore, StateStoreErrors, UnsafeRowPair}
import org.apache.spark.sql.streaming.MapState
import org.apache.spark.sql.types.StructType

/**
 * Class that provides a concrete implementation for map state associated with state
 * variables used in the streaming transformWithState operator.
 * @param store - reference to the StateStore instance to be used for storing state
 * @param stateName - name of logical state partition
 * @param keyExprEnc - Spark SQL encoder for key
 * @param valEncoder - Spark SQL encoder for value
 * @param metrics - metrics to be updated as part of stateful processing
 * @tparam K - type of key for map state variable
 * @tparam V - type of value for map state variable
 */
class MapStateImpl[K, V](
    store: StateStore,
    stateName: String,
    keyExprEnc: ExpressionEncoder[Any],
    userKeyEnc: Encoder[K],
    valEncoder: Encoder[V],
    avroSerde: Option[AvroEncoderSpec],
    metrics: Map[String, SQLMetric] = Map.empty) extends MapState[K, V] with Logging {

  // Pack grouping key and user key together as a prefixed composite key
  private val schemaForCompositeKeyRow: StructType = {
    getCompositeKeySchema(keyExprEnc.schema, userKeyEnc.schema)
  }
  private val schemaForValueRow: StructType = valEncoder.schema

  // If we are using Avro, the avroSerde parameter must be populated
  // else, we will default to using UnsafeRow.
  private val usingAvro: Boolean = avroSerde.isDefined
  private val avroTypesEncoder = new CompositeKeyAvroRowEncoder(
    keyExprEnc, userKeyEnc, valEncoder, stateName, hasTtl = false, avroSerde)
  private val unsafeRowTypesEncoder = new CompositeKeyUnsafeRowEncoder(
    keyExprEnc, userKeyEnc, valEncoder, stateName, hasTtl = false)

  store.createColFamilyIfAbsent(stateName, schemaForCompositeKeyRow, schemaForValueRow,
    PrefixKeyScanStateEncoderSpec(schemaForCompositeKeyRow, 1))

  /** Whether state exists or not. */
  override def exists(): Boolean = {
    if (usingAvro) {
      val encodedGroupingKey = avroTypesEncoder.encodeGroupingKey()
      store.prefixScan(encodedGroupingKey, stateName).nonEmpty
    } else {
      val encodedGroupingKey = unsafeRowTypesEncoder.encodeGroupingKey()
      store.prefixScan(encodedGroupingKey, stateName).nonEmpty
    }
  }

  /** Get the state value if it exists */
  override def getValue(key: K): V = {
    StateStoreErrors.requireNonNullStateValue(key, stateName)

    if (usingAvro) {
      val encodedCompositeKey = avroTypesEncoder.encodeCompositeKey(key)
      val valueBytes = store.get(encodedCompositeKey, stateName)
      if (valueBytes == null) return null.asInstanceOf[V]
      avroTypesEncoder.decodeValue(valueBytes)
    } else {
      val encodedCompositeKey = unsafeRowTypesEncoder.encodeCompositeKey(key)
      val unsafeRowValue = store.get(encodedCompositeKey, stateName)
      if (unsafeRowValue == null) return null.asInstanceOf[V]
      unsafeRowTypesEncoder.decodeValue(unsafeRowValue)
    }
  }

  /** Check if the user key is contained in the map */
  override def containsKey(key: K): Boolean = {
    StateStoreErrors.requireNonNullStateValue(key, stateName)
    getValue(key) != null
  }

  /** Update value for given user key */
  override def updateValue(key: K, value: V): Unit = {
    StateStoreErrors.requireNonNullStateValue(key, stateName)
    StateStoreErrors.requireNonNullStateValue(value, stateName)

    if (usingAvro) {
      val encodedValue = avroTypesEncoder.encodeValue(value)
      val encodedCompositeKey = avroTypesEncoder.encodeCompositeKey(key)
      store.put(encodedCompositeKey, encodedValue, stateName)
    } else {
      val encodedValue = unsafeRowTypesEncoder.encodeValue(value)
      val encodedCompositeKey = unsafeRowTypesEncoder.encodeCompositeKey(key)
      store.put(encodedCompositeKey, encodedValue, stateName)
    }
    TWSMetricsUtils.incrementMetric(metrics, "numUpdatedStateRows")
  }

  /** Get the map associated with grouping key */
  override def iterator(): Iterator[(K, V)] = {
    if (usingAvro) {
      val encodedGroupingKey = avroTypesEncoder.encodeGroupingKey()
      store.prefixScan(encodedGroupingKey, stateName)
        .map { case pair: ByteArrayPair =>
          (avroTypesEncoder.decodeCompositeKey(pair.key),
            avroTypesEncoder.decodeValue(pair.value))
        }
    } else {
      val encodedGroupingKey = unsafeRowTypesEncoder.encodeGroupingKey()
      store.prefixScan(encodedGroupingKey, stateName)
        .map { case pair: UnsafeRowPair =>
          (unsafeRowTypesEncoder.decodeCompositeKey(pair.key),
            unsafeRowTypesEncoder.decodeValue(pair.value))
        }
    }
  }

  /** Get the list of keys present in map associated with grouping key */
  override def keys(): Iterator[K] = {
    iterator().map(_._1)
  }

  /** Get the list of values present in map associated with grouping key */
  override def values(): Iterator[V] = {
    iterator().map(_._2)
  }

  /** Remove user key from map state */
  override def removeKey(key: K): Unit = {
    StateStoreErrors.requireNonNullStateValue(key, stateName)
    if (usingAvro) {
      val compositeKey = avroTypesEncoder.encodeCompositeKey(key)
      store.remove(compositeKey, stateName)
    } else {
      val compositeKey = unsafeRowTypesEncoder.encodeCompositeKey(key)
      store.remove(compositeKey, stateName)
    }
    // Note that for mapState, the rows are flattened. So we count the number of rows removed
    // proportional to the number of keys in the map per grouping key.
    TWSMetricsUtils.incrementMetric(metrics, "numRemovedStateRows")
  }

  /** Remove this state. */
  override def clear(): Unit = {
    keys().foreach { key =>
      removeKey(key)
    }
  }
}