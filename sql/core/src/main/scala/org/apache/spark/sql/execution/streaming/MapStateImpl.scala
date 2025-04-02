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
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.streaming.TransformWithStateKeyValueRowSchemaUtils._
import org.apache.spark.sql.execution.streaming.state.{PrefixKeyScanStateEncoderSpec, StateStore, StateStoreErrors, UnsafeRowPair}
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
    userKeyEnc: ExpressionEncoder[Any],
    valEncoder: ExpressionEncoder[Any],
    metrics: Map[String, SQLMetric] = Map.empty) extends MapState[K, V] with Logging {

  // Pack grouping key and user key together as a prefixed composite key
  private val schemaForCompositeKeyRow: StructType = {
    getCompositeKeySchema(keyExprEnc.schema, userKeyEnc.schema)
  }
  private val schemaForValueRow: StructType = valEncoder.schema
  private val stateTypesEncoder = new CompositeKeyStateEncoder(
    keyExprEnc, userKeyEnc, valEncoder, stateName)

  store.createColFamilyIfAbsent(stateName, schemaForCompositeKeyRow, schemaForValueRow,
    PrefixKeyScanStateEncoderSpec(schemaForCompositeKeyRow, 1))

  /** Whether state exists or not. */
  override def exists(): Boolean = {
    store.prefixScan(stateTypesEncoder.encodeGroupingKey(), stateName).nonEmpty
  }

  /** Get the state value if it exists */
  override def getValue(key: K): V = {
    StateStoreErrors.requireNonNullStateValue(key, stateName)
    val encodedCompositeKey = stateTypesEncoder.encodeCompositeKey(key)
    val unsafeRowValue = store.get(encodedCompositeKey, stateName)

    if (unsafeRowValue == null) return null.asInstanceOf[V]
    stateTypesEncoder.decodeValue(unsafeRowValue).asInstanceOf[V]
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
    val encodedValue = stateTypesEncoder.encodeValue(value)
    val encodedCompositeKey = stateTypesEncoder.encodeCompositeKey(key)
    store.put(encodedCompositeKey, encodedValue, stateName)
    TWSMetricsUtils.incrementMetric(metrics, "numUpdatedStateRows")
  }

  /** Get the map associated with grouping key */
  override def iterator(): Iterator[(K, V)] = {
    val encodedGroupingKey = stateTypesEncoder.encodeGroupingKey()
    store.prefixScan(encodedGroupingKey, stateName)
      .map {
        case iter: UnsafeRowPair =>
          (stateTypesEncoder.decodeCompositeKey(iter.key).asInstanceOf[K],
            stateTypesEncoder.decodeValue(iter.value).asInstanceOf[V])
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
    val compositeKey = stateTypesEncoder.encodeCompositeKey(key)
    store.remove(compositeKey, stateName)
    // Note that for mapState, the rows are flattened. So we count the number of rows removed
    // proportional to the number of keys in the map per grouping key.
    TWSMetricsUtils.incrementMetric(metrics, "numRemovedStateRows")
  }

  /** Remove this state. */
  override def clear(): Unit = {
    keys().foreach { itr =>
      removeKey(itr)
    }
  }
}
