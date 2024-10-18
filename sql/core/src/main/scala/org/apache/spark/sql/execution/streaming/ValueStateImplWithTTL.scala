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
import org.apache.spark.sql.execution.streaming.state.{AvroSerde, NoPrefixKeyStateEncoderSpec, StateStore}
import org.apache.spark.sql.streaming.{TTLConfig, ValueState}

/**
 * Class that provides a concrete implementation for a single value state associated with state
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
class ValueStateImplWithTTL[S](
    store: StateStore,
    stateName: String,
    keyExprEnc: ExpressionEncoder[Any],
    valEncoder: Encoder[S],
    ttlConfig: TTLConfig,
    batchTimestampMs: Long,
    avroSerde: Option[AvroSerde],
    metrics: Map[String, SQLMetric] = Map.empty)
  extends SingleKeyTTLStateImpl(
    stateName, store, keyExprEnc, batchTimestampMs) with ValueState[S] {

  private val stateTypesEncoder = UnsafeRowTypesEncoder(keyExprEnc, valEncoder,
    stateName, hasTtl = true)
  private val ttlExpirationMs =
    StateTTL.calculateExpirationTimeForDuration(ttlConfig.ttlDuration, batchTimestampMs)

  initialize()

  private def initialize(): Unit = {
    store.createColFamilyIfAbsent(stateName,
      keyExprEnc.schema, getValueSchemaWithTTL(valEncoder.schema, true),
      NoPrefixKeyStateEncoderSpec(keyExprEnc.schema))
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

      if (!stateTypesEncoder.isExpired(retRow, batchTimestampMs)) {
        resState
      } else {
        null.asInstanceOf[S]
      }
    } else {
      null.asInstanceOf[S]
    }
  }

  /** Function to update and overwrite state associated with given key */
  override def update(newState: S): Unit = {
    val encodedValue = stateTypesEncoder.encodeValue(newState, ttlExpirationMs)
    val serializedGroupingKey = stateTypesEncoder.encodeGroupingKey()
    store.put(serializedGroupingKey,
      encodedValue, stateName)
    TWSMetricsUtils.incrementMetric(metrics, "numUpdatedStateRows")
    upsertTTLForStateKey(ttlExpirationMs, serializedGroupingKey)
  }

  /** Function to remove state for given key */
  override def clear(): Unit = {
    store.remove(stateTypesEncoder.encodeGroupingKey(), stateName)
    TWSMetricsUtils.incrementMetric(metrics, "numRemovedStateRows")
    clearTTLState()
  }

  def clearIfExpired(groupingKey: UnsafeRow): Long = {
    val retRow = store.get(groupingKey, stateName)

    var result = 0L
    if (retRow != null) {
      if (stateTypesEncoder.isExpired(retRow, batchTimestampMs)) {
        store.remove(groupingKey, stateName)
        TWSMetricsUtils.incrementMetric(metrics, "numRemovedStateRows")
        result = 1L
      }
    }
    result
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
  private[sql] def getTTLValue(): Option[(S, Long)] = {
    val encodedGroupingKey = stateTypesEncoder.encodeGroupingKey()
    val retRow = store.get(encodedGroupingKey, stateName)

    // if the returned row is not null, we want to return the value associated with the
    // ttlExpiration
    if (retRow != null) {
      val ttlExpiration = stateTypesEncoder.decodeTtlExpirationMs(retRow)
      ttlExpiration.map(expiration => (stateTypesEncoder.decodeValue(retRow), expiration))
    } else {
      None
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

