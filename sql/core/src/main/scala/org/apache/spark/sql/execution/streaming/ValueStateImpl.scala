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
import org.apache.spark.sql.execution.streaming.state.{NoPrefixKeyStateEncoderSpec, StateStore}
import org.apache.spark.sql.streaming.ValueState

/**
 * Class that provides a concrete implementation for a single value state associated with state
 * variables used in the streaming transformWithState operator.
 * @param store - reference to the StateStore instance to be used for storing state
 * @param stateName - name of logical state partition
 * @param keyExprEnc - Spark SQL encoder for key
 * @param valEncoder - Spark SQL encoder for value
 * @param metrics - metrics to be updated as part of stateful processing
 * @tparam S - data type of object that will be stored
 */
class ValueStateImpl[S](
    store: StateStore,
    stateName: String,
    keyExprEnc: ExpressionEncoder[Any],
    valEncoder: Encoder[S],
    metrics: Map[String, SQLMetric] = Map.empty)
  extends ValueState[S] with Logging {

  private val stateTypesEncoder = StateTypesEncoder(keyExprEnc, valEncoder, stateName)

  initialize()

  private def initialize(): Unit = {
    store.createColFamilyIfAbsent(stateName, keyExprEnc.schema, valEncoder.schema,
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
      stateTypesEncoder.decodeValue(retRow)
    } else {
      null.asInstanceOf[S]
    }
  }

  /** Function to update and overwrite state associated with given key */
  override def update(newState: S): Unit = {
    val encodedValue = stateTypesEncoder.encodeValue(newState)
    store.put(stateTypesEncoder.encodeGroupingKey(),
      encodedValue, stateName)
    TWSMetricsUtils.incrementMetric(metrics, "numUpdatedStateRows")
  }

  /** Function to remove state for given key */
  override def clear(): Unit = {
    store.remove(stateTypesEncoder.encodeGroupingKey(), stateName)
    TWSMetricsUtils.incrementMetric(metrics, "numRemovedStateRows")
  }
}
