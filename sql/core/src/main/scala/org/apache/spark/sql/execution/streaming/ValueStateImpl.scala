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
import org.apache.spark.sql.execution.streaming.state.{AvroSerde, NoPrefixKeyStateEncoderSpec, StateStore}
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
    avroSerde: Option[AvroSerde],
    metrics: Map[String, SQLMetric] = Map.empty)
  extends ValueState[S] with Logging {

  // If we are using Avro, the avroSerde parameter must be populated
  // else, we will default to using UnsafeRow.
  private val usingAvro: Boolean = avroSerde.isDefined
  private val avroTypesEncoder = new AvroTypesEncoder[S](
    keyExprEnc, valEncoder, stateName, hasTtl = false, avroSerde)
  private val unsafeRowTypesEncoder = new UnsafeRowTypesEncoder[S](
    keyExprEnc, valEncoder, stateName, hasTtl = false)

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
    if (usingAvro) {
      getAvro()
    } else {
      getUnsafeRow()
    }
  }

  private def getAvro(): S = {
    val encodedGroupingKey = avroTypesEncoder.encodeGroupingKey()
    val retRow = store.get(encodedGroupingKey, stateName)
    if (retRow != null) {
      avroTypesEncoder.decodeValue(retRow)
    } else {
      null.asInstanceOf[S]
    }
  }

  private def getUnsafeRow(): S = {
    val encodedGroupingKey = unsafeRowTypesEncoder.encodeGroupingKey()
    val retRow = store.get(encodedGroupingKey, stateName)
    if (retRow != null) {
      unsafeRowTypesEncoder.decodeValue(retRow)
    } else {
      null.asInstanceOf[S]
    }
  }

  /** Function to update and overwrite state associated with given key */
  override def update(newState: S): Unit = {
    if (usingAvro) {
      val encodedValue = avroTypesEncoder.encodeValue(newState)
      store.put(avroTypesEncoder.encodeGroupingKey(),
        encodedValue, stateName)
    } else {
      val encodedValue = unsafeRowTypesEncoder.encodeValue(newState)
      store.put(unsafeRowTypesEncoder.encodeGroupingKey(),
        encodedValue, stateName)
    }
    TWSMetricsUtils.incrementMetric(metrics, "numUpdatedStateRows")
  }

  /** Function to remove state for given key */
  override def clear(): Unit = {
    if (usingAvro) {
      val encodedKey = avroTypesEncoder.encodeGroupingKey()
      store.remove(encodedKey, stateName)
    } else {
      val encodedKey = unsafeRowTypesEncoder.encodeGroupingKey()
      store.remove(encodedKey, stateName)
    }
    TWSMetricsUtils.incrementMetric(metrics, "numRemovedStateRows")
  }
}
