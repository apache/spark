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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.streaming.state.StateStore
import org.apache.spark.sql.streaming.{SerializationType, StateEncoder, ValueState}

/**
 * Class that provides a concrete implementation for a single value state associated with state
 * variables used in the streaming transformWithState operator.
 * @param store - reference to the StateStore instance to be used for storing state
 * @param stateName - name of logical state partition
 * @param keyEnc - Spark SQL encoder for key
 * @tparam K - data type of key
 * @tparam S - data type of object that will be stored
 */
class ValueStateImpl[S](
    store: StateStore,
    stateName: String,
    keyExprEnc: ExpressionEncoder[Any],
    valEnc: Encoder[S],
    serializer: SerializationType.Value) extends ValueState[S] with Logging {

  /** Function to check if state exists. Returns true if present and false otherwise */
  override def exists(): Boolean = {
    getImpl() != null
  }

  /** Function to return Option of value if exists and None otherwise */
  override def getOption(): Option[S] = {
    val retRow = getImpl()
    if (retRow != null) {
      val resState = serializer match {
        case SerializationType.AVRO =>
          StateEncoder.decodeAvroToValue[S](retRow, valEnc)
        case SerializationType.SPARK_SQL =>
          StateEncoder.decodeValSparkSQL[S](retRow, valEnc)
        case SerializationType.JAVA =>
          StateEncoder.decodeValue[S](retRow)
      }
      Some(resState)
    } else {
      None
    }
  }

  /** Function to return associated value with key if exists and null otherwise */
  override def get(): S = {
    val retRow = getImpl()
    if (retRow != null) {
      val resState = serializer match {
        case SerializationType.AVRO =>
          StateEncoder.decodeAvroToValue[S](retRow, valEnc)
        case SerializationType.SPARK_SQL =>
          StateEncoder.decodeValSparkSQL[S](retRow, valEnc)
        case SerializationType.JAVA =>
          StateEncoder.decodeValue[S](retRow)
      }
      resState
    } else {
      null.asInstanceOf[S]
    }
  }

  private def getImpl(): UnsafeRow = {
    store.get(StateEncoder.encodeGroupingKey(stateName, keyExprEnc), stateName)
  }

  /** Function to update and overwrite state associated with given key */
  override def update(newState: S): Unit = {
    val encodedVal = serializer match {
      case SerializationType.AVRO =>
        StateEncoder.encodeValToAvro[S](newState, valEnc)
      case SerializationType.SPARK_SQL =>
        StateEncoder.encodeValSparkSQL[S](newState, valEnc)
      case SerializationType.JAVA =>
        StateEncoder.encodeValue[S](newState)
    }
    store.put(StateEncoder.encodeGroupingKey(stateName, keyExprEnc), encodedVal, stateName)
  }

  /** Function to remove state for given key */
  override def remove(): Unit = {
    store.remove(StateEncoder.encodeGroupingKey(stateName, keyExprEnc), stateName)
  }
}
