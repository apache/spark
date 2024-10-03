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
import org.apache.spark.sql.execution.streaming.state.{NoPrefixKeyStateEncoderSpec, StateStore, StateStoreErrors}
import org.apache.spark.sql.streaming.ListState

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
     metrics: Map[String, SQLMetric] = Map.empty)
  extends ListStateMetricsImpl
  with ListState[S]
  with Logging {

  private val stateTypesEncoder = StateTypesEncoder(keyExprEnc, valEncoder, stateName)

  private def initialize(): Unit = {
    store.createColFamilyIfAbsent(stateName, keyExprEnc.schema, valEncoder.schema,
      NoPrefixKeyStateEncoderSpec(keyExprEnc.schema), useMultipleValuesPerKey = true)
    initMetrics(store, keyExprEnc, stateName)
  }

  initialize()

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
     new Iterator[S] {
       override def hasNext: Boolean = {
         unsafeRowValuesIterator.hasNext
       }

       override def next(): S = {
         val valueUnsafeRow = unsafeRowValuesIterator.next()
         stateTypesEncoder.decodeValue(valueUnsafeRow)
       }
     }
   }

   /** Update the value of the list. */
   override def put(newState: Array[S]): Unit = {
     validateNewState(newState)

     val encodedKey = stateTypesEncoder.encodeGroupingKey()
     var isFirst = true
     var entryCount = getEntryCount(store, encodedKey, stateName)

     newState.foreach { v =>
       val encodedValue = stateTypesEncoder.encodeValue(v)
       if (isFirst) {
         store.put(encodedKey, encodedValue, stateName)
         isFirst = false
       } else {
          store.merge(encodedKey, encodedValue, stateName)
       }
       entryCount += 1
       TWSMetricsUtils.incrementMetric(metrics, "numUpdatedStateRows")
     }
     updateEntryCount(store, encodedKey, stateName, entryCount)
   }

   /** Append an entry to the list. */
   override def appendValue(newState: S): Unit = {
     StateStoreErrors.requireNonNullStateValue(newState, stateName)
     val encodedKey = stateTypesEncoder.encodeGroupingKey()
     val entryCount = getEntryCount(store, encodedKey, stateName)
     store.merge(encodedKey,
         stateTypesEncoder.encodeValue(newState), stateName)
     TWSMetricsUtils.incrementMetric(metrics, "numUpdatedStateRows")
     updateEntryCount(store, encodedKey, stateName, entryCount + 1)
   }

   /** Append an entire list to the existing value. */
   override def appendList(newState: Array[S]): Unit = {
     validateNewState(newState)

     val encodedKey = stateTypesEncoder.encodeGroupingKey()
     var entryCount = getEntryCount(store, encodedKey, stateName)
     newState.foreach { v =>
       val encodedValue = stateTypesEncoder.encodeValue(v)
       store.merge(encodedKey, encodedValue, stateName)
       entryCount += 1
       TWSMetricsUtils.incrementMetric(metrics, "numUpdatedStateRows")
     }
     updateEntryCount(store, encodedKey, stateName, entryCount)
   }

   /** Remove this state. */
   override def clear(): Unit = {
     val encodedKey = stateTypesEncoder.encodeGroupingKey()
     store.remove(encodedKey, stateName)
     val entryCount = getEntryCount(store, encodedKey, stateName)
     TWSMetricsUtils.incrementMetric(metrics, "numRemovedStateRows", entryCount)
     removeEntryCount(store, encodedKey, stateName)
   }

   private def validateNewState(newState: Array[S]): Unit = {
     StateStoreErrors.requireNonNullStateValue(newState, stateName)
     StateStoreErrors.requireNonEmptyListStateValue(newState, stateName)

     newState.foreach { v =>
       StateStoreErrors.requireNonNullStateValue(v, stateName)
     }
   }
 }
