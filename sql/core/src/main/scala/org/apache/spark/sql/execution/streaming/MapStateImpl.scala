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
import org.apache.spark.sql.execution.streaming.state.{StateStore, UnsafeRowPair}
import org.apache.spark.sql.streaming.MapState

class MapStateImpl[K, V](store: StateStore,
                         stateName: String,
                         keyEnc: ExpressionEncoder[Any]) extends MapState[K, V] with Logging {
  store.setUseCompositeKey()

  /** Whether state exists or not. */
  override def exists(): Boolean = {
    !store.prefixScan(StateEncoder.encodeKey(stateName, keyEnc), stateName).isEmpty
  }

  /** Get the state value if it exists */
  override def getValue(key: K): V = {
    require(key != null, "User key cannot be null.")
    val encodedGroupingKey = StateEncoder.encodeKey(stateName, keyEnc)
    val encodeUserKey = StateEncoder.encodeUserKey(key)
    val unsafeRowValue = store.getWithCompositeKey(encodedGroupingKey, encodeUserKey, stateName)
    if (unsafeRowValue == null) {
      throw new UnsupportedOperationException(
        "No value found for given grouping key and user key in the map.")
    }
    StateEncoder.decode(unsafeRowValue)
  }

  /** Check if the user key is contained in the map */
  override def containsKey(key: K): Boolean = {
    require(key != null, "User key cannot be null.")
    try {
      getValue(key) != null
    } catch {
      case _: Exception => false
    }
  }

  /** Update value for given user key */
  override def updateValue(key: K, value: V): Unit = {
    require(key != null, "User key cannot be null.")
    require(value != null, "Value put to map cannot be null.")
    val encodedValue = StateEncoder.encodeValue(value)
    val encodedKey = StateEncoder.encodeKey(stateName, keyEnc)
    val encodedUserKey = StateEncoder.encodeUserKey(key)
    store.putWithCompositeKey(encodedKey, encodedUserKey, encodedValue, stateName)
  }

  /** Get the map associated with grouping key */
  override def getMap(): Map[K, V] = {
    val encodedGroupingKey = StateEncoder.encodeKey(stateName, keyEnc)
    store.prefixScan(encodedGroupingKey, stateName)
      .map {
        case iter: UnsafeRowPair =>
          (StateEncoder.decode(iter.key), StateEncoder.decode(iter.value))
      }.toMap
  }

  /** Get the list of keys present in map associated with grouping key */
  override def getKeys(): Iterator[K] = {
    getMap().keys.iterator
  }

  /** Get the list of values present in map associated with grouping key */
  override def getValues(): Iterator[V] = {
    getMap().values.iterator
  }

  /** Remove user key from map state */
  override def removeKey(key: K): Unit = {
    require(key != null, "User key cannot be null.")
    val encodedKey = StateEncoder.encodeKey(stateName, keyEnc)
    val encodedUserKey = StateEncoder.encodeUserKey(key)
    store.removeWithCompositeKey(encodedKey, encodedUserKey, stateName)
  }

  /** Remove this state. */
  override def remove(): Unit = {
    getKeys().foreach { itr =>
      removeKey(itr)
    }
  }
}
