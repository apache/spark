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
package org.apache.spark.sql.streaming

import org.apache.spark.annotation.{Evolving, Experimental}

@Experimental
@Evolving
/**
 * Interface used for arbitrary stateful operations with the v2 API to capture
 * map value state.
 */
trait MapState[K, V] extends Serializable {
  /** Whether state exists or not. */
  def exists(): Boolean

  /** Get the state value if it exists */
  def getValue(key: K): V

  /** Check if the user key is contained in the map */
  def containsKey(key: K): Boolean

  /** Update value for given user key */
  def updateValue(key: K, value: V) : Unit

  /** Get the map associated with grouping key */
  def iterator(): Iterator[(K, V)]

  /** Get the list of keys present in map associated with grouping key */
  def keys(): Iterator[K]

  /** Get the list of values present in map associated with grouping key */
  def values(): Iterator[V]

  /** Remove user key from map state */
  def removeKey(key: K): Unit

  /** Remove this state. */
  def clear(): Unit
}
