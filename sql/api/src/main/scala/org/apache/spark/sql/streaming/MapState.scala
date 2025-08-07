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

import org.apache.spark.annotation.Evolving

@Evolving
/**
 * Interface used for arbitrary stateful operations with the v2 API to capture map value state.
 */
trait MapState[K, V] extends Serializable {

  /**
   * Function to check whether any user map entry exists for current grouping key or not.
   *
   * @return - true if state exists, false otherwise.
   */
  def exists(): Boolean

  /**
   * Function to get the state value for current grouping key and user map key.
   * If the state exists, the value is returned. If the state does not exist,
   * the default value for the type is returned for AnyVal types and null for AnyRef types.
   *
   * Note that it's always recommended to check whether the state exists or not by calling exists()
   * before calling get().
   *
   * @return - the value of the state if it exists. If the state does not exist, the default value
   *           for the type is returned for AnyVal types and null for AnyRef types.
   */
  def getValue(key: K): V

  /**
   * Function to check if the user map key is contained in the map for the current grouping key.
   *
   * @param key - user map key
   *
   * @return - true if the user key is present in the map, false otherwise.
   */
  def containsKey(key: K): Boolean

  /**
   * Function to add or update the map entry for the current grouping key.
   *
   * Note that this function will add the user map key and value if the user map key is not
   * present in the map associated with the current grouping key.
   * If the user map key is already present in the associated map, the value for the user key
   * will be updated to the new user map value.
   *
   * @param key - user map key
   * @param value - user map value
   */
  def updateValue(key: K, value: V): Unit

  /**
   * Function to return the iterator of user map key-value pairs present in the map for the
   * current grouping key.
   *
   * @return - iterator of user map key-value pairs if the map is not empty
   *           and empty iterator otherwise.
   */
  def iterator(): Iterator[(K, V)]

  /**
   * Function to return the user map keys present in the map for the current grouping key.
   *
   * @return - iterator of user map keys if the map is not empty, empty iterator otherwise.
   */
  def keys(): Iterator[K]

  /**
   * Function to return the user map values present in the map for the current grouping key.
   *
   * @return - iterator of user map values if the map is not empty, empty iterator otherwise.
   */
  def values(): Iterator[V]

  /**
   * Function to remove the user map key from the map for the current grouping key.
   *
   * Note that this function will remove the user map key and its associated value from the map
   * associated with the current grouping key. If the user map key is not present in the map,
   * this function will not do anything.
   *
   * @param key - user map key
   */
  def removeKey(key: K): Unit

  /**
   * Function to remove the state for the current grouping key. Note that this removes the entire
   * map state associated with the current grouping key.
   */
  def clear(): Unit
}
