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

import java.io.Serializable

import org.apache.spark.annotation.{Evolving, Experimental}
import org.apache.spark.sql.Encoder

/**
 * Represents the operation handle provided to the stateful processor used in the arbitrary state
 * API v2.
 */
@Experimental
@Evolving
private[sql] trait StatefulProcessorHandle extends Serializable {

  /**
   * Function to create new or return existing single value state variable of given type with ttl.
   * State values will not be returned past ttlDuration, and will be eventually removed from the
   * state store. Any state update resets the ttl to current processing time plus ttlDuration.
   * Users can use the helper method `TTLConfig.NONE` in Scala or `TTLConfig.NONE()` in Java for
   * the TTLConfig parameter to disable TTL for the state variable.
   *
   * The user must ensure to call this function only within the `init()` method of the
   * StatefulProcessor.
   *
   * @param stateName
   *   \- name of the state variable
   * @param valEncoder
   *   \- SQL encoder for state variable
   * @param ttlConfig
   *   \- the ttl configuration (time to live duration etc.)
   * @tparam T
   *   \- type of state variable
   * @return
   *   \- instance of ValueState of type T that can be used to store state persistently
   */
  def getValueState[T](
      stateName: String,
      valEncoder: Encoder[T],
      ttlConfig: TTLConfig): ValueState[T]

  /**
   * (Scala-specific) Function to create new or return existing single value state variable of
   * given type with ttl. State values will not be returned past ttlDuration, and will be
   * eventually removed from the state store. Any state update resets the ttl to current
   * processing time plus ttlDuration. Users can use the helper method `TTLConfig.NONE` in Scala
   * or `TTLConfig.NONE()` in Java for the TTLConfig parameter to disable TTL for the state
   * variable.
   *
   * The user must ensure to call this function only within the `init()` method of the
   * StatefulProcessor. Note that this API uses the implicit SQL encoder in Scala.
   *
   * @param stateName
   *   \- name of the state variable
   * @param ttlConfig
   *   \- the ttl configuration (time to live duration etc.)
   * @tparam T
   *   \- type of state variable
   * @return
   *   \- instance of ValueState of type T that can be used to store state persistently
   */
  def getValueState[T: Encoder](stateName: String, ttlConfig: TTLConfig): ValueState[T]

  /**
   * Function to create new or return existing list state variable of given type with ttl. State
   * values will not be returned past ttlDuration, and will be eventually removed from the state
   * store. Any values in listState which have expired after ttlDuration will not be returned on
   * get() and will be eventually removed from the state. Users can use the helper method
   * `TTLConfig.NONE` in Scala or `TTLConfig.NONE()` in Java for the TTLConfig parameter to
   * disable TTL for the state variable.
   *
   * The user must ensure to call this function only within the `init()` method of the
   * StatefulProcessor.
   *
   * @param stateName
   *   \- name of the state variable
   * @param valEncoder
   *   \- SQL encoder for state variable
   * @param ttlConfig
   *   \- the ttl configuration (time to live duration etc.)
   * @tparam T
   *   \- type of state variable
   * @return
   *   \- instance of ListState of type T that can be used to store state persistently
   */
  def getListState[T](
      stateName: String,
      valEncoder: Encoder[T],
      ttlConfig: TTLConfig): ListState[T]

  /**
   * (Scala-specific) Function to create new or return existing list state variable of given type
   * with ttl. State values will not be returned past ttlDuration, and will be eventually removed
   * from the state store. Any values in listState which have expired after ttlDuration will not
   * be returned on get() and will be eventually removed from the state. Users can use the helper
   * method `TTLConfig.NONE` in Scala or `TTLConfig.NONE()` in Java for the TTLConfig parameter to
   * disable TTL for the state variable.
   *
   * The user must ensure to call this function only within the `init()` method of the
   * StatefulProcessor. Note that this API uses the implicit SQL encoder in Scala.
   *
   * @param stateName
   *   \- name of the state variable
   * @param ttlConfig
   *   \- the ttl configuration (time to live duration etc.)
   * @tparam T
   *   \- type of state variable
   * @return
   *   \- instance of ListState of type T that can be used to store state persistently
   */
  def getListState[T: Encoder](stateName: String, ttlConfig: TTLConfig): ListState[T]

  /**
   * Function to create new or return existing map state variable of given type with ttl. State
   * values will not be returned past ttlDuration, and will be eventually removed from the state
   * store. Any values in mapState which have expired after ttlDuration will not returned on get()
   * and will be eventually removed from the state. Users can use the helper method
   * `TTLConfig.NONE` in Scala or `TTLConfig.NONE()` in Java for the TTLConfig parameter to
   * disable TTL for the state variable.
   *
   * The user must ensure to call this function only within the `init()` method of the
   * StatefulProcessor.
   *
   * @param stateName
   *   \- name of the state variable
   * @param userKeyEnc
   *   \- spark sql encoder for the map key
   * @param valEncoder
   *   \- SQL encoder for state variable
   * @param ttlConfig
   *   \- the ttl configuration (time to live duration etc.)
   * @tparam K
   *   \- type of key for map state variable
   * @tparam V
   *   \- type of value for map state variable
   * @return
   *   \- instance of MapState of type [K,V] that can be used to store state persistently
   */
  def getMapState[K, V](
      stateName: String,
      userKeyEnc: Encoder[K],
      valEncoder: Encoder[V],
      ttlConfig: TTLConfig): MapState[K, V]

  /**
   * (Scala-specific) Function to create new or return existing map state variable of given type
   * with ttl. State values will not be returned past ttlDuration, and will be eventually removed
   * from the state store. Any values in mapState which have expired after ttlDuration will not be
   * returned on get() and will be eventually removed from the state. Users can use the helper
   * method `TTLConfig.NONE` in Scala or `TTLConfig.NONE()` in Java for the TTLConfig parameter to
   * disable TTL for the state variable.
   *
   * The user must ensure to call this function only within the `init()` method of the
   * StatefulProcessor. Note that this API uses the implicit SQL encoder in Scala.
   *
   * @param stateName
   *   \- name of the state variable
   * @param ttlConfig
   *   \- the ttl configuration (time to live duration etc.)
   * @tparam K
   *   \- type of key for map state variable
   * @tparam V
   *   \- type of value for map state variable
   * @return
   *   \- instance of MapState of type [K,V] that can be used to store state persistently
   */
  def getMapState[K: Encoder, V: Encoder](stateName: String, ttlConfig: TTLConfig): MapState[K, V]

  /** Function to return queryInfo for currently running task */
  def getQueryInfo(): QueryInfo

  /**
   * Function to register a processing/event time based timer for given implicit grouping key and
   * provided timestamp
   * @param expiryTimestampMs
   *   \- timer expiry timestamp in milliseconds
   */
  def registerTimer(expiryTimestampMs: Long): Unit

  /**
   * Function to delete a processing/event time based timer for given implicit grouping key and
   * provided timestamp
   * @param expiryTimestampMs
   *   \- timer expiry timestamp in milliseconds
   */
  def deleteTimer(expiryTimestampMs: Long): Unit

  /**
   * Function to list all the timers registered for given implicit grouping key Note: calling
   * listTimers() within the `handleInputRows` method of the StatefulProcessor will return all the
   * unprocessed registered timers, including the one being fired within the invocation of
   * `handleInputRows`.
   * @return
   *   \- list of all the registered timers for given implicit grouping key
   */
  def listTimers(): Iterator[Long]

  /**
   * Function to delete and purge state variable if defined previously
   * @param stateName
   *   \- name of the state variable
   */
  def deleteIfExists(stateName: String): Unit
}
