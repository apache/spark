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
 * Represents the operation handle provided to the stateful processor used in the
 * arbitrary state API v2.
 */
@Experimental
@Evolving
private[sql] trait StatefulProcessorHandle extends Serializable {

  /**
   * Function to create new or return existing single value state variable of given type
   * The user must ensure to call this function only within the `init()` method of the
   * StatefulProcessor.
   * @param stateName - name of the state variable
   * @param valEncoder - SQL encoder for state variable
   * @tparam T - type of state variable
   * @return - instance of ValueState of type T that can be used to store state persistently
   */
  def getValueState[T](stateName: String, valEncoder: Encoder[T]): ValueState[T]

  /**
   * Creates new or returns existing list state associated with stateName.
   * The ListState persists values of type T.
   *
   * @param stateName  - name of the state variable
   * @param valEncoder - SQL encoder for state variable
   * @tparam T - type of state variable
   * @return - instance of ListState of type T that can be used to store state persistently
   */
  def getListState[T](stateName: String, valEncoder: Encoder[T]): ListState[T]

  /**
   * Creates new or returns existing map state associated with stateName.
   * The MapState persists Key-Value pairs of type [K, V].
   *
   * @param stateName  - name of the state variable
   * @param userKeyEnc  - spark sql encoder for the map key
   * @param valEncoder  - spark sql encoder for the map value
   * @tparam K - type of key for map state variable
   * @tparam V - type of value for map state variable
   * @return - instance of MapState of type [K,V] that can be used to store state persistently
   */
  def getMapState[K, V](
      stateName: String,
      userKeyEnc: Encoder[K],
      valEncoder: Encoder[V]): MapState[K, V]

  /** Function to return queryInfo for currently running task */
  def getQueryInfo(): QueryInfo

  /**
   * Function to register a processing/event time based timer for given implicit grouping key
   * and provided timestamp
   * @param expiryTimestampMs - timer expiry timestamp in milliseconds
   */
  def registerTimer(expiryTimestampMs: Long): Unit

  /**
   * Function to delete a processing/event time based timer for given implicit grouping key
   * and provided timestamp
   * @param expiryTimestampMs - timer expiry timestamp in milliseconds
   */
  def deleteTimer(expiryTimestampMs: Long): Unit

  /**
   * Function to list all the timers registered for given implicit grouping key
   * Note: calling listTimers() within the `handleInputRows` method of the StatefulProcessor
   * will return all the unprocessed registered timers, including the one being fired within the
   * invocation of `handleInputRows`.
   * @return - list of all the registered timers for given implicit grouping key
   */
  def listTimers(): Iterator[Long]

  /**
   * Function to delete and purge state variable if defined previously
   * @param stateName - name of the state variable
   */
  def deleteIfExists(stateName: String): Unit
}
