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

import org.apache.spark.annotation.Evolving

@Evolving
/**
 * Interface used for arbitrary stateful operations with the v2 API to capture single value state.
 */
trait ValueState[S] extends Serializable {

  /**
   * Function to check whether state exists for current grouping key or not.
   *
   * @return - true if state exists, false otherwise.
   */
  def exists(): Boolean

  /**
   * Function to get the state value for the current grouping key.
   * If the state exists, the value is returned. If the state does not exist,
   * the default value for the type is returned for AnyVal types and null for AnyRef types.
   *
   * Note that it's always recommended to check whether the state exists or not by calling exists()
   * before calling get().
   *
   * @return - the value of the state if it exists. If the state does not exist, the default value
   *           for the type is returned for AnyVal types and null for AnyRef types.
   */
  def get(): S

  /**
   * Function to update the value of the state for the current grouping key to the new value.
   *
   * @param newState - the new value
   */
  def update(newState: S): Unit

  /**
   * Function to remove the state for the current grouping key.
   */
  def clear(): Unit
}
