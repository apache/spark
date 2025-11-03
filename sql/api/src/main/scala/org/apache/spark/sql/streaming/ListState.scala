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
 * Interface used for arbitrary stateful operations with the v2 API to capture list value state.
 */
trait ListState[S] extends Serializable {

  /**
   * Function to check whether state exists for current grouping key or not.
   *
   * @return - true if state exists, false otherwise.
   */
  def exists(): Boolean

  /**
   * Function to get the list of elements in the state as an iterator. If the state does not exist,
   * an empty iterator is returned.
   *
   * Note that it's always recommended to check whether the state exists or not by calling exists()
   * before calling get().
   *
   * @return - an iterator of elements in the state if it exists, an empty iterator otherwise.
   */
  def get(): Iterator[S]

  /**
   * Function to update the value of the state with a new list.
   *
   * Note that this will replace the existing value with the new value.
   *
   * @param newState - new list of elements
   */
  def put(newState: Array[S]): Unit

  /**
   * Function to append a single entry to the existing state list.
   *
   * Note that if this is the first time the state is being appended to, the state will be
   * initialized to an empty list before appending the new entry.
   *
   * @param newState - single list element to be appended
   */
  def appendValue(newState: S): Unit

  /**
   * Function to append a list of entries to the existing state list.
   *
   * Note that if this is the first time the state is being appended to, the state will be
   * initialized to an empty list before appending the new entries.
   *
   * @param newState - list of elements to be appended
   */
  def appendList(newState: Array[S]): Unit

  /**
   * Function to remove the state for the current grouping key.
   */
  def clear(): Unit
}
