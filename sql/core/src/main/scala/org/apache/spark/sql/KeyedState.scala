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

package org.apache.spark.sql

import org.apache.spark.annotation.{Experimental, InterfaceStability}
import org.apache.spark.sql.catalyst.plans.logical.LogicalKeyedState

/**
 * :: Experimental ::
 *
 * Wrapper class for interacting with keyed state data in `mapGroupsWithState` and
 * `flatMapGroupsWithState` operations on
 * [[org.apache.spark.sql.KeyValueGroupedDataset KeyValueGroupedDataset]].
 *
 * Important points to note.
 * - State can be `null`. So updating the state to null is not same as removing the state.
 * - Operations on state are not threadsafe. This is to avoid memory barriers.
 * - If the `remove()` is called, then `exists()` will return `false`, and
 *   `getOption()` will return `None`.
 * - After that `update(newState)` is called, then `exists()` will return `true`,
 *   and `getOption()` will return `Some(...)`.
 *
 * Scala example of using `KeyedState`:
 * {{{
 * // A mapping function that maintains an integer state for string keys and returns a string.
 * def mappingFunction(key: String, value: Iterable[Int], state: KeyedState[Int]): Option[String]= {
 *   // Check if state exists
 *   if (state.exists) {
 *     val existingState = state.get  // Get the existing state
 *     val shouldRemove = ...         // Decide whether to remove the state
 *     if (shouldRemove) {
 *       state.remove()     // Remove the state
 *     } else {
 *       val newState = ...
 *       state.update(newState)    // Set the new state
 *     }
 *   } else {
 *     val initialState = ...
 *     state.update(initialState)  // Set the initial state
 *   }
 *   ... // return something
 * }
 *
 * }}}
 *
 * Java example of using `KeyedState`:
 * {{{
 * // A mapping function that maintains an integer state for string keys and returns a string.
 * MapGroupsWithStateFunction<String, Integer, Integer, String> mappingFunction =
 *    new MapGroupsWithStateFunction<String, Integer, Integer, String>() {
 *
 *      @Override
 *      public String call(String key, Optional<Integer> value, KeyedState<Integer> state) {
 *        if (state.exists()) {
 *          int existingState = state.get(); // Get the existing state
 *          boolean shouldRemove = ...; // Decide whether to remove the state
 *          if (shouldRemove) {
 *            state.remove(); // Remove the state
 *          } else {
 *            int newState = ...;
 *            state.update(newState); // Set the new state
 *          }
 *        } else {
 *          int initialState = ...; // Set the initial state
 *          state.update(initialState);
 *        }
 *        ... // return something
 *      }
 *    };
 * }}}
 *
 * @tparam S User-defined type of the state to be stored for each key. Must be encodable into
 *           Spark SQL types (see [[Encoder]] for more details).
 * @since 2.1.1
 */
@Experimental
@InterfaceStability.Evolving
trait KeyedState[S] extends LogicalKeyedState[S] {

  /** Whether state exists or not. */
  def exists: Boolean

  /** Get the state object if it is defined, otherwise throws NoSuchElementException. */
  def get: S

  /**
   * Update the value of the state. Note that null is a valid value, and does not signify removing
   * of the state.
   */
  def update(newState: S): Unit

  /** Remove this keyed state. */
  def remove(): Unit

  /** (scala friendly) Get the state object as an [[Option]]. */
  @inline final def getOption: Option[S] = if (exists) Some(get) else None

  @inline final override def toString: String = {
    getOption.map { _.toString }.getOrElse("<undefined>")
  }
}
