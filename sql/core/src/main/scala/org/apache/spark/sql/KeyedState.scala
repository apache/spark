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

import java.lang.IllegalArgumentException

import org.apache.spark.annotation.{Experimental, InterfaceStability}
import org.apache.spark.sql.catalyst.plans.logical.LogicalKeyedState

/**
 * :: Experimental ::
 *
 * Wrapper class for interacting with keyed state data in `mapGroupsWithState` and
 * `flatMapGroupsWithState` operations on
 * [[KeyValueGroupedDataset]].
 *
 * Detail description on `[map/flatMap]GroupsWithState` operation
 * ------------------------------------------------------------
 * Both, `mapGroupsWithState` and `flatMapGroupsWithState` in [[KeyValueGroupedDataset]]
 * will invoke the user-given function on each group (defined by the grouping function in
 * `Dataset.groupByKey()`) while maintaining user-defined per-group state between invocations.
 * For a static batch Dataset, the function will be invoked once per group. For a streaming
 * Dataset, the function will be invoked for each group repeatedly in every trigger.
 * That is, in every batch of the [[streaming.StreamingQuery StreamingQuery]],
 * the function will be invoked once for each group that has data in the batch.
 *
 * The function is invoked with following parameters.
 *  - The key of the group.
 *  - An iterator containing all the values for this key.
 *  - A user-defined state object set by previous invocations of the given function.
 * In case of a batch Dataset, there is only one invocation and state object will be empty as
 * there is no prior state. Essentially, for batch Datasets, `[map/flatMap]GroupsWithState`
 * is equivalent to `[map/flatMap]Groups`.
 *
 * Important points to note about the function.
 *  - In a trigger, the function will be called only the groups present in the batch. So do not
 *    assume that the function will be called in every trigger for every group that has state.
 *  - There is no guaranteed ordering of values in the iterator in the function, neither with
 *    batch, nor with streaming Datasets.
 *  - All the data will be shuffled before applying the function.
 *
 * Important points to note about using KeyedState.
 *  - The value of the state cannot be null. So updating state with null will throw
 *    `IllegalArgumentException`.
 *  - Operations on `KeyedState` are not thread-safe. This is to avoid memory barriers.
 *  - If `remove()` is called, then `exists()` will return `false`,
 *    `get()` will throw `NoSuchElementException` and `getOption()` will return `None`
 *  - After that, if `update(newState)` is called, then `exists()` will again return `true`,
 *    `get()` and `getOption()`will return the updated value.
 *
 * Scala example of using KeyedState in `mapGroupsWithState`:
 * {{{
 * /* A mapping function that maintains an integer state for string keys and returns a string. */
 * def mappingFunction(key: String, value: Iterator[Int], state: KeyedState[Int]): String = {
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
 * /* A mapping function that maintains an integer state for string keys and returns a string. */
 * MapGroupsWithStateFunction<String, Integer, Integer, String> mappingFunction =
 *    new MapGroupsWithStateFunction<String, Integer, Integer, String>() {
 *
 *      @Override
 *      public String call(String key, Iterator<Integer> value, KeyedState<Integer> state) {
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

  /** Get the state value if it exists, or throw NoSuchElementException. */
  @throws[NoSuchElementException]("when state does not exist")
  def get: S

  /** Get the state value as a scala Option. */
  def getOption: Option[S]

  /**
   * Update the value of the state. Note that `null` is not a valid value, and it throws
   * IllegalArgumentException.
   */
  @throws[IllegalArgumentException]("when updating with null")
  def update(newState: S): Unit

  /** Remove this keyed state. */
  def remove(): Unit
}
