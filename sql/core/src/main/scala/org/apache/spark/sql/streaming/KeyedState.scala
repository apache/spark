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

import org.apache.spark.annotation.{Experimental, InterfaceStability}
import org.apache.spark.sql.{Encoder, KeyValueGroupedDataset}
import org.apache.spark.sql.catalyst.plans.logical.LogicalKeyedState

/**
 * :: Experimental ::
 *
 * Wrapper class for interacting with keyed state data in `mapGroupsWithState` and
 * `flatMapGroupsWithState` operations on
 * [[KeyValueGroupedDataset]].
 *
 * Detail description on `[map/flatMap]GroupsWithState` operation
 * --------------------------------------------------------------
 * Both, `mapGroupsWithState` and `flatMapGroupsWithState` in [[KeyValueGroupedDataset]]
 * will invoke the user-given function on each group (defined by the grouping function in
 * `Dataset.groupByKey()`) while maintaining user-defined per-group state between invocations.
 * For a static batch Dataset, the function will be invoked once per group. For a streaming
 * Dataset, the function will be invoked for each group repeatedly in every trigger.
 * That is, in every batch of the `streaming.StreamingQuery`,
 * the function will be invoked once for each group that has data in the trigger. Furthermore,
 * if timeout is set, then the function will invoked on timed out keys (more detail below).
 *
 * The function is invoked with following parameters.
 *  - The key of the group.
 *  - An iterator containing all the values for this key.
 *  - A user-defined state object set by previous invocations of the given function.
 * In case of a batch Dataset, there is only one invocation and state object will be empty as
 * there is no prior state. Essentially, for batch Datasets, `[map/flatMap]GroupsWithState`
 * is equivalent to `[map/flatMap]Groups` and any updates to the state and/or timeouts have
 * no effect.
 *
 * Important points to note about the function.
 *  - In a trigger, the function will be called only the groups present in the batch. So do not
 *    assume that the function will be called in every trigger for every group that has state.
 *  - There is no guaranteed ordering of values in the iterator in the function, neither with
 *    batch, nor with streaming Datasets.
 *  - All the data will be shuffled before applying the function.
 *  - If timeout is set, then the function will also be called with no values.
 *    See more details on KeyedStateTimeout` below.
 *
 * Important points to note about using `KeyedState`.
 *  - The value of the state cannot be null. So updating state with null will throw
 *    `IllegalArgumentException`.
 *  - Operations on `KeyedState` are not thread-safe. This is to avoid memory barriers.
 *  - If `remove()` is called, then `exists()` will return `false`,
 *    `get()` will throw `NoSuchElementException` and `getOption()` will return `None`
 *  - After that, if `update(newState)` is called, then `exists()` will again return `true`,
 *    `get()` and `getOption()`will return the updated value.
 *
 * Important points to note about using `KeyedStateTimeout`.
 *  - The timeout type is a global param across all the keys (set as `timeout` param in
 *    `[map|flatMap]GroupsWithState`, but the exact timeout duration is configurable per key
 *    (by calling `setTimeout...()` in `KeyedState`).
 *  - When the timeout occurs for a key, the function is called with no values, and
 *    `KeyedState.hasTimedOut()` set to true.
 *  - The timeout is reset for key every time the function is called on the key, that is,
 *    when the key has new data, or the key has timed out. So the user has to set the timeout
 *    duration every time the function is called, otherwise there will not be any timeout set.
 *  - Guarantees provided on processing-time-based timeout of key, when timeout duration is D ms:
 *    - Timeout will never be called before real clock time has advanced by D ms
 *    - Timeout will be called eventually when there is a trigger in the query
 *      (i.e. after D ms). So there is a no strict upper bound on when the timeout would occur.
 *      For example, the trigger interval of the query will affect when the timeout is actually hit.
 *      If there is no data in the stream (for any key) for a while, then their will not be
 *      any trigger and timeout will not be hit until there is data.
 *
 * Scala example of using KeyedState in `mapGroupsWithState`:
 * {{{
 * // A mapping function that maintains an integer state for string keys and returns a string.
 * // Additionally, it sets a timeout to remove the state if it has not received data for an hour.
 * def mappingFunction(key: String, value: Iterator[Int], state: KeyedState[Int]): String = {
 *
 *   if (state.hasTimedOut) {                // If called when timing out, remove the state
 *     state.remove()
 *
 *   } else if (state.exists) {              // If state exists, use it for processing
 *     val existingState = state.get         // Get the existing state
 *     val shouldRemove = ...                // Decide whether to remove the state
 *     if (shouldRemove) {
 *       state.remove()                      // Remove the state
 *
 *     } else {
 *       val newState = ...
 *       state.update(newState)              // Set the new state
 *       state.setTimeoutDuration("1 hour")  // Set the timeout
 *     }
 *
 *   } else {
 *     val initialState = ...
 *     state.update(initialState)            // Set the initial state
 *     state.setTimeoutDuration("1 hour")    // Set the timeout
 *   }
 *   ...
 *   // return something
 * }
 *
 * dataset
 *   .groupByKey(...)
 *   .mapGroupsWithState(KeyedStateTimeout.ProcessingTimeTimeout)(mappingFunction)
 * }}}
 *
 * Java example of using `KeyedState`:
 * {{{
 * // A mapping function that maintains an integer state for string keys and returns a string.
 * // Additionally, it sets a timeout to remove the state if it has not received data for an hour.
 * MapGroupsWithStateFunction<String, Integer, Integer, String> mappingFunction =
 *    new MapGroupsWithStateFunction<String, Integer, Integer, String>() {
 *
 *      @Override
 *      public String call(String key, Iterator<Integer> value, KeyedState<Integer> state) {
 *        if (state.hasTimedOut()) {            // If called when timing out, remove the state
 *          state.remove();
 *
 *        } else if (state.exists()) {            // If state exists, use it for processing
 *          int existingState = state.get();      // Get the existing state
 *          boolean shouldRemove = ...;           // Decide whether to remove the state
 *          if (shouldRemove) {
 *            state.remove();                     // Remove the state
 *
 *          } else {
 *            int newState = ...;
 *            state.update(newState);             // Set the new state
 *            state.setTimeoutDuration("1 hour"); // Set the timeout
 *          }
 *
 *        } else {
 *          int initialState = ...;               // Set the initial state
 *          state.update(initialState);
 *          state.setTimeoutDuration("1 hour");   // Set the timeout
 *        }
 *        ...
*         // return something
 *      }
 *    };
 *
 * dataset
 *     .groupByKey(...)
 *     .mapGroupsWithState(
 *         mappingFunction, Encoders.INT, Encoders.STRING, KeyedStateTimeout.ProcessingTimeTimeout);
 * }}}
 *
 * @tparam S User-defined type of the state to be stored for each key. Must be encodable into
 *           Spark SQL types (see [[Encoder]] for more details).
 * @since 2.2.0
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

  /** Remove this keyed state. Note that this resets any timeout configuration as well. */
  def remove(): Unit

  /**
   * Whether the function has been called because the key has timed out.
   * @note This can return true only when timeouts are enabled in `[map/flatmap]GroupsWithStates`.
   */
  def hasTimedOut: Boolean

  /**
   * Set the timeout duration in ms for this key.
   * @note Timeouts must be enabled in `[map/flatmap]GroupsWithStates`.
   */
  @throws[IllegalArgumentException]("if 'durationMs' is not positive")
  @throws[IllegalStateException]("when state is either not initialized, or already removed")
  @throws[UnsupportedOperationException](
    "if 'timeout' has not been enabled in [map|flatMap]GroupsWithState in a streaming query")
  def setTimeoutDuration(durationMs: Long): Unit

  /**
   * Set the timeout duration for this key as a string. For example, "1 hour", "2 days", etc.
   * @note, Timeouts must be enabled in `[map/flatmap]GroupsWithStates`.
   */
  @throws[IllegalArgumentException]("if 'duration' is not a valid duration")
  @throws[IllegalStateException]("when state is either not initialized, or already removed")
  @throws[UnsupportedOperationException](
    "if 'timeout' has not been enabled in [map|flatMap]GroupsWithState in a streaming query")
  def setTimeoutDuration(duration: String): Unit
}
