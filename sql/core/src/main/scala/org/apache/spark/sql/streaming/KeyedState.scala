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
 *    See more details on `KeyedStateTimeout` below.
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
 *    `[map|flatMap]GroupsWithState`, but the exact timeout duration/timestamp is configurable per
 *    key by calling `setTimeout...()` in `KeyedState`.
 *  - Timeouts can be either based on processing time (i.e.
 *    [[KeyedStateTimeout.ProcessingTimeTimeout]]) or event time (i.e.
 *    [[KeyedStateTimeout.EventTimeTimeout]]).
 *  - With `ProcessingTimeTimeout`, the timeout duration can be set by calling
 *    `KeyedState.setTimeoutDuration`. The timeout will occur when the clock has advanced by the set
 *    duration. Guarantees provided by this timeout with a duration of D ms are as follows:
 *    - Timeout will never be occur before the clock time has advanced by D ms
 *    - Timeout will occur eventually when there is a trigger in the query
 *      (i.e. after D ms). So there is a no strict upper bound on when the timeout would occur.
 *      For example, the trigger interval of the query will affect when the timeout actually occurs.
 *      If there is no data in the stream (for any key) for a while, then their will not be
 *      any trigger and timeout function call will not occur until there is data.
 *    - Since the processing time timeout is based on the clock time, it is affected by the
 *      variations in the system clock (i.e. time zone changes, clock skew, etc.).
 *  - With `EventTimeTimeout`, the user also has to specify the the the event time watermark in
 *    the query using `Dataset.withWatermark()`. With this setting, data that is older than the
 *    watermark are filtered out. The timeout can be enabled for a key by setting a timestamp using
 *    `KeyedState.setTimeoutTimestamp()`, and the timeout would occur when the watermark advances
 *    beyond the set timestamp. You can control the timeout delay by two parameters - (i) watermark
 *    delay and an additional duration beyond the timestamp in the event (which is guaranteed to
 *    > watermark due to the filtering). Guarantees provided by this timeout are as follows:
 *    - Timeout will never be occur before watermark has exceeded the set timeout.
 *    - Similar to processing time timeouts, there is a no strict upper bound on the delay when
 *      the timeout actually occurs. The watermark can advance only when there is data in the
 *      stream, and the event time of the data has actually advanced.
 *  - When the timeout occurs for a key, the function is called for that key with no values, and
 *    `KeyedState.hasTimedOut()` set to true.
 *  - The timeout is reset for key every time the function is called on the key, that is,
 *    when the key has new data, or the key has timed out. So the user has to set the timeout
 *    duration every time the function is called, otherwise there will not be any timeout set.
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
   *
   * @note ProcessingTimeTimeout must be enabled in `[map/flatmap]GroupsWithStates`.
   */
  @throws[IllegalArgumentException]("if 'durationMs' is not positive")
  @throws[IllegalStateException]("when state is either not initialized, or already removed")
  @throws[UnsupportedOperationException](
    "if 'timeout' has not been enabled in [map|flatMap]GroupsWithState in a streaming query")
  def setTimeoutDuration(durationMs: Long): Unit

  /**
   * Set the timeout duration for this key as a string. For example, "1 hour", "2 days", etc.
   *
   * @note, ProcessingTimeTimeout must be enabled in `[map/flatmap]GroupsWithStates`.
   */
  @throws[IllegalArgumentException]("if 'duration' is not a valid duration")
  @throws[IllegalStateException]("when state is either not initialized, or already removed")
  @throws[UnsupportedOperationException](
    "if 'timeout' has not been enabled in [map|flatMap]GroupsWithState in a streaming query")
  def setTimeoutDuration(duration: String): Unit

  @throws[IllegalArgumentException]("if 'timestampMs' is not positive")
  @throws[IllegalStateException]("when state is either not initialized, or already removed")
  @throws[UnsupportedOperationException](
    "if 'timeout' has not been enabled in [map|flatMap]GroupsWithState in a streaming query")
  /**
   * Set the timeout timestamp for this key as milliseconds in epoch time.
   * This timestamp cannot be older than the current watermark.
   *
   * @note, EventTimeTimeout must be enabled in `[map/flatmap]GroupsWithStates`.
   */
  def setTimeoutTimestamp(timestampMs: Long): Unit

  @throws[IllegalArgumentException]("if 'additionalDuration' is invalid")
  @throws[IllegalStateException]("when state is either not initialized, or already removed")
  @throws[UnsupportedOperationException](
    "if 'timeout' has not been enabled in [map|flatMap]GroupsWithState in a streaming query")
  /**
   * Set the timeout timestamp for this key as milliseconds in epoch time and an additional
   * duration as a string (e.g. "1 hour", "2 days", etc.).
   * The final timestamp (including the additional duration) cannot be older than the
   * current watermark.
   *
   * @note, EventTimeTimeout must be enabled in `[map/flatmap]GroupsWithStates`.
   */
  def setTimeoutTimestamp(timestampMs: Long, additionalDuration: String): Unit

  @throws[IllegalStateException]("when state is either not initialized, or already removed")
  @throws[UnsupportedOperationException](
    "if 'timeout' has not been enabled in [map|flatMap]GroupsWithState in a streaming query")
  /**
   * Set the timeout timestamp for this key as a java.sql.Date.
   * This timestamp cannot be older than the current watermark.
   *
   * @note, EventTimeTimeout must be enabled in `[map/flatmap]GroupsWithStates`.
   */
  def setTimeoutTimestamp(timestamp: java.sql.Date): Unit

  @throws[IllegalArgumentException]("if 'additionalDuration' is invalid")
  @throws[IllegalStateException]("when state is either not initialized, or already removed")
  @throws[UnsupportedOperationException](
    "if 'timeout' has not been enabled in [map|flatMap]GroupsWithState in a streaming query")
  /**
   * Set the timeout timestamp for this key as a java.sql.Date and an additional
   * duration as a string (e.g. "1 hour", "2 days", etc.).
   * The final timestamp (including the additional duration) cannot be older than the
   * current watermark.
   *
   * @note, EventTimeTimeout must be enabled in `[map/flatmap]GroupsWithStates`.
   */
  def setTimeoutTimestamp(timestamp: java.sql.Date, additionalDuration: String): Unit
}
