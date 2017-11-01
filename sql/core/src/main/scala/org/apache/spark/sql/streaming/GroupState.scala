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
import org.apache.spark.sql.KeyValueGroupedDataset
import org.apache.spark.sql.catalyst.plans.logical.LogicalGroupState

/**
 * :: Experimental ::
 *
 * Wrapper class for interacting with per-group state data in `mapGroupsWithState` and
 * `flatMapGroupsWithState` operations on `KeyValueGroupedDataset`.
 *
 * Detail description on `[map/flatMap]GroupsWithState` operation
 * --------------------------------------------------------------
 * Both, `mapGroupsWithState` and `flatMapGroupsWithState` in `KeyValueGroupedDataset`
 * will invoke the user-given function on each group (defined by the grouping function in
 * `Dataset.groupByKey()`) while maintaining user-defined per-group state between invocations.
 * For a static batch Dataset, the function will be invoked once per group. For a streaming
 * Dataset, the function will be invoked for each group repeatedly in every trigger.
 * That is, in every batch of the `StreamingQuery`,
 * the function will be invoked once for each group that has data in the trigger. Furthermore,
 * if timeout is set, then the function will invoked on timed out groups (more detail below).
 *
 * The function is invoked with following parameters.
 *  - The key of the group.
 *  - An iterator containing all the values for this group.
 *  - A user-defined state object set by previous invocations of the given function.
 *
 * In case of a batch Dataset, there is only one invocation and state object will be empty as
 * there is no prior state. Essentially, for batch Datasets, `[map/flatMap]GroupsWithState`
 * is equivalent to `[map/flatMap]Groups` and any updates to the state and/or timeouts have
 * no effect.
 *
 * The major difference between `mapGroupsWithState` and `flatMapGroupsWithState` is that the
 * former allows the function to return one and only one record, whereas the latter
 * allows the function to return any number of records (including no records). Furthermore, the
 * `flatMapGroupsWithState` is associated with an operation output mode, which can be either
 * `Append` or `Update`. Semantically, this defines whether the output records of one trigger
 * is effectively replacing the previously output records (from previous triggers) or is appending
 * to the list of previously output records. Essentially, this defines how the Result Table (refer
 * to the semantics in the programming guide) is updated, and allows us to reason about the
 * semantics of later operations.
 *
 * Important points to note about the function (both mapGroupsWithState and flatMapGroupsWithState).
 *  - In a trigger, the function will be called only the groups present in the batch. So do not
 *    assume that the function will be called in every trigger for every group that has state.
 *  - There is no guaranteed ordering of values in the iterator in the function, neither with
 *    batch, nor with streaming Datasets.
 *  - All the data will be shuffled before applying the function.
 *  - If timeout is set, then the function will also be called with no values.
 *    See more details on `GroupStateTimeout` below.
 *
 * Important points to note about using `GroupState`.
 *  - The value of the state cannot be null. So updating state with null will throw
 *    `IllegalArgumentException`.
 *  - Operations on `GroupState` are not thread-safe. This is to avoid memory barriers.
 *  - If `remove()` is called, then `exists()` will return `false`,
 *    `get()` will throw `NoSuchElementException` and `getOption()` will return `None`
 *  - After that, if `update(newState)` is called, then `exists()` will again return `true`,
 *    `get()` and `getOption()`will return the updated value.
 *
 * Important points to note about using `GroupStateTimeout`.
 *  - The timeout type is a global param across all the groups (set as `timeout` param in
 *    `[map|flatMap]GroupsWithState`, but the exact timeout duration/timestamp is configurable per
 *    group by calling `setTimeout...()` in `GroupState`.
 *  - Timeouts can be either based on processing time (i.e.
 *    `GroupStateTimeout.ProcessingTimeTimeout`) or event time (i.e.
 *    `GroupStateTimeout.EventTimeTimeout`).
 *  - With `ProcessingTimeTimeout`, the timeout duration can be set by calling
 *    `GroupState.setTimeoutDuration`. The timeout will occur when the clock has advanced by the set
 *    duration. Guarantees provided by this timeout with a duration of D ms are as follows:
 *    - Timeout will never be occur before the clock time has advanced by D ms
 *    - Timeout will occur eventually when there is a trigger in the query
 *      (i.e. after D ms). So there is a no strict upper bound on when the timeout would occur.
 *      For example, the trigger interval of the query will affect when the timeout actually occurs.
 *      If there is no data in the stream (for any group) for a while, then their will not be
 *      any trigger and timeout function call will not occur until there is data.
 *    - Since the processing time timeout is based on the clock time, it is affected by the
 *      variations in the system clock (i.e. time zone changes, clock skew, etc.).
 *  - With `EventTimeTimeout`, the user also has to specify the the the event time watermark in
 *    the query using `Dataset.withWatermark()`. With this setting, data that is older than the
 *    watermark are filtered out. The timeout can be set for a group by setting a timeout timestamp
 *    using`GroupState.setTimeoutTimestamp()`, and the timeout would occur when the watermark
 *    advances beyond the set timestamp. You can control the timeout delay by two parameters -
 *    (i) watermark delay and an additional duration beyond the timestamp in the event (which
 *    is guaranteed to be newer than watermark due to the filtering). Guarantees provided by this
 *    timeout are as follows:
 *    - Timeout will never be occur before watermark has exceeded the set timeout.
 *    - Similar to processing time timeouts, there is a no strict upper bound on the delay when
 *      the timeout actually occurs. The watermark can advance only when there is data in the
 *      stream, and the event time of the data has actually advanced.
 *  - When the timeout occurs for a group, the function is called for that group with no values, and
 *    `GroupState.hasTimedOut()` set to true.
 *  - The timeout is reset every time the function is called on a group, that is,
 *    when the group has new data, or the group has timed out. So the user has to set the timeout
 *    duration every time the function is called, otherwise there will not be any timeout set.
 *
 * Scala example of using GroupState in `mapGroupsWithState`:
 * {{{
 * // A mapping function that maintains an integer state for string keys and returns a string.
 * // Additionally, it sets a timeout to remove the state if it has not received data for an hour.
 * def mappingFunction(key: String, value: Iterator[Int], state: GroupState[Int]): String = {
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
 *   .mapGroupsWithState(GroupStateTimeout.ProcessingTimeTimeout)(mappingFunction)
 * }}}
 *
 * Java example of using `GroupState`:
 * {{{
 * // A mapping function that maintains an integer state for string keys and returns a string.
 * // Additionally, it sets a timeout to remove the state if it has not received data for an hour.
 * MapGroupsWithStateFunction<String, Integer, Integer, String> mappingFunction =
 *    new MapGroupsWithStateFunction<String, Integer, Integer, String>() {
 *
 *      @Override
 *      public String call(String key, Iterator<Integer> value, GroupState<Integer> state) {
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
 *         mappingFunction, Encoders.INT, Encoders.STRING, GroupStateTimeout.ProcessingTimeTimeout);
 * }}}
 *
 * @tparam S User-defined type of the state to be stored for each group. Must be encodable into
 *           Spark SQL types (see `Encoder` for more details).
 * @since 2.2.0
 */
@Experimental
@InterfaceStability.Evolving
trait GroupState[S] extends LogicalGroupState[S] {

  /** Whether state exists or not. */
  def exists: Boolean

  /** Get the state value if it exists, or throw NoSuchElementException. */
  @throws[NoSuchElementException]("when state does not exist")
  def get: S

  /** Get the state value as a scala Option. */
  def getOption: Option[S]

  /** Update the value of the state. */
  def update(newState: S): Unit

  /** Remove this state. */
  def remove(): Unit

  /**
   * Whether the function has been called because the key has timed out.
   * @note This can return true only when timeouts are enabled in `[map/flatMap]GroupsWithState`.
   */
  def hasTimedOut: Boolean


  /**
   * Set the timeout duration in ms for this key.
   *
   * @note [[GroupStateTimeout Processing time timeout]] must be enabled in
   *       `[map/flatMap]GroupsWithState` for calling this method.
   * @note This method has no effect when used in a batch query.
   */
  @throws[IllegalArgumentException]("if 'durationMs' is not positive")
  @throws[UnsupportedOperationException](
    "if processing time timeout has not been enabled in [map|flatMap]GroupsWithState")
  def setTimeoutDuration(durationMs: Long): Unit


  /**
   * Set the timeout duration for this key as a string. For example, "1 hour", "2 days", etc.
   *
   * @note [[GroupStateTimeout Processing time timeout]] must be enabled in
   *       `[map/flatMap]GroupsWithState` for calling this method.
   * @note This method has no effect when used in a batch query.
   */
  @throws[IllegalArgumentException]("if 'duration' is not a valid duration")
  @throws[UnsupportedOperationException](
    "if processing time timeout has not been enabled in [map|flatMap]GroupsWithState")
  def setTimeoutDuration(duration: String): Unit


  /**
   * Set the timeout timestamp for this key as milliseconds in epoch time.
   * This timestamp cannot be older than the current watermark.
   *
   * @note [[GroupStateTimeout Event time timeout]] must be enabled in
   *       `[map/flatMap]GroupsWithState` for calling this method.
   * @note This method has no effect when used in a batch query.
   */
  @throws[IllegalArgumentException](
    "if 'timestampMs' is not positive or less than the current watermark in a streaming query")
  @throws[UnsupportedOperationException](
    "if processing time timeout has not been enabled in [map|flatMap]GroupsWithState")
  def setTimeoutTimestamp(timestampMs: Long): Unit


  /**
   * Set the timeout timestamp for this key as milliseconds in epoch time and an additional
   * duration as a string (e.g. "1 hour", "2 days", etc.).
   * The final timestamp (including the additional duration) cannot be older than the
   * current watermark.
   *
   * @note [[GroupStateTimeout Event time timeout]] must be enabled in
   *       `[map/flatMap]GroupsWithState` for calling this method.
   * @note This method has no side effect when used in a batch query.
   */
  @throws[IllegalArgumentException](
    "if 'additionalDuration' is invalid or the final timeout timestamp is less than " +
      "the current watermark in a streaming query")
  @throws[UnsupportedOperationException](
    "if event time timeout has not been enabled in [map|flatMap]GroupsWithState")
  def setTimeoutTimestamp(timestampMs: Long, additionalDuration: String): Unit


  /**
   * Set the timeout timestamp for this key as a java.sql.Date.
   * This timestamp cannot be older than the current watermark.
   *
   * @note [[GroupStateTimeout Event time timeout]] must be enabled in
   *       `[map/flatMap]GroupsWithState` for calling this method.
   * @note This method has no side effect when used in a batch query.
   */
  @throws[UnsupportedOperationException](
    "if event time timeout has not been enabled in [map|flatMap]GroupsWithState")
  def setTimeoutTimestamp(timestamp: java.sql.Date): Unit


  /**
   * Set the timeout timestamp for this key as a java.sql.Date and an additional
   * duration as a string (e.g. "1 hour", "2 days", etc.).
   * The final timestamp (including the additional duration) cannot be older than the
   * current watermark.
   *
   * @note [[GroupStateTimeout Event time timeout]] must be enabled in
   *      `[map/flatMap]GroupsWithState` for calling this method.
   * @note This method has no side effect when used in a batch query.
   */
  @throws[IllegalArgumentException]("if 'additionalDuration' is invalid")
  @throws[UnsupportedOperationException](
    "if event time timeout has not been enabled in [map|flatMap]GroupsWithState")
  def setTimeoutTimestamp(timestamp: java.sql.Date, additionalDuration: String): Unit


  /**
   * Get the current event time watermark as milliseconds in epoch time.
   *
   * @note In a streaming query, this can be called only when watermark is set before calling
   *       `[map/flatMap]GroupsWithState`. In a batch query, this method always returns -1.
   */
  @throws[UnsupportedOperationException](
    "if watermark has not been set before in [map|flatMap]GroupsWithState")
  def getCurrentWatermarkMs(): Long


  /**
   * Get the current processing time as milliseconds in epoch time.
   * @note In a streaming query, this will return a constant value throughout the duration of a
   *       trigger, even if the trigger is re-executed.
   */
  def getCurrentProcessingTimeMs(): Long
}
