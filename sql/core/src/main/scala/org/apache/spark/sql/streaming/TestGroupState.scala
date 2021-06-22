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

import org.apache.spark.annotation.{Evolving, Experimental}
import org.apache.spark.api.java.Optional
import org.apache.spark.sql.execution.streaming.GroupStateImpl
import org.apache.spark.sql.execution.streaming.GroupStateImpl._

/**
 * The extended version of [[GroupState]] interface with extra getters of state machine fields
 * to improve testability of the [[GroupState]] implementations
 * which inherit from the extended interface.
 *
 * Scala example of using `TestGroupState`:
 * {{{
 * import org.apache.spark.sql.streaming.TestGroupState
 * // other imports
 *
 * // test class setups
 *
 * test("Structured Streaming state update function") {
 *   // Creates the prevState input for the state transition function
 *   // with desired configs. The create() API would guarantee that
 *   // the generated instance has the same behavior as the one built by
 *   // engine with the same configs.
 *   var prevState = TestGroupState.create[UserStatus](
 *     optionalState = Optional.empty[UserStatus],
 *     timeoutConf = EventTimeTimeout,
 *     batchProcessingTimeMs = 1L,
 *     eventTimeWatermarkMs = Optional.of(1L),
 *     hasTimedOut = false)
 *
 *   val userId: String = ...
 *   val actions: Iterator[UserAction] = ...
 *
 *   // Asserts the prevState is in init state without updates.
 *   assert(!prevState.hasUpdated)
 *
 *   // Calls the state transition function with the test previous state
 *   //  with desired configs.
 *   updateState(userId, actions, prevState)
 *
 *   // Asserts the test GroupState object has been updated after calling
 *   // the state transition function
 *   assert(prevState.hasUpdated)
 * }
 * }}}
 *
 * Java example of using `TestGroupSate`:
 * {{{
 * import org.apache.spark.sql.streaming.TestGroupState;
 * // other imports
 *
 * // test class setups
 *
 * // test `flatMapGroupsWithState` state transition function `updateState()`
 * public void testUpdateState() {
 *   // Creates the prevState input for the state transition function
 *   // with desired configs. The create() API would guarantee that
 *   // the generated instance has the same behavior as the one built by
 *   // engine with the same configs.
 *   TestGroupState prevState = new TestGroupState<UserStatus>().create(
 *     optionalState = Optional.<UserStatus>empty(),
 *     timeoutConf = EventTimeTimeout,
 *     batchProcessingTimeMs = 1L,
 *     eventTimeWatermarkMs = Optional.of(1L),
 *     hasTimedOut = false);
 *
 *   String userId = ...;
 *   ArrayList<UserAction> actions = ...;
 *
 *   // Asserts the prevState is in init state without updates.
 *   assertTrue(!prevState.hasUpdated());
 *
 *   // Calls the state transition function with the test previous state
 *   //  with desired configs.
 *   updateState(userId, actions, prevState);
 *
 *   // Asserts the test GroupState object has been updated after calling
 *   // the state transition function
 *   assertTrue(prevState.hasUpdated());
 * }
 * }}}
 *
 * @tparam S User-defined type of the state to be stored for each group. Must be encodable into
 *           Spark SQL types (see `Encoder` for more details).
 * @since 3.2.0
 */
@Experimental
@Evolving
trait TestGroupState[S] extends GroupState[S] {
  /** Whether the state has been marked for removing */
  def isRemoved: Boolean

  /** Whether the state has been updated but not removed */
  def isUpdated: Boolean

  /**
   * Returns the timestamp if setTimeoutTimestamp is called.
   * Or, batch processing time + the duration will be returned when
   * setTimeoutDuration is called.
   *
   * Otherwise, returns Optional.empty if not set.
   */
  def getTimeoutTimestampMs: Optional[Long]
}

object TestGroupState {

  /**
   * Creates TestGroupState instances for general testing purposes.
   *
   * @param optionalState         Optional value of the state.
   * @param timeoutConf           Type of timeout configured. Based on this, different operations
   *                              will be supported.
   * @param batchProcessingTimeMs Processing time of current batch, used to calculate timestamp
   *                              for processing time timeouts.
   * @param eventTimeWatermarkMs  Optional value of event time watermark in ms. Set as None if
   *                              watermark is not present. Otherwise, event time watermark
   *                              should be a positive long and the timestampMs
   *                              set through setTimeoutTimestamp.
   *                              cannot be less than the set eventTimeWatermarkMs.
   * @param hasTimedOut           Whether the key for which this state wrapped is being created is
   *                              getting timed out or not.
   * @return a [[TestGroupState]] instance that's built with the user specified configs.
   */
  @throws[IllegalArgumentException]("if 'batchProcessingTimeMs' is not positive")
  @throws[IllegalArgumentException]("if 'eventTimeWatermarkMs' present but is not 0 or positive")
  @throws[UnsupportedOperationException](
    "if 'hasTimedOut' is true however there's no timeout configured")
  def create[S](
      optionalState: Optional[S],
      timeoutConf: GroupStateTimeout,
      batchProcessingTimeMs: Long,
      eventTimeWatermarkMs: Optional[Long],
      hasTimedOut: Boolean): TestGroupState[S] = {
    GroupStateImpl.createForStreaming[S](
      Option(optionalState.orNull),
      batchProcessingTimeMs,
      eventTimeWatermarkMs.orElse(NO_TIMESTAMP),
      timeoutConf,
      hasTimedOut,
      eventTimeWatermarkMs.isPresent())
  }
}
