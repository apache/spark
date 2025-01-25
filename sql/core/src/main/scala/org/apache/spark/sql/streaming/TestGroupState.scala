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
import org.apache.spark.api.java.Optional
import org.apache.spark.sql.execution.streaming.GroupStateImpl
import org.apache.spark.sql.execution.streaming.GroupStateImpl._

/**
 * :: Experimental ::
 *
 * The extended version of [[GroupState]] interface with extra getters of state machine fields
 * to improve testability of the [[GroupState]] implementations
 * which inherit from the extended interface.
 *
 * Scala example of using `TestGroupState`:
 * {{{
 * // Please refer to ScalaDoc of `GroupState` for the Scala definition of `mappingFunction()`
 *
 * import org.apache.spark.api.java.Optional
 * import org.apache.spark.sql.streaming.GroupStateTimeout
 * import org.apache.spark.sql.streaming.TestGroupState
 * // other imports
 *
 * // test class setups
 *
 * test("MapGroupsWithState state transition function") {
 *   // Creates the prevState input for the state transition function
 *   // with desired configs. The `create()` API would guarantee that
 *   // the generated instance has the same behavior as the one built by
 *   // engine with the same configs.
 *   val prevState = TestGroupState.create[Int](
 *     optionalState = Optional.empty[Int],
 *     timeoutConf = NoTimeout,
 *     batchProcessingTimeMs = 1L,
 *     eventTimeWatermarkMs = Optional.of(1L),
 *     hasTimedOut = false)
 *
 *   val key: String = ...
 *   val values: Iterator[Int] = ...
 *
 *   // Asserts the prevState is in init state without updates.
 *   assert(!prevState.isUpdated)
 *
 *   // Calls the state transition function with the test previous state
 *   // with desired configs.
 *   mappingFunction(key, values, prevState)
 *
 *   // Asserts the test GroupState object has been updated but not removed
 *   // after calling the state transition function
 *   assert(prevState.isUpdated)
 *   assert(!prevState.isRemoved)
 * }
 * }}}
 *
 * Java example of using `TestGroupSate`:
 * {{{
 * // Please refer to ScalaDoc of `GroupState` for the Java definition of `mappingFunction()`
 *
 * import org.apache.spark.api.java.Optional;
 * import org.apache.spark.sql.streaming.GroupStateTimeout;
 * import org.apache.spark.sql.streaming.TestGroupState;
 * // other imports
 *
 * // test class setups
 *
 * // test `MapGroupsWithState` state transition function `mappingFunction()`
 * public void testMappingFunctionWithTestGroupState() {
 *   // Creates the prevState input for the state transition function
 *   // with desired configs. The `create()` API would guarantee that
 *   // the generated instance has the same behavior as the one built by
 *   // engine with the same configs.
 *   TestGroupState<Int> prevState = TestGroupState.create(
 *     Optional.empty(),
 *     GroupStateTimeout.NoTimeout(),
 *     1L,
 *     Optional.of(1L),
 *     false);
 *
 *   String key = ...;
 *   Integer[] values = ...;
 *
 *   // Asserts the prevState is in init state without updates.
 *   Assertions.assertFalse(prevState.isUpdated());
 *
 *   // Calls the state transition function with the test previous state
 *   // with desired configs.
 *   mappingFunction.call(key, Arrays.asList(values).iterator(), prevState);
 *
 *   // Asserts the test GroupState object has been updated but not removed
 *   // after calling the state transition function
 *   Assertions.assertTrue(prevState.isUpdated());
 *   Assertions.assertFalse(prevState.isRemoved());
 * }
 * }}}
 *
 * @tparam S User-defined type of the state to be stored for each group. Must be encodable into
 *           Spark SQL types (see `Encoder` for more details).
 * @since 3.2.0
 */
@Evolving
trait TestGroupState[S] extends GroupState[S] {
  /** Whether the state has been marked for removing */
  def isRemoved: Boolean

  /** Whether the state has been updated but not removed */
  def isUpdated: Boolean

  /**
   * Returns the timestamp if `setTimeoutTimestamp()` is called.
   * Or, returns batch processing time + the duration when
   * `setTimeoutDuration()` is called.
   *
   * Otherwise, returns `Optional.empty` if not set.
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
   * @param eventTimeWatermarkMs  Optional value of event time watermark in ms. Set as
   *                              `Optional.empty` if watermark is not present.
   *                              Otherwise, event time watermark should be a positive long
   *                              and the timestampMs set through `setTimeoutTimestamp()`
   *                              cannot be less than `eventTimeWatermarkMs`.
   * @param hasTimedOut           Whether the key for which this state wrapped is being created is
   *                              getting timed out or not.
   * @return a [[TestGroupState]] instance built with the user specified configs.
   */
  @throws[IllegalArgumentException]("if 'batchProcessingTimeMs' is less than 0")
  @throws[IllegalArgumentException]("if 'eventTimeWatermarkMs' is present but less than 0")
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
