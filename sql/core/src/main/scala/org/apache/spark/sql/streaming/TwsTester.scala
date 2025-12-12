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

import scala.reflect.ClassTag

import org.apache.spark.sql.execution.streaming.operators.stateful.transformwithstate.statefulprocessor.ImplicitGroupingKeyTracker
import org.apache.spark.sql.execution.streaming.operators.stateful.transformwithstate.testing.InMemoryStatefulProcessorHandle

/**
 * Testing utility for transformWithState stateful processors.
 *
 * This class enables unit testing of StatefulProcessor business logic by simulating the
 * behavior of transformWithState. It processes input rows and returns output rows equivalent
 * to those that would be produced by the processor in an actual Spark streaming query.
 *
 * '''Supported:'''
 *  - Processing input rows and producing output rows via `test()`.
 *  - Initial state setup via constructor parameter.
 *  - Direct state manipulation via `setValueState`, `setListState`, `setMapState`.
 *  - Direct state inspection via `peekValueState`, `peekListState`, `peekMapState`.
 *
 * '''Not Supported:'''
 *  - '''Timers''': Only TimeMode.None is supported. If the processor attempts to register or
 *    use timers (as if in TimeMode.EventTime or TimeMode.ProcessingTime), an
 *    UnsupportedOperationException or NullPointerException will be thrown.
 *  - '''TTL''': State TTL configurations are ignored. All state persists indefinitely.
 *
 * '''Use Cases:'''
 *  - '''Primary''': Unit testing business logic in `handleInputRows` implementations.
 *  - '''Not recommended''': End-to-end testing or performance testing - use actual Spark
 *    streaming queries for those scenarios.
 *
 * @param processor the StatefulProcessor to test.
 * @param initialState initial state for each key as a list of (key, state) tuples.
 * @tparam K the type of grouping key.
 * @tparam I the type of input rows.
 * @tparam O the type of output rows.
 * @since 4.0.2
 */
class TwsTester[K, I, O](
    val processor: StatefulProcessor[K, I, O],
    val initialState: List[(K, Any)] = List()) {
  private val handle = new InMemoryStatefulProcessorHandle()
  processor.setHandle(handle)
  processor.init(OutputMode.Append, TimeMode.None)
  processor match {
    case p: StatefulProcessorWithInitialState[K @unchecked, I @unchecked, O @unchecked, s] =>
      handleInitialState[s]()
    case _ =>
      require(
        initialState.isEmpty,
        "Passed initial state, but the stateful processor doesn't support initial state."
      )
  }

  private def handleInitialState[S](): Unit = {
    val p = processor.asInstanceOf[StatefulProcessorWithInitialState[K, I, O, S]]
    initialState.foreach {
      case (key, state) =>
        ImplicitGroupingKeyTracker.setImplicitKey(key)
        p.handleInitialState(key, state.asInstanceOf[S], null)
    }
  }

  /**
   * Processes input rows through the stateful processor, grouped by key.
   *
   * This corresponds to processing one microbatch. {@code handleInputRows} will be called once for
   * each key that appears in {@code input}.
   *
   * To simulate real-time mode, call this method repeatedly in a loop, passing a list with a single
   * (key, input row) tuple per call.
   *
   * @param input list of (key, input row) tuples to process
   * @return all output rows produced by the processor
   */
  def test(input: List[(K, I)]): List[O] = {
    var ans: List[O] = List()
    for ((key, v) <- input.groupBy(_._1)) {
      ImplicitGroupingKeyTracker.setImplicitKey(key)
      ans = ans ++ processor.handleInputRows(key, v.map(_._2).iterator, null).toList
    }
    ans
  }

  /** Sets the value state for a given key. */
  def setValueState[T](stateName: String, key: K, value: T): Unit = {
    ImplicitGroupingKeyTracker.setImplicitKey(key)
    handle.setValueState[T](stateName, value)
  }

  /** Retrieves the value state for a given key. */
  def peekValueState[T](stateName: String, key: K): Option[T] = {
    ImplicitGroupingKeyTracker.setImplicitKey(key)
    handle.peekValueState[T](stateName)
  }

  /** Sets the list state for a given key. */
  def setListState[T](stateName: String, key: K, value: List[T])(implicit ct: ClassTag[T]): Unit = {
    ImplicitGroupingKeyTracker.setImplicitKey(key)
    handle.setListState[T](stateName, value)
  }

  /** Retrieves the list state for a given key. */
  def peekListState[T](stateName: String, key: K): List[T] = {
    ImplicitGroupingKeyTracker.setImplicitKey(key)
    handle.peekListState[T](stateName)
  }

  /** Sets the map state for a given key. */
  def setMapState[MK, MV](stateName: String, key: K, value: Map[MK, MV]): Unit = {
    ImplicitGroupingKeyTracker.setImplicitKey(key)
    handle.setMapState[MK, MV](stateName, value)
  }

  /** Retrieves the map state for a given key. */
  def peekMapState[MK, MV](stateName: String, key: K): Map[MK, MV] = {
    ImplicitGroupingKeyTracker.setImplicitKey(key)
    handle.peekMapState[MK, MV](stateName)
  }
}
