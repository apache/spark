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
import org.apache.spark.sql.execution.streaming.operators.stateful.transformwithstate.timers.{ExpiredTimerInfoImpl, TimerValuesImpl}
import org.apache.spark.sql.streaming.{TimeMode}
import java.time.{Clock, Instant, ZoneId}

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
 *  - Timers in ProcessingTime mode.
 *  - TTL for ValueState, ListState, and MapState (use ProcessingTime mode and
 *    `advanceProcessingTime` to test expiry).
 *
 * '''Not Supported:'''
 *  - Timers in EventTime mode.
 *
 * '''Use Cases:'''
 *  - '''Primary''': Unit testing business logic in `handleInputRows` implementations.
 *  - '''Not recommended''': End-to-end testing or performance testing - use actual Spark
 *    streaming queries for those scenarios.
 *
 * @param processor the StatefulProcessor to test.
 * @param initialState initial state for each key as a list of (key, state) tuples.
 * @param timeMode time mode (None, ProcessingTime or EventTime).
 * @param realTimeMode whether input rows should be processed one-by-one (separate call to 
 *     handleInputRows) for each input row.
 * @tparam K the type of grouping key.
 * @tparam I the type of input rows.
 * @tparam O the type of output rows.
 * @since 4.0.2
 */
class TwsTester[K, I, O](
    val processor: StatefulProcessor[K, I, O],
    val initialState: List[(K, Any)] = List(),
    val timeMode: TimeMode = TimeMode.None,
    val realTimeMode: Boolean = false) {
  val clock: Clock = new Clock {
    override def instant(): Instant = Instant.ofEpochMilli(currentProcessingTimeMs)
    override def getZone: ZoneId = ZoneId.systemDefault()
    override def withZone(zone: ZoneId): Clock = this
  }
      
  private val handle = new InMemoryStatefulProcessorHandle(timeMode, clock)

  require(timeMode != TimeMode.EventTime, "EventTime is not supported.")

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
   * Processes input rows for a single key through the stateful processor.
   *
   * @param key the grouping key
   * @param values input rows to process
   * @return all output rows produced by the processor
   */
  def test(key: K, values: List[I]): List[O] = {
    ImplicitGroupingKeyTracker.setImplicitKey(key)
    val timerValues = getTimerValues()
    if (realTimeMode) {
      values.flatMap(value => processor.handleInputRows(key, Iterator.single(value), timerValues)).toList
    } else {
      processor.handleInputRows(key, values.iterator, timerValues).toList
    }    
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

  /** Deletes state for given key. */
  def deleteState(stateName: String, key: K): Unit = {
    ImplicitGroupingKeyTracker.setImplicitKey(key)
    handle.deleteState(stateName)
  }

  // Logic for dealing with timers.
  private var currentProcessingTimeMs: Long = 0

  private def handleExpiredTimers(): List[O] = {
    if (timeMode == TimeMode.None) {
      return List()
    }
    val timerValues = getTimerValues()

    var ans: List[O] = List()
    for (key <- handle.timers.getAllKeysWithTimers[K]()) {
      ImplicitGroupingKeyTracker.setImplicitKey(key)
      val expiredTimers: List[Long] = handle.listTimers().filter(_ <= currentProcessingTimeMs).toList
      for (timerExpiryTimeMs <- expiredTimers) {
        val expiredTimerInfo = new ExpiredTimerInfoImpl(Some(timerExpiryTimeMs))
        ans = ans ++ processor.handleExpiredTimer(key, timerValues, expiredTimerInfo).toList
        handle.deleteTimer(timerExpiryTimeMs)
      }
    }
    ans
  }

  /**
   * Advances the simulated processing time and fires all expired timers.
   *
   * Call this after `test()` to simulate time passage and trigger any timers registered
   * with `registerTimer()`. Timers with expiry time <= current processing time will fire,
   * invoking `handleExpiredTimer` for each. This mirrors Spark's behavior where timers
   * are processed after input data within a microbatch.
   *
   * @param durationMs the amount of time to advance in milliseconds
   * @return output rows emitted by `handleExpiredTimer` for all fired timers
   */
  def advanceProcessingTime(durationMs: Long) : List[O] = {
    require(timeMode != TimeMode.None, "Timers are not supported with TimeMode.None.")
    currentProcessingTimeMs += durationMs
    return handleExpiredTimers()
  }

  private def getTimerValues(): TimerValues = {
    new TimerValuesImpl(if (timeMode != TimeMode.None) Some(currentProcessingTimeMs)  else None, None)
  } 
}
