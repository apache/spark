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

import org.apache.spark.util.ManualClock

import org.apache.spark.sql.execution.streaming.operators.stateful.transformwithstate.statefulprocessor.ImplicitGroupingKeyTracker
import org.apache.spark.sql.execution.streaming.operators.stateful.transformwithstate.testing.InMemoryStatefulProcessorHandle
import org.apache.spark.sql.execution.streaming.operators.stateful.transformwithstate.timers.{ExpiredTimerInfoImpl, TimerValuesImpl}

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
 *  - Direct state manipulation via `updateValueState`, `updateListState`, `updateMapState`.
 *  - Direct state inspection via `peekValueState`, `peekListState`, `peekMapState`.
 *  - Timers in ProcessingTime mode (use `advanceProcessingTime` to fire timers).
 *  - Timers in EventTime mode (use `advanceWatermark` to manually advance the watermark
 *    and fire expired timers).
 *  - TTL for ValueState, ListState, and MapState (use ProcessingTime mode and
 *    `advanceProcessingTime` to test expiry).
 *
 * '''Not Supported:'''
 *  - '''Automatic watermark propagation''': In production Spark streaming, the watermark is
 *    computed from event times and propagated at the end of each microbatch. TwsTester does
 *    not simulate this behavior because it processes keys individually rather than in batches.
 *    To test watermark-dependent logic, use `advanceWatermark()` to manually set the watermark
 *    to the desired value before calling `test()`.
 *  - '''Late event filtering''': Since TwsTester does not track event times or automatically
 *    advance the watermark, it also does not filter late events. All input rows passed to
 *    `test()` will be processed regardless of their event time.
 *
 * '''Use Cases:'''
 *  - '''Primary''': Unit testing business logic in `handleInputRows` implementations.
 *  - '''Not recommended''': End-to-end testing or performance testing - use actual Spark
 *    streaming queries for those scenarios.
 *
 * @param processor the StatefulProcessor to test.
 * @param initialState initial state for each key as a list of (key, state) tuples.
 * @param timeMode time mode (None, ProcessingTime or EventTime).
 * @param outputMode output mode (Append, Update, or Complete).
 * @param isRealTimeMode whether input rows should be processed one-by-one (separate call to
 *     handleInputRows) for each input row.
 * @tparam K the type of grouping key.
 * @tparam I the type of input rows.
 * @tparam O the type of output rows.
 * @since 4.2.0
 */
class TwsTester[K, I, O](
    val processor: StatefulProcessor[K, I, O],
    val initialState: List[(K, Any)] = List(),
    val timeMode: TimeMode = TimeMode.None,
    val outputMode: OutputMode = OutputMode.Append,
    val isRealTimeMode: Boolean = false) {
  
  private val processingTimeClock = new ManualClock(0L)
  private val handle = new InMemoryStatefulProcessorHandle(timeMode, processingTimeClock)

  processor.setHandle(handle)
  processor.init(outputMode, timeMode)
  processor match {
    case p: StatefulProcessorWithInitialState[K @unchecked, I @unchecked, O @unchecked, s] =>
      handleInitialState[s]()
    case _ =>
      require(
        initialState.isEmpty,
        "Initial state is provided, but the stateful processor doesn't support initial state."
      )
  }

  private def handleInitialState[S](): Unit = {
    val p = processor.asInstanceOf[StatefulProcessorWithInitialState[K, I, O, S]]
    initialState.foreach { case (key, state) =>
      ImplicitGroupingKeyTracker.setImplicitKey(key)
      p.handleInitialState(key, state.asInstanceOf[S], getTimerValues())
    }
  }

  /**
   * Processes input rows for a single key through the stateful processor.
   *
   * All input rows are passed directly to `handleInputRows` without filtering. In EventTime
   * mode, the current watermark value is available to the processor via `TimerValues`, but
   * the watermark is not automatically advanced based on event times. Use `advanceWatermark()`
   * to manually advance the watermark if needed.
   *
   * @param key the grouping key
   * @param values input rows to process
   * @return all output rows produced by the processor
   */
  def test(key: K, values: List[I]): List[O] = {
    if (isRealTimeMode) {
      values.flatMap(value => testInternal(key, List(value))).toList
    } else {
      testInternal(key, values)
    }
  }

  private def testInternal(key: K, values: List[I]): List[O] = {
    ImplicitGroupingKeyTracker.setImplicitKey(key)
    val timerValues = getTimerValues()
    processor.handleInputRows(key, values.iterator, timerValues).toList
  }

  /** Sets the value state for a given key. */
  def updateValueState[T](stateName: String, key: K, value: T): Unit = {
    ImplicitGroupingKeyTracker.setImplicitKey(key)
    handle.updateValueState[T](stateName, value)
  }

  /** Retrieves the value state for a given key. */
  def peekValueState[T](stateName: String, key: K): Option[T] = {
    ImplicitGroupingKeyTracker.setImplicitKey(key)
    handle.peekValueState[T](stateName)
  }

  /** Sets the list state for a given key. */
  def updateListState[T](stateName: String, key: K, value: List[T])(implicit ct: ClassTag[T]): Unit = {
    ImplicitGroupingKeyTracker.setImplicitKey(key)
    handle.updateListState[T](stateName, value)
  }

  /** Retrieves the list state for a given key. */
  def peekListState[T](stateName: String, key: K): List[T] = {
    ImplicitGroupingKeyTracker.setImplicitKey(key)
    handle.peekListState[T](stateName)
  }

  /** Sets the map state for a given key. */
  def updateMapState[MK, MV](stateName: String, key: K, value: Map[MK, MV]): Unit = {
    ImplicitGroupingKeyTracker.setImplicitKey(key)
    handle.updateMapState[MK, MV](stateName, value)
  }

  /** Retrieves the map state for a given key. */
  def peekMapState[MK, MV](stateName: String, key: K): Map[MK, MV] = {
    ImplicitGroupingKeyTracker.setImplicitKey(key)
    handle.peekMapState[MK, MV](stateName)
  }

  /** Deletes state for a given key. */
  def deleteState(stateName: String, key: K): Unit = {
    ImplicitGroupingKeyTracker.setImplicitKey(key)
    handle.deleteState(stateName)
  }

  // Logic for dealing with timers.
  private var currentWatermarkMs: Long = 0L

  private def handleExpiredTimers(): List[O] = {
    if (timeMode == TimeMode.None) {
      return List()
    }
    val timerValues = getTimerValues()
    val expiryThreshold = if (timeMode == TimeMode.ProcessingTime()) {
      processingTimeClock.getTimeMillis()
    } else if (timeMode == TimeMode.EventTime()) {
      currentWatermarkMs
    } else {
      0L
    }

    var ans: List[O] = List()
    for ((timerExpiryTimeMs, key) <- handle.timers.listExpiredTimers[K](expiryThreshold)) {
      ImplicitGroupingKeyTracker.setImplicitKey(key)
      val expiredTimerInfo = new ExpiredTimerInfoImpl(Some(timerExpiryTimeMs))
      ans = ans ++ processor.handleExpiredTimer(key, timerValues, expiredTimerInfo).toList
      handle.deleteTimer(timerExpiryTimeMs)
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
  def advanceProcessingTime(durationMs: Long): List[O] = {
    require(
      timeMode == TimeMode.ProcessingTime(),
      "advanceProcessingTime is only supported with TimeMode.ProcessingTime."
    )
    processingTimeClock.advance(durationMs)
    handleExpiredTimers()
  }

  /**
   * Advances the watermark and fires all expired event-time timers.
   *
   * Use this in EventTime mode to manually advance the watermark. This is the only way to
   * advance the watermark in TwsTester, as automatic watermark propagation based on event
   * times is not supported. Timers with expiry time <= new watermark will fire.
   *
   * @param durationMs the amount of time to advance the watermark in milliseconds
   * @return output rows emitted by `handleExpiredTimer` for all fired timers
   */
  def advanceWatermark(durationMs: Long): List[O] = {
    require(
      timeMode == TimeMode.EventTime(),
      "advanceWatermark is only supported with TimeMode.EventTime."
    )
    currentWatermarkMs += durationMs
    handleExpiredTimers()
  }

  private def getTimerValues(): TimerValues = {
    val processingTimeOpt = if (timeMode != TimeMode.None) Some(processingTimeClock.getTimeMillis()) else None
    val watermarkOpt = if (timeMode == TimeMode.EventTime()) Some(currentWatermarkMs) else None
    new TimerValuesImpl(processingTimeOpt, watermarkOpt)
  }
}
