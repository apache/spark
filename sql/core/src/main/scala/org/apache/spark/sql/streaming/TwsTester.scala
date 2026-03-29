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
import org.apache.spark.util.ManualClock

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
 *  - Timers in ProcessingTime mode (use `setProcessingTime` to fire timers).
 *  - Timers in EventTime mode (use `setWatermark` to manually set the watermark
 *    and fire expired timers).
 *  - Late event filtering in EventTime mode.
 *
 * '''Not Supported:'''
 *  - '''TTL'''. States persist indefinitely, even if TTLConfig is set.
 *  - '''Automatic watermark propagation''': In production Spark streaming, the watermark is
 *    computed from event times and propagated at the end of each microbatch. TwsTester does
 *    not simulate this behavior because it processes keys individually rather than in batches.
 *    To test watermark-dependent logic, use `setWatermark()` to manually set the watermark
 *    to the desired value before calling `test()`.
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
 * @param eventTimeExtractor function to extract event time (in milliseconds) from input rows.
 *                           Required when using TimeMode.EventTime. Used for late event filtering.
 * @tparam K the type of grouping key.
 * @tparam I the type of input rows.
 * @tparam O the type of output rows.
 * @since 4.2.0
 */
class TwsTester[K, I, O](
    processor: StatefulProcessor[K, I, O],
    initialState: List[(K, Any)] = List(),
    timeMode: TimeMode = TimeMode.None,
    outputMode: OutputMode = OutputMode.Append,
    eventTimeExtractor: Option[I => Long] = None) {

  if (timeMode == TimeMode.EventTime()) {
    require(
      eventTimeExtractor.isDefined,
      "eventTimeExtractor is required when using TimeMode.EventTime."
    )
  }

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
    initialState.foreach {
      case (key, state) =>
        ImplicitGroupingKeyTracker.setImplicitKey(key)
        p.handleInitialState(key, state.asInstanceOf[S], getTimerValues())
    }
  }

  /**
   * Processes input rows for a single key through the stateful processor.
   *
   * In EventTime mode, late events (where event time &lt;= current watermark) are filtered out
   * before reaching the processor, matching the behavior of real Spark streaming.
   *
   * The watermark is not automatically advanced based on event times. Use `setWatermark()`
   * to manually set the watermark before calling `test()`.
   *
   * @param key the grouping key
   * @param values input rows to process
   * @return all output rows produced by the processor
   */
  def test(key: K, values: List[I]): List[O] = {
    ImplicitGroupingKeyTracker.setImplicitKey(key)
    val timerValues = getTimerValues()
    val filteredValues = filterLateEvents(values)
    processor.handleInputRows(key, filteredValues.iterator, timerValues).toList
  }

  /** Filters out late events based on watermark and eventTimeExtractor. */
  private def filterLateEvents(values: List[I]): List[I] = {
    if (timeMode != TimeMode.EventTime()) {
      values
    } else {
      values.filter(eventTimeExtractor.get(_) > currentWatermarkMs)
    }
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
  def updateListState[T](stateName: String, key: K, value: List[T])(
      implicit ct: ClassTag[T]): Unit = {
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
   * Sets the simulated processing time and fires all expired timers.
   *
   * Call this after `test()` to simulate time passage and trigger any timers registered
   * with `registerTimer()`. Timers with expiry time &lt;= current processing time will fire,
   * invoking `handleExpiredTimer` for each. This mirrors Spark's behavior where timers
   * are processed after input data within a microbatch.
   *
   * @param currentTimeMs the processing time to set in milliseconds
   * @return output rows emitted by `handleExpiredTimer` for all fired timers
   */
  def setProcessingTime(currentTimeMs: Long): List[O] = {
    require(
      timeMode == TimeMode.ProcessingTime(),
      "setProcessingTime is only supported with TimeMode.ProcessingTime."
    )
    require(
      currentTimeMs > processingTimeClock.getTimeMillis(),
      "Processing time must move forward."
    )
    processingTimeClock.setTime(currentTimeMs)
    handleExpiredTimers()
  }

  /**
   * Sets the watermark and fires all expired event-time timers.
   *
   * Use this in EventTime mode to manually set the watermark. This is the only way to
   * set the watermark in TwsTester, as automatic watermark propagation based on event
   * times is not supported. Timers with expiry time &lt;= new watermark will fire.
   *
   * @param currentWatermarkMs the watermark to set in milliseconds
   * @return output rows emitted by `handleExpiredTimer` for all fired timers
   */
  def setWatermark(currentWatermarkMs: Long): List[O] = {
    require(
      timeMode == TimeMode.EventTime(),
      "setWatermark is only supported with TimeMode.EventTime."
    )
    require(currentWatermarkMs > this.currentWatermarkMs, "Watermark must move forward.")
    this.currentWatermarkMs = currentWatermarkMs
    handle.setWatermark(currentWatermarkMs)
    handleExpiredTimers()
  }

  private def getTimerValues(): TimerValues = {
    val processingTimeOpt =
      if (timeMode != TimeMode.None) Some(processingTimeClock.getTimeMillis()) else None
    val watermarkOpt = if (timeMode == TimeMode.EventTime()) Some(currentWatermarkMs) else None
    new TimerValuesImpl(processingTimeOpt, watermarkOpt)
  }
}
