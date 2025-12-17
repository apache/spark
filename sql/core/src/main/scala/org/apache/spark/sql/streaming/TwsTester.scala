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

import java.sql.Timestamp
import java.time.{Clock, Instant, ZoneId}

import scala.reflect.ClassTag

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
 *  - Direct state manipulation via `setValueState`, `setListState`, `setMapState`.
 *  - Direct state inspection via `peekValueState`, `peekListState`, `peekMapState`.
 *  - Timers in ProcessingTime mode (use `advanceProcessingTime` to fire timers).
 *  - Timers in EventTime mode (use `eventTimeExtractor` and `watermarkDelayMs` to configure;
 *    watermark advances automatically based on event times, or use `advanceWatermark` manually).
 *  - Late event filtering in EventTime mode (events older than the current watermark are dropped).
 *  - TTL for ValueState, ListState, and MapState (use ProcessingTime mode and
 *    `advanceProcessingTime` to test expiry).
 *
 * '''Testing EventTime Mode:'''
 *  To test with EventTime, provide `eventTimeExtractor` (a function extracting the event
 *  timestamp from each input row) and `watermarkDelayMs` (the watermark delay in milliseconds).
 *  The watermark is computed as `max(event_time_seen) - watermarkDelayMs` and is updated
 *  automatically after each `test()` call. Late events (with event time older than the current
 *  watermark) are filtered out before processing, matching production behavior. Timers with
 *  expiry time <= watermark will fire. You can also manually advance the watermark using
 *  `advanceWatermark()`.
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
 * @param realTimeMode whether input rows should be processed one-by-one (separate call to
 *     handleInputRows) for each input row.
 * @param eventTimeExtractor function to extract event time from input rows. Required if and
 *     only if timeMode is EventTime.
 * @param watermarkDelayMs watermark delay in milliseconds. The watermark is computed as
 *     `max(event_time) - watermarkDelayMs`. Required if and only if timeMode is EventTime.
 * @tparam K the type of grouping key.
 * @tparam I the type of input rows.
 * @tparam O the type of output rows.
 * @since 4.0.2
 */
class TwsTester[K, I, O](
    val processor: StatefulProcessor[K, I, O],
    val initialState: List[(K, Any)] = List(),
    val timeMode: TimeMode = TimeMode.None,
    val outputMode: OutputMode = OutputMode.Append,
    val realTimeMode: Boolean = false,
    val eventTimeExtractor: Option[I => Timestamp] = None,
    val watermarkDelayMs: Long = 0L) {
  val clock: Clock = new Clock {
    override def instant(): Instant = Instant.ofEpochMilli(currentProcessingTimeMs)
    override def getZone: ZoneId = ZoneId.systemDefault()
    override def withZone(zone: ZoneId): Clock = this
  }

  private val handle = new InMemoryStatefulProcessorHandle(timeMode, clock)

  if (timeMode == TimeMode.EventTime) {
    require(
      eventTimeExtractor.isDefined,
      "eventTimeExtractor must be provided when timeMode is EventTime."
    )
  }

  processor.setHandle(handle)
  processor.init(outputMode, timeMode)
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
        p.handleInitialState(key, state.asInstanceOf[S], getTimerValues())
    }
  }

  /**
   * Processes input rows for a single key through the stateful processor.
   *
   * In EventTime mode, late events (with event time older than the current watermark) are
   * filtered out before processing. After processing, the watermark is updated based on
   * the maximum event time seen (using `eventTimeExtractor`) minus `watermarkDelayMs`,
   * and any expired timers are fired.
   *
   * @param key the grouping key
   * @param values input rows to process
   * @return all output rows produced by the processor (including any from expired timers
   *         in EventTime mode)
   */
  def test(key: K, values: List[I]): List[O] = {
    if (realTimeMode) {
      values.flatMap(value => testInternal(key, List(value))).toList
    } else {
      testInternal(key, values)
    }
  }

  private def testInternal(key: K, values: List[I]): List[O] = {
    ImplicitGroupingKeyTracker.setImplicitKey(key)
    val timerValues = getTimerValues()
    val filteredValues = filterLateEvents(values)
    var result: List[O] =
      processor.handleInputRows(key, filteredValues.iterator, timerValues).toList
    if (timeMode == TimeMode.EventTime()) {
      updateWatermarkFromEventTime(values)
      result ++= handleExpiredTimers()
    }
    result
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

  /** Deletes state for a given key. */
  def deleteState(stateName: String, key: K): Unit = {
    ImplicitGroupingKeyTracker.setImplicitKey(key)
    handle.deleteState(stateName)
  }

  // Logic for dealing with timers.
  private var currentProcessingTimeMs: Long = 0
  private var currentWatermarkMs: Long = 0

  private def handleExpiredTimers(): List[O] = {
    if (timeMode == TimeMode.None) {
      return List()
    }
    val timerValues = getTimerValues()
    val expiryThreshold = if (timeMode == TimeMode.ProcessingTime()) {
      currentProcessingTimeMs
    } else if (timeMode == TimeMode.EventTime()) {
      currentWatermarkMs
    } else {
      0L
    }

    var ans: List[O] = List()
    for (key <- handle.timers.getAllKeysWithTimers[K]()) {
      ImplicitGroupingKeyTracker.setImplicitKey(key)
      val expiredTimers: List[Long] = handle.listTimers().filter(_ <= expiryThreshold).toList
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
  def advanceProcessingTime(durationMs: Long): List[O] = {
    require(
      timeMode == TimeMode.ProcessingTime(),
      "advanceProcessingTime is only supported with TimeMode.ProcessingTime."
    )
    currentProcessingTimeMs += durationMs
    handleExpiredTimers()
  }

  /**
   * Advances the watermark and fires all expired event-time timers.
   *
   * Use this in EventTime mode to manually advance the watermark beyond what the
   * event times in the data would produce. Timers with expiry time <= new watermark will fire.
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

  private def updateWatermarkFromEventTime(values: List[I]): Unit = {
    require(timeMode == TimeMode.EventTime())
    require(eventTimeExtractor.isDefined)
    val extractor = eventTimeExtractor.get
    values.foreach { value =>
      val eventTimeMs = extractor(value).getTime
      currentWatermarkMs = Math.max(currentWatermarkMs, eventTimeMs - watermarkDelayMs)
    }
  }

  private def getTimerValues(): TimerValues = {
    val processingTimeOpt = if (timeMode != TimeMode.None) Some(currentProcessingTimeMs) else None
    val watermarkOpt = if (timeMode == TimeMode.EventTime()) Some(currentWatermarkMs) else None
    new TimerValuesImpl(processingTimeOpt, watermarkOpt)
  }

  /** Filters out late events based on the current watermark, in EventTime mode. */
  private def filterLateEvents(values: List[I]): List[I] = {
    if (timeMode != TimeMode.EventTime()) {
      return values
    }
    val extractor = eventTimeExtractor.get
    values.filter(extractor(_).getTime >= currentWatermarkMs)
  }
}
