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
package org.apache.spark.sql.execution.streaming.operators.stateful.transformwithstate.testing

import java.time.Clock

import scala.reflect.ClassTag

import org.apache.spark.sql.execution.streaming.operators.stateful.transformwithstate.statefulprocessor.ImplicitGroupingKeyTracker
import org.apache.spark.sql.execution.streaming.operators.stateful.transformwithstate.timers.ExpiredTimerInfoImpl
import org.apache.spark.sql.execution.streaming.operators.stateful.transformwithstate.timers.TimerValuesImpl
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.streaming.StatefulProcessor
import org.apache.spark.sql.streaming.TimeMode

/**
 * Testing utility for transformWithState stateful processors. Provides in-memory state management
 * and simplified input processing for unit testing StatefulProcessor implementations.
 *
 * @param processor the StatefulProcessor to test
 * @param clock the clock to use for time-based operations, defaults to system UTC
 * @param timeMode time mode that will be passed to transformWithState (defaults to TimeMode.None)
 * @param outputMode output mode that will be passed to transformWithState (defaults to
 *                   OutputMode.Append).
 * @tparam K the type of grouping key
 * @tparam I the type of input rows
 * @tparam O the type of output rows
 */
class TwsTester[K, I, O](
    val processor: StatefulProcessor[K, I, O],
    val clock: Clock = Clock.systemUTC(),
    val timeMode: TimeMode = TimeMode.None,
    val outputMode: OutputMode = OutputMode.Append) {
  private val handle = new InMemoryStatefulProcessorHandleImpl(timeMode, null, clock)

  processor.setHandle(handle)
  processor.init(outputMode, timeMode)

  /**
   * Processes input rows through the stateful processor, grouped by key.
   *
   * @param input list of (key, input row) tuples to process
   * @return all output rows produced by the processor
   */
  def test(input: List[(K, I)]): List[O] = {
    val currentTimeMs = clock.instant().toEpochMilli()
    // TODO: support TimeMode.EventTime.
    val timerValues = new TimerValuesImpl(Some(currentTimeMs), None)
    var ans: List[O] = handleExpiredTimers(currentTimeMs)

    for ((key, v) <- input.groupBy(_._1)) {
      ImplicitGroupingKeyTracker.setImplicitKey(key)
      ans = ans ++ processor.handleInputRows(key, v.map(_._2).iterator, timerValues).toList
      ImplicitGroupingKeyTracker.removeImplicitKey()
    }
    ans
  }

  private def handleExpiredTimers(currentTimeMs: Long): List[O] = {
    var ans: List[O] = List()
    for (key <- handle.getAllKeysWithTimers[K]()) {
      ImplicitGroupingKeyTracker.setImplicitKey(key)
      var timersToRemove: List[Long] = List()
      for (expiryTimestampMs <- handle.listTimers()) {
        if (expiryTimestampMs <= currentTimeMs) {
          val timerValues = new TimerValuesImpl(Some(currentTimeMs), None)
          val expiredTimerInfo = new ExpiredTimerInfoImpl(Some(expiryTimestampMs))
          ans = ans ++ processor.handleExpiredTimer(key, timerValues, expiredTimerInfo).toList
          timersToRemove = timersToRemove ++ List(expiryTimestampMs)
        }
      }
      for (timerExpiryTimeMs <- timersToRemove) {
        handle.deleteTimer(timerExpiryTimeMs)
      }
      ImplicitGroupingKeyTracker.removeImplicitKey()
    }
    ans
  }

  /**
   * Convenience method to process a single input row for a given key.
   *
   * @param key the grouping key
   * @param inputRow the input row to process
   * @return all output rows produced by the processor
   */
  def testOneRow(key: K, inputRow: I): List[O] = test(List((key, inputRow)))

  /**
   * Tests how value state is changed after processing one row.
   *
   * @param key the grouping key
   * @param inputRow the input row to process
   * @param stateName the name os value state
   * @param stateIn the old value of the value state
   * @tparam S the type of value state
   * @return output rows produced by the processor and new value of the value state
   */
  def testOneRowWithValueState[S](
      key: K,
      inputRow: I,
      stateName: String,
      stateIn: S): (List[O], S) = {
    setValueState[S](stateName, key, stateIn)
    val outputRows = testOneRow(key, inputRow)
    (outputRows, peekValueState[S](stateName, key).get)
  }

  /**
   * Sets the value state for a given key.
   *
   * @param stateName the name of the value state variable
   * @param key the grouping key
   * @param value the value to set
   * @tparam T the type of the state value
   */
  def setValueState[T](stateName: String, key: K, value: T): Unit = {
    ImplicitGroupingKeyTracker.setImplicitKey(key)
    handle.setValueState[T](stateName, value)
    ImplicitGroupingKeyTracker.removeImplicitKey()
  }

  /**
   * Retrieves the value state for a given key without modifying it.
   *
   * @param stateName the name of the value state variable
   * @param key the grouping key
   * @tparam T the type of the state value
   * @return Some(value) if state exists for the key, None otherwise
   */
  def peekValueState[T](stateName: String, key: K): Option[T] = {
    ImplicitGroupingKeyTracker.setImplicitKey(key)
    val result: Option[T] = handle.peekValueState[T](stateName)
    ImplicitGroupingKeyTracker.removeImplicitKey()
    return result
  }

  /**
   * Sets the list state for a given key.
   *
   * @param stateName the name of the list state variable
   * @param key the grouping key
   * @param value the list of values to set
   * @param ct implicit class tag for type T
   * @tparam T the type of elements in the list state
   */
  def setListState[T](stateName: String, key: K, value: List[T])(implicit ct: ClassTag[T]): Unit = {
    ImplicitGroupingKeyTracker.setImplicitKey(key)
    handle.setListState[T](stateName, value)
    ImplicitGroupingKeyTracker.removeImplicitKey()
  }

  /**
   * Retrieves the list state for a given key without modifying it.
   *
   * @param stateName the name of the list state variable
   * @param key the grouping key
   * @tparam T the type of elements in the list state
   * @return the list of values, or an empty list if no state exists for the key
   */
  def peekListState[T](stateName: String, key: K): List[T] = {
    ImplicitGroupingKeyTracker.setImplicitKey(key)
    val result: List[T] = handle.peekListState[T](stateName)
    ImplicitGroupingKeyTracker.removeImplicitKey()
    return result
  }

  /**
   * Sets the map state for a given key.
   *
   * @param stateName the name of the map state variable
   * @param key the grouping key
   * @param value the map of key-value pairs to set
   * @tparam MK the type of keys in the map state
   * @tparam MV the type of values in the map state
   */
  def setMapState[MK, MV](stateName: String, key: K, value: Map[MK, MV]): Unit = {
    ImplicitGroupingKeyTracker.setImplicitKey(key)
    handle.setMapState[MK, MV](stateName, value)
    ImplicitGroupingKeyTracker.removeImplicitKey()
  }

  /**
   * Retrieves the map state for a given key without modifying it.
   *
   * @param stateName the name of the map state variable
   * @param key the grouping key
   * @tparam MK the type of keys in the map state
   * @tparam MV the type of values in the map state
   * @return the map of key-value pairs, or an empty map if no state exists for the key
   */
  def peekMapState[MK, MV](stateName: String, key: K): Map[MK, MV] = {
    ImplicitGroupingKeyTracker.setImplicitKey(key)
    val result: Map[MK, MV] = handle.peekMapState[MK, MV](stateName)
    ImplicitGroupingKeyTracker.removeImplicitKey()
    return result
  }
}
