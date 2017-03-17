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

package org.apache.spark.sql.execution.streaming

import org.apache.commons.lang3.StringUtils

import org.apache.spark.sql.streaming.KeyedState
import org.apache.spark.unsafe.types.CalendarInterval

/**
 * Internal implementation of the [[KeyedState]] interface. Methods are not thread-safe.
 * @param optionalValue Optional value of the state
 * @param batchProcessingTimeMs Processing time of current batch, used to calculate timestamp
 *                              for processing time timeouts
 * @param isTimeoutEnabled Whether timeout is enabled. This will be used to check whether the user
 *                         is allowed to configure timeouts.
 * @param hasTimedOut     Whether the key for which this state wrapped is being created is
 *                        getting timed out or not.
 */
private[sql] class KeyedStateImpl[S](
    optionalValue: Option[S],
    batchProcessingTimeMs: Long,
    isTimeoutEnabled: Boolean,
    override val hasTimedOut: Boolean) extends KeyedState[S] {

  import KeyedStateImpl._

  // Constructor to create dummy state when using mapGroupsWithState in a batch query
  def this(optionalValue: Option[S]) = this(
    optionalValue, -1, isTimeoutEnabled = false, hasTimedOut = false)
  private var value: S = optionalValue.getOrElse(null.asInstanceOf[S])
  private var defined: Boolean = optionalValue.isDefined
  private var updated: Boolean = false // whether value has been updated (but not removed)
  private var removed: Boolean = false // whether value has been removed
  private var timeoutTimestamp: Long = TIMEOUT_TIMESTAMP_NOT_SET

  // ========= Public API =========
  override def exists: Boolean = defined

  override def get: S = {
    if (defined) {
      value
    } else {
      throw new NoSuchElementException("State is either not defined or has already been removed")
    }
  }

  override def getOption: Option[S] = {
    if (defined) {
      Some(value)
    } else {
      None
    }
  }

  override def update(newValue: S): Unit = {
    if (newValue == null) {
      throw new IllegalArgumentException("'null' is not a valid state value")
    }
    value = newValue
    defined = true
    updated = true
    removed = false
  }

  override def remove(): Unit = {
    defined = false
    updated = false
    removed = true
    timeoutTimestamp = TIMEOUT_TIMESTAMP_NOT_SET
  }

  override def setTimeoutDuration(durationMs: Long): Unit = {
    if (!isTimeoutEnabled) {
      throw new UnsupportedOperationException(
        "Cannot set timeout information without enabling timeout in map/flatMapGroupsWithState")
    }
    if (!defined) {
      throw new IllegalStateException(
        "Cannot set timeout information without any state value, " +
          "state has either not been initialized, or has already been removed")
    }

    if (durationMs <= 0) {
      throw new IllegalArgumentException("Timeout duration must be positive")
    }
    if (!removed && batchProcessingTimeMs != NO_BATCH_PROCESSING_TIMESTAMP) {
      timeoutTimestamp = durationMs + batchProcessingTimeMs
    } else {
      // This is being called in a batch query, hence no processing timestamp.
      // Just ignore any attempts to set timeout.
    }
  }

  override def setTimeoutDuration(duration: String): Unit = {
    if (StringUtils.isBlank(duration)) {
      throw new IllegalArgumentException(
        "The window duration, slide duration and start time cannot be null or blank.")
    }
    val intervalString = if (duration.startsWith("interval")) {
      duration
    } else {
      "interval " + duration
    }
    val cal = CalendarInterval.fromString(intervalString)
    if (cal == null) {
      throw new IllegalArgumentException(
        s"The provided duration ($duration) is not valid.")
    }
    if (cal.milliseconds < 0 || cal.months < 0) {
      throw new IllegalArgumentException("Timeout duration must be positive")
    }

    val delayMs = {
      val millisPerMonth = CalendarInterval.MICROS_PER_DAY / 1000 * 31
      cal.milliseconds + cal.months * millisPerMonth
    }
    setTimeoutDuration(delayMs)
  }

  override def toString: String = {
    s"KeyedState(${getOption.map(_.toString).getOrElse("<undefined>")})"
  }

  // ========= Internal API =========

  /** Whether the state has been marked for removing */
  def hasRemoved: Boolean = removed

  /** Whether the state has been updated */
  def hasUpdated: Boolean = updated

  /** Return timeout timestamp or `TIMEOUT_TIMESTAMP_NOT_SET` if not set */
  def getTimeoutTimestamp: Long = timeoutTimestamp
}


private[sql] object KeyedStateImpl {
  // Value used in the state row to represent the lack of any timeout timestamp
  val TIMEOUT_TIMESTAMP_NOT_SET = -1L

  // Value to represent that no batch processing timestamp is passed to KeyedStateImpl. This is
  // used in batch queries where there are no streaming batches and timeouts.
  val NO_BATCH_PROCESSING_TIMESTAMP = -1L
}
