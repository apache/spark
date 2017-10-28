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

import java.sql.Date

import org.apache.commons.lang3.StringUtils

import org.apache.spark.sql.catalyst.plans.logical.{EventTimeTimeout, ProcessingTimeTimeout}
import org.apache.spark.sql.execution.streaming.GroupStateImpl._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}
import org.apache.spark.unsafe.types.CalendarInterval


/**
 * Internal implementation of the [[GroupState]] interface. Methods are not thread-safe.
 *
 * @param optionalValue Optional value of the state
 * @param batchProcessingTimeMs Processing time of current batch, used to calculate timestamp
 *                              for processing time timeouts
 * @param timeoutConf     Type of timeout configured. Based on this, different operations will
 *                        be supported.
 * @param hasTimedOut     Whether the key for which this state wrapped is being created is
 *                        getting timed out or not.
 */
private[sql] class GroupStateImpl[S] private(
    optionalValue: Option[S],
    batchProcessingTimeMs: Long,
    eventTimeWatermarkMs: Long,
    timeoutConf: GroupStateTimeout,
    override val hasTimedOut: Boolean,
    watermarkPresent: Boolean) extends GroupState[S] {

  private var value: S = optionalValue.getOrElse(null.asInstanceOf[S])
  private var defined: Boolean = optionalValue.isDefined
  private var updated: Boolean = false // whether value has been updated (but not removed)
  private var removed: Boolean = false // whether value has been removed
  private var timeoutTimestamp: Long = NO_TIMESTAMP

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
  }

  override def setTimeoutDuration(durationMs: Long): Unit = {
    if (timeoutConf != ProcessingTimeTimeout) {
      throw new UnsupportedOperationException(
        "Cannot set timeout duration without enabling processing time timeout in " +
          "[map|flatMap]GroupsWithState")
    }
    if (durationMs <= 0) {
      throw new IllegalArgumentException("Timeout duration must be positive")
    }
    timeoutTimestamp = durationMs + batchProcessingTimeMs
  }

  override def setTimeoutDuration(duration: String): Unit = {
    setTimeoutDuration(parseDuration(duration))
  }

  override def setTimeoutTimestamp(timestampMs: Long): Unit = {
    checkTimeoutTimestampAllowed()
    if (timestampMs <= 0) {
      throw new IllegalArgumentException("Timeout timestamp must be positive")
    }
    if (eventTimeWatermarkMs != NO_TIMESTAMP && timestampMs < eventTimeWatermarkMs) {
      throw new IllegalArgumentException(
        s"Timeout timestamp ($timestampMs) cannot be earlier than the " +
          s"current watermark ($eventTimeWatermarkMs)")
    }
    timeoutTimestamp = timestampMs
  }

  override def setTimeoutTimestamp(timestampMs: Long, additionalDuration: String): Unit = {
    checkTimeoutTimestampAllowed()
    setTimeoutTimestamp(parseDuration(additionalDuration) + timestampMs)
  }

  override def setTimeoutTimestamp(timestamp: Date): Unit = {
    checkTimeoutTimestampAllowed()
    setTimeoutTimestamp(timestamp.getTime)
  }

  override def setTimeoutTimestamp(timestamp: Date, additionalDuration: String): Unit = {
    checkTimeoutTimestampAllowed()
    setTimeoutTimestamp(timestamp.getTime + parseDuration(additionalDuration))
  }

  override def getCurrentWatermarkMs(): Long = {
    if (!watermarkPresent) {
      throw new UnsupportedOperationException(
        "Cannot get event time watermark timestamp without setting watermark before " +
          "[map|flatMap]GroupsWithState")
    }
    eventTimeWatermarkMs
  }

  override def getCurrentProcessingTimeMs(): Long = {
    batchProcessingTimeMs
  }

  override def toString: String = {
    s"GroupState(${getOption.map(_.toString).getOrElse("<undefined>")})"
  }

  // ========= Internal API =========

  /** Whether the state has been marked for removing */
  def hasRemoved: Boolean = removed

  /** Whether the state has been updated */
  def hasUpdated: Boolean = updated

  /** Return timeout timestamp or `TIMEOUT_TIMESTAMP_NOT_SET` if not set */
  def getTimeoutTimestamp: Long = timeoutTimestamp

  private def parseDuration(duration: String): Long = {
    if (StringUtils.isBlank(duration)) {
      throw new IllegalArgumentException(
        "Provided duration is null or blank.")
    }
    val intervalString = if (duration.startsWith("interval")) {
      duration
    } else {
      "interval " + duration
    }
    val cal = CalendarInterval.fromString(intervalString)
    if (cal == null) {
      throw new IllegalArgumentException(
        s"Provided duration ($duration) is not valid.")
    }
    if (cal.milliseconds < 0 || cal.months < 0) {
      throw new IllegalArgumentException(s"Provided duration ($duration) is not positive")
    }

    val millisPerMonth = CalendarInterval.MICROS_PER_DAY / 1000 * 31
    cal.milliseconds + cal.months * millisPerMonth
  }

  private def checkTimeoutTimestampAllowed(): Unit = {
    if (timeoutConf != EventTimeTimeout) {
      throw new UnsupportedOperationException(
        "Cannot set timeout timestamp without enabling event time timeout in " +
          "[map|flatMapGroupsWithState")
    }
  }
}


private[sql] object GroupStateImpl {
  // Value used represent the lack of valid timestamp as a long
  val NO_TIMESTAMP = -1L

  def createForStreaming[S](
      optionalValue: Option[S],
      batchProcessingTimeMs: Long,
      eventTimeWatermarkMs: Long,
      timeoutConf: GroupStateTimeout,
      hasTimedOut: Boolean,
      watermarkPresent: Boolean): GroupStateImpl[S] = {
    new GroupStateImpl[S](
      optionalValue, batchProcessingTimeMs, eventTimeWatermarkMs,
      timeoutConf, hasTimedOut, watermarkPresent)
  }

  def createForBatch(
      timeoutConf: GroupStateTimeout,
      watermarkPresent: Boolean): GroupStateImpl[Any] = {
    new GroupStateImpl[Any](
      optionalValue = None,
      batchProcessingTimeMs = System.currentTimeMillis,
      eventTimeWatermarkMs = NO_TIMESTAMP,
      timeoutConf,
      hasTimedOut = false,
      watermarkPresent)
  }
}
