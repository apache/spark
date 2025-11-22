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
package org.apache.spark.sql.streaming.processors

import java.sql.Timestamp

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.streaming.{ExpiredTimerInfo, OutputMode, StatefulProcessor, TimeMode, TimerValues, TTLConfig, ValueState}

/**
 * Event time window processor that demonstrates event time timer usage.
 *
 * Input: (eventId, eventTime) as (String, Timestamp)
 * Output: (userId, status, count) as (String, String, Long)
 *
 * Behavior:
 * - Tracks event count per user window
 * - On first event, registers a timer for windowDurationMs from the first event time
 * - Accumulates events in the window
 * - When timer expires (based on watermark), emits window summary and starts new window
 */
class EventTimeWindowProcessor(val windowDurationMs: Long = 10000L)
    extends StatefulProcessor[String, (String, Timestamp), (String, String, Long)] {

  @transient private var eventCountState: ValueState[Long] = _
  @transient private var windowEndTimeState: ValueState[Long] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    eventCountState =
      getHandle.getValueState[Long]("eventCount", Encoders.scalaLong, TTLConfig.NONE)
    windowEndTimeState =
      getHandle.getValueState[Long]("windowEndTime", Encoders.scalaLong, TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[(String, Timestamp)],
      timerValues: TimerValues
  ): Iterator[(String, String, Long)] = {
    val events = inputRows.toList
    val currentCount = if (eventCountState.exists()) eventCountState.get() else 0L
    val newCount = currentCount + events.size
    eventCountState.update(newCount)

    // If this is the first event in a window, register timer
    if (!windowEndTimeState.exists()) {
      val firstEventTime = events.head._2.getTime
      val windowEnd = firstEventTime + windowDurationMs
      getHandle.registerTimer(windowEnd)
      windowEndTimeState.update(windowEnd)
      Iterator.single((key, "WINDOW_START", newCount))
    } else {
      Iterator.single((key, "WINDOW_CONTINUE", newCount))
    }
  }

  override def handleExpiredTimer(
      key: String,
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo
  ): Iterator[(String, String, Long)] = {
    val count = if (eventCountState.exists()) eventCountState.get() else 0L

    // Clear window state
    eventCountState.clear()
    windowEndTimeState.clear()

    Iterator.single((key, "WINDOW_END", count))
  }
}

