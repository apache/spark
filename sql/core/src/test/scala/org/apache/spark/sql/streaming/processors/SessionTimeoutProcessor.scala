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

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.streaming.{ExpiredTimerInfo, OutputMode, StatefulProcessor, TimeMode, TimerValues, TTLConfig, ValueState}

/**
 * Session timeout processor that demonstrates timer usage.
 *
 * Input: (userId, activityType) as (String, String)
 * Output: (userId, event, count) as (String, String, Long)
 *
 * Behavior:
 * - Tracks activity count per user session
 * - Registers a timeout timer on first activity (5 seconds from current time)
 * - Updates timer on each new activity (resets 5-second countdown)
 * - When timer expires, emits ("userId", "SESSION_TIMEOUT", activityCount) and clears state
 * - Regular activities emit ("userId", "ACTIVITY", currentCount)
 */
class SessionTimeoutProcessor(val timeoutDurationMs: Long = 5000L)
    extends StatefulProcessor[String, (String, String), (String, String, Long)] {

  @transient private var activityCountState: ValueState[Long] = _
  @transient private var timerState: ValueState[Long] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    activityCountState =
      getHandle.getValueState[Long]("activityCount", Encoders.scalaLong, TTLConfig.NONE)
    timerState = getHandle.getValueState[Long]("timer", Encoders.scalaLong, TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[(String, String)],
      timerValues: TimerValues
  ): Iterator[(String, String, Long)] = {
    val currentCount = if (activityCountState.exists()) activityCountState.get() else 0L
    val newCount = currentCount + inputRows.size
    activityCountState.update(newCount)

    // Delete old timer if exists and register new one
    if (timerState.exists()) {
      getHandle.deleteTimer(timerState.get())
    }

    val newTimerMs = timerValues.getCurrentProcessingTimeInMs() + timeoutDurationMs
    getHandle.registerTimer(newTimerMs)
    timerState.update(newTimerMs)

    Iterator.single((key, "ACTIVITY", newCount))
  }

  override def handleExpiredTimer(
      key: String,
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo
  ): Iterator[(String, String, Long)] = {
    val count = if (activityCountState.exists()) activityCountState.get() else 0L

    // Clear session state
    activityCountState.clear()
    timerState.clear()

    Iterator.single((key, "SESSION_TIMEOUT", count))
  }
}

