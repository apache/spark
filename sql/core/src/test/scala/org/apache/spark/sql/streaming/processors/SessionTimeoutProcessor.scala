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
 * Processor that registers a processing time timer on first input and emits a message on expiry.
 */
class SessionTimeoutProcessor
    extends StatefulProcessor[String, String, (String, String)] {

  @transient private var lastSeenState: ValueState[Long] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    lastSeenState = getHandle.getValueState[Long]("lastSeen", Encoders.scalaLong, TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[String],
      timerValues: TimerValues
  ): Iterator[(String, String)] = {
    val currentTime = timerValues.getCurrentProcessingTimeInMs()

    // Clear any existing timer if we have previous state
    if (lastSeenState.exists()) {
      val oldTimerTime = lastSeenState.get() + 10000 // old timeout was 10s after last seen
      getHandle.deleteTimer(oldTimerTime)
    }

    // Update last seen time and register new timer
    lastSeenState.update(currentTime)
    getHandle.registerTimer(currentTime + 10000) // 10 second timeout

    inputRows.map(value => (key, s"received:$value"))
  }

  override def handleExpiredTimer(
      key: String,
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo
  ): Iterator[(String, String)] = {
    lastSeenState.clear()
    Iterator.single((key, "session-expired"))
  }
}

