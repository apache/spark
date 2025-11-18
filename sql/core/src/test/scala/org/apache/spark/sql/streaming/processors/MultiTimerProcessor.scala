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
import org.apache.spark.sql.streaming.{ExpiredTimerInfo, MapState, OutputMode, StatefulProcessor, TimeMode, TimerValues, TTLConfig}

/**
 * Multi-timer processor that registers multiple timers with different delays.
 *
 * Input: (userId, command) as (String, String)
 * Output: (userId, timerType, timestamp) as (String, String, Long)
 *
 * On first input, registers three timers: SHORT (1s), MEDIUM (3s), LONG (5s)
 * When each timer expires, emits the timer type and timestamp
 */
class MultiTimerProcessor
    extends StatefulProcessor[String, (String, String), (String, String, Long)] {

  @transient private var timerTypeMapState: MapState[Long, String] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    timerTypeMapState = getHandle.getMapState[Long, String](
      "timerTypeMap",
      Encoders.scalaLong,
      Encoders.STRING,
      TTLConfig.NONE
    )
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[(String, String)],
      timerValues: TimerValues
  ): Iterator[(String, String, Long)] = {
    inputRows.size // consume iterator

    val baseTime = timerValues.getCurrentProcessingTimeInMs()
    val shortTimer = baseTime + 1000L
    val mediumTimer = baseTime + 3000L
    val longTimer = baseTime + 5000L

    getHandle.registerTimer(shortTimer) // SHORT - 1 second
    getHandle.registerTimer(mediumTimer) // MEDIUM - 3 seconds
    getHandle.registerTimer(longTimer) // LONG - 5 seconds

    timerTypeMapState.updateValue(shortTimer, "SHORT")
    timerTypeMapState.updateValue(mediumTimer, "MEDIUM")
    timerTypeMapState.updateValue(longTimer, "LONG")

    Iterator.empty
  }

  override def handleExpiredTimer(
      key: String,
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo
  ): Iterator[(String, String, Long)] = {
    val expiryTime = expiredTimerInfo.getExpiryTimeInMs()
    val timerType = timerTypeMapState.getValue(expiryTime)

    // Clean up the timer type from the map
    timerTypeMapState.removeKey(expiryTime)

    Iterator.single((key, timerType, expiryTime))
  }
}

