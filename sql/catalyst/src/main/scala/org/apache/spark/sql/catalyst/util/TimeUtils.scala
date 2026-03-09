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

package org.apache.spark.sql.catalyst.util

import java.time.{DateTimeException, LocalTime}
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoField

import org.apache.spark.unsafe.types.UTF8String

/**
 * Helper functions for TIME type operations.
 * TIME is stored as Long representing microseconds since midnight (00:00:00.000000).
 * Valid range: 0 to 86,399,999,999 (24 hours in microseconds - 1)
 */
object TimeUtils {

  // Constants
  val MICROS_PER_SECOND: Long = 1000000L
  val MICROS_PER_MINUTE: Long = 60 * MICROS_PER_SECOND
  val MICROS_PER_HOUR: Long = 60 * MICROS_PER_MINUTE
  val MICROS_PER_DAY: Long = 24 * MICROS_PER_HOUR

  // Default formatter for TIME: HH:mm:ss.SSSSSS
  private val DEFAULT_TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS")
  private val TIME_FORMATTER_NO_MICROS = DateTimeFormatter.ofPattern("HH:mm:ss")

  /**
   * Converts a string to TIME (microseconds since midnight).
   * Supports formats:
   * - HH:mm:ss.SSSSSS (with microseconds)
   * - HH:mm:ss (without microseconds)
   * - HH:mm (without seconds)
   *
   * @param s the time string
   * @return microseconds since midnight, or None if parsing fails
   */
  def stringToTime(s: UTF8String): Option[Long] = {
    if (s == null) return None

    val str = s.toString.trim
    if (str.isEmpty) return None

    try {
      // Try parsing with different formats
      val localTime = if (str.contains(".")) {
        LocalTime.parse(str, DEFAULT_TIME_FORMATTER)
      } else if (str.count(_ == ':') == 2) {
        LocalTime.parse(str, TIME_FORMATTER_NO_MICROS)
      } else {
        LocalTime.parse(str)
      }

      Some(localTimeToMicros(localTime))
    } catch {
      case _: DateTimeException => None
    }
  }

  /**
   * Converts TIME (microseconds since midnight) to string.
   *
   * @param micros microseconds since midnight
   * @return formatted time string HH:mm:ss.SSSSSS
   */
  def timeToString(micros: Long): UTF8String = {
    require(micros >= 0 && micros < MICROS_PER_DAY,
      s"Time value $micros is out of valid range [0, $MICROS_PER_DAY)")

    val localTime = microsToLocalTime(micros)
    UTF8String.fromString(localTime.format(DEFAULT_TIME_FORMATTER))
  }

  /**
   * Converts java.time.LocalTime to microseconds since midnight.
   */
  def localTimeToMicros(localTime: LocalTime): Long = {
    localTime.getLong(ChronoField.MICRO_OF_DAY)
  }

  /**
   * Converts microseconds since midnight to java.time.LocalTime.
   */
  def microsToLocalTime(micros: Long): LocalTime = {
    require(micros >= 0 && micros < MICROS_PER_DAY,
      s"Time value $micros is out of valid range [0, $MICROS_PER_DAY)")
    LocalTime.ofNanoOfDay(micros * 1000)
  }

  /**
   * Extracts hour from TIME value (0-23).
   */
  def getHour(micros: Long): Int = {
    (micros / MICROS_PER_HOUR).toInt
  }

  /**
   * Extracts minute from TIME value (0-59).
   */
  def getMinute(micros: Long): Int = {
    ((micros % MICROS_PER_HOUR) / MICROS_PER_MINUTE).toInt
  }

  /**
   * Extracts second from TIME value (0-59).
   */
  def getSecond(micros: Long): Int = {
    ((micros % MICROS_PER_MINUTE) / MICROS_PER_SECOND).toInt
  }

  /**
   * Extracts microsecond from TIME value (0-999999).
   */
  def getMicrosecond(micros: Long): Int = {
    (micros % MICROS_PER_SECOND).toInt
  }

  /**
   * Creates TIME from hour, minute, second, and microsecond components.
   *
   * @param hour 0-23
   * @param minute 0-59
   * @param second 0-59
   * @param microsecond 0-999999
   * @return microseconds since midnight
   */
  def makeTime(hour: Int, minute: Int, second: Int, microsecond: Int): Long = {
    require(hour >= 0 && hour < 24, s"Hour must be in range [0, 23], got $hour")
    require(minute >= 0 && minute < 60, s"Minute must be in range [0, 59], got $minute")
    require(second >= 0 && second < 60, s"Second must be in range [0, 59], got $second")
    require(microsecond >= 0 && microsecond < MICROS_PER_SECOND,
      s"Microsecond must be in range [0, 999999], got $microsecond")

    hour * MICROS_PER_HOUR +
      minute * MICROS_PER_MINUTE +
      second * MICROS_PER_SECOND +
      microsecond
  }

  /**
   * Extracts time component from timestamp (microseconds since epoch).
   *
   * @param timestampMicros timestamp in microseconds since epoch
   * @param zoneId time zone for conversion
   * @return time in microseconds since midnight
   */
  def extractTimeFromTimestamp(timestampMicros: Long, zoneId: java.time.ZoneId): Long = {
    val instant = java.time.Instant.ofEpochSecond(
      timestampMicros / MICROS_PER_SECOND,
      (timestampMicros % MICROS_PER_SECOND) * 1000
    )
    val localTime = instant.atZone(zoneId).toLocalTime
    localTimeToMicros(localTime)
  }

  /**
   * Validates that a time value is within valid range.
   */
  def isValidTime(micros: Long): Boolean = {
    micros >= 0 && micros < MICROS_PER_DAY
  }

  /**
   * Returns current time in microseconds since midnight.
   */
  def currentTime(zoneId: java.time.ZoneId): Long = {
    localTimeToMicros(LocalTime.now(zoneId))
  }
}

// Made with Bob
