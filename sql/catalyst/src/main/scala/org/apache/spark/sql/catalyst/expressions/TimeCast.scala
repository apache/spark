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

package org.apache.spark.sql.catalyst.expressions

import java.time.ZoneId

import org.apache.spark.sql.catalyst.util.{DateTimeUtils, TimeUtils}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Cast expressions for TIME type conversions.
 * This object provides casting functions between TimeType and other data types.
 */
object TimeCast {

  /**
   * Cast from String to TimeType.
   * Parses time strings in format HH:mm:ss[.SSSSSS]
   */
  def castStringToTime(from: UTF8String): Any = {
    if (from == null) return null
    TimeUtils.stringToTime(from) match {
      case Some(micros) => micros
      case None => null
    }
  }

  /**
   * Cast from TimeType to String.
   * Formats time as HH:mm:ss.SSSSSS
   */
  def castTimeToString(timeMicros: Long): UTF8String = {
    TimeUtils.timeToString(timeMicros)
  }

  /**
   * Cast from Long to TimeType.
   * Interprets the long value as microseconds since midnight.
   */
  def castLongToTime(micros: Long): Any = {
    if (TimeUtils.isValidTime(micros)) micros else null
  }

  /**
   * Cast from Int to TimeType.
   * Interprets the int value as seconds since midnight.
   */
  def castIntToTime(seconds: Int): Any = {
    val micros = seconds.toLong * TimeUtils.MICROS_PER_SECOND
    if (TimeUtils.isValidTime(micros)) micros else null
  }

  /**
   * Cast from TimestampType to TimeType.
   * Extracts the time component from a timestamp.
   */
  def castTimestampToTime(timestampMicros: Long, zoneId: ZoneId): Long = {
    TimeUtils.extractTimeFromTimestamp(timestampMicros, zoneId)
  }

  /**
   * Cast from TimestampNTZType to TimeType.
   * Extracts the time component from a timestamp without timezone.
   */
  def castTimestampNTZToTime(timestampMicros: Long): Long = {
    TimeUtils.extractTimeFromTimestamp(timestampMicros, ZoneId.of("UTC"))
  }

  /**
   * Cast from TimeType to TimestampType.
   * Combines time with epoch date (1970-01-01) for deterministic conversion.
   * The zoneId is used to convert the local datetime to a timestamp.
   */
  def castTimeToTimestamp(timeMicros: Long, zoneId: ZoneId): Long = {
    val epochDate = java.time.LocalDate.of(1970, 1, 1)
    val localTime = TimeUtils.microsToLocalTime(timeMicros)
    val localDateTime = java.time.LocalDateTime.of(epochDate, localTime)
    val instant = localDateTime.atZone(zoneId).toInstant
    DateTimeUtils.instantToMicros(instant)
  }

  /**
   * Cast from TimeType to TimestampNTZType.
   * Combines time with epoch date (1970-01-01).
   */
  def castTimeToTimestampNTZ(timeMicros: Long): Long = {
    val epochDate = java.time.LocalDate.ofEpochDay(0)
    val localTime = TimeUtils.microsToLocalTime(timeMicros)
    val localDateTime = java.time.LocalDateTime.of(epochDate, localTime)
    DateTimeUtils.localDateTimeToMicros(localDateTime)
  }

  /**
   * Cast from TimeType to Long.
   * Returns the internal representation (microseconds since midnight).
   */
  def castTimeToLong(timeMicros: Long): Long = timeMicros

  /**
   * Cast from TimeType to Int.
   * Returns seconds since midnight (truncates microseconds).
   */
  def castTimeToInt(timeMicros: Long): Int = {
    (timeMicros / TimeUtils.MICROS_PER_SECOND).toInt
  }

  /**
   * Cast from DateType to TimeType.
   * Returns midnight (00:00:00.000000).
   */
  def castDateToTime(days: Int): Long = 0L

  /**
   * Cast from TimeType to DateType.
   * Returns epoch date (1970-01-01).
   */
  def castTimeToDate(timeMicros: Long): Int = 0
}

// Made with Bob
