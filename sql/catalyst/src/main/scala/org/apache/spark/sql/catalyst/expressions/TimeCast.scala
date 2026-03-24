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

import org.apache.spark.sql.catalyst.trees.SQLQueryContext
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, TimeUtils}
import org.apache.spark.sql.types.Decimal
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
   * Cast from String to TimeType in ANSI mode.
   * Throws an exception if the string is not a valid time.
   */
  def castStringToTimeAnsi(from: UTF8String, errorContext: SQLQueryContext): Long = {
    if (from == null) throw new NullPointerException("from is null")
    TimeUtils.stringToTime(from) match {
      case Some(micros) => micros
      case None =>
        throw new IllegalArgumentException(
          s"Cannot cast '$from' to TIME. $errorContext")
    }
  }

  /**
   * Cast from TimeType to String.
   * Formats time as HH:mm:ss or HH:mm:ss.SSSSSS (omits microseconds if zero)
   */
  def castTimeToString(timeMicros: Long): UTF8String = {
    TimeUtils.timeToStringForCast(timeMicros)
  }

  /**
   * Cast from Long to TimeType.
   * Interprets the long value as seconds since midnight.
   */
  def castLongToTime(seconds: Long): Any = {
    val micros = seconds * TimeUtils.MICROS_PER_SECOND
    if (TimeUtils.isValidTime(micros)) micros else null
  }

  /**
   * Cast from Long to TimeType in ANSI mode.
   * Throws an exception if the value is out of range.
   */
  def castLongToTimeAnsi(seconds: Long, errorContext: SQLQueryContext): Long = {
    val micros = seconds * TimeUtils.MICROS_PER_SECOND
    if (TimeUtils.isValidTime(micros)) {
      micros
    } else {
      val maxSeconds = 86400L - 1
      throw new IllegalArgumentException(
        s"Cannot cast $seconds to TIME. Valid range is 0 to $maxSeconds. $errorContext")
    }
  }

  /**
   * Cast from Double to TimeType.
   * Interprets the double value as seconds since midnight.
   */
  def castDoubleToTime(seconds: Double): Any = {
    if (seconds.isNaN || seconds.isInfinite) return null
    val micros = (seconds * TimeUtils.MICROS_PER_SECOND).toLong
    if (TimeUtils.isValidTime(micros)) micros else null
  }

  /**
   * Cast from Double to TimeType in ANSI mode.
   */
  def castDoubleToTimeAnsi(seconds: Double, errorContext: SQLQueryContext): Long = {
    if (seconds.isNaN || seconds.isInfinite) {
      throw new IllegalArgumentException(s"Cannot cast $seconds to TIME. $errorContext")
    }
    val micros = (seconds * TimeUtils.MICROS_PER_SECOND).toLong
    if (TimeUtils.isValidTime(micros)) {
      micros
    } else {
      val maxSeconds = 86400.0 - 0.000001
      throw new IllegalArgumentException(
        s"Cannot cast $seconds to TIME. Valid range is 0 to $maxSeconds. $errorContext")
    }
  }

  /**
   * Cast from Decimal to TimeType.
   */
  def castDecimalToTime(d: Decimal, precision: Int, scale: Int): Any = {
    if (d == null) return null
    val seconds = d.toBigDecimal
    val micros = (seconds * TimeUtils.MICROS_PER_SECOND).toLong
    if (TimeUtils.isValidTime(micros)) micros else null
  }

  /**
   * Cast from Decimal to TimeType in ANSI mode.
   */
  def castDecimalToTimeAnsi(
      d: Decimal,
      precision: Int,
      scale: Int,
      errorContext: SQLQueryContext): Long = {
    if (d == null) throw new NullPointerException("decimal is null")
    val seconds = d.toBigDecimal
    val micros = (seconds * TimeUtils.MICROS_PER_SECOND).toLong
    if (TimeUtils.isValidTime(micros)) {
      micros
    } else {
      throw new IllegalArgumentException(
        s"Cannot cast $d to TIME. Valid range is 0 to 86399.999999. $errorContext")
    }
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
   * Returns seconds since midnight (truncates microseconds).
   */
  def castTimeToLong(timeMicros: Long): Long = {
    timeMicros / TimeUtils.MICROS_PER_SECOND
  }

  /**
   * Cast from TimeType to Int.
   * Returns seconds since midnight (truncates microseconds).
   */
  def castTimeToInt(timeMicros: Long): Int = {
    (timeMicros / TimeUtils.MICROS_PER_SECOND).toInt
  }

  /**
   * Cast from TimeType to Short.
   */
  def castTimeToShort(timeMicros: Long): Short = castTimeToInt(timeMicros).toShort

  /**
   * Cast from TimeType to Byte.
   */
  def castTimeToByte(timeMicros: Long): Byte = castTimeToInt(timeMicros).toByte

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
