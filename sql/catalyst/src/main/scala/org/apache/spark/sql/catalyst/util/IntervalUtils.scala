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

import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParseException}
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.CalendarInterval

object IntervalUtils {
  final val MONTHS_PER_YEAR: Int = 12
  final val MONTHS_PER_QUARTER: Byte = 3
  final val YEARS_PER_MILLENNIUM: Int = 1000
  final val YEARS_PER_CENTURY: Int = 100
  final val YEARS_PER_DECADE: Int = 10
  final val MICROS_PER_HOUR: Long =
    DateTimeUtils.MILLIS_PER_HOUR * DateTimeUtils.MICROS_PER_MILLIS
  final val MICROS_PER_MINUTE: Long =
    DateTimeUtils.MILLIS_PER_MINUTE * DateTimeUtils.MICROS_PER_MILLIS
  final val DAYS_PER_MONTH: Byte = 30
  final val MICROS_PER_MONTH: Long = DAYS_PER_MONTH * DateTimeUtils.SECONDS_PER_DAY
  /* 365.25 days per year assumes leap year every four years */
  final val MICROS_PER_YEAR: Long = (36525L * DateTimeUtils.MICROS_PER_DAY) / 100

  def getYears(interval: CalendarInterval): Int = {
    interval.months / MONTHS_PER_YEAR
  }

  def getMillenniums(interval: CalendarInterval): Int = {
    getYears(interval) / YEARS_PER_MILLENNIUM
  }

  def getCenturies(interval: CalendarInterval): Int = {
    getYears(interval) / YEARS_PER_CENTURY
  }

  def getDecades(interval: CalendarInterval): Int = {
    getYears(interval) / YEARS_PER_DECADE
  }

  def getMonths(interval: CalendarInterval): Byte = {
    (interval.months % MONTHS_PER_YEAR).toByte
  }

  def getQuarters(interval: CalendarInterval): Byte = {
    (getMonths(interval) / MONTHS_PER_QUARTER + 1).toByte
  }

  def getDays(interval: CalendarInterval): Long = {
    interval.microseconds / DateTimeUtils.MICROS_PER_DAY
  }

  def getHours(interval: CalendarInterval): Byte = {
    ((interval.microseconds % DateTimeUtils.MICROS_PER_DAY) / MICROS_PER_HOUR).toByte
  }

  def getMinutes(interval: CalendarInterval): Byte = {
    ((interval.microseconds % MICROS_PER_HOUR) / MICROS_PER_MINUTE).toByte
  }

  def getMicroseconds(interval: CalendarInterval): Long = {
    interval.microseconds % MICROS_PER_MINUTE
  }

  def getSeconds(interval: CalendarInterval): Decimal = {
    Decimal(getMicroseconds(interval), 8, 6)
  }

  def getMilliseconds(interval: CalendarInterval): Decimal = {
    Decimal(getMicroseconds(interval), 8, 3)
  }

  // Returns total number of seconds with microseconds fractional part in the given interval.
  def getEpoch(interval: CalendarInterval): Decimal = {
    var result = interval.microseconds
    result += MICROS_PER_YEAR * (interval.months / MONTHS_PER_YEAR)
    result += MICROS_PER_MONTH * (interval.months % MONTHS_PER_YEAR)
    Decimal(result, 18, 6)
  }

  /**
   * Converts a string to [[CalendarInterval]] case-insensitively.
   *
   * @throws IllegalArgumentException if the input string is not in valid interval format.
   */
  def fromString(str: String): CalendarInterval = {
    if (str == null) throw new IllegalArgumentException("Interval string cannot be null")
    try {
      CatalystSqlParser.parseInterval(str)
    } catch {
      case e: ParseException =>
        val ex = new IllegalArgumentException(s"Invalid interval string: $str\n" + e.message)
        ex.setStackTrace(e.getStackTrace)
        throw ex
    }
  }

  /**
   * A safe version of `fromString`. It returns null for invalid input string.
   */
  def safeFromString(str: String): CalendarInterval = {
    try {
      fromString(str)
    } catch {
      case _: IllegalArgumentException => null
    }
  }
}
