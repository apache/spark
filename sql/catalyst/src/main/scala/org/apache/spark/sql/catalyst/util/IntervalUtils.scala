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

import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.CalendarInterval

object IntervalUtils {
  final val MONTHS_PER_YEAR: Int = 12
  final val MONTHS_PER_QUARTER: Byte = 3
  final val YEARS_PER_MILLENNIUM: Int = 1000
  final val YEARS_PER_CENTURY: Int = 100
  final val YEARS_PER_DECADE: Int = 10
  final val MICROS_PER_HOUR: Long = DateTimeUtils.MILLIS_PER_HOUR * DateTimeUtils.MICROS_PER_MILLIS
  final val MICROS_PER_MINUTE: Long = {
    DateTimeUtils.MILLIS_PER_MINUTE * DateTimeUtils.MICROS_PER_MILLIS
  }
  // The average year of the Gregorian calendar 365.2425 days long, see
  // https://en.wikipedia.org/wiki/Gregorian_calendar
  // Leap year occurs every 4 years, except for years that are divisible by 100
  // and not divisible by 400. So, the mean length of of the Gregorian calendar year is:
  //  1 mean year = (365 + 1/4 - 1/100 + 1/400) days = 365.2425 days
  // The mean year length in seconds is:
  //  60 * 60 * 24 * 365.2425 = 31556952.0 = 12 * 2629746
  final val SECONDS_PER_MONTH: Int = 2629746
  final val MICROS_PER_MONTH: Long = SECONDS_PER_MONTH * DateTimeUtils.MICROS_PER_SECOND

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
    val monthsDurationUs = Math.multiplyExact(interval.months, MICROS_PER_MONTH)
    val result = Math.addExact(interval.microseconds, monthsDurationUs)
    Decimal(result, 18, 6)
  }
}
