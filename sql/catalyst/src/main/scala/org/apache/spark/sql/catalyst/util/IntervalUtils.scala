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

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.CalendarInterval

object IntervalUtils {
  val MONTHS_PER_YEAR: Int = 12
  val MONTHS_PER_QUARTER: Byte = 3
  val YEARS_PER_MILLENNIUM: Int = 1000
  val YEARS_PER_CENTURY: Int = 100
  val YEARS_PER_DECADE: Int = 10
  val MICROS_PER_HOUR: Long = DateTimeUtils.MILLIS_PER_HOUR * DateTimeUtils.MICROS_PER_MILLIS
  val MICROS_PER_MINUTE: Long = DateTimeUtils.MILLIS_PER_MINUTE * DateTimeUtils.MICROS_PER_MILLIS
  val DAYS_PER_MONTH: Byte = 30
  val MICROS_PER_MONTH: Long = DAYS_PER_MONTH * DateTimeUtils.SECONDS_PER_DAY
  /* 365.25 days per year assumes leap year every four years */
  val MICROS_PER_YEAR: Long = (36525L * DateTimeUtils.MICROS_PER_DAY) / 100

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
   * Gets interval duration
   *
   * @param cal - the interval to get duration
   * @param daysPerMonth - the number of days per one month
   * @param targetUnit - time units of the result
   * @return duration in the specified time units
   */
  def getDuration(
      cal: CalendarInterval,
      daysPerMonth: Int,
      targetUnit: TimeUnit): Long = {
    val monthsDuration = Math.multiplyExact(daysPerMonth * DateTimeUtils.MICROS_PER_DAY, cal.months)
    val result = Math.addExact(cal.microseconds, monthsDuration)
    targetUnit.convert(result, TimeUnit.MICROSECONDS)
  }

  /**
   * Checks the interval is negative
   *
   * @param cal - the checked interval
   * @param daysPerMonth - the number of days per one month
   * @return true if duration of the given interval is less than 0 otherwise false
   */
  def isNegative(cal: CalendarInterval, daysPerMonth: Int): Boolean = {
    getDuration(cal, daysPerMonth, TimeUnit.MICROSECONDS) < 0
  }
}
