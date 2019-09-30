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

import org.apache.spark.unsafe.types.CalendarInterval

object IntervalUtils {
  val MONTHS_PER_YEAR: Int = 12
  val MONTHS_PER_QUARTER: Byte = 3
  val YEARS_PER_MILLENNIUM: Int = 1000
  val YEARS_PER_CENTURY: Int = 100
  val YEARS_PER_DECADE: Int = 10
  val MICROS_PER_HOUR: Long = DateTimeUtils.MILLIS_PER_HOUR * DateTimeUtils.MICROS_PER_MILLIS
  val MICROS_PER_MINUTE: Long = DateTimeUtils.MILLIS_PER_MINUTE * DateTimeUtils.MICROS_PER_MILLIS

  def getYear(interval: CalendarInterval): Int = {
    interval.months / MONTHS_PER_YEAR
  }

  def getMillennium(interval: CalendarInterval): Int = {
    getYear(interval) / YEARS_PER_MILLENNIUM
  }

  def getCentury(interval: CalendarInterval): Int = {
    getYear(interval) / YEARS_PER_CENTURY
  }

  def getDecade(interval: CalendarInterval): Int = {
    getYear(interval) / YEARS_PER_DECADE
  }

  def getMonth(interval: CalendarInterval): Byte = {
    (interval.months % MONTHS_PER_YEAR).toByte
  }

  def getQuarter(interval: CalendarInterval): Byte = {
    (getMonth(interval) / MONTHS_PER_QUARTER + 1).toByte
  }

  def getDay(interval: CalendarInterval): Long = {
    interval.microseconds / DateTimeUtils.MICROS_PER_DAY
  }

  def getHour(interval: CalendarInterval): Byte = {
    ((interval.microseconds % DateTimeUtils.MICROS_PER_DAY) / MICROS_PER_HOUR).toByte
  }

  def getMinute(interval: CalendarInterval): Byte = {
    ((interval.microseconds % MICROS_PER_HOUR) / MICROS_PER_MINUTE).toByte
  }
}
