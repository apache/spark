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
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

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

  private object ParseState extends Enumeration {
    val PREFIX,
        BEGIN_VALUE,
        PARSE_SIGN,
        POSITIVE_VALUE_TO_LONG,
        NEGATIVE_VALUE_TO_LONG,
        BEGIN_UNIT_NAME,
        UNIT_NAME_SUFFIX,
        END_UNIT_NAME = Value
  }
  private final val intervalStr = UTF8String.fromString("interval")
  private final val yearStr = UTF8String.fromString("year")
  private final val monthStr = UTF8String.fromString("month")
  private final val weekStr = UTF8String.fromString("week")
  private final val dayStr = UTF8String.fromString("day")
  private final val hourStr = UTF8String.fromString("hour")
  private final val minuteStr = UTF8String.fromString("minute")
  private final val secondStr = UTF8String.fromString("second")
  private final val millisStr = UTF8String.fromString("millisecond")
  private final val microsStr = UTF8String.fromString("microsecond")

  def stringToInterval(input: UTF8String): Option[CalendarInterval] = {
    import ParseState._

    if (input == null) {
      return None
    }
    // scalastyle:off caselocale .toLowerCase
    val s = input.trim.toLowerCase
    // scalastyle:on
    val bytes = s.getBytes
    if (bytes.length == 0) {
      return None
    }
    var state = PREFIX
    var i = 0
    var currentValue: Long = 0
    var months: Long = 0
    var microseconds: Long = 0

    while (i < bytes.length) {
      val b = bytes(i)
      state match {
        case PREFIX =>
          if (s.startsWith(intervalStr)) {
            if (s.numBytes() == intervalStr.numBytes()) {
              return None
            } else {
              i += intervalStr.numBytes()
            }
          }
          state = BEGIN_VALUE
        case BEGIN_VALUE =>
          b match {
            case ' ' => i += 1
            case _ => state = PARSE_SIGN
          }
        case PARSE_SIGN =>
          b match {
            case '-' =>
              state = NEGATIVE_VALUE_TO_LONG
              i += 1
            case '+' =>
              state = POSITIVE_VALUE_TO_LONG
              i += 1
            case _ if '0' <= b && b <= '9' =>
              state = POSITIVE_VALUE_TO_LONG
            case _ => return None
          }
          currentValue = 0
        case POSITIVE_VALUE_TO_LONG | NEGATIVE_VALUE_TO_LONG =>
          if ('0' <= b && b <= '9') {
            currentValue = Math.addExact(Math.multiplyExact(10, currentValue), (b - '0'))
          } else if (b == ' ') {
            if (state == NEGATIVE_VALUE_TO_LONG) {
              currentValue = -currentValue
            }
            state = BEGIN_UNIT_NAME
          } else return None
          i += 1
        case BEGIN_UNIT_NAME =>
          if (b == ' ') {
            i += 1
          } else {
            b match {
              case 'y' if s.matchAt(yearStr, i) =>
                months = Math.addExact(months, Math.multiplyExact(12, currentValue))
                i += yearStr.numBytes()
              case 'w' if s.matchAt(weekStr, i) =>
                val daysUs = Math.multiplyExact(
                  Math.multiplyExact(7, currentValue),
                  DateTimeUtils.MICROS_PER_DAY)
                microseconds = Math.addExact(microseconds, daysUs)
                i += weekStr.numBytes()
              case 'd' if s.matchAt(dayStr, i) =>
                val daysUs = Math.multiplyExact(currentValue, DateTimeUtils.MICROS_PER_DAY)
                microseconds = Math.addExact(microseconds, daysUs)
                i += dayStr.numBytes()
              case 'h' if s.matchAt(hourStr, i) =>
                val hoursUs = Math.multiplyExact(currentValue, MICROS_PER_HOUR)
                microseconds = Math.addExact(microseconds, hoursUs)
                i += hourStr.numBytes()
              case 's' if s.matchAt(secondStr, i) =>
                val secondsUs = Math.multiplyExact(currentValue, DateTimeUtils.MICROS_PER_SECOND)
                microseconds = Math.addExact(microseconds, secondsUs)
                i += secondStr.numBytes()
              case 'm' =>
                if (s.matchAt(monthStr, i)) {
                  months = Math.addExact(months, currentValue)
                  i += monthStr.numBytes()
                } else if (s.matchAt(minuteStr, i)) {
                  val minutesUs = Math.multiplyExact(currentValue, MICROS_PER_MINUTE)
                  microseconds = Math.addExact(microseconds, minutesUs)
                  i += minuteStr.numBytes()
                } else if (s.matchAt(millisStr, i)) {
                  val millisUs = Math.multiplyExact(currentValue, DateTimeUtils.MICROS_PER_MILLIS)
                  microseconds = Math.addExact(microseconds, millisUs)
                  i += millisStr.numBytes()
                } else if (s.matchAt(microsStr, i)) {
                  microseconds = Math.addExact(microseconds, currentValue)
                  i += microsStr.numBytes()
                } else return None
              case _ => return None
            }
            state = UNIT_NAME_SUFFIX
          }
        case UNIT_NAME_SUFFIX =>
          if (b == 's') {
            state = END_UNIT_NAME
          } else if (b == ' ') {
            state = BEGIN_VALUE
          } else {
            return None
          }
          i += 1
        case END_UNIT_NAME =>
          if (b == ' ') {
            i += 1
            state = BEGIN_VALUE
          } else {
            return None
          }
      }
    }

    val result = state match {
      case UNIT_NAME_SUFFIX | END_UNIT_NAME | BEGIN_VALUE =>
        Some(new CalendarInterval(Math.toIntExact(months), microseconds))
      case _ => None
    }

    result
  }
}
