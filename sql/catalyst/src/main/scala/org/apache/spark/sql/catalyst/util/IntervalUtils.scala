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

import scala.util.control.NonFatal

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

  private def toLongWithRange(
      fieldName: String,
      s: String,
      minValue: Long,
      maxValue: Long): Long = {
    val result = if (s == null) 0L else s.toLong
    require(minValue <= result && result <= maxValue,
      s"$fieldName $result outside range [$minValue, $maxValue]")

    result
  }

  private val yearMonthPattern = "^([+|-])?(\\d+)-(\\d+)$".r

  /**
   * Parse YearMonth string in form: [+|-]YYYY-MM
   *
   * adapted from HiveIntervalYearMonth.valueOf
   */
  def fromYearMonthString(input: String): CalendarInterval = {
    require(input != null, "Interval year-month string must be not null")
    def toInterval(yearStr: String, monthStr: String): CalendarInterval = {
      try {
        val years = toLongWithRange("year", yearStr, 0, Integer.MAX_VALUE).toInt
        val months = toLongWithRange("month", monthStr, 0, 11).toInt
        val totalMonths = Math.addExact(Math.multiplyExact(years, 12), months)
        new CalendarInterval(totalMonths, 0)
      } catch {
        case NonFatal(e) =>
          throw new IllegalArgumentException(
            s"Error parsing interval year-month string: ${e.getMessage}", e)
      }
    }
    assert(input.length == input.trim.length)
    input match {
      case yearMonthPattern("-", yearStr, monthStr) =>
        toInterval(yearStr, monthStr).negate()
      case yearMonthPattern(_, yearStr, monthStr) =>
        toInterval(yearStr, monthStr)
      case _ =>
        throw new IllegalArgumentException(
          s"Interval string does not match year-month format of 'y-m': $input")
    }
  }

  /**
   * Parse dayTime string in form: [-]d HH:mm:ss.nnnnnnnnn and [-]HH:mm:ss.nnnnnnnnn
   *
   * adapted from HiveIntervalDayTime.valueOf
   */
  def fromDayTimeString(s: String): CalendarInterval = {
    fromDayTimeString(s, UnitName.day, UnitName.second)
  }

  object UnitName extends Enumeration {
    val microsecond = Value(0, "microsecond")
    val millisecond = Value(1, "millisecond")
    val second = Value(2, "second")
    val minute = Value(3, "minute")
    val hour = Value(4, "hour")
    val day = Value(5, "day")
    val week = Value(6, "week")
    val month = Value(7, "month")
    val year = Value(8, "year")
  }

  val unitValueProps: Map[UnitName.Value, (Long, Long, Long => Long)] = Map(
    UnitName.minute -> (0, 59, Math.multiplyExact(_, MICROS_PER_MINUTE)),
    UnitName.hour -> (0, 23, Math.multiplyExact(_, MICROS_PER_HOUR)),
    UnitName.day -> (0, Integer.MAX_VALUE, Math.multiplyExact(_, DateTimeUtils.MICROS_PER_DAY))
  )

  private val signRe = "(?<sign>[+|-])?"
  private val dayRe = "((?<day>\\d+)\\s+)?"
  private val hourRe = "(?<hour>\\d{1,2}+)"
  private val minuteRe = "(?<minute>\\d{1,2}+)"
  private val secondRe = "(?<second>(\\d{1,2}+)(\\.(\\d{1,9}+))?)"

  private val dayTimeRe = Map(
    (UnitName.minute, UnitName.second) -> (s"^$signRe$minuteRe:$secondRe$$").r,
    (UnitName.hour, UnitName.minute) -> (s"^$signRe$hourRe:$minuteRe$$").r,
    (UnitName.hour, UnitName.second) -> (s"^$signRe$hourRe:$minuteRe:$secondRe$$").r,
    (UnitName.day, UnitName.hour) -> (s"^$signRe$dayRe$hourRe$$").r,
    (UnitName.day, UnitName.minute) -> (s"^$signRe$dayRe$hourRe:$minuteRe$$").r,
    (UnitName.day, UnitName.second) -> (s"^$signRe$dayRe$hourRe:$minuteRe:$secondRe$$").r
  )

  private def unitsRange(start: UnitName.Value, end: UnitName.Value): Seq[UnitName.Value] = {
    (start.id to end.id).map(UnitName(_))
  }

  /**
   * Parse dayTime string in form: [-]d HH:mm:ss.nnnnnnnnn and [-]HH:mm:ss.nnnnnnnnn
   *
   * adapted from HiveIntervalDayTime.valueOf.
   * Below interval conversion patterns are supported:
   * - DAY TO (HOUR|MINUTE|SECOND)
   * - HOUR TO (MINUTE|SECOND)
   * - MINUTE TO SECOND
   */
  def fromDayTimeString(
      input: String,
      from: UnitName.Value,
      to: UnitName.Value): CalendarInterval = {
    require(input != null, "Interval day-time string must be not null")
    assert(input.length == input.trim.length)
    require(dayTimeRe.contains(from -> to),
      s"Cannot support (interval '$input' $from to $to) expression")
    val pattern = dayTimeRe(from, to).pattern
    val m = pattern.matcher(input)
    require(m.matches, s"Interval string must match day-time format of '$pattern': $input")

    def toLong(unitName: UnitName.Value): Long = {
      val name = unitName.toString
      val (minValue, maxValue, toMicros) = unitValueProps(unitName)
      toMicros(toLongWithRange(name, m.group(name), minValue, maxValue))
    }

    try {
      val micros = unitsRange(to, from).map {
        case name @ (UnitName.day | UnitName.hour | UnitName.minute) => toLong(name)
        case UnitName.second => parseSecondNano(m.group(UnitName.second.toString))
        case _ =>
          throw new IllegalArgumentException(
            s"Cannot support (interval '$input' $from to $to) expression")
      }.reduce((x: Long, y: Long) => Math.addExact(x, y))
      val sign = if (m.group("sign") != null && m.group("sign") == "-") -1 else 1
      new CalendarInterval(0, sign * micros)
    } catch {
      case e: Exception =>
        throw new IllegalArgumentException(
          s"Error parsing interval day-time string: ${e.getMessage}", e)
    }
  }

  def fromUnitStrings(units: Array[String], values: Array[String]): CalendarInterval = {
    assert(units.length == values.length)
    var months: Int = 0
    var microseconds: Long = 0
    var i = 0
    while (i < units.length) {
      try {
        units(i) match {
          case "year" =>
            months = Math.addExact(months, Math.multiplyExact(values(i).toInt, 12))
          case "month" =>
            months = Math.addExact(months, values(i).toInt)
          case "week" =>
            val weeksUs = Math.multiplyExact(values(i).toLong, 7 * DateTimeUtils.MICROS_PER_DAY)
            microseconds = Math.addExact(microseconds, weeksUs)
          case "day" =>
            val daysUs = Math.multiplyExact(values(i).toLong, DateTimeUtils.MICROS_PER_DAY)
            microseconds = Math.addExact(microseconds, daysUs)
          case "hour" =>
            val hoursUs = Math.multiplyExact(values(i).toLong, MICROS_PER_HOUR)
            microseconds = Math.addExact(microseconds, hoursUs)
          case "minute" =>
            val minutesUs = Math.multiplyExact(values(i).toLong, MICROS_PER_MINUTE)
            microseconds = Math.addExact(microseconds, minutesUs)
          case "second" =>
            microseconds = Math.addExact(microseconds, parseSecondNano(values(i)))
          case "millisecond" =>
            val millisUs = Math.multiplyExact(values(i).toLong, DateTimeUtils.MICROS_PER_MILLIS)
            microseconds = Math.addExact(microseconds, millisUs)
          case "microsecond" =>
            microseconds = Math.addExact(microseconds, values(i).toLong)
        }
      } catch {
        case e: Exception =>
          throw new IllegalArgumentException(s"Error parsing interval string: ${e.getMessage}", e)
      }
      i += 1
    }
    new CalendarInterval(months, microseconds)
  }

  // Parses a string with nanoseconds, truncates the result and returns microseconds
  private def parseNanos(nanosStr: String, isNegative: Boolean): Long = {
    if (nanosStr != null) {
      val maxNanosLen = 9
      val alignedStr = if (nanosStr.length < maxNanosLen) {
        (nanosStr + "000000000").substring(0, maxNanosLen)
      } else nanosStr
      val nanos = toLongWithRange("nanosecond", alignedStr, 0L, 999999999L)
      val micros = nanos / DateTimeUtils.NANOS_PER_MICROS
      if (isNegative) -micros else micros
    } else {
      0L
    }
  }

  /**
   * Parse second_nano string in ss.nnnnnnnnn format to microseconds
   */
  private def parseSecondNano(secondNano: String): Long = {
    def parseSeconds(secondsStr: String): Long = {
      toLongWithRange(
        "second",
        secondsStr,
        Long.MinValue / DateTimeUtils.MICROS_PER_SECOND,
        Long.MaxValue / DateTimeUtils.MICROS_PER_SECOND) * DateTimeUtils.MICROS_PER_SECOND
    }

    secondNano.split("\\.") match {
      case Array(secondsStr) => parseSeconds(secondsStr)
      case Array("", nanosStr) => parseNanos(nanosStr, false)
      case Array(secondsStr, nanosStr) =>
        val seconds = parseSeconds(secondsStr)
        Math.addExact(seconds, parseNanos(nanosStr, seconds < 0))
      case _ =>
        throw new IllegalArgumentException(
          "Interval string does not match second-nano format of ss.nnnnnnnnn")
    }
  }
}
