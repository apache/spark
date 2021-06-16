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

import java.time.{Duration, Period}
import java.time.temporal.ChronoUnit
import java.util.concurrent.TimeUnit

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.util.DateTimeConstants._
import org.apache.spark.sql.catalyst.util.DateTimeUtils.millisToMicros
import org.apache.spark.sql.catalyst.util.IntervalStringStyles.{ANSI_STYLE, HIVE_STYLE, IntervalStyle}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DayTimeIntervalType, Decimal}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

// The style of textual representation of intervals
object IntervalStringStyles extends Enumeration {
  type IntervalStyle = Value
  val ANSI_STYLE, HIVE_STYLE = Value
}

object IntervalUtils {

  object IntervalUnit extends Enumeration {
    type IntervalUnit = Value

    val NANOSECOND = Value(0, "nanosecond")
    val MICROSECOND = Value(1, "microsecond")
    val MILLISECOND = Value(2, "millisecond")
    val SECOND = Value(3, "second")
    val MINUTE = Value(4, "minute")
    val HOUR = Value(5, "hour")
    val DAY = Value(6, "day")
    val WEEK = Value(7, "week")
    val MONTH = Value(8, "month")
    val YEAR = Value(9, "year")
  }
  import IntervalUnit._

  def getYears(months: Int): Int = months / MONTHS_PER_YEAR

  def getYears(interval: CalendarInterval): Int = getYears(interval.months)

  def getMonths(months: Int): Byte = (months % MONTHS_PER_YEAR).toByte

  def getMonths(interval: CalendarInterval): Byte = getMonths(interval.months)

  def getDays(microseconds: Long): Int = (microseconds / MICROS_PER_DAY).toInt

  def getDays(interval: CalendarInterval): Int = {
    val daysInMicroseconds = getDays(interval.microseconds)
    Math.addExact(interval.days, daysInMicroseconds)
  }

  def getHours(microseconds: Long): Byte = {
    ((microseconds % MICROS_PER_DAY) / MICROS_PER_HOUR).toByte
  }

  def getHours(interval: CalendarInterval): Byte = getHours(interval.microseconds)

  def getMinutes(microseconds: Long): Byte = {
    ((microseconds % MICROS_PER_HOUR) / MICROS_PER_MINUTE).toByte
  }

  def getMinutes(interval: CalendarInterval): Byte = getMinutes(interval.microseconds)

  def getSeconds(microseconds: Long): Decimal = {
    Decimal(microseconds % MICROS_PER_MINUTE, 8, 6)
  }

  def getSeconds(interval: CalendarInterval): Decimal = getSeconds(interval.microseconds)

  private def toLongWithRange(
      fieldName: IntervalUnit,
      s: String,
      minValue: Long,
      maxValue: Long): Long = {
    val result = if (s == null) 0L else s.toLong
    require(minValue <= result && result <= maxValue,
      s"$fieldName $result outside range [$minValue, $maxValue]")

    result
  }

  private val yearMonthPatternString = "([+|-])?(\\d+)-(\\d+)"
  private val yearMonthRegex = (s"^$yearMonthPatternString$$").r
  private val yearMonthLiteralRegex =
    (s"(?i)^INTERVAL\\s+([+|-])?'$yearMonthPatternString'\\s+YEAR\\s+TO\\s+MONTH$$").r

  def castStringToYMInterval(input: UTF8String): Int = {
    input.trimAll().toString match {
      case yearMonthRegex("-", year, month) => toYMInterval(year, month, -1)
      case yearMonthRegex(_, year, month) => toYMInterval(year, month, 1)
      case yearMonthLiteralRegex(firstSign, secondSign, year, month) =>
        (firstSign, secondSign) match {
          case ("-", "-") => toYMInterval(year, month, 1)
          case ("-", _) => toYMInterval(year, month, -1)
          case (_, "-") => toYMInterval(year, month, -1)
          case (_, _) => toYMInterval(year, month, 1)
        }
      case _ => throw new IllegalArgumentException(
        s"Interval string does not match year-month format of `[+|-]y-m` " +
          s"or `INTERVAL [+|-]'[+|-]y-m' YEAR TO MONTH`: ${input.toString}")
    }
  }

  /**
   * Parse YearMonth string in form: [+|-]YYYY-MM
   *
   * adapted from HiveIntervalYearMonth.valueOf
   */
  def fromYearMonthString(input: String): CalendarInterval = {
    require(input != null, "Interval year-month string must be not null")
    input.trim match {
      case yearMonthRegex("-", yearStr, monthStr) =>
        new CalendarInterval(toYMInterval(yearStr, monthStr, -1), 0, 0)
      case yearMonthRegex(_, yearStr, monthStr) =>
        new CalendarInterval(toYMInterval(yearStr, monthStr, 1), 0, 0)
      case _ =>
        throw new IllegalArgumentException(
          s"Interval string does not match year-month format of 'y-m': $input")
    }
  }

  def toYMInterval(yearStr: String, monthStr: String, sign: Int): Int = {
    try {
      val years = toLongWithRange(YEAR, yearStr, 0, Integer.MAX_VALUE / MONTHS_PER_YEAR)
      val totalMonths = sign * (years * MONTHS_PER_YEAR + toLongWithRange(MONTH, monthStr, 0, 11))
      Math.toIntExact(totalMonths)
    } catch {
      case NonFatal(e) =>
        throw new IllegalArgumentException(
          s"Error parsing interval year-month string: ${e.getMessage}", e)
    }
  }

  private val daySecondPatternString =
    "([+|-])?(\\d+) (\\d{1,2}):(\\d{1,2}):(\\d{1,2})(\\.\\d{1,9})?"
  private val daySecondRegex = (s"^$daySecondPatternString$$").r
  private val daySecondLiteralRegex =
    (s"(?i)^INTERVAL\\s+([+|-])?\\'$daySecondPatternString\\'\\s+DAY\\s+TO\\s+SECOND$$").r

  def castStringToDTInterval(
      input: UTF8String,
      // TODO(SPARK-35735): Take into account day-time interval fields in cast
      startField: Byte,
      endField: Byte): Long = {
    def secondAndMicro(second: String, micro: String): String = {
      if (micro != null) {
        s"$second$micro"
      } else {
        second
      }
    }

    input.trimAll().toString match {
      case daySecondRegex("-", day, hour, minute, second, micro) =>
        toDTInterval(day, hour, minute, secondAndMicro(second, micro), -1)
      case daySecondRegex(_, day, hour, minute, second, micro) =>
        toDTInterval(day, hour, minute, secondAndMicro(second, micro), 1)
      case daySecondLiteralRegex(firstSign, secondSign, day, hour, minute, second, micro) =>
        (firstSign, secondSign) match {
          case ("-", "-") => toDTInterval(day, hour, minute, secondAndMicro(second, micro), 1)
          case ("-", _) => toDTInterval(day, hour, minute, secondAndMicro(second, micro), -1)
          case (_, "-") => toDTInterval(day, hour, minute, secondAndMicro(second, micro), -1)
          case (_, _) => toDTInterval(day, hour, minute, secondAndMicro(second, micro), 1)
        }
      case _ =>
        throw new IllegalArgumentException(
          s"Interval string must match day-time format of `d h:m:s.n` " +
            s"or `INTERVAL [+|-]'[+|-]d h:m:s.n' DAY TO SECOND`: ${input.toString}, " +
            s"$fallbackNotice")
    }
  }

  def toDTInterval(
      dayStr: String,
      hourStr: String,
      minuteStr: String,
      secondStr: String,
      sign: Int): Long = {
    var micros = 0L
    val days = toLongWithRange(DAY, dayStr, 0, Int.MaxValue).toInt
    micros = Math.addExact(micros, sign * days * MICROS_PER_DAY)
    val hours = toLongWithRange(HOUR, hourStr, 0, 23)
    micros = Math.addExact(micros, sign * hours * MICROS_PER_HOUR)
    val minutes = toLongWithRange(MINUTE, minuteStr, 0, 59)
    micros = Math.addExact(micros, sign * minutes * MICROS_PER_MINUTE)
    micros = Math.addExact(micros, sign * parseSecondNano(secondStr))
    micros
  }

  /**
   * Parse dayTime string in form: [-]d HH:mm:ss.nnnnnnnnn and [-]HH:mm:ss.nnnnnnnnn
   *
   * adapted from HiveIntervalDayTime.valueOf
   */
  def fromDayTimeString(s: String): CalendarInterval = {
    fromDayTimeString(s, DAY, SECOND)
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
  def fromDayTimeString(input: String, from: IntervalUnit, to: IntervalUnit): CalendarInterval = {
    if (SQLConf.get.getConf(SQLConf.LEGACY_FROM_DAYTIME_STRING)) {
      parseDayTimeLegacy(input, from, to)
    } else {
      parseDayTime(input, from, to)
    }
  }

  private val dayTimePatternLegacy =
    "^([+|-])?((\\d+) )?((\\d+):)?(\\d+):(\\d+)(\\.(\\d+))?$".r

  private val fallbackNotice = s"set ${SQLConf.LEGACY_FROM_DAYTIME_STRING.key} to true " +
    "to restore the behavior before Spark 3.0."

  /**
   * Legacy method of parsing a string in a day-time format. It ignores the `from` bound,
   * and takes into account only the `to` bound by truncating the result. For example,
   * if the input string is "2 12:30:15", `from` is "hour" and `to` is "second", the result
   * is "2 days 12 hours 30 minutes".
   *
   * @param input The day-time string
   * @param from The interval units from which the input strings begins
   * @param to The interval units at which the input string ends
   * @return an instance of `CalendarInterval` if parsing completes successfully otherwise
   *         the exception `IllegalArgumentException` is raised.
   */
  private def parseDayTimeLegacy(
      input: String,
      from: IntervalUnit,
      to: IntervalUnit): CalendarInterval = {
    require(input != null, "Interval day-time string must be not null")
    assert(input.length == input.trim.length)
    val m = dayTimePatternLegacy.pattern.matcher(input)
    require(m.matches, s"Interval string must match day-time format of 'd h:m:s.n': $input, " +
      s"$fallbackNotice")

    try {
      val sign = if (m.group(1) != null && m.group(1) == "-") -1 else 1
      val days = if (m.group(2) == null) {
        0
      } else {
        toLongWithRange(DAY, m.group(3), 0, Integer.MAX_VALUE).toInt
      }
      var hours: Long = 0L
      var minutes: Long = 0L
      var seconds: Long = 0L
      if (m.group(5) != null || from == MINUTE) { // 'HH:mm:ss' or 'mm:ss minute'
        hours = toLongWithRange(HOUR, m.group(5), 0, 23)
        minutes = toLongWithRange(MINUTE, m.group(6), 0, 59)
        seconds = toLongWithRange(SECOND, m.group(7), 0, 59)
      } else if (m.group(8) != null) { // 'mm:ss.nn'
        minutes = toLongWithRange(MINUTE, m.group(6), 0, 59)
        seconds = toLongWithRange(SECOND, m.group(7), 0, 59)
      } else { // 'HH:mm'
        hours = toLongWithRange(HOUR, m.group(6), 0, 23)
        minutes = toLongWithRange(SECOND, m.group(7), 0, 59)
      }
      // Hive allow nanosecond precision interval
      var secondsFraction = parseNanos(m.group(9), seconds < 0)
      to match {
        case HOUR =>
          minutes = 0
          seconds = 0
          secondsFraction = 0
        case MINUTE =>
          seconds = 0
          secondsFraction = 0
        case SECOND =>
          // No-op
        case _ =>
          throw new IllegalArgumentException(
            s"Cannot support (interval '$input' $from to $to) expression")
      }
      var micros = secondsFraction
      micros = Math.addExact(micros, Math.multiplyExact(hours, MICROS_PER_HOUR))
      micros = Math.addExact(micros, Math.multiplyExact(minutes, MICROS_PER_MINUTE))
      micros = Math.addExact(micros, Math.multiplyExact(seconds, MICROS_PER_SECOND))
      new CalendarInterval(0, sign * days, sign * micros)
    } catch {
      case e: Exception =>
        throw new IllegalArgumentException(
          s"Error parsing interval day-time string: ${e.getMessage}", e)
    }
  }

  private val signRe = "(?<sign>[+|-])"
  private val dayRe = "(?<day>\\d+)"
  private val hourRe = "(?<hour>\\d{1,2})"
  private val minuteRe = "(?<minute>\\d{1,2})"
  private val secondRe = "(?<second>(\\d{1,2})(\\.(\\d{1,9}))?)"

  private val dayTimePattern = Map(
    (MINUTE, SECOND) -> s"^$signRe?$minuteRe:$secondRe$$".r,
    (HOUR, MINUTE) -> s"^$signRe?$hourRe:$minuteRe$$".r,
    (HOUR, SECOND) -> s"^$signRe?$hourRe:$minuteRe:$secondRe$$".r,
    (DAY, HOUR) -> s"^$signRe?$dayRe $hourRe$$".r,
    (DAY, MINUTE) -> s"^$signRe?$dayRe $hourRe:$minuteRe$$".r,
    (DAY, SECOND) -> s"^$signRe?$dayRe $hourRe:$minuteRe:$secondRe$$".r
  )

  private def unitsRange(start: IntervalUnit, end: IntervalUnit): Seq[IntervalUnit] = {
    (start.id to end.id).map(IntervalUnit(_))
  }

  /**
   * Parses an input string in the day-time format defined by the `from` and `to` bounds.
   * It supports the following formats:
   * - [+|-]D+ H[H]:m[m]:s[s][.SSSSSSSSS] for DAY TO SECOND
   * - [+|-]D+ H[H]:m[m] for DAY TO MINUTE
   * - [+|-]D+ H[H] for DAY TO HOUR
   * - [+|-]H[H]:m[m]s[s][.SSSSSSSSS] for HOUR TO SECOND
   * - [+|-]H[H]:m[m] for HOUR TO MINUTE
   * - [+|-]m[m]:s[s][.SSSSSSSSS] for MINUTE TO SECOND
   *
   * Note: the seconds fraction is truncated to microseconds.
   *
   * @param input The input string to parse.
   * @param from The interval unit from which the input string begins.
   * @param to The interval unit at where the input string ends.
   * @return an instance of `CalendarInterval` if the input string was parsed successfully
   *         otherwise throws an exception.
   * @throws IllegalArgumentException The input string has incorrect format and cannot be parsed.
   * @throws ArithmeticException An interval unit value is out of valid range or the resulted
   *                             interval fields `days` or `microseconds` are out of the valid
   *                             ranges.
   */
  private def parseDayTime(
      input: String,
      from: IntervalUnit,
      to: IntervalUnit): CalendarInterval = {
    require(input != null, "Interval day-time string must be not null")
    val regexp = dayTimePattern.get(from -> to)
    require(regexp.isDefined, s"Cannot support (interval '$input' $from to $to) expression")
    val pattern = regexp.get.pattern
    val m = pattern.matcher(input.trim)
    require(m.matches, s"Interval string must match day-time format of '$pattern': $input, " +
      s"$fallbackNotice")
    var micros: Long = 0L
    var days: Int = 0
    unitsRange(to, from).foreach {
      case unit @ DAY =>
        days = toLongWithRange(unit, m.group(unit.toString), 0, Int.MaxValue).toInt
      case unit @ HOUR =>
        val parsed = toLongWithRange(unit, m.group(unit.toString), 0, 23)
        micros = Math.addExact(micros, parsed * MICROS_PER_HOUR)
      case unit @ MINUTE =>
        val parsed = toLongWithRange(unit, m.group(unit.toString), 0, 59)
        micros = Math.addExact(micros, parsed * MICROS_PER_MINUTE)
      case unit @ SECOND =>
        micros = Math.addExact(micros, parseSecondNano(m.group(unit.toString)))
      case _ =>
        throw new IllegalArgumentException(
          s"Cannot support (interval '$input' $from to $to) expression")
    }
    val sign = if (m.group("sign") != null && m.group("sign") == "-") -1 else 1
    new CalendarInterval(0, sign * days, sign * micros)
  }

  // Parses a string with nanoseconds, truncates the result and returns microseconds
  private def parseNanos(nanosStr: String, isNegative: Boolean): Long = {
    if (nanosStr != null) {
      val maxNanosLen = 9
      val alignedStr = if (nanosStr.length < maxNanosLen) {
        (nanosStr + "000000000").substring(0, maxNanosLen)
      } else nanosStr
      val nanos = toLongWithRange(NANOSECOND, alignedStr, 0L, 999999999L)
      val micros = nanos / NANOS_PER_MICROS
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
        SECOND,
        secondsStr,
        Long.MinValue / MICROS_PER_SECOND,
        Long.MaxValue / MICROS_PER_SECOND) * MICROS_PER_SECOND
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

  /**
   * Gets interval duration
   *
   * @param interval The interval to get duration
   * @param targetUnit Time units of the result
   * @param daysPerMonth The number of days per one month. The default value is 31 days
   *                     per month. This value was taken as the default because it is used
   *                     in Structured Streaming for watermark calculations. Having 31 days
   *                     per month, we can guarantee that events are not dropped before
   *                     the end of any month (February with 29 days or January with 31 days).
   * @return Duration in the specified time units
   */
  def getDuration(
      interval: CalendarInterval,
      targetUnit: TimeUnit,
      daysPerMonth: Int = 31): Long = {
    val monthsDuration = Math.multiplyExact(
      daysPerMonth * MICROS_PER_DAY,
      interval.months)
    val daysDuration = Math.multiplyExact(
      MICROS_PER_DAY,
      interval.days)
    val result = Math.addExact(interval.microseconds, Math.addExact(daysDuration, monthsDuration))
    targetUnit.convert(result, TimeUnit.MICROSECONDS)
  }

  /**
   * Checks the interval is negative
   *
   * @param interval The checked interval
   * @param daysPerMonth The number of days per one month. The default value is 31 days
   *                     per month. This value was taken as the default because it is used
   *                     in Structured Streaming for watermark calculations. Having 31 days
   *                     per month, we can guarantee that events are not dropped before
   *                     the end of any month (February with 29 days or January with 31 days).
   * @return true if duration of the given interval is less than 0 otherwise false
   */
  def isNegative(interval: CalendarInterval, daysPerMonth: Int = 31): Boolean = {
    getDuration(interval, TimeUnit.MICROSECONDS, daysPerMonth) < 0
  }

  /**
   * Makes an interval from months, days and micros with the fractional part.
   * The overflow style here follows the way of ansi sql standard and the natural rules for
   * intervals as defined in the Gregorian calendar. Thus, the days fraction will be added
   * to microseconds but the months fraction will not be added to days, and it will throw
   * exception if any part overflows.
   */
  private def fromDoubles(
      monthsWithFraction: Double,
      daysWithFraction: Double,
      microsWithFraction: Double): CalendarInterval = {
    val truncatedMonths = Math.toIntExact(monthsWithFraction.toLong)
    val truncatedDays = Math.toIntExact(daysWithFraction.toLong)
    val micros = microsWithFraction + MICROS_PER_DAY * (daysWithFraction - truncatedDays)
    new CalendarInterval(truncatedMonths, truncatedDays, micros.round)
  }

  /**
   * Makes an interval from months, days and micros with the fractional part.
   * The overflow style here follows the way of casting [[java.lang.Double]] to integrals and the
   * natural rules for intervals as defined in the Gregorian calendar. Thus, the days fraction
   * will be added to microseconds but the months fraction will not be added to days, and there may
   * be rounding or truncation in months(or day and microseconds) part.
   */
  private def safeFromDoubles(
      monthsWithFraction: Double,
      daysWithFraction: Double,
      microsWithFraction: Double): CalendarInterval = {
    val truncatedDays = daysWithFraction.toInt
    val micros = microsWithFraction + MICROS_PER_DAY * (daysWithFraction - truncatedDays)
    new CalendarInterval(monthsWithFraction.toInt, truncatedDays, micros.round)
  }

  /**
   * Unary minus, return the negated the calendar interval value.
   *
   * @throws ArithmeticException if the result overflows any field value
   */
  def negateExact(interval: CalendarInterval): CalendarInterval = {
    val months = Math.negateExact(interval.months)
    val days = Math.negateExact(interval.days)
    val microseconds = Math.negateExact(interval.microseconds)
    new CalendarInterval(months, days, microseconds)
  }

  /**
   * Unary minus, return the negated the calendar interval value.
   */
  def negate(interval: CalendarInterval): CalendarInterval = {
    new CalendarInterval(-interval.months, -interval.days, -interval.microseconds)
  }

  /**
   * Return a new calendar interval instance of the sum of two intervals.
   *
   * @throws ArithmeticException if the result overflows any field value
   */
  def addExact(left: CalendarInterval, right: CalendarInterval): CalendarInterval = {
    val months = Math.addExact(left.months, right.months)
    val days = Math.addExact(left.days, right.days)
    val microseconds = Math.addExact(left.microseconds, right.microseconds)
    new CalendarInterval(months, days, microseconds)
  }

  /**
   * Return a new calendar interval instance of the sum of two intervals.
   */
  def add(left: CalendarInterval, right: CalendarInterval): CalendarInterval = {
    val months = left.months + right.months
    val days = left.days + right.days
    val microseconds = left.microseconds + right.microseconds
    new CalendarInterval(months, days, microseconds)
  }

  /**
   * Return a new calendar interval instance of the left interval minus the right one.
   *
   * @throws ArithmeticException if the result overflows any field value
   */
  def subtractExact(left: CalendarInterval, right: CalendarInterval): CalendarInterval = {
    val months = Math.subtractExact(left.months, right.months)
    val days = Math.subtractExact(left.days, right.days)
    val microseconds = Math.subtractExact(left.microseconds, right.microseconds)
    new CalendarInterval(months, days, microseconds)
  }

  /**
   * Return a new calendar interval instance of the left interval minus the right one.
   */
  def subtract(left: CalendarInterval, right: CalendarInterval): CalendarInterval = {
    val months = left.months - right.months
    val days = left.days - right.days
    val microseconds = left.microseconds - right.microseconds
    new CalendarInterval(months, days, microseconds)
  }

  /**
   * Return a new calendar interval instance of the left interval times a multiplier.
   */
  def multiply(interval: CalendarInterval, num: Double): CalendarInterval = {
    safeFromDoubles(num * interval.months, num * interval.days, num * interval.microseconds)
  }

  /**
   * Return a new calendar interval instance of the left interval times a multiplier.
   *
   * @throws ArithmeticException if the result overflows any field value
   */
  def multiplyExact(interval: CalendarInterval, num: Double): CalendarInterval = {
    fromDoubles(num * interval.months, num * interval.days, num * interval.microseconds)
  }

  /**
   * Return a new calendar interval instance of the left interval divides by a dividend.
   */
  def divide(interval: CalendarInterval, num: Double): CalendarInterval = {
    if (num == 0) return null
    safeFromDoubles(interval.months / num, interval.days / num, interval.microseconds / num)
  }

  /**
   * Return a new calendar interval instance of the left interval divides by a dividend.
   *
   * @throws ArithmeticException if the result overflows any field value or divided by zero
   */
  def divideExact(interval: CalendarInterval, num: Double): CalendarInterval = {
    if (num == 0) throw QueryExecutionErrors.divideByZeroError()
    fromDoubles(interval.months / num, interval.days / num, interval.microseconds / num)
  }

  private object ParseState extends Enumeration {
    type ParseState = Value

    val PREFIX,
        TRIM_BEFORE_SIGN,
        SIGN,
        TRIM_BEFORE_VALUE,
        VALUE,
        VALUE_FRACTIONAL_PART,
        TRIM_BEFORE_UNIT,
        UNIT_BEGIN,
        UNIT_SUFFIX,
        UNIT_END = Value
  }
  private final val intervalStr = UTF8String.fromString("interval")
  private def unitToUtf8(unit: IntervalUnit): UTF8String = {
    UTF8String.fromString(unit.toString)
  }
  private final val yearStr = unitToUtf8(YEAR)
  private final val monthStr = unitToUtf8(MONTH)
  private final val weekStr = unitToUtf8(WEEK)
  private final val dayStr = unitToUtf8(DAY)
  private final val hourStr = unitToUtf8(HOUR)
  private final val minuteStr = unitToUtf8(MINUTE)
  private final val secondStr = unitToUtf8(SECOND)
  private final val millisStr = unitToUtf8(MILLISECOND)
  private final val microsStr = unitToUtf8(MICROSECOND)

  /**
   * A safe version of `stringToInterval`. It returns null for invalid input string.
   */
  def safeStringToInterval(input: UTF8String): CalendarInterval = {
    try {
      stringToInterval(input)
    } catch {
      case _: IllegalArgumentException => null
    }
  }

  /**
   * Converts a string to [[CalendarInterval]] case-insensitively.
   *
   * @throws IllegalArgumentException if the input string is not in valid interval format.
   */
  def stringToInterval(input: UTF8String): CalendarInterval = {
    import ParseState._
    def throwIAE(msg: String, e: Exception = null) = {
      throw new IllegalArgumentException(s"Error parsing '$input' to interval, $msg", e)
    }

    if (input == null) {
      throwIAE("interval string cannot be null")
    }
    // scalastyle:off caselocale .toLowerCase
    val s = input.trimAll().toLowerCase
    // scalastyle:on
    val bytes = s.getBytes
    if (bytes.isEmpty) {
      throwIAE("interval string cannot be empty")
    }
    var state = PREFIX
    var i = 0
    var currentValue: Long = 0
    var isNegative: Boolean = false
    var months: Int = 0
    var days: Int = 0
    var microseconds: Long = 0
    var fractionScale: Int = 0
    val initialFractionScale = (NANOS_PER_SECOND / 10).toInt
    var fraction: Int = 0
    var pointPrefixed: Boolean = false

    def trimToNextState(b: Byte, next: ParseState): Unit = {
      if (Character.isWhitespace(b)) {
        i += 1
      } else {
        state = next
      }
    }

    def currentWord: String = {
      val sep = "\\s+"
      val strings = s.toString.split(sep)
      val lenRight = s.substring(i, s.numBytes()).toString.split(sep).length
      strings(strings.length - lenRight)
    }

    while (i < bytes.length) {
      val b = bytes(i)
      state match {
        case PREFIX =>
          if (s.startsWith(intervalStr)) {
            if (s.numBytes() == intervalStr.numBytes()) {
              throwIAE("interval string cannot be empty")
            } else if (!Character.isWhitespace(bytes(i + intervalStr.numBytes()))) {
              throwIAE(s"invalid interval prefix $currentWord")
            } else {
              i += intervalStr.numBytes() + 1
            }
          }
          state = TRIM_BEFORE_SIGN
        case TRIM_BEFORE_SIGN => trimToNextState(b, SIGN)
        case SIGN =>
          currentValue = 0
          fraction = 0
          // We preset next state from SIGN to TRIM_BEFORE_VALUE. If we meet '.' in the SIGN state,
          // it means that the interval value we deal with here is a numeric with only fractional
          // part, such as '.11 second', which can be parsed to 0.11 seconds. In this case, we need
          // to reset next state to `VALUE_FRACTIONAL_PART` to go parse the fraction part of the
          // interval value.
          state = TRIM_BEFORE_VALUE
          // We preset the scale to an invalid value to track fraction presence in the UNIT_BEGIN
          // state. If we meet '.', the scale become valid for the VALUE_FRACTIONAL_PART state.
          fractionScale = -1
          pointPrefixed = false
          b match {
            case '-' =>
              isNegative = true
              i += 1
            case '+' =>
              isNegative = false
              i += 1
            case _ if '0' <= b && b <= '9' =>
              isNegative = false
            case '.' =>
              isNegative = false
              fractionScale = initialFractionScale
              pointPrefixed = true
              i += 1
              state = VALUE_FRACTIONAL_PART
            case _ => throwIAE( s"unrecognized number '$currentWord'")
          }
        case TRIM_BEFORE_VALUE => trimToNextState(b, VALUE)
        case VALUE =>
          b match {
            case _ if '0' <= b && b <= '9' =>
              try {
                currentValue = Math.addExact(Math.multiplyExact(10, currentValue), (b - '0'))
              } catch {
                case e: ArithmeticException => throwIAE(e.getMessage, e)
              }
            case _ if Character.isWhitespace(b) => state = TRIM_BEFORE_UNIT
            case '.' =>
              fractionScale = initialFractionScale
              state = VALUE_FRACTIONAL_PART
            case _ => throwIAE(s"invalid value '$currentWord'")
          }
          i += 1
        case VALUE_FRACTIONAL_PART =>
          if ('0' <= b && b <= '9' && fractionScale > 0) {
            fraction += (b - '0') * fractionScale
            fractionScale /= 10
          } else if (Character.isWhitespace(b) &&
              (!pointPrefixed || fractionScale < initialFractionScale)) {
            fraction /= NANOS_PER_MICROS.toInt
            state = TRIM_BEFORE_UNIT
          } else if ('0' <= b && b <= '9') {
            throwIAE(s"interval can only support nanosecond precision, '$currentWord' is out" +
              s" of range")
          } else {
            throwIAE(s"invalid value '$currentWord'")
          }
          i += 1
        case TRIM_BEFORE_UNIT => trimToNextState(b, UNIT_BEGIN)
        case UNIT_BEGIN =>
          // Checks that only seconds can have the fractional part
          if (b != 's' && fractionScale >= 0) {
            throwIAE(s"'$currentWord' cannot have fractional part")
          }
          if (isNegative) {
            currentValue = -currentValue
            fraction = -fraction
          }
          try {
            b match {
              case 'y' if s.matchAt(yearStr, i) =>
                val monthsInYears = Math.multiplyExact(MONTHS_PER_YEAR, currentValue)
                months = Math.toIntExact(Math.addExact(months, monthsInYears))
                i += yearStr.numBytes()
              case 'w' if s.matchAt(weekStr, i) =>
                val daysInWeeks = Math.multiplyExact(DAYS_PER_WEEK, currentValue)
                days = Math.toIntExact(Math.addExact(days, daysInWeeks))
                i += weekStr.numBytes()
              case 'd' if s.matchAt(dayStr, i) =>
                days = Math.addExact(days, Math.toIntExact(currentValue))
                i += dayStr.numBytes()
              case 'h' if s.matchAt(hourStr, i) =>
                val hoursUs = Math.multiplyExact(currentValue, MICROS_PER_HOUR)
                microseconds = Math.addExact(microseconds, hoursUs)
                i += hourStr.numBytes()
              case 's' if s.matchAt(secondStr, i) =>
                val secondsUs = Math.multiplyExact(currentValue, MICROS_PER_SECOND)
                microseconds = Math.addExact(Math.addExact(microseconds, secondsUs), fraction)
                i += secondStr.numBytes()
              case 'm' =>
                if (s.matchAt(monthStr, i)) {
                  months = Math.addExact(months, Math.toIntExact(currentValue))
                  i += monthStr.numBytes()
                } else if (s.matchAt(minuteStr, i)) {
                  val minutesUs = Math.multiplyExact(currentValue, MICROS_PER_MINUTE)
                  microseconds = Math.addExact(microseconds, minutesUs)
                  i += minuteStr.numBytes()
                } else if (s.matchAt(millisStr, i)) {
                  val millisUs = millisToMicros(currentValue)
                  microseconds = Math.addExact(microseconds, millisUs)
                  i += millisStr.numBytes()
                } else if (s.matchAt(microsStr, i)) {
                  microseconds = Math.addExact(microseconds, currentValue)
                  i += microsStr.numBytes()
                } else throwIAE(s"invalid unit '$currentWord'")
              case _ => throwIAE(s"invalid unit '$currentWord'")
            }
          } catch {
            case e: ArithmeticException => throwIAE(e.getMessage, e)
          }
          state = UNIT_SUFFIX
        case UNIT_SUFFIX =>
          b match {
            case 's' => state = UNIT_END
            case _ if Character.isWhitespace(b) => state = TRIM_BEFORE_SIGN
            case _ => throwIAE(s"invalid unit '$currentWord'")
          }
          i += 1
        case UNIT_END =>
          if (Character.isWhitespace(b) ) {
            i += 1
            state = TRIM_BEFORE_SIGN
          } else {
            throwIAE(s"invalid unit '$currentWord'")
          }
      }
    }

    val result = state match {
      case UNIT_SUFFIX | UNIT_END | TRIM_BEFORE_SIGN =>
        new CalendarInterval(months, days, microseconds)
      case TRIM_BEFORE_VALUE => throwIAE(s"expect a number after '$currentWord' but hit EOL")
      case VALUE | VALUE_FRACTIONAL_PART =>
        throwIAE(s"expect a unit name after '$currentWord' but hit EOL")
      case _ => throwIAE(s"unknown error when parsing '$currentWord'")
    }

    result
  }

  def makeInterval(
      years: Int,
      months: Int,
      weeks: Int,
      days: Int,
      hours: Int,
      mins: Int,
      secs: Decimal): CalendarInterval = {
    val totalMonths = Math.addExact(months, Math.multiplyExact(years, MONTHS_PER_YEAR))
    val totalDays = Math.addExact(days, Math.multiplyExact(weeks, DAYS_PER_WEEK))
    assert(secs.scale == 6, "Seconds fractional must have 6 digits for microseconds")
    var micros = secs.toUnscaledLong
    micros = Math.addExact(micros, Math.multiplyExact(hours, MICROS_PER_HOUR))
    micros = Math.addExact(micros, Math.multiplyExact(mins, MICROS_PER_MINUTE))

    new CalendarInterval(totalMonths, totalDays, micros)
  }

  // The amount of seconds that can cause overflow in the conversion to microseconds
  private final val minDurationSeconds = Math.floorDiv(Long.MinValue, MICROS_PER_SECOND)

  /**
   * Converts this duration to the total length in microseconds.
   * <p>
   * If this duration is too large to fit in a [[Long]] microseconds, then an
   * exception is thrown.
   * <p>
   * If this duration has greater than microsecond precision, then the conversion
   * will drop any excess precision information as though the amount in nanoseconds
   * was subject to integer division by one thousand.
   *
   * @return The total length of the duration in microseconds
   * @throws ArithmeticException If numeric overflow occurs
   */
  def durationToMicros(duration: Duration): Long = {
    val seconds = duration.getSeconds
    if (seconds == minDurationSeconds) {
      val microsInSeconds = (minDurationSeconds + 1) * MICROS_PER_SECOND
      val nanoAdjustment = duration.getNano
      assert(0 <= nanoAdjustment && nanoAdjustment < NANOS_PER_SECOND,
        "Duration.getNano() must return the adjustment to the seconds field " +
        "in the range from 0 to 999999999 nanoseconds, inclusive.")
      Math.addExact(microsInSeconds, (nanoAdjustment - NANOS_PER_SECOND) / NANOS_PER_MICROS)
    } else {
      val microsInSeconds = Math.multiplyExact(seconds, MICROS_PER_SECOND)
      Math.addExact(microsInSeconds, duration.getNano / NANOS_PER_MICROS)
    }
  }

  /**
   * Obtains a [[Duration]] representing a number of microseconds.
   *
   * @param micros The number of microseconds, positive or negative
   * @return A [[Duration]], not null
   */
  def microsToDuration(micros: Long): Duration = Duration.of(micros, ChronoUnit.MICROS)

  /**
   * Gets the total number of months in this period.
   * <p>
   * This returns the total number of months in the period by multiplying the
   * number of years by 12 and adding the number of months.
   * <p>
   *
   * @return The total number of months in the period, may be negative
   * @throws ArithmeticException If numeric overflow occurs
   */
  def periodToMonths(period: Period): Int = {
    val monthsInYears = Math.multiplyExact(period.getYears, MONTHS_PER_YEAR)
    Math.addExact(monthsInYears, period.getMonths)
  }

  /**
   * Obtains a [[Period]] representing a number of months. The days unit will be zero, and the years
   * and months units will be normalized.
   *
   * <p>
   * The months unit is adjusted to have an absolute value < 12, with the years unit being adjusted
   * to compensate. For example, the method returns "2 years and 3 months" for the 27 input months.
   * <p>
   * The sign of the years and months units will be the same after normalization.
   * For example, -13 months will be converted to "-1 year and -1 month".
   *
   * @param months The number of months, positive or negative
   * @return The period of months, not null
   */
  def monthsToPeriod(months: Int): Period = Period.ofMonths(months).normalized()

  /**
   * Converts an year-month interval as a number of months to its textual representation
   * which conforms to the ANSI SQL standard.
   *
   * @param months The number of months, positive or negative
   * @param style The style of textual representation of the interval
   * @return Year-month interval string
   */
  def toYearMonthIntervalString(months: Int, style: IntervalStyle): String = {
    var sign = ""
    var absMonths: Long = months
    if (months < 0) {
      sign = "-"
      absMonths = -absMonths
    }
    val payload = s"$sign${absMonths / MONTHS_PER_YEAR}-${absMonths % MONTHS_PER_YEAR}"
    style match {
      case ANSI_STYLE => s"INTERVAL '$payload' YEAR TO MONTH"
      case HIVE_STYLE => payload
    }
  }

  /**
   * Converts a day-time interval as a number of microseconds to its textual representation
   * which conforms to the ANSI SQL standard.
   *
   * @param micros The number of microseconds, positive or negative
   * @param style The style of textual representation of the interval
   * @return Day-time interval string
   */
  def toDayTimeIntervalString(
      micros: Long,
      style: IntervalStyle,
      startField: Byte,
      endField: Byte): String = {
    var sign = ""
    var rest = micros
    val from = DayTimeIntervalType.fieldToString(startField).toUpperCase
    val to = DayTimeIntervalType.fieldToString(endField).toUpperCase
    if (micros < 0) {
      if (micros == Long.MinValue) {
        // Especial handling of minimum `Long` value because negate op overflows `Long`.
        // seconds = 106751991 * (24 * 60 * 60) + 4 * 60 * 60 + 54 = 9223372036854
        // microseconds = -9223372036854000000L-775808 == Long.MinValue
        val minIntervalString = style match {
          case ANSI_STYLE =>
            val baseStr = "-106751991 04:00:54.775808"
            val fromPos = startField match {
              case DayTimeIntervalType.DAY => 0
              case DayTimeIntervalType.HOUR => 11
              case DayTimeIntervalType.MINUTE => 14
              case DayTimeIntervalType.SECOND => 17
            }
            val toPos = endField match {
              case DayTimeIntervalType.DAY => 10
              case DayTimeIntervalType.HOUR => 13
              case DayTimeIntervalType.MINUTE => 16
              case DayTimeIntervalType.SECOND => baseStr.length
            }
            val postfix = if (startField == endField) from else s"$from TO $to"
            s"INTERVAL '${baseStr.substring(fromPos, toPos)}' $postfix"
          case HIVE_STYLE => "-106751991 04:00:54.775808000"
        }
        return minIntervalString
      } else {
        sign = "-"
        rest = -rest
      }
    }
    val secondsWithFraction = rest % MICROS_PER_MINUTE
    rest /= MICROS_PER_MINUTE
    val minutes = rest % MINUTES_PER_HOUR
    rest /= MINUTES_PER_HOUR
    val hours = rest % HOURS_PER_DAY
    val days = rest / HOURS_PER_DAY
    val leadSecZero = if (secondsWithFraction < 10 * MICROS_PER_SECOND) "0" else ""
    val intervalString = style match {
      case ANSI_STYLE =>
        val secStr = java.math.BigDecimal.valueOf(secondsWithFraction, 6)
          .stripTrailingZeros()
          .toPlainString()
        val formatBuilder = new StringBuilder("INTERVAL '")
        if (startField == endField) {
          startField match {
            case DayTimeIntervalType.DAY => formatBuilder.append(s"$sign$days' ")
            case DayTimeIntervalType.HOUR => formatBuilder.append(f"$hours%02d' ")
            case DayTimeIntervalType.MINUTE => formatBuilder.append(f"$minutes%02d' ")
            case DayTimeIntervalType.SECOND => formatBuilder.append(s"$leadSecZero$secStr' ")
          }
          formatBuilder.append(from).toString
        } else {
          val formatArgs = new mutable.ArrayBuffer[Long]
          if (startField <= DayTimeIntervalType.DAY && DayTimeIntervalType.DAY < endField) {
            formatBuilder.append(s"$sign$days ")
          }
          if (startField <= DayTimeIntervalType.HOUR && DayTimeIntervalType.HOUR < endField) {
            formatBuilder.append("%02d:")
            formatArgs.append(hours)
          }
          if (startField <= DayTimeIntervalType.MINUTE && DayTimeIntervalType.MINUTE < endField) {
            formatBuilder.append("%02d:")
            formatArgs.append(minutes)
          }
          endField match {
            case DayTimeIntervalType.HOUR =>
              formatBuilder.append("%02d' ")
              formatArgs.append(hours)
            case DayTimeIntervalType.MINUTE =>
              formatBuilder.append("%02d' ")
              formatArgs.append(minutes)
            case DayTimeIntervalType.SECOND =>
              formatBuilder.append(s"$leadSecZero$secStr' ")
          }
          formatBuilder.append(s"$from TO $to").toString.format(formatArgs.toSeq: _*)
        }
      case HIVE_STYLE =>
        val seconds = secondsWithFraction / MICROS_PER_SECOND
        val nanos = (secondsWithFraction % MICROS_PER_SECOND) * NANOS_PER_MICROS
        f"$sign$days $hours%02d:$minutes%02d:$seconds%02d.$nanos%09d"
    }
    intervalString
  }
}
