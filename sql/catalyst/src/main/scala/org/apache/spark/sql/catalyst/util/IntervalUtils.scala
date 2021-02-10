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

import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.util.DateTimeConstants._
import org.apache.spark.sql.catalyst.util.DateTimeUtils.millisToMicros
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

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

  def getYears(interval: CalendarInterval): Int = {
    interval.months / MONTHS_PER_YEAR
  }

  def getMonths(interval: CalendarInterval): Byte = {
    (interval.months % MONTHS_PER_YEAR).toByte
  }

  def getDays(interval: CalendarInterval): Int = {
    val daysInMicroseconds = (interval.microseconds / MICROS_PER_DAY).toInt
    Math.addExact(interval.days, daysInMicroseconds)
  }

  def getHours(interval: CalendarInterval): Long = {
    (interval.microseconds % MICROS_PER_DAY) / MICROS_PER_HOUR
  }

  def getMinutes(interval: CalendarInterval): Byte = {
    ((interval.microseconds % MICROS_PER_HOUR) / MICROS_PER_MINUTE).toByte
  }

  def getSeconds(interval: CalendarInterval): Decimal = {
    Decimal(interval.microseconds % MICROS_PER_MINUTE, 8, 6)
  }

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
        val years = toLongWithRange(YEAR, yearStr, 0, Integer.MAX_VALUE).toInt
        val months = toLongWithRange(MONTH, monthStr, 0, 11).toInt
        val totalMonths = Math.addExact(Math.multiplyExact(years, 12), months)
        new CalendarInterval(totalMonths, 0, 0)
      } catch {
        case NonFatal(e) =>
          throw new IllegalArgumentException(
            s"Error parsing interval year-month string: ${e.getMessage}", e)
      }
    }
    input.trim match {
      case yearMonthPattern("-", yearStr, monthStr) =>
        negateExact(toInterval(yearStr, monthStr))
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
    if (num == 0) throw new ArithmeticException("divide by zero")
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
}
