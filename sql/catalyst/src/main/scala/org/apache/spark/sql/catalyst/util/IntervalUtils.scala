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

import java.util.Locale
import java.util.concurrent.TimeUnit

import scala.util.control.NonFatal

import org.apache.spark.{SparkIllegalArgumentException, SparkThrowable}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.util.DateTimeConstants._
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.{DayTimeIntervalType => DT, Decimal, YearMonthIntervalType => YM}
import org.apache.spark.sql.types.DayTimeIntervalType.{DAY, HOUR, MINUTE, SECOND}
import org.apache.spark.sql.types.YearMonthIntervalType.{MONTH, YEAR}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

object IntervalUtils extends SparkIntervalUtils {

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
      fieldName: UTF8String,
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
  private val yearMonthIndividualPatternString = "([+|-])?(\\d+)"
  private val yearMonthIndividualRegex = (s"^$yearMonthIndividualPatternString$$").r
  private val yearMonthIndividualLiteralRegex =
    (s"(?i)^INTERVAL\\s+([+|-])?'$yearMonthIndividualPatternString'\\s+(YEAR|MONTH)$$").r

  private def finalSign(firstSign: String, secondSign: String = null): Int = {
    (firstSign, secondSign) match {
      case ("-", "-") => 1
      case ("-", _) => -1
      case (_, "-") => -1
      case (_, _) => 1
    }
  }

  private def throwIllegalIntervalFormatException(
      input: UTF8String,
      startFiled: Byte,
      endField: Byte,
      intervalStr: String,
      typeName: String,
      fallBackNotice: Boolean = false) = {
    throw new SparkIllegalArgumentException(
      errorClass = {
        if (fallBackNotice) "INVALID_INTERVAL_FORMAT.UNMATCHED_FORMAT_STRING_WITH_NOTICE"
        else "INVALID_INTERVAL_FORMAT.UNMATCHED_FORMAT_STRING"
      },
      messageParameters = Map(
        "intervalStr" -> intervalStr,
        "supportedFormat" -> supportedFormat((intervalStr, startFiled, endField))
          .map(format => s"`$format`").mkString(", "),
        "typeName" -> typeName,
        "input" -> input.toString))
  }

  val supportedFormat = Map(
    ("year-month", YM.YEAR, YM.MONTH) -> Seq("[+|-]y-m", "INTERVAL [+|-]'[+|-]y-m' YEAR TO MONTH"),
    ("year-month", YM.YEAR, YM.YEAR) -> Seq("[+|-]y", "INTERVAL [+|-]'[+|-]y' YEAR"),
    ("year-month", YM.MONTH, YM.MONTH) -> Seq("[+|-]m", "INTERVAL [+|-]'[+|-]m' MONTH"),
    ("day-time", DT.DAY, DT.DAY) -> Seq("[+|-]d", "INTERVAL [+|-]'[+|-]d' DAY"),
    ("day-time", DT.DAY, DT.HOUR) -> Seq("[+|-]d h", "INTERVAL [+|-]'[+|-]d h' DAY TO HOUR"),
    ("day-time", DT.DAY, DT.MINUTE) ->
      Seq("[+|-]d h:m", "INTERVAL [+|-]'[+|-]d h:m' DAY TO MINUTE"),
    ("day-time", DT.DAY, DT.SECOND) ->
      Seq("[+|-]d h:m:s.n", "INTERVAL [+|-]'[+|-]d h:m:s.n' DAY TO SECOND"),
    ("day-time", DT.HOUR, DT.HOUR) -> Seq("[+|-]h", "INTERVAL [+|-]'[+|-]h' HOUR"),
    ("day-time", DT.HOUR, DT.MINUTE) -> Seq("[+|-]h:m", "INTERVAL [+|-]'[+|-]h:m' HOUR TO MINUTE"),
    ("day-time", DT.HOUR, DT.SECOND) ->
      Seq("[+|-]h:m:s.n", "INTERVAL [+|-]'[+|-]h:m:s.n' HOUR TO SECOND"),
    ("day-time", DT.MINUTE, DT.MINUTE) -> Seq("[+|-]m", "INTERVAL [+|-]'[+|-]m' MINUTE"),
    ("day-time", DT.MINUTE, DT.SECOND) ->
      Seq("[+|-]m:s.n", "INTERVAL [+|-]'[+|-]m:s.n' MINUTE TO SECOND"),
    ("day-time", DT.SECOND, DT.SECOND) -> Seq("[+|-]s.n", "INTERVAL [+|-]'[+|-]s.n' SECOND")
  )

  def castStringToYMInterval(
      input: UTF8String,
      startField: Byte,
      endField: Byte): Int = {

    def checkTargetType(targetStartField: Byte, targetEndField: Byte): Boolean =
      startField == targetStartField && endField == targetEndField

    input.trimAll().toString match {
      case yearMonthRegex(sign, year, month) if checkTargetType(YM.YEAR, YM.MONTH) =>
        toYMInterval(year, month, input.trimAll().toString, finalSign(sign))
      case yearMonthLiteralRegex(firstSign, secondSign, year, month)
        if checkTargetType(YM.YEAR, YM.MONTH) =>
        toYMInterval(year, month, input.trimAll().toString, finalSign(firstSign, secondSign))
      case yearMonthIndividualRegex(firstSign, value) =>
        safeToInterval("year-month", input.trimAll().toString) {
          val sign = finalSign(firstSign)
          if (endField == YM.YEAR) {
            sign * Math.toIntExact(value.toLong * MONTHS_PER_YEAR)
          } else if (startField == YM.MONTH) {
            Math.toIntExact(sign * value.toLong)
          } else {
            throwIllegalIntervalFormatException(
              input, startField, endField, "year-month", YM(startField, endField).typeName)
          }
        }
      case yearMonthIndividualLiteralRegex(firstSign, secondSign, value, unit) =>
        safeToInterval("year-month", input.trimAll().toString) {
          val sign = finalSign(firstSign, secondSign)
          unit.toUpperCase(Locale.ROOT) match {
            case "YEAR" if checkTargetType(YM.YEAR, YM.YEAR) =>
              sign * Math.toIntExact(value.toLong * MONTHS_PER_YEAR)
            case "MONTH" if checkTargetType(YM.MONTH, YM.MONTH) =>
              Math.toIntExact(sign * value.toLong)
            case _ => throwIllegalIntervalFormatException(input, startField, endField,
              "year-month", YM(startField, endField).typeName)
          }
        }
      case _ => throwIllegalIntervalFormatException(input, startField, endField,
        "year-month", YM(startField, endField).typeName)
    }
  }

  /**
   * Parse year-month interval in form: [+|-]YYYY-MM
   *
   * adapted from HiveIntervalYearMonth.valueOf
   */
  def fromYearMonthString(input: String): CalendarInterval = {
    fromYearMonthString(input, YM.YEAR, YM.MONTH)
  }

  /**
   * Parse year-month interval in form: [+|-]YYYY-MM
   *
   * adapted from HiveIntervalYearMonth.valueOf
   * Below interval conversion patterns are supported:
   * - YEAR TO (YEAR|MONTH)
   */
  def fromYearMonthString(input: String, startField: Byte, endField: Byte): CalendarInterval = {
    require(input != null, "Interval year-month string must be not null")
    val months = castStringToYMInterval(UTF8String.fromString(input), startField, endField)
    new CalendarInterval(months, 0, 0)
  }

  private def safeToInterval[T](interval: String, input: String)(f: => T): T = {
    try {
      f
    } catch {
      case e: SparkThrowable => throw e
      case NonFatal(e) =>
        throw new SparkIllegalArgumentException(
          errorClass = "INVALID_INTERVAL_FORMAT.INTERVAL_PARSING",
          messageParameters = Map("input" -> input, "interval" -> interval, "msg" -> e.getMessage),
          cause = e)
    }
  }

  private def toYMInterval(year: String, month: String, input: String, sign: Int): Int = {
    safeToInterval("year-month", input) {
      val years = toLongWithRange(yearStr, year, 0, Integer.MAX_VALUE / MONTHS_PER_YEAR)
      val totalMonths =
        sign * (years * MONTHS_PER_YEAR + toLongWithRange(monthStr, month, 0, 11))
      Math.toIntExact(totalMonths)
    }
  }

  private val normalPattern = "(\\d{1,2})"
  private val dayBoundPattern = "(\\d{1,9})"
  private val hourBoundPattern = "(\\d{1,10})"
  private val minuteBoundPattern = "(\\d{1,12})"
  private val secondBoundPattern = "(\\d{1,13})"
  private val microPattern = "(\\.\\d{1,9})"

  private val dayHourPatternString = s"([+|-])?$dayBoundPattern $normalPattern"
  private val dayHourRegex = (s"^$dayHourPatternString$$").r
  private val dayHourLiteralRegex =
    (s"(?i)^INTERVAL\\s+([+|-])?\\'$dayHourPatternString\\'\\s+DAY\\s+TO\\s+HOUR$$").r

  private val dayMinutePatternString = s"([+|-])?$dayBoundPattern $normalPattern:$normalPattern"
  private val dayMinuteRegex = (s"^$dayMinutePatternString$$").r
  private val dayMinuteLiteralRegex =
    (s"(?i)^INTERVAL\\s+([+|-])?\\'$dayMinutePatternString\\'\\s+DAY\\s+TO\\s+MINUTE$$").r

  private val daySecondPatternString =
    s"([+|-])?$dayBoundPattern $normalPattern:$normalPattern:$normalPattern$microPattern?"
  private val daySecondRegex = (s"^$daySecondPatternString$$").r
  private val daySecondLiteralRegex =
    (s"(?i)^INTERVAL\\s+([+|-])?\\'$daySecondPatternString\\'\\s+DAY\\s+TO\\s+SECOND$$").r

  private val hourMinutePatternString = s"([+|-])?$hourBoundPattern:$normalPattern"
  private val hourMinuteRegex = (s"^$hourMinutePatternString$$").r
  private val hourMinuteLiteralRegex =
    (s"(?i)^INTERVAL\\s+([+|-])?\\'$hourMinutePatternString\\'\\s+HOUR\\s+TO\\s+MINUTE$$").r

  private val hourSecondPatternString =
    s"([+|-])?$hourBoundPattern:$normalPattern:$normalPattern$microPattern?"
  private val hourSecondRegex = (s"^$hourSecondPatternString$$").r
  private val hourSecondLiteralRegex =
    (s"(?i)^INTERVAL\\s+([+|-])?\\'$hourSecondPatternString\\'\\s+HOUR\\s+TO\\s+SECOND$$").r

  private val minuteSecondPatternString =
    s"([+|-])?$minuteBoundPattern:$normalPattern$microPattern?"
  private val minuteSecondRegex = (s"^$minuteSecondPatternString$$").r
  private val minuteSecondLiteralRegex =
    (s"(?i)^INTERVAL\\s+([+|-])?\\'$minuteSecondPatternString\\'\\s+MINUTE\\s+TO\\s+SECOND$$").r

  private val dayTimeIndividualPatternString = s"([+|-])?$secondBoundPattern(\\.\\d{1,9})?"
  private val dayTimeIndividualRegex = (s"^$dayTimeIndividualPatternString$$").r
  private val dayTimeIndividualLiteralRegex =
    (s"(?i)^INTERVAL\\s+([+|-])?'$dayTimeIndividualPatternString'\\s+(DAY|HOUR|MINUTE|SECOND)$$").r

  def castStringToDTInterval(
      input: UTF8String,
      startField: Byte,
      endField: Byte): Long = {

    def secondAndMicro(second: String, micro: String): String = {
      if (micro != null) {
        s"$second$micro"
      } else {
        second
      }
    }

    def checkTargetType(targetStartField: Byte, targetEndField: Byte): Boolean =
      startField == targetStartField && endField == targetEndField

    input.trimAll().toString match {
      case dayHourRegex(sign, day, hour) if checkTargetType(DT.DAY, DT.HOUR) =>
        toDTInterval(day, hour, "0", "0", finalSign(sign))
      case dayHourLiteralRegex(firstSign, secondSign, day, hour)
        if checkTargetType(DT.DAY, DT.HOUR) =>
        toDTInterval(day, hour, "0", "0", finalSign(firstSign, secondSign))
      case dayMinuteRegex(sign, day, hour, minute) if checkTargetType(DT.DAY, DT.MINUTE) =>
        toDTInterval(day, hour, minute, "0", finalSign(sign))
      case dayMinuteLiteralRegex(firstSign, secondSign, day, hour, minute)
        if checkTargetType(DT.DAY, DT.MINUTE) =>
        toDTInterval(day, hour, minute, "0", finalSign(firstSign, secondSign))
      case daySecondRegex(sign, day, hour, minute, second, micro)
        if checkTargetType(DT.DAY, DT.SECOND) =>
        toDTInterval(day, hour, minute, secondAndMicro(second, micro), finalSign(sign))
      case daySecondLiteralRegex(firstSign, secondSign, day, hour, minute, second, micro)
        if checkTargetType(DT.DAY, DT.SECOND) =>
        toDTInterval(day, hour, minute, secondAndMicro(second, micro),
          finalSign(firstSign, secondSign))

      case hourMinuteRegex(sign, hour, minute) if checkTargetType(DT.HOUR, DT.MINUTE) =>
        toDTInterval(hour, minute, "0", finalSign(sign))
      case hourMinuteLiteralRegex(firstSign, secondSign, hour, minute)
        if checkTargetType(DT.HOUR, DT.MINUTE) =>
        toDTInterval(hour, minute, "0", finalSign(firstSign, secondSign))
      case hourSecondRegex(sign, hour, minute, second, micro)
        if checkTargetType(DT.HOUR, DT.SECOND) =>
        toDTInterval(hour, minute, secondAndMicro(second, micro), finalSign(sign))
      case hourSecondLiteralRegex(firstSign, secondSign, hour, minute, second, micro)
        if checkTargetType(DT.HOUR, DT.SECOND) =>
        toDTInterval(hour, minute, secondAndMicro(second, micro), finalSign(firstSign, secondSign))

      case minuteSecondRegex(sign, minute, second, micro)
        if checkTargetType(DT.MINUTE, DT.SECOND) =>
        toDTInterval(minute, secondAndMicro(second, micro), finalSign(sign))
      case minuteSecondLiteralRegex(firstSign, secondSign, minute, second, micro)
        if checkTargetType(DT.MINUTE, DT.SECOND) =>
        toDTInterval(minute, secondAndMicro(second, micro), finalSign(firstSign, secondSign))

      case dayTimeIndividualRegex(firstSign, value, suffix) =>
        safeToInterval("day-time", input.trimAll().toString) {
          val sign = finalSign(firstSign)
          (startField, endField) match {
            case (DT.DAY, DT.DAY) if suffix == null && value.length <= 9 =>
              sign * value.toLong * MICROS_PER_DAY
            case (DT.HOUR, DT.HOUR) if suffix == null && value.length <= 10 =>
              sign * value.toLong * MICROS_PER_HOUR
            case (DT.MINUTE, DT.MINUTE) if suffix == null && value.length <= 12 =>
              sign * value.toLong * MICROS_PER_MINUTE
            case (DT.SECOND, DT.SECOND) if value.length <= 13 =>
              sign match {
                case 1 => parseSecondNano(secondAndMicro(value, suffix))
                case -1 => parseSecondNano(s"-${secondAndMicro(value, suffix)}")
              }
            case (_, _) => throwIllegalIntervalFormatException(input, startField, endField,
              "day-time", DT(startField, endField).typeName, true)
          }
        }
      case dayTimeIndividualLiteralRegex(firstSign, secondSign, value, suffix, unit) =>
        safeToInterval("day-time", input.trimAll().toString) {
          val sign = finalSign(firstSign, secondSign)
          unit.toUpperCase(Locale.ROOT) match {
            case "DAY" if suffix == null && value.length <= 9 && checkTargetType(DT.DAY, DT.DAY) =>
              sign * value.toLong * MICROS_PER_DAY
            case "HOUR" if suffix == null && value.length <= 10
              && checkTargetType(DT.HOUR, DT.HOUR) =>
              sign * value.toLong * MICROS_PER_HOUR
            case "MINUTE" if suffix == null && value.length <= 12
              && checkTargetType(DT.MINUTE, DT.MINUTE) =>
              sign * value.toLong * MICROS_PER_MINUTE
            case "SECOND" if value.length <= 13 && checkTargetType(DT.SECOND, DT.SECOND) =>
              sign match {
                case 1 => parseSecondNano(secondAndMicro(value, suffix))
                case -1 => parseSecondNano(s"-${secondAndMicro(value, suffix)}")
              }
            case _ => throwIllegalIntervalFormatException(input, startField, endField,
              "day-time", DT(startField, endField).typeName, true)
          }
        }
      case _ => throwIllegalIntervalFormatException(input, startField, endField,
        "day-time", DT(startField, endField).typeName, true)
    }
  }

  def toDTInterval(day: String, hour: String, minute: String, second: String, sign: Int): Long = {
    var micros = 0L
    val days = toLongWithRange(dayStr, day, 0, MAX_DAY).toInt
    micros = Math.addExact(micros, sign * days * MICROS_PER_DAY)
    val hours = toLongWithRange(hourStr, hour, 0, 23)
    micros = Math.addExact(micros, sign * hours * MICROS_PER_HOUR)
    val minutes = toLongWithRange(minuteStr, minute, 0, 59)
    micros = Math.addExact(micros, sign * minutes * MICROS_PER_MINUTE)
    micros = Math.addExact(micros, sign * parseSecondNano(second, 0, 59))
    micros
  }

  def toDTInterval(hour: String, minute: String, second: String, sign: Int): Long = {
    var micros = 0L
    val hours = toLongWithRange(hourStr, hour, 0, MAX_HOUR)
    micros = Math.addExact(micros, sign * hours * MICROS_PER_HOUR)
    val minutes = toLongWithRange(minuteStr, minute, 0, 59)
    micros = Math.addExact(micros, sign * minutes * MICROS_PER_MINUTE)
    micros = Math.addExact(micros, sign * parseSecondNano(second, 0, 59))
    micros
  }

  def toDTInterval(minute: String, second: String, sign: Int): Long = {
    var micros = 0L
    val minutes = toLongWithRange(minuteStr, minute, 0, MAX_MINUTE)
    micros = Math.addExact(micros, sign * minutes * MICROS_PER_MINUTE)
    micros = Math.addExact(micros, sign * parseSecondNano(second, 0, 59))
    micros
  }

  def castDayTimeStringToInterval(
      input: String,
      startField: Byte,
      endField: Byte): CalendarInterval = {
    val micros = castStringToDTInterval(UTF8String.fromString(input), startField, endField)
    new CalendarInterval(0, (micros / MICROS_PER_DAY).toInt, micros % MICROS_PER_DAY)
  }

  /**
   * Parse day-time interval in form: [-]d HH:mm:ss.nnnnnnnnn and [-]HH:mm:ss.nnnnnnnnn
   *
   * adapted from HiveIntervalDayTime.valueOf
   */
  def fromDayTimeString(s: String): CalendarInterval = {
    fromDayTimeString(s, DT.DAY, DT.SECOND)
  }

  /**
   * Parse day-time interval in form: [-]d HH:mm:ss.nnnnnnnnn and [-]HH:mm:ss.nnnnnnnnn
   *
   * adapted from HiveIntervalDayTime.valueOf.
   * Below interval conversion patterns are supported:
   * - DAY TO (DAY|HOUR|MINUTE|SECOND)
   * - HOUR TO (HOUR|MINUTE|SECOND)
   * - MINUTE TO (MINUTE|SECOND)
   */
  def fromDayTimeString(input: String, from: Byte, to: Byte): CalendarInterval = {
    require(input != null, "Interval day-time string must be not null")
    if (SQLConf.get.getConf(SQLConf.LEGACY_FROM_DAYTIME_STRING)) {
      parseDayTimeLegacy(input, from, to)
    } else {
      castDayTimeStringToInterval(input, from, to)
    }
  }

  /**
   * Parse all kinds of interval literals including unit-to-unit form and unit list form
   */
  def fromIntervalString(input: String): CalendarInterval = try {
    if (input.toLowerCase(Locale.ROOT).trim.startsWith("interval")) {
      CatalystSqlParser.parseExpression(input) match {
        case Literal(months: Int, _: YearMonthIntervalType) => new CalendarInterval(months, 0, 0)
        case Literal(micros: Long, _: DayTimeIntervalType) => new CalendarInterval(0, 0, micros)
        case Literal(cal: CalendarInterval, CalendarIntervalType) => cal
      }
    } else {
      stringToInterval(UTF8String.fromString(input))
    }
  } catch {
    case NonFatal(e) =>
      throw QueryCompilationErrors.cannotParseIntervalError(input, e)
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
      from: Byte,
      to: Byte): CalendarInterval = {
    assert(input.length == input.trim.length)
    val m = dayTimePatternLegacy.pattern.matcher(input)
    require(m.matches, s"Interval string must match day-time format of 'd h:m:s.n': $input, " +
      s"$fallbackNotice")

    try {
      val sign = if (m.group(1) != null && m.group(1) == "-") -1 else 1
      val days = if (m.group(2) == null) {
        0
      } else {
        toLongWithRange(dayStr, m.group(3), 0, Integer.MAX_VALUE).toInt
      }
      var hours: Long = 0L
      var minutes: Long = 0L
      var seconds: Long = 0L
      if (m.group(5) != null || from == DT.MINUTE) { // 'HH:mm:ss' or 'mm:ss minute'
        hours = toLongWithRange(hourStr, m.group(5), 0, 23)
        minutes = toLongWithRange(minuteStr, m.group(6), 0, 59)
        seconds = toLongWithRange(secondStr, m.group(7), 0, 59)
      } else if (m.group(8) != null) { // 'mm:ss.nn'
        minutes = toLongWithRange(minuteStr, m.group(6), 0, 59)
        seconds = toLongWithRange(secondStr, m.group(7), 0, 59)
      } else { // 'HH:mm'
        hours = toLongWithRange(hourStr, m.group(6), 0, 23)
        minutes = toLongWithRange(secondStr, m.group(7), 0, 59)
      }
      // Hive allow nanosecond precision interval
      var secondsFraction = parseNanos(m.group(9), seconds < 0)
      to match {
        case DT.HOUR =>
          minutes = 0
          seconds = 0
          secondsFraction = 0
        case DT.MINUTE =>
          seconds = 0
          secondsFraction = 0
        case DT.SECOND =>
          // No-op
        case _ => throw new SparkIllegalArgumentException(
          errorClass = "INTERVAL_ERROR.UNSUPPORTED_FROM_TO_EXPRESSION",
          messageParameters = Map(
            "input" -> input,
            "from" -> DT.fieldToString(from),
            "to" -> DT.fieldToString(to)))
      }
      var micros = secondsFraction
      micros = Math.addExact(micros, Math.multiplyExact(hours, MICROS_PER_HOUR))
      micros = Math.addExact(micros, Math.multiplyExact(minutes, MICROS_PER_MINUTE))
      micros = Math.addExact(micros, Math.multiplyExact(seconds, MICROS_PER_SECOND))
      new CalendarInterval(0, sign * days, sign * micros)
    } catch {
      case e: Exception =>
        throw new SparkIllegalArgumentException(
          errorClass = "INVALID_INTERVAL_FORMAT.DAY_TIME_PARSING",
          messageParameters = Map(
            "msg" -> e.getMessage,
            "input" -> input),
          cause = e)
    }
  }

  // Parses a string with nanoseconds, truncates the result and returns microseconds
  private def parseNanos(nanos: String, isNegative: Boolean): Long = {
    if (nanos != null) {
      val maxNanosLen = 9
      val alignedStr = if (nanos.length < maxNanosLen) {
        (nanos + "000000000").substring(0, maxNanosLen)
      } else nanos
      val nanoSecond = toLongWithRange(nanosStr, alignedStr, 0L, 999999999L)
      val microSecond = nanoSecond / NANOS_PER_MICROS
      if (isNegative) -microSecond else microSecond
    } else {
      0L
    }
  }

  /**
   * Parse second_nano string in ss.nnnnnnnnn format to microseconds
   */
  private def parseSecondNano(
      secondNano: String,
      minSecond: Long = MIN_SECOND,
      maxSecond: Long = MAX_SECOND): Long = {
    def parseSeconds(secondsStr: String): Long = {
      toLongWithRange(secondStr, secondsStr, minSecond, maxSecond) * MICROS_PER_SECOND
    }

    secondNano.split("\\.") match {
      case Array(secondsStr) => parseSeconds(secondsStr)
      case Array("", nanosStr) => parseNanos(nanosStr, false)
      case Array(secondsStr, nanosStr) =>
        val seconds = parseSeconds(secondsStr)
        Math.addExact(seconds, parseNanos(nanosStr, seconds < 0))
      case _ => throw new SparkIllegalArgumentException(
        errorClass = "INVALID_INTERVAL_FORMAT.SECOND_NANO_FORMAT",
        messageParameters = Map("input" -> secondNano))
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
    val truncatedMonths = MathUtils.toIntExact(monthsWithFraction.toLong)
    val truncatedDays = MathUtils.toIntExact(daysWithFraction.toLong)
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
    val months = MathUtils.negateExact(interval.months)
    val days = MathUtils.negateExact(interval.days)
    val microseconds = MathUtils.negateExact(interval.microseconds)
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
    val months = MathUtils.addExact(left.months, right.months)
    val days = MathUtils.addExact(left.days, right.days)
    val microseconds = MathUtils.addExact(left.microseconds, right.microseconds)
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
    val months = MathUtils.subtractExact(left.months, right.months)
    val days = MathUtils.subtractExact(left.days, right.days)
    val microseconds = MathUtils.subtractExact(left.microseconds, right.microseconds)
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
    if (num == 0) throw QueryExecutionErrors.intervalDividedByZeroError(null)
    fromDoubles(interval.months / num, interval.days / num, interval.microseconds / num)
  }

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

  def makeDayTimeInterval(
      days: Int,
      hours: Int,
      mins: Int,
      secs: Decimal): Long = {
    assert(secs.scale == 6, "Seconds fractional must have 6 digits for microseconds")
    var micros = secs.toUnscaledLong
    micros = Math.addExact(micros, Math.multiplyExact(days, MICROS_PER_DAY))
    micros = Math.addExact(micros, Math.multiplyExact(hours, MICROS_PER_HOUR))
    micros = Math.addExact(micros, Math.multiplyExact(mins, MICROS_PER_MINUTE))
    micros
  }

  def intToYearMonthInterval(v: Int, startField: Byte, endField: Byte): Int = {
    endField match {
      case YEAR =>
        try {
          Math.multiplyExact(v, MONTHS_PER_YEAR)
        } catch {
          case _: ArithmeticException =>
            throw QueryExecutionErrors.castingCauseOverflowError(
              v,
              IntegerType,
              YearMonthIntervalType(startField, endField))
        }
      case MONTH => v
    }
  }

  def longToYearMonthInterval(v: Long, startField: Byte, endField: Byte): Int = {
    val vInt = v.toInt
    if (v != vInt) {
      throw QueryExecutionErrors.castingCauseOverflowError(
        v,
        LongType,
        YearMonthIntervalType(startField, endField))
    }
    intToYearMonthInterval(vInt, startField, endField)
  }

  def decimalToYearMonthInterval(
      d: Decimal, p: Int, s: Int, startField: Byte, endField: Byte): Int = {
    try {
      val months = if (endField == YEAR) d.toBigDecimal * MONTHS_PER_YEAR else d.toBigDecimal
      months.setScale(0, BigDecimal.RoundingMode.HALF_UP).toIntExact
    } catch {
      case _: ArithmeticException =>
        throw QueryExecutionErrors.castingCauseOverflowError(
          d,
          DecimalType(p, s),
          YearMonthIntervalType(startField, endField))
    }
  }

  def yearMonthIntervalToInt(v: Int, startField: Byte, endField: Byte): Int = {
    endField match {
      case YEAR => v / MONTHS_PER_YEAR
      case MONTH => v
    }
  }

  def yearMonthIntervalToShort(v: Int, startField: Byte, endField: Byte): Short = {
    val vInt = yearMonthIntervalToInt(v, startField, endField)
    val vShort = vInt.toShort
    if (vInt != vShort) {
      throw QueryExecutionErrors.castingCauseOverflowError(
        v,
        YearMonthIntervalType(startField, endField),
        ShortType)
    }
    vShort
  }

  def yearMonthIntervalToByte(v: Int, startField: Byte, endField: Byte): Byte = {
    val vInt = yearMonthIntervalToInt(v, startField, endField)
    val vByte = vInt.toByte
    if (vInt != vByte) {
      throw QueryExecutionErrors.castingCauseOverflowError(
        v,
        YearMonthIntervalType(startField, endField),
        ByteType)
    }
    vByte
  }

  def intToDayTimeInterval(v: Int, startField: Byte, endField: Byte): Long = {
    endField match {
      case DAY =>
        try {
          Math.multiplyExact(v, MICROS_PER_DAY)
        } catch {
          case _: ArithmeticException =>
            throw QueryExecutionErrors.castingCauseOverflowError(
              v,
              IntegerType,
              DayTimeIntervalType(startField, endField))
        }
      case HOUR => v * MICROS_PER_HOUR
      case MINUTE => v * MICROS_PER_MINUTE
      case SECOND => v * MICROS_PER_SECOND
    }
  }

  def longToDayTimeInterval(v: Long, startField: Byte, endField: Byte): Long = {
    try {
      endField match {
        case DAY => Math.multiplyExact(v, MICROS_PER_DAY)
        case HOUR => Math.multiplyExact(v, MICROS_PER_HOUR)
        case MINUTE => Math.multiplyExact(v, MICROS_PER_MINUTE)
        case SECOND => Math.multiplyExact(v, MICROS_PER_SECOND)
      }
    } catch {
      case _: ArithmeticException =>
        throw QueryExecutionErrors.castingCauseOverflowError(
          v,
          LongType,
          DayTimeIntervalType(startField, endField))
    }
  }

  def dayTimeIntervalToLong(v: Long, startField: Byte, endField: Byte): Long = {
    endField match {
      case DAY => v / MICROS_PER_DAY
      case HOUR => v / MICROS_PER_HOUR
      case MINUTE => v / MICROS_PER_MINUTE
      case SECOND => v / MICROS_PER_SECOND
    }
  }

  def dayTimeIntervalToDecimal(v: Long, endField: Byte): Decimal = {
    endField match {
      case DAY => Decimal(v / MICROS_PER_DAY)
      case HOUR => Decimal(v / MICROS_PER_HOUR)
      case MINUTE => Decimal(v / MICROS_PER_MINUTE)
      case SECOND => Decimal(v, Decimal.MAX_LONG_DIGITS + 1, 6)
    }
  }

  def decimalToDayTimeInterval(
      d: Decimal, p: Int, s: Int, startField: Byte, endField: Byte): Long = {
    try {
      val micros = endField match {
        case DAY => d.toBigDecimal * MICROS_PER_DAY
        case HOUR => d.toBigDecimal * MICROS_PER_HOUR
        case MINUTE => d.toBigDecimal * MICROS_PER_MINUTE
        case SECOND => d.toBigDecimal * MICROS_PER_SECOND
      }
      micros.setScale(0, BigDecimal.RoundingMode.HALF_UP).toLongExact
    } catch {
      case _: ArithmeticException =>
        throw QueryExecutionErrors.castingCauseOverflowError(
          d, DecimalType(p, s), DT(startField, endField))
    }
  }

  def dayTimeIntervalToInt(v: Long, startField: Byte, endField: Byte): Int = {
    val vLong = dayTimeIntervalToLong(v, startField, endField)
    val vInt = vLong.toInt
    if (vLong != vInt) {
      throw QueryExecutionErrors.castingCauseOverflowError(
        v,
        DayTimeIntervalType(startField, endField),
        IntegerType)
    }
    vInt
  }

  def dayTimeIntervalToShort(v: Long, startField: Byte, endField: Byte): Short = {
    val vLong = dayTimeIntervalToLong(v, startField, endField)
    val vShort = vLong.toShort
    if (vLong != vShort) {
      throw QueryExecutionErrors.castingCauseOverflowError(
        v,
        DayTimeIntervalType(startField, endField),
        ShortType)
    }
    vShort
  }

  def dayTimeIntervalToByte(v: Long, startField: Byte, endField: Byte): Byte = {
    val vLong = dayTimeIntervalToLong(v, startField, endField)
    val vByte = vLong.toByte
    if (vLong != vByte) {
      throw QueryExecutionErrors.castingCauseOverflowError(
        v,
        DayTimeIntervalType(startField, endField),
        ByteType)
    }
    vByte
  }
}
